package org.apache.cassandra.db.data;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.CompoundDenseCellName;
import org.apache.cassandra.db.composites.CompoundSparseCellName;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.db.composites.SimpleDenseCellName;
import org.apache.cassandra.db.composites.SimpleSparseCellName;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;
import org.apache.cassandra.utils.memory.ByteBufferPool;
import org.apache.cassandra.utils.memory.NativeAllocation;
import org.apache.cassandra.utils.memory.NativeAllocator;

/**
 * Packs a CellName AND a Cell into one off-heap representation.
 * Layout is:
 *
 * Note we store the ColumnIdentifier in full as bytes. This seems an okay tradeoff for now, as we just
 * look it back up again when we need to, and in the near future we hope to switch to ints, longs or
 * UUIDs representing column identifiers on disk, at which point we can switch that here as well.
 *
 * [Descendants][timestamp][value offset][name size]][name extra][name offset deltas][cell names][value]
 * [ arbitrary ][   8b    ][     4b     ][    2b   ][     1b    ][     each 2b      ][ arb < 64k][ arb ]
 *
 * descendants: any overriding classes will put their state here
 * name offsets are deltas from their base offset, and don't include the first offset, or the end position of the final entry,
 * i.e. there will be size - 1 entries, and each is a delta that is added to the offset of the position of the first name
 * (which is always CELL_NAME_OFFSETS_OFFSET + (2 * (size - 1))). The length of the final name fills up any remaining
 * space upto the value offset
 * name extra:  lowest 2 bits indicate the clustering size delta (i.e. how many name items are NOT part of the clustering key)
 *              the next 2 bits indicate the CellNameType
 *              the next bit indicates if the column is a static or clustered/dynamic column
 */
public class NativeCell extends NativeAllocation implements Cell, CellName
{
    private static final long SIZE = ObjectSizes.measure(new NativeCell());
    private static int VALUE_OFFSET_OFFSET = 8;
    private static int CELL_NAME_SIZE_OFFSET = 12;
    private static int CELL_NAME_EXTRA_OFFSET = 14;
    private static int CELL_NAME_OFFSETS_OFFSET = 15;
    private static int CELL_NAME_SIZE_DELTA_MASK = 3;
    private static int CELL_NAME_TYPE_SHIFT = 2;
    private static int CELL_NAME_TYPE_MASK = 7;

    private static enum NameType
    {
        COMPOUND_DENSE(0 << 2), COMPOUND_SPARSE(1 << 2), COMPOUND_SPARSE_STATIC(2 << 2), SIMPLE_DENSE(3 << 2), SIMPLE_SPARSE(4 << 2);
        static final NameType[] TYPES = NameType.values();
        final int bits;
        NameType(int bits)
        {
            this.bits = bits;
        }
        static NameType typeOf(CellName name)
        {
            if (name instanceof CompoundDenseCellName)
            {
                assert !name.isStatic();
                return COMPOUND_DENSE;
            }
            if (name instanceof CompoundSparseCellName)
                return name.isStatic() ? COMPOUND_SPARSE_STATIC : COMPOUND_SPARSE;
            if (name instanceof SimpleDenseCellName)
            {
                assert !name.isStatic();
                return SIMPLE_DENSE;
            }
            if (name instanceof SimpleSparseCellName)
            {
                assert !name.isStatic();
                return SIMPLE_SPARSE;
            }
            if (name instanceof NativeCell)
                return ((NativeCell) name).nametype();
            throw new AssertionError();
        }
    }

    NativeCell() {}

    public NativeCell(NativeAllocator allocator, OpOrder.Group writeOp, Cell copyOf)
    {
        int size = sizeOf(copyOf);
        allocator.allocate(this, size, writeOp);
        construct(this, copyOf);
    }

    static int sizeOf(Cell cell)
    {
        int size = CELL_NAME_OFFSETS_OFFSET + Math.max(0, cell.name().size() - 1) * 2 + cell.value().remaining();
        CellName name = cell.name();
        for (int i = 0 ; i < name.size() ; i++)
            size += name.get(i).remaining();
        return size;
    }

    static void construct(NativeCell construct, Cell from)
    {
        construct.internalGetLong(construct.internalOffset(), from.timestamp());
        CellName name = from.name();
        int nameSize = name.size();
        int offset = construct.internalOffset() + CELL_NAME_SIZE_OFFSET;
        construct.internalSetShort(offset, (short) nameSize);
        assert nameSize - name.clusteringSize() <= 2;
        byte cellNameExtraBits = (byte) ((nameSize - name.clusteringSize()) | NameType.typeOf(name).bits);
        construct.internalSetByte(offset += 2, cellNameExtraBits);
        offset += 1;
        short cellNameDelta = 0;
        for (int i = 1 ; i < nameSize ; i++)
        {
            cellNameDelta += name.get(i - 1).remaining();
            construct.internalSetShort(offset, cellNameDelta);
            offset += 2;
        }
        for (int i = 0 ; i < nameSize ; i++)
        {
            ByteBuffer bb = name.get(i);
            construct.internalSetBytes(offset, bb);
            offset += bb.remaining();
        }
        construct.internalSetInt(construct.internalOffset() + VALUE_OFFSET_OFFSET, offset);
        construct.internalSetBytes(offset, from.value());
    }

    // the offset at which to read the short that gives the names
    private int offsetForNameDelta(int i)
    {
        return internalOffset() + CELL_NAME_OFFSETS_OFFSET + ((i - 1) * 2);
    }

    int offsetForValue()
    {
        return internalGetInt(internalOffset() + VALUE_OFFSET_OFFSET);
    }

    private int clusteringSizeDelta()
    {
        return internalGetByte(internalOffset() + CELL_NAME_EXTRA_OFFSET) & CELL_NAME_SIZE_DELTA_MASK;
    }

    public boolean isStatic()
    {
        return nametype() == NameType.COMPOUND_SPARSE_STATIC;
    }

    private NameType nametype()
    {
        return NameType.TYPES[(((int) this.internalGetByte(this.internalOffset() + CELL_NAME_EXTRA_OFFSET)) >> CELL_NAME_TYPE_SHIFT) & CELL_NAME_TYPE_MASK];
    }

    public CellName name()
    {
        return this;
    }

    public ByteBuffer value()
    {
        int offset = offsetForValue();
        return internalGetByteBuffer(offset, ((int) internalSize()) - offset);
    }

    public long timestamp()
    {
        return internalGetLong(internalOffset());
    }

    protected int internalOffset()
    {
        return 0;
    }

    public long minTimestamp()
    {
        return timestamp();
    }

    public long maxTimestamp()
    {
        return timestamp();
    }

    public long unsharedHeapSize()
    {
        return SIZE;
    }

    public int clusteringSize()
    {
        return size() - clusteringSizeDelta();
    }

    public ColumnIdentifier cql3ColumnName(CFMetaData metadata)
    {
        switch (nametype())
        {
            case SIMPLE_SPARSE:
                ByteBuffer buffer = get(clusteringSize());
                return metadata.getColumnDefinition(buffer).name;
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                buffer = get(clusteringSize());
                if (buffer.remaining() == 0)
                    return CompoundSparseCellNameType.rowMarkerId;
                return metadata.getColumnDefinition(buffer).name;
            case SIMPLE_DENSE:
            case COMPOUND_DENSE:
                return null;
            default:
                throw new AssertionError();
        }
    }

    public ByteBuffer collectionElement()
    {
        return isCollectionCell() ? get(size() - 1) : null;
    }

    // we always have a collection element if our clustering size is 2 less than our total size,
    // and we never have one otherwiss
    public boolean isCollectionCell()
    {
        return clusteringSizeDelta() == 2;
    }

    public boolean isSameCQL3RowAs(CellNameType type, CellName other)
    {
        switch (nametype())
        {
            case SIMPLE_DENSE:
            case COMPOUND_DENSE:
                return type.compare(this, other) == 0;
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                int clusteringSize = clusteringSize();
                if (clusteringSize != other.clusteringSize() || other.isStatic() != isStatic())
                    return false;
                for (int i = 0; i < clusteringSize; i++)
                    if (type.subtype(i).compare(get(i), other.get(i)) != 0)
                        return false;
                return true;
            case SIMPLE_SPARSE:
                return true;
            default:
                throw new AssertionError();
        }
    }

    public int size()
    {
        return internalGetShort(internalOffset() + CELL_NAME_SIZE_OFFSET);
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public ByteBuffer get(int i)
    {
        // remember to take dense/sparse into account, and only return EOC when not dense
        int size = size();
        assert i >= 0 && i < size();
        int cellNamesOffset = offsetForNameDelta(size);
        int startDelta = i == 0 ? 0 : internalGetShort(offsetForNameDelta(i));
        int endDelta = i < size - 1 ? internalGetShort(offsetForNameDelta(i + 1)) : offsetForValue() - cellNamesOffset;
        return internalGetByteBuffer(cellNamesOffset + startDelta, endDelta - startDelta);
    }

    public EOC eoc()
    {
        return EOC.NONE;
    }

    public Composite withEOC(EOC eoc)
    {
        throw new UnsupportedOperationException();
    }

    public Composite start()
    {
        throw new UnsupportedOperationException();
    }

    public Composite end()
    {
        throw new UnsupportedOperationException();
    }

    public ColumnSlice slice()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isPrefixOf(Composite c)
    {
        if (size() > c.size())
            return false;

        for (int i = 0; i < size(); i++)
        {
            if (!get(i).equals(c.get(i)))
                return false;
        }
        return true;
    }

    public ByteBuffer toByteBuffer()
    {
        // for simple sparse we just return our one name buffer
        switch (nametype())
        {
            case SIMPLE_DENSE:
            case SIMPLE_SPARSE:
                return get(0);
            case COMPOUND_DENSE:
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                // This is the legacy format of composites.
                // See org.apache.cassandra.db.marshal.CompositeType for details.
                ByteBuffer result = ByteBuffer.allocate(dataSize());
                if (isStatic())
                    ByteBufferUtil.writeShortLength(result, CompositeType.STATIC_MARKER);

                for (int i = 0; i < size(); i++)
                {
                    ByteBuffer bb = get(i);
                    ByteBufferUtil.writeShortLength(result, bb.remaining());
                    result.put(bb);
                    result.put((byte)0);
                }
                result.flip();
                return result;
            default:
                throw new AssertionError();
        }
    }

    // this is the NAME dataSize, only!
    public int dataSize()
    {
        switch (nametype())
        {
            case SIMPLE_DENSE:
            case SIMPLE_SPARSE:
                return offsetForValue() - offsetForNameDelta(size());
            case COMPOUND_DENSE:
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                int size = size();
                return offsetForValue() - offsetForNameDelta(size) + 3 * size + (isStatic() ? 2 : 0);
            default:
                throw new AssertionError();
        }
    }

    private static final ByteBuffer[] EMPTY = new ByteBuffer[0];

    public CellName copy(CFMetaData cfm, ByteBufferAllocator allocator)
    {
        ByteBuffer[] r;
        switch (nametype())
        {
            case SIMPLE_DENSE:
                return CellNames.simpleDense(allocator.clone(get(0)));

            case COMPOUND_DENSE:
                r = new ByteBuffer[size()];
                for (int i = 0 ; i < r.length ; i++)
                    r[i] = allocator.clone(get(i));
                return CellNames.compoundDense(r);

            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                int clusteringSize = clusteringSize();
                r = clusteringSize == 0 ? EMPTY : new ByteBuffer[clusteringSize()];
                for (int i = 0 ; i < clusteringSize ; i++)
                    r[i] = allocator.clone(get(i));

                ByteBuffer nameBuffer = get(r.length);
                ColumnIdentifier name;

                if (nameBuffer.limit() == 0)
                {
                    name = CompoundSparseCellNameType.rowMarkerId;
                }
                else
                {
                    ColumnDefinition def = cfm.getColumnDefinition(nameBuffer);
                    if (def != null)
                    {
                        name = def.name;
                    }
                    else if (cfm.cfType == ColumnFamilyType.Super)
                    {
                        name = new ColumnIdentifier(allocator.clone(nameBuffer), "");
                    }
                    else throw new AssertionError();
                }

                if (clusteringSizeDelta() == 2)
                {
                    ByteBuffer element = allocator.clone(get(size() - 1));
                    return CellNames.compoundSparseWithCollection(r, element, name, isStatic());
                }
                return CellNames.compoundSparse(r, name, isStatic());

            case SIMPLE_SPARSE:
                ColumnDefinition def = cfm.getColumnDefinition(get(0));
                if (def != null)
                    return CellNames.simpleSparse(def.name);
                return CellNames.simpleSparse(new ColumnIdentifier(allocator.clone(get(0)), ""));
        }
        throw new IllegalStateException();
    }

    public void free(ByteBufferPool.Allocator allocator)
    {
        throw new IllegalStateException();
    }

    public long unsharedHeapSizeExcludingData()
    {
        return SIZE;
    }

    public Cell withUpdatedName(CellName newName)
    {
        throw new UnsupportedOperationException();
    }
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        throw new UnsupportedOperationException();
    }
    public boolean isMarkedForDelete(long now)
    {
        return Impl.isMarkedForDelete(this, now);
    }
    public boolean isLive(long now)
    {
        return Impl.isLive(this, now);
    }
    public long getMarkedForDeleteAt()
    {
        return Impl.getMarkedForDeleteAt();
    }
    public int cellDataSize()
    {
        return Impl.cellDataSize(this);
    }
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return Impl.serializedSize(this, type, typeSizes);
    }
    public int serializationFlags()
    {
        return Impl.serializationFlags();
    }
    public Cell diff(Cell cell)
    {
        return Impl.diff(this, cell);
    }
    public void updateDigest(MessageDigest digest)
    {
        Impl.updateDigest(this, digest);
    }
    public int getLocalDeletionTime()
    {
        return Impl.getLocalDeletionTime();
    }
    public Cell reconcile(Cell cell)
    {
        return Impl.reconcile(this, cell);
    }
    public Cell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return Impl.localCopy(this, cfMetaData, allocator);
    }
    public Cell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp)
    {
        return allocator.clone(this, cfMetaData, writeOp);
    }
    public String getString(CellNameType comparator)
    {
        return Impl.getString(this, comparator);
    }
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        Impl.validateFields(this, metadata);
    }

    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;
        if (obj instanceof CellName)
            return equals((CellName) obj);
        if (obj instanceof Cell)
            return Cell.Impl.equals(this, (Cell) obj);
        return false;
    }

    public boolean equals(CellName that)
    {
        int size = this.size();
        if (size != that.size())
            return false;

        for (int i = 0 ; i < size ; i++)
            if (!get(i).equals(that.get(i)))
                return false;
        return true;
    }

}
