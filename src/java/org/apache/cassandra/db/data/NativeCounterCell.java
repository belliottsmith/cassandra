package org.apache.cassandra.db.data;

import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeCounterCell extends NativeCell implements CounterCell, CellName
{
    public long timestampOfLastDelete()
    {
        return internalGetLong(0);
    }

    protected int internalOffset()
    {
        return 8;
    }

    public NativeCounterCell(NativeAllocator allocator, OpOrder.Group writeOp, CounterCell copyOf)
    {
        int size = sizeOf(copyOf);
        allocator.allocate(this, size, writeOp);
        construct(this, copyOf);
    }

    static int sizeOf(CounterCell cell)
    {
        return 8 + NativeCell.sizeOf(cell);
    }

    static void construct(NativeCounterCell cell, CounterCell from)
    {
        cell.internalGetLong(0, from.timestampOfLastDelete());
        NativeCell.construct(cell, from);
    }

    public Cell withUpdatedName(CellName newName)
    {
        throw new UnsupportedOperationException();
    }
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        throw new UnsupportedOperationException();
    }
    public long total()
    {
        return CounterCell.Impl.total(this);
    }
    public int cellDataSize()
    {
        return CounterCell.Impl.cellDataSize(this);
    }
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return CounterCell.Impl.serializedSize(this, type, typeSizes);
    }
    public Cell diff(Cell cell)
    {
        return CounterCell.Impl.diff(this, cell);
    }
    public void updateDigest(MessageDigest digest)
    {
        CounterCell.Impl.updateDigest(this, digest);
    }
    public Cell reconcile(Cell cell)
    {
        return CounterCell.Impl.reconcile(this, cell);
    }
    public CounterCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return CounterCell.Impl.localCopy(this, cfMetaData, allocator);
    }
    public CounterCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp)
    {
        return allocator.clone(this, cfMetaData, writeOp);
    }
    public String getString(CellNameType comparator)
    {
        return CounterCell.Impl.getString(this, comparator);
    }
    public int serializationFlags()
    {
        return CounterCell.Impl.serializationFlags();
    }
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        CounterCell.Impl.validateFields(this, metadata);
    }
    public Cell markLocalToBeCleared()
    {
        return CounterCell.Impl.markLocalToBeCleared(this);
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;
        if (obj instanceof CellName)
            return equals((CellName) obj);
        if (obj instanceof CounterCell)
            return CounterCell.Impl.equals(this, (CounterCell) obj);
        return false;
    }

}
