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

public class NativeExpiringCell extends NativeCell implements ExpiringCell, CellName
{

    protected int internalOffset()
    {
        return 8;
    }

    public int getTimeToLive()
    {
        return internalGetInt(0);
    }

    public int getLocalDeletionTime()
    {
        return internalGetInt(4);
    }

    public NativeExpiringCell(NativeAllocator allocator, OpOrder.Group writeOp, ExpiringCell copyOf)
    {
        int size = sizeOf(copyOf);
        allocator.allocate(this, size, writeOp);
        construct(this, copyOf);
    }

    static int sizeOf(ExpiringCell cell)
    {
        return 8 + NativeCell.sizeOf(cell);
    }

    static void construct(NativeExpiringCell cell, ExpiringCell from)
    {
        cell.internalSetInt(0, from.getTimeToLive());
        cell.internalSetInt(4, from.getLocalDeletionTime());
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
    public int cellDataSize()
    {
        return ExpiringCell.Impl.cellDataSize(this);
    }
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return ExpiringCell.Impl.serializedSize(this, type, typeSizes);
    }
    public Cell diff(Cell cell)
    {
        return ExpiringCell.Impl.diff(this, cell);
    }
    public void updateDigest(MessageDigest digest)
    {
        ExpiringCell.Impl.updateDigest(this, digest);
    }
    public Cell reconcile(Cell cell)
    {
        return ExpiringCell.Impl.reconcile(this, cell);
    }
    public ExpiringCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return ExpiringCell.Impl.localCopy(this, cfMetaData, allocator);
    }
    public ExpiringCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp)
    {
        return allocator.clone(this, cfMetaData, writeOp);
    }
    public String getString(CellNameType comparator)
    {
        return ExpiringCell.Impl.getString(this, comparator);
    }
    public int serializationFlags()
    {
        return ExpiringCell.Impl.serializationFlags();
    }
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        ExpiringCell.Impl.validateFields(this, metadata);
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;
        if (obj instanceof CellName)
            return equals((CellName) obj);
        if (obj instanceof ExpiringCell)
            return ExpiringCell.Impl.equals(this, (ExpiringCell) obj);
        return false;
    }

}
