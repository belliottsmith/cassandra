package org.apache.cassandra.db.data;

import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeDeletedCell extends NativeCell implements DeletedCell, CellName
{

    public NativeDeletedCell(NativeAllocator allocator, OpOrder.Group writeOp, DeletedCell copyOf)
    {
        super(allocator, writeOp, copyOf);
    }
    public Cell withUpdatedName(CellName newName)
    {
        throw new UnsupportedOperationException();
    }
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLocalDeletionTime()
    {
        // can optimise this to avoid allocating ByteBuffer
        return DeletedCell.Impl.getLocalDeletionTime(this);
    }

    @Override
    public boolean isMarkedForDelete(long now)
    {
        return DeletedCell.Impl.isMarkedForDelete(now);
    }
    @Override
    public long getMarkedForDeleteAt()
    {
        return DeletedCell.Impl.getMarkedForDeleteAt(this);
    }
    @Override
    public void updateDigest(MessageDigest digest)
    {
        DeletedCell.Impl.updateDigest(this, digest);
    }
    @Override
    public Cell reconcile(Cell cell)
    {
        return DeletedCell.Impl.reconcile(this, cell);
    }
    @Override
    public DeletedCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return DeletedCell.Impl.localCopy(this, cfMetaData, allocator);
    }
    @Override
    public DeletedCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp)
    {
        return allocator.clone(this, cfMetaData, writeOp);
    }
    @Override
    public int serializationFlags()
    {
        return DeletedCell.Impl.serializationFlags();
    }
    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        DeletedCell.Impl.validateFields(this, metadata);
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;
        if (obj instanceof CellName)
            return equals((CellName) obj);
        if (obj instanceof DeletedCell)
            return DeletedCell.Impl.equals(this, (DeletedCell) obj);
        return false;
    }
}
