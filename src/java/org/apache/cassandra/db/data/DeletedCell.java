package org.apache.cassandra.db.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

public interface DeletedCell extends Cell
{

    DeletedCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator);

    DeletedCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp);

    static class Impl extends Cell.Impl
    {
        static boolean isMarkedForDelete(long now)
        {
            return true;
        }

        static long getMarkedForDeleteAt(DeletedCell me)
        {
            return me.timestamp();
        }

        static void updateDigest(DeletedCell me, MessageDigest digest)
        {
            digest.update(me.name().toByteBuffer().duplicate());

            DataOutputBuffer buffer = new DataOutputBuffer(9);
            try
            {
                buffer.writeLong(me.timestamp());
                buffer.writeByte(serializationFlags());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            digest.update(buffer.getData(), 0, buffer.getLength());
        }

        static int getLocalDeletionTime(DeletedCell me)
        {
            ByteBuffer value = me.value();
            return value.getInt(value.position());
        }

        static Cell reconcile(Cell me, Cell cell)
        {
            if (cell instanceof DeletedCell)
                return Cell.Impl.reconcile(me, cell);
            return cell.reconcile(me);
        }

        static DeletedCell localCopy(DeletedCell me, CFMetaData cfMetaData, ByteBufferAllocator allocator)
        {
            return new BufferDeletedCell(me.name().copy(cfMetaData, allocator), allocator.clone(me.value()), me.timestamp());
        }

        static int serializationFlags()
        {
            return ColumnSerializer.DELETION_MASK;
        }

        static void validateFields(DeletedCell me, CFMetaData metadata) throws MarshalException
        {
            Cell.Impl.validateName(me, metadata);
            if (me.value().remaining() != 4)
                throw new MarshalException("A tombstone value should be 4 bytes long");
            if (me.getLocalDeletionTime() < 0)
                throw new MarshalException("The local deletion time should not be negative");
        }

        static boolean equals(DeletedCell me, Object that)
        {
            if (that == me)
                return true;
            if (!(that instanceof DeletedCell))
                return false;
            return equals(me, (DeletedCell) that);
        }
        static boolean equals(DeletedCell me, DeletedCell that)
        {
            return Cell.Impl.equals(me, that) && me.getLocalDeletionTime() == that.getLocalDeletionTime();
        }
    }
}
