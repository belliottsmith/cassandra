package org.apache.cassandra.db.data;

import java.io.IOException;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

public interface ExpiringCell extends Cell
{
    int MAX_TTL = 20 * 365 * 24 * 60 * 60; // 20 years in seconds

    int getTimeToLive();
    int getLocalDeletionTime();

    ExpiringCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator);

    ExpiringCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp);

    static class Impl extends Cell.Impl
    {

        static int cellDataSize(ExpiringCell me)
        {
            return Cell.Impl.cellDataSize(me) + TypeSizes.NATIVE.sizeof(me.getLocalDeletionTime()) + TypeSizes.NATIVE.sizeof(me.getTimeToLive());
        }

        static int serializedSize(ExpiringCell me, CellNameType type, TypeSizes typeSizes)
        {
        /*
         * An expired column adds to a Cell :
         *    4 bytes for the localExpirationTime
         *  + 4 bytes for the timeToLive
        */
            return Cell.Impl.serializedSize(me, type, typeSizes) + typeSizes.sizeof(me.getLocalDeletionTime()) + typeSizes.sizeof(me.getTimeToLive());
        }

        static void updateDigest(ExpiringCell me, MessageDigest digest)
        {
            digest.update(me.name().toByteBuffer().duplicate());
            digest.update(me.value().duplicate());

            DataOutputBuffer buffer = new DataOutputBuffer(13);
            try
            {
                buffer.writeLong(me.timestamp());
                buffer.writeByte(me.serializationFlags());
                buffer.writeInt(me.getTimeToLive());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            digest.update(buffer.getData(), 0, buffer.getLength());
        }

        static ExpiringCell localCopy(ExpiringCell me, CFMetaData cfMetaData, ByteBufferAllocator allocator)
        {
            return new BufferExpiringCell(me.name().copy(cfMetaData, allocator), allocator.clone(me.value()), me.timestamp(), me.getTimeToLive(), me.getLocalDeletionTime());
        }

        static String getString(ExpiringCell me, CellNameType comparator)
        {
            return String.format("%s!%d", Cell.Impl.getString(me, comparator), me.getTimeToLive());
        }

        static boolean isMarkedForDelete(ExpiringCell me, long now)
        {
            return (int) (now / 1000) >= me.getLocalDeletionTime();
        }

        static long getMarkedForDeleteAt(ExpiringCell me)
        {
            return me.timestamp();
        }

        static int serializationFlags()
        {
            return ColumnSerializer.EXPIRATION_MASK;
        }

        static void validateFields(ExpiringCell me, CFMetaData metadata) throws MarshalException
        {
            Cell.Impl.validateFields(me, metadata);
            if (me.getTimeToLive() <= 0)
                throw new MarshalException("A column TTL should be > 0");
            if (me.getLocalDeletionTime() < 0)
                throw new MarshalException("The local expiration time should not be negative");
        }

        static boolean equals(ExpiringCell me, Object that)
        {
            if (that == me)
                return true;
            if (!(that instanceof ExpiringCell))
                return false;
            return equals(me, (ExpiringCell) that);
        }
        static boolean equals(ExpiringCell me, ExpiringCell that)
        {
            return Cell.Impl.equals(me, that) && me.getLocalDeletionTime() == that.getLocalDeletionTime() && me.getTimeToLive() == that.getTimeToLive();
        }
    }
}
