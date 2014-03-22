package org.apache.cassandra.db.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

public interface CounterCell extends Cell
{
    long timestampOfLastDelete();

    long total();

    Cell markLocalToBeCleared();

    CounterCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator);

    CounterCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp);

    static class Impl extends Cell.Impl
    {
        protected static final CounterContext contextManager = CounterContext.instance();

        static long total(CounterCell me)
        {
            return contextManager.total(me.value());
        }

        static int cellDataSize(CounterCell me)
        {
            // A counter column adds 8 bytes for timestampOfLastDelete to Cell.
            return Cell.Impl.cellDataSize(me) + TypeSizes.NATIVE.sizeof(me.timestampOfLastDelete());
        }

        static int serializedSize(CounterCell me, CellNameType type, TypeSizes typeSizes)
        {
            return Cell.Impl.serializedSize(me, type, typeSizes) + typeSizes.sizeof(me.timestampOfLastDelete());
        }

        static Cell diff(CounterCell me, Cell cell)
        {
            assert (cell instanceof CounterCell) || (cell instanceof DeletedCell) : "Wrong class type: " + cell.getClass();

            if (me.timestamp() < cell.timestamp())
                return cell;

            // Note that if at that point, cell can't be a tombstone. Indeed,
            // cell is the result of merging us with other nodes results, and
            // merging a CounterCell with a tombstone never return a tombstone
            // unless that tombstone timestamp is greater that the CounterCell
            // one.
            assert cell instanceof CounterCell : "Wrong class type: " + cell.getClass();

            if (me.timestampOfLastDelete() < ((CounterCell) cell).timestampOfLastDelete())
                return cell;
            CounterContext.Relationship rel = contextManager.diff(cell.value(), me.value());
            if (rel == CounterContext.Relationship.GREATER_THAN || rel == CounterContext.Relationship.DISJOINT)
                return cell;
            return null;
        }

        /*
         * We have to special case digest creation for counter column because
         * we don't want to include the information about which shard of the
         * context is a delta or not, since this information differs from node to
         * node.
         */
        static void updateDigest(CounterCell me, MessageDigest digest)
        {
            digest.update(me.name().toByteBuffer().duplicate());
            // We don't take the deltas into account in a digest
            contextManager.updateDigest(digest, me.value());
            DataOutputBuffer buffer = new DataOutputBuffer(17);
            try
            {
                buffer.writeLong(me.timestamp());
                buffer.writeByte(me.serializationFlags());
                buffer.writeLong(me.timestampOfLastDelete());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            digest.update(buffer.getData(), 0, buffer.getLength());
        }

        static Cell reconcile(CounterCell me, Cell cell)
        {
            // live + tombstone: track last tombstone
            if (cell.isMarkedForDelete(Long.MIN_VALUE)) // cannot be an expired cell, so the current time is irrelevant
            {
                // live < tombstone
                if (me.timestamp() < cell.timestamp())
                {
                    return cell;
                }
                // live last delete >= tombstone
                if (me.timestampOfLastDelete() >= cell.timestamp())
                {
                    return me;
                }
                // live last delete < tombstone
                return new BufferCounterCell(me.name(), me.value(), me.timestamp(), cell.timestamp());
            }

            assert cell instanceof CounterCell : "Wrong class type: " + cell.getClass();

            // live < live last delete
            if (me.timestamp() < ((CounterCell) cell).timestampOfLastDelete())
                return cell;
            // live last delete > live
            if (me.timestampOfLastDelete() > cell.timestamp())
                return me;

            // live + live. return one of the cells if its context is a superset of the other's, or merge them otherwise
            ByteBuffer context = contextManager.merge(me.value(), cell.value());
            if (context == me.value() && me.timestamp() >= cell.timestamp() && me.timestampOfLastDelete() >= ((CounterCell) cell).timestampOfLastDelete())
                return me;
            else if (context == cell.value() && cell.timestamp() >= me.timestamp() && ((CounterCell) cell).timestampOfLastDelete() >= me.timestampOfLastDelete())
                return cell;
            else // merge clocks and timsestamps.
                return new BufferCounterCell(me.name(),
                                             context,
                                             Math.max(me.timestamp(), cell.timestamp()),
                                             Math.max(me.timestampOfLastDelete(), ((CounterCell) cell).timestampOfLastDelete()));
        }

        static CounterCell localCopy(CounterCell me, CFMetaData cfMetaData, ByteBufferAllocator allocator)
        {
            return new BufferCounterCell(me.name().copy(cfMetaData, allocator), allocator.clone(me.value()), me.timestamp(), me.timestampOfLastDelete());
        }

        static String getString(CounterCell me, CellNameType comparator)
        {
            return String.format("%s:false:%s@%d!%d",
                                 comparator.getString(me.name()),
                                 contextManager.toString(me.value()),
                                 me.timestamp(),
                                 me.timestampOfLastDelete());
        }

        static int serializationFlags()
        {
            return ColumnSerializer.COUNTER_MASK;
        }

        static void validateFields(Cell me, CFMetaData metadata) throws MarshalException
        {
            Cell.Impl.validateName(me, metadata);
            // We cannot use the value validator as for other columns as the CounterColumnType validate a long,
            // which is not the internal representation of counters
            contextManager.validateContext(me.value());
        }

        static Cell markLocalToBeCleared(CounterCell me)
        {
            ByteBuffer marked = contextManager.markLocalToBeCleared(me.value());
            return marked == me.value() ? me : new BufferCounterCell(me.name(), marked, me.timestamp(), me.timestampOfLastDelete());
        }

        static boolean equals(CounterCell me, Object that)
        {
            if (that == me)
                return true;
            if (!(that instanceof CounterCell))
                return false;
            return equals(me, (CounterCell) that);
        }
        static boolean equals(CounterCell me, CounterCell that)
        {
            return Cell.Impl.equals(me, that) && that.timestampOfLastDelete() == me.timestampOfLastDelete();
        }
    }
}
