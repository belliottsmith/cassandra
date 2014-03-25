package org.apache.cassandra.utils.memory;

import org.apache.cassandra.db.data.DataAllocator;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Represents an action to take on any unsafe memory we access during a transaction to make it safe
 * outside of the transaction. There are three such actions:
 * Unsafe:
 *   Do nothing. It will be unsafe unless otherwise protected.
 * OnHeap:
 *   Copy the memory onto the heap so it is managed by the JVM. This is useful in situations where
 *   the code complexity for ensuring safety of access would be too great, or too error prone.
 * Refer:
 *   Register the memory with one of our managed GC roots, until setDone() is called on it (which
 *   indicates we no longer need the reference)
 * Impossible:
 *   Simply a placeholder instead of null for the case where no RefAction makes sense. Will throw an exception
 *   is anything except setDone() is called upon it
 */
public abstract class RefAction implements AutoCloseable
{

    static final class OnHeap extends RefAction
    {
        static final OnHeap INSTANCE = new OnHeap();

        public RefAction subAction()
        {
            return this;
        }

        public boolean copyOnHeap(DataAllocator.DataPool pool)
        {
            return pool.needToCopyOnHeap();
        }
    }

    static final class Unsafe extends RefAction
    {
        static final Unsafe INSTANCE = new Unsafe();

        public RefAction subAction()
        {
            return this;
        }

        public boolean copyOnHeap(DataAllocator.DataPool pool)
        {
            return false;
        }
    }

    static final class Impossible extends RefAction
    {
        static final Impossible INSTANCE = new Impossible();

        public RefAction subAction()
        {
            throw new IllegalStateException();
        }

        public boolean copyOnHeap(DataAllocator.DataPool pool)
        {
            throw new IllegalStateException();
        }

        public void complete(Pool.AllocatorGroup group, OpOrder.Group readOp, Object val)
        {
            throw new IllegalStateException();
        }
    }

    public static RefAction allocateOnHeap()
    {
        return OnHeap.INSTANCE;
    }

    public static RefAction unsafe()
    {
        return Unsafe.INSTANCE;
    }

    public static RefAction impossible()
    {
        return Impossible.INSTANCE;
    }

    public static RefAction refer()
    {
        return null;
    }

    /**
     * Complete the action on the given value, retrieved from the provided group, during the readOp transaction.
     * In general this is a no-op, except for the Refer action, which registers it with a GC root.
     *
     * @param group group val came from
     * @param readOp read was guarded by this txn
     * @param val the value we retrieved
     */
    public void complete(NativePool.Group group, OpOrder.Group readOp, Object val)
    {
    }

    /**
     * Indicate the memory guarded by this RefAction is no longer need (by the action that started the RefAction)
     */
    public void close()
    {

    }

    /**
     * Reset the state
     */
    public void reset()
    {

    }

    /**
     * @return the action to take on any sub-operation that will ultimately be guarded by this RefAction.
     */
    public abstract RefAction subAction();

    /**
     * @return the allocator to copy any memory to, if any
     */
    public abstract boolean copyOnHeap(DataAllocator.DataPool pool);

}
