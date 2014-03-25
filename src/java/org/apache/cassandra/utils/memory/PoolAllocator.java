/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils.memory;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

public abstract class PoolAllocator<G extends Pool.AllocatorGroup<P>, P extends Pool>
{

    public final G group;
    private final SubAllocator onHeap;
    private final SubAllocator offHeap;
    volatile LifeCycle state = LifeCycle.LIVE;

    static final AtomicReferenceFieldUpdater<PoolAllocator, LifeCycle> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(PoolAllocator.class, LifeCycle.class, "state");

    /**
     *  LIVE         - The allocator is in use
     *  DISCARDING   - The allocator will be discarded shortly. It is expected that during this phase
     *                 allocations gradually cease. In practice this is enforced by redirecting all new writes
     *                 to a new replacement memtable/allocator; so once earlier writes have finished we can discard.
     *  DISCARDED    - The allocator is no longer ostensibly in use. No more allocations may be performed against it,
     *                 and the resources it holds will *eventually* be returned for use by the pool.
     */
    static enum LifeCycle
    {
        LIVE, DISCARDING, DISCARDED;
        LifeCycle transition(LifeCycle targetState)
        {
            switch (targetState)
            {
                case DISCARDING:
                    assert this == LifeCycle.LIVE;
                    return LifeCycle.DISCARDING;
                case DISCARDED:
                    assert this == LifeCycle.DISCARDING;
                    return LifeCycle.DISCARDED;
            }
            throw new IllegalStateException();
        }
    }

    PoolAllocator(G group)
    {
        this.group = group;
        this.onHeap = group.pool.onHeap.newAllocator();
        this.offHeap = group.pool.offHeap.newAllocator();
    }

    public SubAllocator onHeap()
    {
        return onHeap;
    }

    public SubAllocator offHeap()
    {
        return offHeap;
    }

    protected void transition(LifeCycle newState)
    {
        LifeCycle cur = state;
        if (stateUpdater.compareAndSet(this, cur, state.transition(newState)))
            return;
        throw new AssertionError();
    }

    /**
     * Mark this allocator reclaiming; this will permit any outstanding allocations to temporarily
     * overshoot the maximum memory limit so that flushing can begin immediately
     */
    public void setDiscarding()
    {
        transition(LifeCycle.DISCARDING);
        // mark the memory owned by this allocator as reclaiming
        onHeap.markAllReclaiming();
        offHeap.markAllReclaiming();
    }

    /**
     * Indicate the memory and resources owned by this allocator are no longer referenced,
     * and can be reclaimed/reused.
     */
    public void setDiscarded()
    {
        transition(LifeCycle.DISCARDED);
        // release any memory owned by this allocator; automatically signals waiters
        onHeap.releaseAll();
        offHeap.releaseAll();
    }

    public boolean isLive()
    {
        return state == LifeCycle.LIVE;
    }

    /**
     * Created by a SubPool to represent memory that is temporarily allocated to this allocator.
     * When the allocator needs more memory, it allocates it through this object, which acquires
     * (and maybe allocates) memory in its parent SubPool and accounts for it here as well. Once the allocator is done
     * it relinquishes the resources through this class, which ensures the resources are freed in the parent SubPool.
     */
    public static final class SubAllocator
    {
        // the tracker we are owning memory from
        private final Pool.SubPool parent;

        // the amount of memory/resource owned by this object
        private volatile long owns;
        // the amount of memory we are reporting to collect; this may be inaccurate, but is close
        // and is used only to ensure that once we have reclaimed we mark the tracker with the same amount
        private volatile long reclaiming;

        SubAllocator(Pool.SubPool parent)
        {
            this.parent = parent;
        }

        // should only be called once we know we will never allocate to the object again.
        // currently no corroboration/enforcement of this is performed.
        void releaseAll()
        {
            parent.adjustAcquired(-ownsUpdater.getAndSet(this, 0), false);
            parent.adjustReclaiming(-reclaimingUpdater.getAndSet(this, 0));
        }

        // allocate memory in the tracker, and mark ourselves as owning it
        public void allocate(long size, OpOrder.Group opGroup)
        {
            while (true)
            {
                if (parent.tryAllocate(size))
                {
                    acquired(size);
                    return;
                }
                WaitQueue.Signal signal = opGroup.safeIsBlockingSignal(parent.hasRoom().register());
                boolean allocated = parent.tryAllocate(size);
                if (allocated || opGroup.isBlocking())
                {
                    signal.cancel();
                    if (allocated) // if we allocated, take ownership
                        acquired(size);
                    else // otherwise we're blocking so we're permitted to overshoot our constraints, to just allocate without blocking
                        allocated(size);
                    return;
                }
                else
                    signal.awaitUninterruptibly();
            }
        }

        // retroactively mark an amount allocated amd acquired in the tracker, and owned by us
        void allocated(long size)
        {
            parent.adjustAcquired(size, true);
            ownsUpdater.addAndGet(this, size);
        }

        // retroactively mark an amount acquired in the tracker, and owned by us
        void acquired(long size)
        {
            parent.adjustAcquired(size, false);
            ownsUpdater.addAndGet(this, size);
        }

        void release(long size)
        {
            parent.adjustAcquired(-size, false);
            ownsUpdater.addAndGet(this, -size);
        }

        // if this.owns > size, subtract size from this.owns (atomically), otherwise return false.
        // is used by recycle to take ownership of a portion what we own without returning it to the pool
        boolean transferAcquired(int size)
        {
            while (true)
            {
                long cur = owns;
                long next = cur - size;
                if (next < 0)
                    return false;
                if (ownsUpdater.compareAndSet(this, cur, next))
                    return true;
            }
        }

        // subtract at most size from this.reclaiming (atomically), and adjust the parent's reclaiming by
        // the difference between what we wanted to subtract and we actually subtracted.
        // this is used to atomically correct the reclaiming amount after it was speculatively set before an absolute
        // figure could be established, and to take ownership of the reclamation away from this allocator for delayed recycling
        void transferReclaiming(int size)
        {
            while (true)
            {
                long cur = reclaiming;
                long next = Math.max(0, cur - size);
                if (cur == 0 || reclaimingUpdater.compareAndSet(this, cur, next))
                {
                    parent.adjustReclaiming(size - (cur - next));
                    return;
                }
            }
        }

        // mark everything we currently own as reclaiming, both here and in our parent
        void markAllReclaiming()
        {
            while (true)
            {
                long cur = owns;
                long prev = reclaiming;
                if (reclaimingUpdater.compareAndSet(this, prev, cur))
                {
                    parent.adjustReclaiming(cur - prev);
                    return;
                }
            }
        }

        public long owns()
        {
            return owns;
        }

        public float ownershipRatio()
        {
            long limit = parent.limit;
            if (limit == 0L)
                return 0f;
            float r = owns / (float) limit;
            assert !Float.isNaN(r);
            return r;
        }

        public WaitQueue.Signal hasRoomSignal()
        {
            return parent.hasRoom().register();
        }

        private static final AtomicLongFieldUpdater<SubAllocator> ownsUpdater = AtomicLongFieldUpdater.newUpdater(SubAllocator.class, "owns");
        private static final AtomicLongFieldUpdater<SubAllocator> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(SubAllocator.class, "reclaiming");
    }
}
