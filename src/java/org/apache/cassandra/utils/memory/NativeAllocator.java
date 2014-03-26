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

import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.NonBlockingQueueView;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.nio.ByteBuffer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * This allocator uses native memory, with a complex life-cycle explained in {@link NativeCleaner}
 *
 * Here we will discuss the life/state-cycle of the allocator itself only.
 * There are two parallel state-cycles: Gc and LifeCycle. LifeCycle is a linear transition managing
 * the disposal of an allocator, whereas Gc is a repeatable cycle managing the phases of Gc, obviously.
 *
 * LifeCycle:
 *  LIVE         - The allocator is in use
 *  DISCARDING   - The allocator will be discarded shortly. It is expected that during this phase
 *                 allocations gradually cease. In practice this is enforced by redirecting all new writes
 *                 to a new replacement memtable/allocator; so once earlier writes have finished we can discard.
 *                 Once we start discarding we trigger a Gc, and from that point forbid any future Gc.
 *  DISCARDED    - The allocator is no longer ostensibly in use. No more allocations may be performed against it,
 *                 and the resources it holds will *eventually* be returned for use by the pool.
 *                 Resources are returned under the same conditions as recycling regions freed up by GC in {@link NativeCleaner}
 * Gc:
 *  INACTIVE     - No Gc is in progress. We may transition to REALLOCATING at any time.
 *  REALLOCATING - We have started a Gc, and are currently reallocating the still live references from any
 *                 regions we intend to recycle
 *  COLLECTING   - We have finished reallocating, and are now either compacting data we are keeping, are marking
 *                 those references that are still in use, or are collecting any regions we have freed.  Note that there
 *                 may be delayed recycles associated with this allocator after exiting this state
 *  FORBIDDEN    - No Gc is in progress, and no Gc (or change of Gc state) is now permitted. Entered when
 *                 exiting COLLECTING if we are DISCARDING or DISCARDED.
 */
public class NativeAllocator extends PoolAllocator<NativePool.Group, NativePool>
{

    /**
     * The Regions we are currently allocating from - we don't immediately move all allocations to a new Region if
     * an allocation fails, instead we only advance if there is a small fraction of the total room left, or we
     * have failed a number of times to allocate to the Region. This should help prevent wasted space.
     */
    final NonBlockingQueue<NativeRegion> allocatingFrom = new NonBlockingQueue<>();

    /**
     * All the regions we own. It is a view on allocatingFrom that is never advanced, but on which
     * removes are performed to clean up. See {@link NonBlockingQueue} for details.
     */
    final NonBlockingQueueView<NativeRegion> allocated = allocatingFrom.view();

    protected NativeAllocator(NativePool.Group group)
    {
        super(group);
    }

    public void allocate(NativeAllocation allocation, int size, OpOrder.Group writeOp)
    {
        assert size >= 0;
        size += 4;
        while (true)
        {
            if (tryAllocate(size, allocation, false))
                return;

            WaitQueue.Signal signal = writeOp.safeIsBlockingSignal(offHeap().hasRoomSignal());

            boolean allocated = tryAllocate(size, allocation, false);
            if (allocated || writeOp.isBlocking())
            {
                signal.cancel();
                if (!allocated)
                    tryAllocate(size, allocation, true);
                return;
            }
            else
            {
                group.pool.cleaner.forceClean();
                signal.awaitUninterruptibly();
            }
        }
    }

    private boolean tryAllocate(int size, NativeAllocation allocation, boolean isBlocking)
    {
        NativePool pool = group.pool;

        if (size >= pool.regionSize)
        {
            NativeRegion region = pool.tryAllocateRegion(size);
            if (region == null)
                return false;
            fulfillFromNewRegion(size, allocation, region);
            return true;
        }

        while (true)
        {
            // attempt to allocate from any regions we consider to have space left in them
            NativeRegion last = allocatingFrom.tail();
            for (NativeRegion region : allocatingFrom)
            {
                last = region;

                if (region.allocate(size, allocation))
                    return true;

                // if we failed to allocate, maybe remove ourselves from the
                // collection of regions we try to allocate from in future
                if (!region.mayContinueAllocating())
                    if (allocatingFrom.advanceIfHead(region))
                        region.finishAllocating();
            }

            // if we fail, allocate a new region either from the pool, or directly
            NativeRegion newRegion = pool.tryAllocateRegion(pool.regionSize);
            if (newRegion == null)
            {
                if (!isBlocking)
                    return false;
                // if we're blocking, force allocate a new region that is at least as big as we want, and no
                // smaller than the overAllocatedRegionSize, which is a smaller size used for when the pool is full but
                // we need to allocate anyway
                newRegion = new NativeRegion(Math.max(size, pool.overAllocatedRegionSize), pool.isGcEnabled());
                if (newRegion.size() == size)
                {
                    fulfillFromNewRegion(size, allocation, newRegion);
                    return true;
                }
            }

            // try to append to our list - if the list has been appended to already, put it back in the pool
            // and try allocating from the Region somebody beat us to adding, otherwise set ourselves as its owner
            offHeap().acquired(pool.regionSize);
            if (!allocatingFrom.appendIfTail(last, newRegion))
            {
                offHeap().release(pool.regionSize);
                pool.recycle(newRegion, false);
            }
        }
    }

    private void fulfillFromNewRegion(int size, NativeAllocation allocation, NativeRegion region)
    {
        if (!region.allocate(size, allocation))
            throw new AssertionError();
        offHeap().acquired(size);
        adopt(region);
    }

    void adopt(NativeRegion region)
    {
        allocatingFrom.append(region);
    }

    /**
     * atomically transition to targetLifecycle (always), and targetGc (if possible).
     * this is used by setDiscarding() to ensure no future GCs occur after the one currently running (if any)
     * or the one started by setDiscarding (otherwise).
     *
     * If the switch to Marking fails it means we are already GCing; once we are set Discarding any Gc not already
     * "started" (as declared by the state) will be forbidden. So we want to ensure that when we set Discarding
     * the Gc state is >= Marking. When finishing the final we are now guaranteed to be in the Discarding state,
     * so we transition to Gc.Forbidden instead of Inactive.
     *
     * @param targetGc the target Gc state (only applied when current state permits)
     * @param targetLifecycle the target LifeCycle state (always applied_
     * @return true if we successfully switched to targetGc, and false if we only switched targetLifecycle
     */
    boolean transition(Gc targetGc, LifeCycle targetLifecycle)
    {
        while (true)
        {
            State cur = state;
            State next = cur.transition(targetGc);
            boolean success = next != null;
            if (!success)
            {
                if (targetLifecycle == null)
                    return false;
                next = cur.transition(targetLifecycle);
            }
            else if (targetLifecycle != null)
            {
                next = next.transition(targetLifecycle);
            }
            if (cur == next || stateUpdater.compareAndSet(this, cur, next))
                return success;
        }
    }

    boolean transition(Gc targetState)
    {
        return transition(targetState, null);
    }

    public void setDiscarding()
    {
        group.live.remove(this);
        onHeap().markAllReclaiming();
        offHeap().markAllReclaiming();

        // if we aren't already GCing, and GC is enabled, we force a GC of this allocator before
        // we discard it, in case we can quickly reclaim some of its space
        if (!group.pool.isGcEnabled())
            transition(LifeCycle.DISCARDING);
        else if (transition(Gc.REALLOCATING, LifeCycle.DISCARDING))
            NativeCleaner.unsafeGc(this);
    }

    public void setDiscarded()
    {
        transition(LifeCycle.DISCARDED);
        doDiscard();
    }

    // we permit GC to repeat the discard work, in case it finished after we discarded,
    // as it may have given us some new regions
    void doDiscard()
    {
        OpOrder.Barrier readBarrier = group.reads.newBarrier();
        readBarrier.issue();

        // place all regions that aren't already being discarded (say, by GC) into a map that will be used
        // to mark them referenced by any read ops started before our barrier
        final Multimap<NativeRegion.MarkKey, NativeDelayedRecycle> markLookup = ArrayListMultimap.create(2 * (int) (offHeap().owns() / group.pool.regionSize), 1);
        for (NativeRegion region : allocated)
        {
            if (region.transition(NativeRegion.State.LIVE, NativeRegion.State.DISCARDING))
            {
                markLookup.put(region.markKey, new NativeDelayedRecycle(group.pool, region));
                if (!offHeap().transferAcquired(region.size()))
                    throw new AssertionError();
                offHeap().transferReclaiming(region.size());
            }
        }

        if (!markLookup.isEmpty())
        {
            readBarrier.await();
            // must wait until after the read barrier to ensure all referrers are populated
            group.mark(readBarrier, markLookup);

            for (NativeDelayedRecycle marked : markLookup.values())
                marked.unmark();
        }

        onHeap().releaseAll();
    }
}
