/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.OpOrder;

import org.slf4j.*;

import java.lang.ref.SoftReference;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.google.common.collect.Multimap;


/**
 * This class encapsulates the main body of functionality for collecting garbage from an {@link NativePool}
 *
 * Memory in a pool is managed through four (yes, four) different mechanisms in its lifecycle:
 *
 * 1) Initially memory is managed like malloc/free: it is considered referenced at least until the allocation is
 *    matched by a corresponding free()
 * 2) The memory is then protected by the associated OpOrder(s); an allocation is only available for collection
 *    once all operations started prior to the free() have completed
 * 3) During this time any operation protected by the read/write OpOrder may optionally 'ref' {@link Referrer} an object,
 *    or objects, that each reference (in the normal java sense) some allocations; once the read operation finishes these
 *    will have been registered with one or more GC roots {@link Referrers} - one per allocator group referenced.
 * 4) When an allocator discard occurs, any extant {@link Referrer} are walked that were created during
 *    operations started prior to the collect phase, and any regions that are reachable by any of these 'refs' are
 *    switched to a refcount phase. When each ref completes it decrements the count of any such regions it reached.
 *    Once this count hits 0, the region is finally eligible for reuse.
 */
public class NativePool extends Pool
{

    private static final Logger logger = LoggerFactory.getLogger(NativePool.class);

    public final int regionSize;
    final int overAllocatedRegionSize; // size of region to allocate if we're full but need temporary room

    /** A queue of regions we have previously used but are now spare */
    private final NonBlockingQueue<NativeRegion> recycled = new NonBlockingQueue<>();

    /** The set of live allocators associated with this pool */
    final NonBlockingQueue<SoftReference<Group>> groups = new NonBlockingQueue<>();

    public NativePool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        this(1 << 20, maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
    }

    public NativePool(int regionSize, long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
        this.regionSize = regionSize;
        this.overAllocatedRegionSize = Math.max(regionSize >> 8, Math.min(regionSize, 8 << 10));
    }

    public Group newAllocatorGroup(String name, OpOrder reads, OpOrder writes)
    {
        return new Group(name, this, reads, writes);
    }

    SubPool getSubPool(boolean onHeap, long limit, float cleanThreshold)
    {
        return onHeap ? super.getSubPool(true, limit, cleanThreshold)
                      : new OffHeapSubPool(limit, cleanThreshold);
    }

    public static class Group extends AllocatorGroup<NativePool>
    {

        /** The set of live allocators associated with this group */
        protected final ConcurrentHashMap<NativeAllocator, Boolean> live = new ConcurrentHashMap<>(16, .75f, 1);

        /** Our GC roots */
        final Referrers referrers = new Referrers();

        public Group(String name, NativePool parent, OpOrder reads, OpOrder writes)
        {
            super(name, parent, reads, writes);
            parent.groups.append(new SoftReference<>(this));
        }

        public NativeAllocator newAllocator()
        {
            NativeAllocator allocator = new NativeAllocator(this);
            live.put(allocator, Boolean.TRUE);
            return allocator;
        }

        void mark(OpOrder.Barrier readBarrier, Multimap<NativeRegion.MarkKey, NativeDelayedRecycle> collecting)
        {
            referrers.mark(readBarrier, collecting);
        }

    }

    /**
     * Either take a region from the recycle queue, or allocate a new one if there is room.
     * If neither is true, return null.
     * @param size
     * @return
     */
    NativeRegion tryAllocateRegion(int size)
    {
        while (true)
        {
            /**
             * tryAllocate encapsulates knowledge of if we can satisfy our request without allocating
             * success means we need to allocate a new region; failure means we may be able to satisfy
             * from our pool, or that we shouldn't wait
             */
            if (!recycled.isEmpty() && size == regionSize)
            {
                // try returning a recycled region
                NativeRegion region = recycled.advance();
                if (region != null)
                    return region;
            }
            else if (offHeap.tryAllocate(size))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Allocating new region with size {}, total allocated {}", size, offHeap.allocated());
                return new NativeRegion(size);
            }
            else if (!recycled.isEmpty() && size > regionSize)
            {
                // if we're allocating something oversized, and there is room in recycled regions, but not unallocated
                // so free up enough recycled regions to make space for us, and try again
                int reclaim = 0;
                while (reclaim < size)
                {
                    NativeRegion region = recycled.advance();
                    if (region == null)
                        break;
                    free(region);
                    reclaim += regionSize;
                }
            }
            else
                return null;

        }
    }

    /**
     * deallocate the region
     *
     * @param region
     */
    private void free(NativeRegion region)
    {
        offHeap.adjustAllocated(-region.size());
        region.releaseNativeMemory();
    }

    /**
     * Either put the region back in the pool, or free it up if it's oversized or we're over limit.
     * When discarding an OffHeapAllocator we release the memory ownership before we recycle here,
     * so no need to account for it. We are simply reusing the region.
     *
     * Note that the region provided should be region.recycle() unless it was never exposed.
     *
     * @param region
     */
    void recycle(NativeRegion region, boolean hasBeenUsed)
    {
        if (region.size() != regionSize || offHeap.isExceeded())
            free(region);
        else
            recycled.append(hasBeenUsed ? region.recycle() : region);
    }

    private final class OffHeapSubPool extends SubPool
    {

        /** The amount of memory allocators from this pool are currently using */
        volatile long used;

        public OffHeapSubPool(long limit, float cleanThreshold)
        {
            super(limit, cleanThreshold);
        }

        public long used()
        {
            return used;
        }

        void adjustAcquired(long size, boolean alsoAllocated)
        {
            usedUpdater.addAndGet(this, size);
            if (size >= 0)
            {
                if (alsoAllocated)
                    adjustAllocated(size);
                maybeClean();
            }
            else
            {
                hasRoom.signalAll();
            }
        }
    }

    private static final AtomicLongFieldUpdater<OffHeapSubPool> usedUpdater = AtomicLongFieldUpdater.newUpdater(OffHeapSubPool.class, "used");
}