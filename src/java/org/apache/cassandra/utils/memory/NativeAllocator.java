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

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.concurrent.OpOrder;
import sun.misc.Unsafe;

/**
 * The SlabAllocator is a bump-the-pointer allocator that allocates
 * large (2MB by default) regions and then doles them out to threads that request
 * slices into the array.
 * <p/>
 * The purpose of this class is to combat heap fragmentation in long lived
 * objects: by ensuring that all allocations with similar lifetimes
 * only to large regions of contiguous memory, we ensure that large blocks
 * get freed up at the same time.
 * <p/>
 * Otherwise, variable length byte arrays allocated end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 */
public class NativeAllocator extends PoolAllocator
{
    private static final Logger logger = LoggerFactory.getLogger(NativeAllocator.class);

    private final static int REGION_SIZE = 1024 * 1024;
    private final static int MAX_CLONED_SIZE = 128 * 1024; // bigger than this don't go in the region

    // globally stash any Regions we allocate but are beaten to using, and use these up before allocating any more
    private static final ConcurrentLinkedQueue<Region> RACE_ALLOCATED = new ConcurrentLinkedQueue<>();

    private final AtomicReference<Region> currentRegion = new AtomicReference<Region>();
    private final AtomicInteger regionCount = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<Region> regions = new ConcurrentLinkedQueue<>();
    private AtomicLong unslabbed = new AtomicLong(0);

    protected NativeAllocator(SubAllocator onHeap, SubAllocator offHeap)
    {
        super(onHeap, offHeap);
    }

    public void allocate(NativeAllocation allocation, int size, OpOrder.Group opGroup)
    {
        assert size >= 0;
        size += 4;
        offHeap().allocate(size, opGroup);
        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        if (size > MAX_CLONED_SIZE)
        {
            unslabbed.addAndGet(size);
            Region region = new Region(unsafe.allocateMemory(size), size);
            regions.add(region);
            if (!region.allocate(allocation, size))
                throw new AssertionError();
            return;
        }

        while (true)
        {
            Region region = getRegion();

            // Try to allocate from this region
            if (region.allocate(allocation, size))
                return;

            // not enough space!
            currentRegion.compareAndSet(region, null);
        }
    }

    public void setDiscarded()
    {
        for (Region region : regions)
            unsafe.freeMemory(region.peer);
        super.setDiscarded();
    }

    /**
     * Get the current region, or, if there is no current region, allocate a new one
     */
    private Region getRegion()
    {
        while (true)
        {
            // Try to get the region
            Region region = currentRegion.get();
            if (region != null)
                return region;

            // No current region, so we want to allocate one. We race
            // against other allocators to CAS in a Region, and if we fail we stash the region for re-use
            region = RACE_ALLOCATED.poll();
            if (region == null)
                region = new Region(unsafe.allocateMemory(REGION_SIZE), REGION_SIZE);
            if (currentRegion.compareAndSet(null, region))
            {
                regions.add(region);
                regionCount.incrementAndGet();
                logger.trace("{} regions now allocated in {}", regionCount, this);
                return region;
            }

            // someone else won race - that's fine, we'll try to grab theirs
            // in the next iteration of the loop.
            RACE_ALLOCATED.add(region);
        }
    }

    /**
     * A region of memory out of which allocations are sliced.
     *
     * This serves two purposes:
     *  - to provide a step between initialization and allocation, so that racing to CAS a
     *    new region in is harmless
     *  - encapsulates the allocation offset
     */
    private static class Region
    {
        /**
         * Actual underlying data
         */
        private final long peer;

        private final long capacity;

        /**
         * Offset for the next allocation, or the sentinel value -1
         * which implies that the region is still uninitialized.
         */
        private AtomicInteger nextFreeOffset = new AtomicInteger(0);

        /**
         * Total number of allocations satisfied from this buffer
         */
        private AtomicInteger allocCount = new AtomicInteger();

        /**
         * Create an uninitialized region. Note that memory is not allocated yet, so
         * this is cheap.
         *
         * @param peer peer
         */
        private Region(long peer, long capacity)
        {
            this.peer = peer;
            this.capacity = capacity;
        }

        /**
         * Try to allocate <code>size</code> bytes from the region.
         *
         * @return the successful allocation, or null to indicate not-enough-space
         */
        boolean allocate(NativeAllocation allocation, int size)
        {
            while (true)
            {
                int oldOffset = nextFreeOffset.get();

                if (oldOffset + size > capacity) // capacity == remaining
                    return false;

                // Try to atomically claim this region
                if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size))
                {
                    // we got the alloc
                    allocCount.incrementAndGet();
                    allocation.setPeer(peer + oldOffset);
                    allocation.setRealSize(size);
                    return true;
                }
                // we raced and lost alloc, try again
            }
        }

        @Override
        public String toString()
        {
            return "Region@" + System.identityHashCode(this) +
                   " allocs=" + allocCount.get() + "waste=" +
                   (capacity - nextFreeOffset.get());
        }
    }


    static final Unsafe unsafe;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }
}
