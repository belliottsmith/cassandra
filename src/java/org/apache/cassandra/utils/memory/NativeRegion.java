package org.apache.cassandra.utils.memory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.IAllocator;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A small contiguous portion of memory that is managed by an {@link NativePool}, is assigned to specific a
 * {@link NativeAllocator} from which it serves allocation requests.
 *
 * A brief outline of the life-cycle of an allocation, and the region itself:
 *
 * Region is LIVE: we may allocate and free
 *
 * allocate:
 * 1)  allocOffset is bumped atomically by the desired size, if doing so wouldn't overflow the buffer
 *     (otherwise returns failure)
 * 2a) If GC is disabled, we're done, we just return our memory slice
 * 2b) Otherwise we set the markKey and parent of the allocation, and place it in our child collection
 * 3)  We then increment allocSize by the size of the allocation. Note that this is not ordered wrt other
 *     modifications to allocSize (unlike our update to allocOffset). We use it only to provide ordering
 *     guarantees to GC Candidate Selection that children() is fully populated when allocSize == allocOffset.
 *     It also provides the primitive memory ordering (volatile semantics) for this and setting of the markKey/parent.
 *
 * free:
 * 1) We simply atomically swap the parent to null, or fail if already nulled by another;
 * 2) We then bump freeSize by the size of the allocation in the parent we atomically cleared; this is used only
 *    for GC candidate selection to make ordering the regions cheap
 *
 * Region is DISCARDING: We may only free().
 * This state indicates we are either GCing the region, or the owning allocator is discarded.
 * The region will remain DISCARDING until all references to it have been cleared.
 *
 * Region is DISCARDED: The region should no longer be referenced, so no more operations are now permitted.
 *
 * Region is RECYCLED: The region has (or is in the process of) recycling its resources back into the owning pool
 *
 */
final class NativeRegion
{

    static final int MAX_SKIP_COUNT = 128; // if we fail this many allocations in a region, we retire it
    static final int MAX_SKIP_SIZE = 1024; // if we fail an allocation in a region with less than this remaining, we retire it

    /**
     * Point any retired memory to a location that is protected, so that the VM SIGSEGVs if we attempt to access it;
     * this prevents any risk of silent bugs. for now we leave it on by default, at least until after the beta phase.
     */

    private static final IAllocator memoryAllocator = DatabaseDescriptor.getoffHeapMemoryAllocator();

    // used to co-ordinate between GC and setDiscarded()
    static enum State { LIVE, DISCARDING, DISCARDED, RECYCLED }

    static class MarkKey {}

    /** Pointer to raw memory location of region, and size of memory allocated for us there */
    final long peer;
    final int size;

    final MarkKey markKey;
    volatile NativeAllocation children;

    private volatile State state = State.LIVE;

    /** Offset for the next allocation */
    private volatile int allocOffset;

    /**
     * Total size of allocations satisfied from this buffer.
     */
    // TODO: not necessary if we always wait for writers to finish before enumerating children to copy
    private volatile int allocSize;

    // TODO: can encode in top bits of allocSize to save space
    /** Total number of allocations we have tried but failed to satisfy with this buffer */
    private volatile int skipCount;

    // TODO: we can stop maintaining freeSize, and lazily refresh it whenever we come to GC; just maintain a boolean flag
    // indicating if it needs updating. then we could change free to a volatile store only (and maybe not even volatile)
    /** Total size of space we have previously allocated and since freed */
    private volatile int freeSize;


    NativeRegion(int size, boolean isGcEnabled)
    {
        this(memoryAllocator.allocate(size), size, isGcEnabled);
    }

    private NativeRegion(NativeRegion recycle)
    {
        // we never recycle regions that are limited to only one child, as these are always oversized regions
        this(recycle.peer, recycle.size, recycle.freeSize >= 0);
    }

    private NativeRegion(long peer, int size, boolean isGcEnabled)
    {
        this.peer = peer;
        this.size = size;
        this.markKey = new MarkKey();
        this.freeSize = isGcEnabled ? 0 : -1;
    }

    /**
     * @param size the amount of memory to allocate - MUST INCLUDE 4byte SIZE HEADER
     * @param allocation the allocation object to initialise to the allocated location
     * @return a ByteBuffer of size if there was sufficient room, or null otherwise
     */
    boolean allocate(int size, NativeAllocation allocation)
    {
        while (true)
        {
            int oldOffset = allocOffset;
            if (oldOffset + size > this.size)
            {
                // count the failure so we can retire this region if we fail too many times
                skipCountUpdater.incrementAndGet(this);
                return false;
            }

            // Try to atomically claim this region
            if (allocOffsetUpdater.compareAndSet(this, oldOffset, oldOffset + size))
            {
                allocation.setPeer(peer + oldOffset);
                allocation.setRealSize(size);
                if (freeSize < 0)
                {
                    allocation.parent = markKey;
                    return true;
                }
                allocation.parent = this;
                while (true)
                {
                    NativeAllocation next = children;
                    allocation.next = next;
                    if (childrenUpdater.compareAndSet(this, next, allocation))
                        break;
                }
                allocSizeUpdater.addAndGet(this, size);
                return true;
            }
        }
    }

    int reallocate(int size)
    {
        int oldOffset = allocOffset;
        if (oldOffset + size > this.size)
        {
            // count the failure so we can retire this region if we fail too many times
            skipCount++;
            return -1;
        }

        int newOffset = oldOffset + size;
        allocOffsetUpdater.lazySet(this, newOffset);
        allocSizeUpdater.lazySet(this, newOffset);
        return oldOffset;
    }

    void unorderedAdopt(NativeAllocation allocation)
    {
        allocation.next = children;
        childrenUpdater.lazySet(this, allocation);
    }

    /** @return false if we no longer consider the region capable of allocating space */
    boolean mayContinueAllocating()
    {
        return size - allocOffset >= MAX_SKIP_SIZE && skipCount <= MAX_SKIP_COUNT;
    }

    /** move our allocOffset and allocSize to the end, and mark any unused space as free */
    void finishAllocating()
    {
        if (allocOffset == size)
            return;
        int delta = size - allocOffsetUpdater.getAndSet(this, size);
        if (delta != 0)
        {
            if (freeSize >= 0)
                freeSizeUpdater.addAndGet(this, delta);
            allocSizeUpdater.addAndGet(this, delta);
        }
    }

    /** @return true if the buffer is still allocating space */
    boolean isAllocating()
    {
        return allocSize != size;
    }

    boolean isLive()
    {
        return state == State.LIVE;
    }

    boolean isDiscarded()
    {
        return state.compareTo(State.DISCARDED) >= 0;
    }

    public int size()
    {
        return size;
    }

    public float freeRatio()
    {
        if (freeSize == size())
            return 1f;
        return freeSize / (float) size();
    }

    public boolean isAllFree()
    {
        return freeSize == size();
    }

    /**
     * Account for the removal of the provided buffer, without checking if the buffer is associated with us.
     * This is used by GC for dealing with races against another free() of the buffer it was copying here.
     */
    void free(int size)
    {
        freeSizeUpdater.addAndGet(this, size);
    }

    /** CAS the state, returning success */
    boolean transition(State exp, State upd)
    {
        return state == exp && stateUpdater.compareAndSet(this, exp, upd);
    }

    StateSnapper snapper()
    {
        return new StateSnapper(this);
    }

    void releaseNativeMemory()
    {
        memoryAllocator.free(peer);
    }

    /**
     * only to be called once the region has been discarded - recycles reusable resources and creates a new region over
     * the same buffer to be returned to the pool for reuse
     */
    NativeRegion recycle()
    {
        assert state == State.DISCARDED;
        state = State.RECYCLED;
        // PARANOID GC: if the mark key is the same as ours, the buffer should no longer be referenced,
        // so set its address to 0
        if (NativePool.PARANOID_RECYCLE)
        {
            for (NativeAllocation child : NativeAllocation.iterate(children))
                if (child.parent == markKey)
                    child.setPeer(0);
            children = null;
        }
        return new NativeRegion(this);
    }

    private static final AtomicIntegerFieldUpdater<NativeRegion> allocOffsetUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeRegion.class, "allocOffset");
    private static final AtomicIntegerFieldUpdater<NativeRegion> allocSizeUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeRegion.class, "allocSize");
    private static final AtomicIntegerFieldUpdater<NativeRegion> skipCountUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeRegion.class, "skipCount");
    private static final AtomicIntegerFieldUpdater<NativeRegion> freeSizeUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeRegion.class, "freeSize");
    private static final AtomicReferenceFieldUpdater<NativeRegion, State> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(NativeRegion.class, State.class, "state");
    private static final AtomicReferenceFieldUpdater<NativeRegion, NativeAllocation> childrenUpdater = AtomicReferenceFieldUpdater.newUpdater(NativeRegion.class, NativeAllocation.class, "children");

    /**
     * Wraps an OffHeapRegion, snapping the state of the new local allocation region, so that if we fail to allocate
     * complete room to collect a candidate region, we can reset the new region before placing at the end of the allocator
     * queue we are collecting
     */
    static final class StateSnapper
    {
        final NativeRegion region;
        int allocOffset;
        int allocSize;
        int skipCount;
        int freeSize;

        private StateSnapper(NativeRegion region)
        {
            this.region = region;
        }

        void snap()
        {
            allocOffset = region.allocOffset;
            allocSize = region.allocSize;
            freeSize = region.freeSize;
            skipCount = region.skipCount;
        }

        void reset()
        {
            region.allocOffset = allocOffset;
            region.allocSize = allocSize;
            region.freeSize = freeSize;
            region.skipCount = skipCount;
        }
    }

}