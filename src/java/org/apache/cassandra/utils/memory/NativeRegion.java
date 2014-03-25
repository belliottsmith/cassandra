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

    private volatile State state = State.LIVE;

    private volatile int allocOffset;

    // TODO: can encode in top bits of allocSize to save space
    /** Total number of allocations we have tried but failed to satisfy with this buffer */
    private volatile int skipCount;


    NativeRegion(int size)
    {
        this(memoryAllocator.allocate(size), size);
    }

    private NativeRegion(NativeRegion recycle)
    {
        // we never recycle regions that are limited to only one child, as these are always oversized regions
        this(recycle.peer, recycle.size);
    }

    private NativeRegion(long peer, int size)
    {
        this.peer = peer;
        this.size = size;
        this.markKey = new MarkKey();
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
                allocation.parent = this;
                return true;
            }
        }
    }

    /** @return false if we no longer consider the region capable of allocating space */
    boolean mayContinueAllocating()
    {
        return size - allocOffset >= MAX_SKIP_SIZE && skipCount <= MAX_SKIP_COUNT;
    }

    /** move our allocOffset and allocSize to the end, and mark any unused space as free */
    void finishAllocating()
    {
        allocOffsetUpdater.getAndSet(this, size);
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

    /** CAS the state, returning success */
    boolean transition(State exp, State upd)
    {
        return state == exp && stateUpdater.compareAndSet(this, exp, upd);
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
        return new NativeRegion(this);
    }

    private static final AtomicIntegerFieldUpdater<NativeRegion> allocOffsetUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeRegion.class, "allocOffset");
    private static final AtomicIntegerFieldUpdater<NativeRegion> skipCountUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeRegion.class, "skipCount");
    private static final AtomicReferenceFieldUpdater<NativeRegion, State> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(NativeRegion.class, State.class, "state");

}