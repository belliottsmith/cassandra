package org.apache.cassandra.utils.memory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A region that is waiting to be recycled, but is maybe currently referenced by some operation(s);
 * once referrers reaches zero, it will be returned to the pool for reuse
 */
final class NativeDelayedRecycle
{
    private final NativePool pool;
    final NativeRegion region;
    private volatile int referrers = 1;

    private static final AtomicIntegerFieldUpdater<NativeDelayedRecycle> referrersUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeDelayedRecycle.class, "referrers");

    NativeDelayedRecycle(NativePool pool, NativeRegion region)
    {
        this.pool = pool;
        this.region = region;
    }

    void unmark()
    {
        int result = referrersUpdater.decrementAndGet(this);
        if (0 == result)
        {
            if (!region.transition(NativeRegion.State.DISCARDING, NativeRegion.State.DISCARDED))
                throw new AssertionError();
            pool.recycle(region, true);
            pool.offHeap.adjustAcquired(-region.size(), false);
            pool.offHeap.adjustReclaiming(-region.size());
        }
        assert result >= 0;
    }

    // during the mark phase it is impossible for referrers to hit 0, so we can safely increment knowing that
    // we will not be remarking something already unmarked
    void mark()
    {
        referrersUpdater.incrementAndGet(this);
    }
}
