package org.apache.cassandra.utils.memory;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.collect.Multimap;

import org.apache.cassandra.utils.concurrent.UnsafeAtomicReferenceArrayUpdater;
import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * This class can be seen to essentially manage a collection of GC roots.
 * A GC root (ThreadLocalCollection) is maintained per thread that registers a Referrer with us.
 */
class Referrers
{

    /**
     * Very simple collection designed to be very fast to add to; basically a circular list that is only modified
     * by one thread, which scans the list from the position it last successfully inserted into for the first unused
     * spot. If there are none, it doubles the length of the list.
     *
     * It is designed to be read by another thread, so enforces memory ordering on its modifications, but never performs
     * a full volatile write. It is expected to be guarded by other volatile operations (in particular, at least one
     * OpOrder.Group.close())
     */
    private static final class ThreadLocalCollection
    {
        private volatile Referrer[] referrers = new Referrer[4];
        int index = 0;

        void lazyAdd(Referrer referrer)
        {
            Referrer[] referrers = this.referrers;
            int len = referrers.length;
            for (int i = 0 ; i < len ; i++)
            {
                int j = (index + i) & (len - 1);
                Referrer prev = referrers[j];
                if (prev == null || prev.isDone())
                {
                    referrerArrayUpdater.putOrdered(referrers, j, referrer);
                    index = j;
                    return;
                }
            }

            referrerUpdater.lazySet(this, referrers = Arrays.copyOf(referrers, len * 2));
            referrers[len] = referrer;
            index = len;
        }

        static final UnsafeAtomicReferenceArrayUpdater<Referrer> referrerArrayUpdater = new UnsafeAtomicReferenceArrayUpdater<>(Referrer[].class);
        static final AtomicReferenceFieldUpdater<ThreadLocalCollection, Referrer[]> referrerUpdater = AtomicReferenceFieldUpdater.newUpdater(ThreadLocalCollection.class, Referrer[].class, "referrers");
    }

    private final NonBlockingQueue<SoftReference<ThreadLocalCollection>> allReferrers = new NonBlockingQueue<>();

    private final ThreadLocal<ThreadLocalCollection> perThreadReferrers = new ThreadLocal<ThreadLocalCollection>()
    {
        protected ThreadLocalCollection initialValue()
        {
            ThreadLocalCollection referrers = new ThreadLocalCollection();
            allReferrers.append(new SoftReference<>(referrers));
            return referrers;
        }
    };

    // add the referrer to the thread's GC root, but do not provide memory ordering guarantees;
    // these should be enforced by the caller (usually piggybacked on OpOrder.Group.close())
    public void lazyAdd(Referrer referrer)
    {
        perThreadReferrers.get().lazyAdd(referrer);
    }

    // walk the GC root and mark any of the mapped regions
    public void mark(OpOrder.Barrier readBarrier, Multimap<NativeRegion.MarkKey, NativeDelayedRecycle> collecting)
    {
        Iterator<SoftReference<ThreadLocalCollection>> iter = allReferrers.iterator();
        while (iter.hasNext())
        {
            SoftReference<ThreadLocalCollection> ref = iter.next();
            ThreadLocalCollection collection = ref.get();
            if (collection == null)
            {
                iter.remove();
                continue;
            }

            for (Referrer referrer : collection.referrers)
            {
                if (referrer != null && !referrer.isDone())
                    referrer.mark(readBarrier, collecting);
            }
        }
    }

}
