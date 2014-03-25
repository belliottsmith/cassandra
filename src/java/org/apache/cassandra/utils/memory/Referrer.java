package org.apache.cassandra.utils.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;

import org.apache.cassandra.db.data.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.data.DataAllocator;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.utils.memory.NativeRegion.MarkKey;

/**
 * Represents a set of unmanaged (by JVM) allocations that are reachable from somewhere in the application,
 * and so must not be collected by our management process.
 *
 * This class is intended to be modified by exactly one thread in all but the clearly marked cases.
 * It is expected to be guarded by an OpOrder, i.e. any readers will wait for any writers to finish before
 * reading the contents, and we piggyback off the OpOrder synchronisation so that we can perform single-threaded
 * modifications where possible.
 *
 * The class is optimised for the case where we have only one (or few) referents, against just one allocator group
 */
public class Referrer extends RefAction
{
    // TODO: disambiguate those marked regions that require only ordering guarantees for access and those that are actually referenced,
    // so that we may be able to clear some early

    private NativePool.Group group;         // the allocator group our first reference is from
    private OpOrder.Group earliestReadOp; // the first readOp we were used in. generally this should be the same readOp all references were made in.

    // typical usage only needs one referent, but in some cases we may reference more. in those cases
    // we keep anything from the same AllocatorGroup in this Referrer, spilling over into overflow.
    // if we touch more than one group, we also manage a map of AllocatorGroup->Referrer inside of overflow
    private Object referent;
    private Overflow overflow;

    // a simple linked-list we maintain of any regions we have marked and are now contributing towards the ref-count of;
    // it is maintained as a LIFO stack, as this is cheaper and easier to implement.
    // note that this is the only variable in this class that is intended to be modified by multiple threads concurrently
    // once this is (atomically) set to null, this referrer is finished, so markers can coordinate aborting with setDone()
    private volatile MarkChain marked = NONE;

    private static final MarkChain NONE = new MarkChain(null);
    private static final AtomicReferenceFieldUpdater<Referrer, MarkChain> markedUpdater = AtomicReferenceFieldUpdater.newUpdater(Referrer.class, MarkChain.class, "marked");

    private static final class Overflow
    {
        List<Object> referents;
        Map<NativePool.Group, Referrer> referrers;
    }

    private static final class MarkChain
    {
        final NativeDelayedRecycle marked;
        volatile MarkChain next;
        private MarkChain(NativeDelayedRecycle marked)
        {
            this.marked = marked;
        }
    }

    Referrer() { }

    // should always be called by the same thread as created the Referrer
    public void complete(NativePool.Group group, OpOrder.Group readOp, Object referent)
    {
        if (this.referent == null)
        {
            // we're empty, so set our initial state
            group.referrers.lazyAdd(this);
            this.referent = referent;
            this.earliestReadOp = readOp;
            this.group = group;
        }
        else if (this.group == group)
        {
            // we're not empty, but we're addign a record for the same group, so
            if (overflow == null)
                overflow = new Overflow();
            if (overflow.referents == null)
                overflow.referents = new ArrayList<>();
            overflow.referents.add(referent);
        }
        else
        {
            if (overflow == null)
                overflow = new Overflow();
            if (overflow.referrers == null)
                overflow.referrers = new HashMap<>();
            Referrer other = overflow.referrers.get(group);
            if (other == null)
                overflow.referrers.put(group, other = new Referrer());
            other.complete(group, readOp, referent);
        }
    }

    // mark the any of the mapped regions provided that we reference
    void mark(OpOrder.Barrier markReadBarrier, Multimap<MarkKey, NativeDelayedRecycle> marking)
    {
        Object referent = this.referent;
        OpOrder.Group refOrdered = this.earliestReadOp;

        if (referent == null || refOrdered == null)
            return;

        // if the barrier doesn't accept us, we're in the future, so we're already safe as we can't have seen
        // any of the pointers we want to invalidate
        if (!markReadBarrier.isAfter(refOrdered))
            return;

        Marker marker = new Marker(marking);
        mark(referent, marker);

        Overflow overflow = this.overflow;
        if (overflow != null && overflow.referents != null)
            for (Object o : overflow.referents)
                if (!mark(o, marker))
                    return;

        // we do not need to (and should not) mark any members of extraReferrers, as each _group_ is marked
        // independently, and they will have been added to their group's referrers list
    }

    // mark the referent; we hard-code traversal strategies for each type of object we will ever reference
    private boolean mark(Object referent, Marker marker)
    {
        if (referent instanceof ColumnFamily)
        {
            ColumnFamily cf = (ColumnFamily) referent;
            for (Cell cell : cf)
                if (cell instanceof NativeAllocation && Boolean.FALSE == marker.apply((NativeAllocation) cell))
                    return false;
            return true;
        }
        else if (referent instanceof Row)
        {
            Row row = (Row) referent;
            if (row.key instanceof NativeAllocation)
                if (marker.apply((NativeAllocation) row.key) == Boolean.FALSE)
                    return false;
            return mark(row.cf, marker);
        }
        else if (referent instanceof Collection)
        {
            for (Object o : ((Collection) referent))
                if (!mark(o, marker))
                    return false;
            return true;
        }
        else if (referent instanceof NativeAllocation)
            return Boolean.TRUE == marker.apply((NativeAllocation) referent);
        else
            throw new AssertionError();
    }

    // performs the actual marking of each BB
    private final class Marker implements Function<NativeAllocation, Boolean>
    {
        final Set<NativeDelayedRecycle> visited = new HashSet<>();
        final Multimap<MarkKey, NativeDelayedRecycle> marking;

        private Marker(Multimap<MarkKey, NativeDelayedRecycle> marking)
        {
            this.marking = marking;
        }

        public Boolean apply(NativeAllocation allocation)
        {
            MarkKey markKey;
            Object parent = allocation.parent;
            markKey = parent instanceof MarkKey ? (MarkKey) parent : ((NativeRegion)parent).markKey;

            if (marking.containsKey(markKey))
            {
                for (NativeDelayedRecycle mark : marking.get(markKey))
                {
                    // we only want to mark each region once, so we check if we've already visited it
                    if (visited.add(mark))
                    {
                        // we speculatively mark, then attempt to add to the set of marked regions; if we fail
                        // we immediately unmark and return false, as the Referrer is done, so no point marking anymore
                        mark.mark();
                        MarkChain newHead = new MarkChain(mark);
                        while (true)
                        {
                            MarkChain currentHead = marked;
                            if (currentHead == null)
                            {
                                mark.unmark();
                                return Boolean.FALSE;
                            }
                            newHead.next = currentHead;
                            if (markedUpdater.compareAndSet(Referrer.this, currentHead, newHead))
                                break;
                        }
                    }
                }
            }
            return Boolean.TRUE;
        }
    }

    public void reset()
    {
        // we still CAS NONE -> NONE to get memory ordering on changes to the other values.
        // we may also end up referencing regions we aren't really referencing because of races to mark
        // and update the MarkChain, but this is acceptable
        clear(NONE);
    }

    public void close()
    {
        clear(null);
    }

    private void clear(MarkChain newChain)
    {
        // set any proxied referrers in overflow to done
        if (overflow != null && overflow.referrers != null)
            for (Referrer referrer : overflow.referrers.values())
                referrer.close();

        // clear out references
        group = null;
        referent = null;
        earliestReadOp = null;
        overflow = null;

        // finally atomically clear our collection of marked regions, and unmark them all
        // we do this last to provide memory ordering to the prior clearing of references
        while (true)
        {
            MarkChain current = this.marked;
            if (markedUpdater.compareAndSet(this, current, newChain))
            {
                while (current != NONE)
                {
                    current.marked.unmark();
                    current = current.next;
                }
                break;
            }
        }
    }

    public boolean isDone()
    {
        return marked == null;
    }

    public RefAction subAction()
    {
        return RefAction.unsafe();
    }

    public boolean copyOnHeap(DataAllocator.DataPool pool)
    {
        return false;
    }
}

