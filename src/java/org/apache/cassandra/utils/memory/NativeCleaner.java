package org.apache.cassandra.utils.memory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.NonBlockingQueueView;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OptBlockingQueue;
import org.apache.cassandra.utils.concurrent.SafeRemoveIterator;

import org.slf4j.*;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import static org.apache.cassandra.utils.memory.NativeRegion.MarkKey;
import static org.apache.cassandra.utils.memory.NativeRegion.StateSnapper;

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
 * 4) When a GC (or allocator discard) occurs, any extant {@link Referrer} are walked that were created during
 *    operations started prior to the collect phase, and any regions that are reachable by any of these 'refs' are
 *    switched to a refcount phase. When each ref completes it decrements the count of any such regions it reached.
 *    Once this count hits 0, the region is finally eligible for reuse.
 *
 * Garbage collection is composed of the following phases:
 *
 * Candidate selection:
 *    Run over either the pool or one allocator, it partially sorts the regions in order of
 *    memory that can be reclaimed. Only regions that have already been fully allocated are considered as candidates.
 * Reallocation:
 *    The candidates are walked from best-to-worst, and a GC thread is started for each allocator we encounter.
 *    For each candidate, if there is space either in the allocator's GC state, or in the pool, to compact its still live
 *    (not freed) allocations, then a new (re)allocation is made for them and they are passed to the collector thread to process.
 * Compaction:
 *    The GC thread for each allocator waits until any prior write operations are completed before proceeding to copy
 *    any retained allocations to the new locations assigned during the previous phase. The original objects are updated
 *    to point to the new locations, and their mark key (used in next phase) is updated to point to the new region.
 * Marking:
 *    Once all compaction is done, we wait for any prior read operations to complete, by which point we reach stage (3)
 *    of the lifecycle of any of the allocations not compacted. Now we walk any 'refs' that may reference allocations
 *    we're collecting (i.e. those that are registered with the {@link NativePool.Group} of the allocator we're GCing)
 *    and register any of the regions we're recycling with the Referrer, whilst incrementing the ref-count on the region.
 * Delayed? Recycle:
 *    Any regions that were not ref-count-incremented during the mark phase are immediately recycled, and any that were
 *    enter a limbo period, until the reference is cleared.
 *
 * Garbage collection is triggered in one of three ways:
 *
 * 1) One of the pool clean thresholds is breached, so the cleaner thread is triggered; in this instance the private
 *    gc(Pool) method is used, which scans all allocators and groups for the best candidates to collect
 * 2) An allocator completely fails to allocate new memory. In this case before blocking we attempt a GC. This is
 *    potentially an expensive waste of time, but since there is no more memory to allocate for writes, it's probably
 *    time that would otherwise be unused, and may result in some freed memory.
 * 3) An allocator is marked discardING. This is currently triggered by a memtable flush. In this instance we
 *    force a GC if one isn't already running, and perform the reallocation phase in the discarding thread.
 *
 * Some notes:
 * We assume that writers never create references that persist outside of the life of their OpOrder transaction
 *
 */

class NativeCleaner extends PoolCleanerThread<NativePool>
{
    private static final Logger logger = LoggerFactory.getLogger(NativeCleaner.class);

    private static final ExecutorService collectors = Executors.newCachedThreadPool(new NamedThreadFactory("OffHeapGC"));
    private static final ConcurrentHashMap<NativeAllocator, GarbageCollector> running = new ConcurrentHashMap<>();
    private static final OpOrder gcOperations = new OpOrder();

    static final boolean PARANOID_RECYCLE = !Objects.equals("off", System.getProperty("cassandra.paranoidgc"));

    public NativeCleaner(NativePool pool, Runnable clean)
    {
        super(pool, clean);
    }

    void clean()
    {
        if (pool.isGcEnabled())
            gc(pool, false);
        if (!pool.isGcEnabled() || needsCleaning())
            super.clean();
    }

    // force an out-of-band gc; only collect anything we can collect for free, since we call this when under memory pressure
    void forceClean()
    {
        gc(pool, true);
    }

    // wait for any running GCs to finish
    public OpOrder.Barrier getGCBarrier()
    {
        return gcOperations.newBarrier();
    }

    /**
     * Perform a full GC cycle on the provided pool
     *
     * @param pool the pool to GC
     * @param freeToCollectOnly if true, we only try to collect those regions that can be collected without compaction
     * (i.e. are completely free)
     */
    private static void gc(NativePool pool, boolean freeToCollectOnly)
    {
        // we set success to false as soon as we fail to allocate room for migrating regions, and from then only collect
        boolean success = true;
        List<Candidate> candidates = Candidate.get(pool, freeToCollectOnly);
        if (logger.isTraceEnabled())
            logger.trace("Starting Global GC with {} candidates", candidates.size());
        Map<NativeAllocator, GarbageCollector> collectors = new HashMap<>();
        Set<NativeAllocator> couldNotStart = new HashSet<>();
        /**
         * loop through all candidates in descending order of amount we expect to reclaim;
         * as soon as we fail to allocate compaction space for an allocator we stop starting
         * new collectors and simply attempt to utilise any remaining space available to us
         * in the existing collectors, closing them as they fail to accept more work.
         */
        for (Candidate candidate : candidates)
        {
            GarbageCollector gc = collectors.get(candidate.allocator);
            if (gc == null)
            {
                // only allocate a new collector if all previous selections have succeeded
                // (otherwise we're probably still out of room)
                if (!success)
                    continue;
                // don't want to start a collection at the bottom end of what we can collect;
                // if we fail to start a new collection first time around, leave it until next time
                if (couldNotStart.contains(candidate.allocator))
                    continue;
                gc = GarbageCollector.start(candidate.allocator, true);
                if (gc == null)
                {
                    couldNotStart.add(candidate.allocator);
                    continue;
                }
                collectors.put(candidate.allocator, gc);
            }
            if (!gc.reallocate(candidate.region))
            {
                success = false;
                gc.finishReallocating();
                collectors.remove(gc.allocator);
                if (collectors.isEmpty())
                    break;
            }
        }
        for (GarbageCollector gc : collectors.values())
            gc.finishReallocating();
    }

    /** Forces a GC of the allocator. Does not affect or check transition state, so must be protected outside of call. */
    static void unsafeGc(NativeAllocator allocator)
    {
        GarbageCollector collector = GarbageCollector.start(allocator, false);
        for (Candidate candidate : Candidate.get(allocator, false))
            if (!collector.reallocate(candidate.region))
                break;
        collector.finishReallocating();
    }

    // CANDIDATE SELECTION PHASE:

    // see header description
    private static final class Candidate
    {
        final NativeRegion region;
        final NativeAllocator allocator;
        private Candidate(NativeRegion region, NativeAllocator allocator)
        {
            this.region = region;
            this.allocator = allocator;
        }

        // when selecting candidates, we bucket them to vaguely similar ratios of expected collection result,
        // and just select from descending sized buckets in the order we originally visited. this is almost as good
        // as sorting, but much cheaper. here we also define our lowest free ratio necessary to trigger a collection
        private static final float[] DEFAULT_BUCKET_RATIOS = new float[] { 0.1f, 0.25f, 0.5f, 0.8f, 0.99f };
        private static final float[] ONLY_FREE_RATIOS = new float[] { 1.0f };

        private static List<Candidate> get(NativePool pool, boolean freeToCollectOnly)
        {
            float[] ratios = freeToCollectOnly ? ONLY_FREE_RATIOS : DEFAULT_BUCKET_RATIOS;
            List<Candidate>[] buckets = buckets(ratios);
            Iterator<SoftReference<NativePool.Group>> iter = pool.groups.iterator();
            while (iter.hasNext())
            {
                NativePool.Group group = iter.next().get();
                if (group == null)
                {
                    iter.remove();
                    continue;
                }
                for (NativeAllocator allocator : group.live.keySet())
                    addCandidates(allocator, buckets, ratios);
            }
            return flatten(buckets);
        }

        private static List<Candidate> get(NativeAllocator allocator, boolean freeToCollectOnly)
        {
            float[] ratios = freeToCollectOnly ? ONLY_FREE_RATIOS : DEFAULT_BUCKET_RATIOS;
            return flatten(addCandidates(allocator, buckets(ratios), ratios));
        }

        private static List<Candidate>[] buckets(float[] ratios)
        {
            final List<Candidate>[] buckets = new List[ratios.length];
            for (int i = 0 ; i < buckets.length ; i++)
                buckets[i] = new ArrayList<>();
            return buckets;
        }

        private static List<Candidate> flatten(List<Candidate>[] buckets)
        {
            final List<Candidate> flat = new ArrayList<>();
            for (int i = buckets.length - 1 ; i >= 0 ; i--)
                flat.addAll(buckets[i]);
            return flat;
        }

        private static List<Candidate>[] addCandidates(NativeAllocator allocator, List<Candidate>[] buckets, float[] ratios)
        {
            // TODO: don't consider a region a candidate if it contains allocations copied from a prior region
            // that have not been released yet (so we don't slowly spread a stuck allocation)
            Iterator<NativeRegion> iter = allocator.allocated.iterator();
            while (iter.hasNext())
            {
                NativeRegion region = iter.next();
                if (region.isAllocating())
                    continue;
                if (region.isLive())
                {
                    float freeRatio = region.freeRatio();
                    int pos = Arrays.binarySearch(ratios, freeRatio);
                    if (pos < 0)
                        pos = -1 - pos;
                    pos -= 1;
                    if (pos >= 0)
                        buckets[pos].add(new Candidate(region, allocator));
                }
            }
            return buckets;
        }

    }

    // REALLOCATION AND COMPACTION PHASES:

    /**
     * A collection of retained allocations representing all live allocations from a region we are reclaiming
     */
    private static final class CompactRegion
    {
        final NativeRegion regionIn;
        final List<Move> moves = new ArrayList<>();
        private CompactRegion(NativeRegion regionIn)
        {
            this.regionIn = regionIn;
        }

        /**
         * A single live allocation retention: from a region we are reclaiming, into a preallocated position in a target region
         */
        private static final class Move
        {
            final NativeAllocation in;
            final int out;
            final NativeRegion regionOut;
            private Move(NativeAllocation in, int out, NativeRegion regionOut)
            {
                this.in = in;
                this.out = out;
                this.regionOut = regionOut;
            }
        }
    }

    /**
     * A GC runnable operating over a single allocator; one is spawned per allocator with regions
     * targeted for collection/compaction. The mark() operation is performed (usually) by the cleaner gc() call,
     * which decides what memory to keep and allocates new space for it before placing it on the compact queue
     */
    private static final class GarbageCollector implements Runnable
    {
        final NativeAllocator allocator;

        // mark state
        final NonBlockingQueue<StateSnapper> allocatingFrom = new NonBlockingQueue<>(); // regions we're compacting to with space free
        final NonBlockingQueueView<StateSnapper> allocatedFrom = allocatingFrom.view(); // all regions we've compacted to

        final OptBlockingQueue<CompactRegion> compact = new OptBlockingQueue<>();       // compaction work
        final NonBlockingQueue<NativeRegion> collect = new NonBlockingQueue<>();       // all regions we're collecting (superset of those in compact.regionIn)
        private long reclaimingSize = 0;
        private int reclaimingCount = 0;

        GarbageCollector(NativeAllocator allocator)
        {
            this.allocator = allocator;
        }

        static GarbageCollector start(NativeAllocator allocator, boolean safe)
        {
            if (safe && !allocator.transition(PoolAllocator.Gc.REALLOCATING))
                return null;
            assert !running.containsKey(allocator);
            if (logger.isTraceEnabled())
                logger.trace("Starting GC of allocator{}", System.identityHashCode(allocator));
            GarbageCollector collector = new GarbageCollector(allocator);
            running.put(allocator, collector);
            collectors.execute(collector);
            return collector;
        }

        // REALLOCATION PHASE

        // TODO: move this phase into the GC threads so that they can be run after waiting for any write barrier, so we can retire allocSize from OffHeapRegion
        boolean reallocate(NativeRegion region)
        {
            assert !region.isAllocating();

            // move the region to GC state - if we fail this could be because we're still allocating from the region
            // or the allocator is being discarded, or because we previously GC'd it and failed to remove, so just return
            if (!region.transition(NativeRegion.State.LIVE, NativeRegion.State.DISCARDING))
                return true;

            if (region.isAllFree())
            {
                // don't bother marking children if we can collect everything
                adjustReclaiming(region);
                collect.append(region);
                return true;
            }

            // we speculatively allocate in the available regions, so we snap the current state in case we fail
            for (StateSnapper newRegion : allocatingFrom)
                newRegion.snap();

            AllocList reallocList = new AllocList();
            AllocList freeList = new AllocList();
            NativeAllocation cursor = region.children;

            boolean success = true;
            final CompactRegion compact = new CompactRegion(region);
            while (cursor != null)
            {
                // skip any that have been freed/moved to another region
                if (cursor.parent != region)
                {
                    assert cursor.parent == region.markKey;
                    freeList.append(cursor);
                    cursor = cursor.next;
                    continue;
                }

                int replacement = -1;
                NativeRegion regionOfReplacement = null;
                // try and allocate in our local pool
                for (StateSnapper snapper : allocatingFrom)
                {
                    replacement = snapper.region.reallocate(cursor.getRealSize());
                    if (replacement >= 0)
                    {
                        regionOfReplacement = snapper.region;
                        break;
                    }
                }
                if (replacement < 0)
                {
                    // have to allocate a new region if possible
                    regionOfReplacement = allocator.group.pool.tryAllocateRegion(allocator.group.pool.regionSize);
                    if (regionOfReplacement == null)
                    {
                        success = false;
                        break;
                    }
                    // add the new region to our local pool, and snap it
                    StateSnapper snap = regionOfReplacement.snapper();
                    snap.snap();
                    allocator.offHeap().acquired(regionOfReplacement.size());
                    allocatingFrom.append(snap);

                    replacement = regionOfReplacement.reallocate(cursor.getRealSize());
                    assert replacement >= 0;
                }
                compact.moves.add(new CompactRegion.Move(cursor, replacement, regionOfReplacement));

                reallocList.append(cursor);
                cursor = cursor.next;
            }

            if (!success)
            {
                // we've dismantled the child list, so we need to put it back together again; we do it in this method
                // so that the objects are scalar replaceable (stack allocation)
                reallocList.append(freeList);
                reallocList.append(cursor);
                abort(region, reallocList.head);
                return false;
            }

            region.children = PARANOID_RECYCLE ? freeList.head : null;
            commit(region, compact);
            return true;
        }

        void abort(NativeRegion region, NativeAllocation children)
        {
            // reset the regions so we can reuse the memory
            for (StateSnapper newRegion : allocatingFrom)
                newRegion.reset();

            region.children = children;

            if (!region.transition(NativeRegion.State.DISCARDING, NativeRegion.State.LIVE))
                throw new AssertionError();
            if (PoolAllocator.LifeCycle.DISCARDED == allocator.state.lifeCycle)
            {
                // if we have finished our GC after the allocator has been discarded, we need to collect
                // the region anyway, so reclaim it for ourselves
                if (region.transition(NativeRegion.State.LIVE, NativeRegion.State.DISCARDING))
                {
                    adjustReclaiming(region);
                    collect.append(region);
                }
            }
        }

        private static class AllocList
        {
            NativeAllocation head;
            NativeAllocation tail;
            void append(NativeAllocation alloc)
            {
                if (head == null)
                    head = tail = alloc;
                else
                    tail.next = tail = alloc;
            }
            void append(AllocList list)
            {
                if (head == null)
                {
                    head = list.head;
                    tail = list.tail;
                }
                else if (list.tail != null)
                {
                    tail.next = tail = list.tail;
                }
            }
        }

        void commit(NativeRegion region, CompactRegion compact)
        {
            // move past regions we don't expect to be able to allocate from again
            for (StateSnapper newRegion : allocatingFrom)
                if (!newRegion.region.mayContinueAllocating())
                    allocatingFrom.advanceIfHead(newRegion);

            adjustReclaiming(region);
            collect.append(region);
            this.compact.append(compact);

            if (logger.isTraceEnabled())
            {
                logger.trace("Marked {} allocs, with {} bytes for retention from OffHeapAllocator@{}@{}", compact.moves.size(),
                             reclaimingSize, System.identityHashCode(allocator), allocator.group.name);
            }
        }

        void adjustReclaiming(NativeRegion region)
        {
            // transfer ownership of the memory to us
            if (!allocator.offHeap().transferAcquired(region.size()))
                throw new AssertionError();
            reclaimingSize += region.size();
            reclaimingCount += 1;
            allocator.offHeap().transferReclaiming(region.size());
        }

        void finishReallocating()
        {
            // and signal the GC to finish
            allocator.transition(PoolAllocator.Gc.COLLECTING);
            // signal the collector thread there's no more work
            compact.append(null);
        }


        // COMPACTION PHASE

        /**
         * This method does the bulk of the actual collection. Once passed the allocations we're compacting
         * and their target locations, it deals with copying the data and synchronising the release of the
         * resources that we've freed up.
         */
        @Override
        public void run()
        {
            {   // we're only copying buffers that were *allocated* prior to this starting, however we need to also
                // be certain they've been populated by the writer that allocated them, so we wait for any writers
                // to complete; we do this with awaitSafe(), not await(), because the writers won't block until
                // after completing any write to any buffer it has allocated, and this way we won't deadlock, and don't
                // need to use markBlocking()
                OpOrder.Barrier writeBarrier = allocator.group.writes.newBarrier();
                writeBarrier.issue();
                writeBarrier.awaitSafe();
            }

            long retaining = 0;
            int allocCount = 0;
            CompactRegion compact;

            // TODO: if we allocate two read barriers, one here and one later, we can avoid marking anything that wasn't
            // compacted, but whose read op finished after the first barrier

            // map of markKey -> delay recycle, for all regions we're collecting
            // we use a multi-map so that we can set the mark key of any compacted regions to their new region during the
            // compaction pass. (so new markKey maps to all regions it receives data from), the corollary being we might
            // end up marking something as referenced when it isn't really. this should hopefully be rare, though, and
            // is relatively benign.
            final Multimap<MarkKey, NativeDelayedRecycle> markLookup = ArrayListMultimap.create(16, 2);

            try (OpOrder.Group gcOperation = gcOperations.start())
            {

                while (null != (compact = this.compact.advanceOrWait()))
                {
                    NativeRegion regionIn = compact.regionIn;
                    markLookup.put(regionIn.markKey, new NativeDelayedRecycle(allocator.group.pool, regionIn));

                    NativeRegion regionOut = null;
                    for (CompactRegion.Move move : compact.moves)
                    {
                        if (regionOut != move.regionOut)
                            markLookup.putAll((regionOut = move.regionOut).markKey, markLookup.get(regionIn.markKey));

                        // assign ourselves to the new region by atomically swapping our parent
                        if (!move.in.swapParent(regionIn, move.regionOut))
                        {
                            // if we fail, we've raced with somebody who freed the memory, so
                            // free it from the new region
                            assert move.in.parent == regionIn.markKey;
                            move.regionOut.free(move.in.getRealSize());
                        }
                        else
                        {
                            move.regionOut.unorderedAdopt(move.in);
                            // move the data to the new location
                            move.in.migrate(move.regionOut.peer + move.out);
                            retaining += move.in.getRealSize();
                        }
                    }
                    allocCount += compact.moves.size();
                }

                if (collect.isEmpty())
                    return;

                // go through all our to-collect regions in case we're collecting ones that are entirely empty,
                // as they won't have been added to the markLookup
                for (NativeRegion region : collect)
                    if (!markLookup.containsKey(region.markKey))
                        markLookup.put(region.markKey, new NativeDelayedRecycle(allocator.group.pool, region));

                // wait for any outstanding 'reads' to complete. we treat writes as a reads here, as they also need to read consistently
                OpOrder.Barrier readBarrier = allocator.group.reads.newBarrier();
                readBarrier.issue();
                OpOrder.Barrier writeBarrier = allocator.group.writes.newBarrier();
                writeBarrier.issue();
                readBarrier.await();
                // we wait until after all prior reads have completed before visiting the referrers to ensure they are populated
                allocator.group.mark(readBarrier, markLookup);
                // we only awaitSafe() on the write barrier, as we just need ordering guarantees (that they're using the latest addresses).
                writeBarrier.awaitSafe();

                // unmark each OFDR, so that only those marked by a Referrer are left; we want to unmark only once, so check equality with the markKey
                for (Map.Entry<MarkKey, Collection<NativeDelayedRecycle>> entry : markLookup.asMap().entrySet())
                    for (NativeDelayedRecycle delayedRecycle : entry.getValue())
                        if (delayedRecycle.region.markKey == entry.getKey())
                            delayedRecycle.unmark();

                // tidy up the allocator's collection of regions
                SafeRemoveIterator<NativeRegion> iter = allocator.allocated.iterator();
                while (iter.hasNext())
                    if (iter.next().isDiscarded())
                        iter.safeRemove();

                // have the allocator adopt the regions we've migrated its live data to.
                // we only insert these into the allocator's owned collection once we are finished, so that there are
                // no races between our copying/adoption and the cleaner that may be collecting data to start us again
                for (StateSnapper snap : allocatedFrom)
                    allocator.adopt(snap.region);

                // in the unlikely event we finish a GC after the allocator is discarded, we repeat the discard work
                // to free up any regions we just allocated
                if (allocator.state.lifeCycle == PoolAllocator.LifeCycle.DISCARDED)
                    allocator.doDiscard();

                if (logger.isDebugEnabled())
                {
                    logger.debug("Collected {} bytes from {} regions, retaining {} bytes in {} allocs from {}@{}",
                                 reclaimingSize, reclaimingCount, retaining, allocCount, allocator.group.name, System.identityHashCode(allocator));
                }
            }
            catch (InterruptedException e)
            {
                throw new IllegalStateException();
            }
            catch (Throwable t)
            {
                logger.error("Fatal exception during GC", t);
            }
            finally
            {
                running.remove(allocator, this);
                allocator.transition(PoolAllocator.Gc.INACTIVE);
            }
        }

    }

 }