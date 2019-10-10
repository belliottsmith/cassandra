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
package org.apache.cassandra.db;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;

public class Memtable implements Comparable<Memtable>
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    @VisibleForTesting
    public static final MemtablePool MEMORY_POOL = DatabaseDescriptor.getMemtableAllocatorPool();
    private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.parseInt(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));

    private final MemtableAllocator allocator;
    private final AtomicLong liveDataSize = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    // the write barrier for directing writes to this memtable during a switch
    private volatile OpOrder.Barrier writeBarrier;
    /**
     * The ReplayPosition <i>up to which</i> we can guarantee a contiguous range of ReplayPosition whose
     * mutations occur exclusively in this Memtable.  There is no precise <i>start</i> to this contiguous range;
     * there is an indeterminate region between this point in the previous Memtable, and in this Memtable,
     * during which operations from both Memtable occur in the CommitLog.
     *
     * In the CommitLog (number representing records for a Memtable, created in numerical order):
     * [111110101111111111111121111212122222222223]  Records that _appear_ in (and are flushed by) the Memtable
     * [111111111111111111111122222222222222222223]  Records that are invalidated by the Memtable
     *                        ^                  ^
     *                        |                  |
     *                      pCUB                CUB
     *
     * NOTE this means that those records marked {@code 1} that occur after pCUB will not be invalidated after
     * the flush of {@code Memtable 1}, so they will be replayed and applied redundantly if {@code Memtable 2}
     * does not flush before the process terminates.
     * 
     * i.e., Given:
     *  CUB:  contiguous upper bound; no operations before this land in the next Memtable
     *        commitLogContiguousUpperBound
     *  LB:   strict lower bound; no operations before this occur in this Memtable
     *        occurs at an unknown position, such that pCUB<=LB<=CUB
     *  CLB:  contiguous lower bound; no operations from the prior Memtable occur after this
     *        occurs at an unknown position, such that LB<=CLB<=CUB and pUB==CLB
     *  UB:   strict upper bound; no operations after this occur in this Memtable
     *        occurs at an unknown position, such that CUB<=UB<=nCLB and nCLB==UB
     *  pX:   prevX (the value of X for the prior Memtable)
     *  nX:   nextX (the value of X for the following Memtable)
     * ..?:   occurs at an unknown position following (or equal to) the preceding named position;
     *        can occur at any point up to (and potentially including) the next named position
     *        (see the list immediately above for inequality relationships)
     *
     * We have a contiguous region of ReplayPosition exclusively occurring in:
     * [...pCUB]                   Prior Memtable
     *        (..?)[CLB......CUB]  This Memtable
     *        [..............CUB]  Invalidated by this Memtable
     *
     * We have at least one write with a ReplayPosition in the range:
     * [...pCUB............pUB]                 Prior Memtable
     *        (..?)[LB(..?)CLB(..?)CUB(..?)UB]  This Memtable
     *        [....................CUB]         Invalidated by this Memtable
     */
    private volatile ReplayPosition commitLogContiguousUpperBound;
    /**
     * The previous Memtable's {@link #commitLogContiguousUpperBound}
     * Note that this is <i>not</i> guaranteed to be the "contiguousLowerBound" - there may be a region at the beginning
     * where operations from the prior Memtable can be found, however this is the earliest point in the commit log
     * that any mutations that occur in this Memtable may be found, and also the point from which we are responsible
     * for invalidating the CommitLog
     */
    private ReplayPosition prevCommitLogContiguousUpperBound;

    /**
     * Must be <= prevCommitLogContiguousUpperBound once our predecessor
     * has been finalised, and this is enforced in {@link #setPrevCommitLogContiguousUpperBound}
     *
     * The Memtable cannot hold any mutations with positions in the commit log less than this, and nor
     * can it hold any mutations between this and {@link #prevCommitLogContiguousUpperBound}, however
     * this value can be known in advance of {@link #prevCommitLogContiguousUpperBound}.
     *
     * This is used for sorting and inequality operations wrt flushing commit log bounds where
     * {@link #prevCommitLogContiguousUpperBound} cannot be guaranteed to be known.
     */
    private final ReplayPosition beforeCommitLogLowerBound = CommitLog.instance.getContext();

    public int compareTo(Memtable that)
    {
        return this.beforeCommitLogLowerBound.compareTo(that.beforeCommitLogLowerBound);
    }

    // We index the memtable by PartitionPosition only for the purpose of being able
    // to select key range using Token.KeyBound. However put() ensures that we
    // actually only store DecoratedKey.
    private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();
    public final ColumnFamilyStore cfs;
    private final long creationNano = System.nanoTime();

    // The smallest timestamp for all partitions stored in this memtable
    private long minTimestamp = Long.MAX_VALUE;

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    private final ColumnsCollector columnsCollector;
    private final StatsCollector statsCollector = new StatsCollector();

    // only to be used by init(), to setup the very first memtable for the cfs
    public Memtable(ColumnFamilyStore cfs)
    {
        this(null, cfs);
    }
    public Memtable(ReplayPosition prevCommitLogContiguousUpperBound, ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = cfs.metadata.comparator;
        if (prevCommitLogContiguousUpperBound != null)
            setPrevCommitLogContiguousUpperBound(prevCommitLogContiguousUpperBound);
        this.cfs.scheduleFlush();
        this.columnsCollector = new ColumnsCollector(cfs.metadata.partitionColumns());
    }

    // ONLY to be used for testing, to create a mock Memtable
    @VisibleForTesting
    public Memtable(CFMetaData metadata)
    {
        this.initialComparator = metadata.comparator;
        this.cfs = null;
        this.allocator = null;
        this.columnsCollector = new ColumnsCollector(metadata.partitionColumns());
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    public long getOperations()
    {
        return currentOperations.get();
    }

    @VisibleForTesting
    public void setDiscarding(OpOrder.Barrier writeBarrier)
    {
        assert this.writeBarrier == null;
        this.writeBarrier = writeBarrier;
        allocator.setDiscarding();
    }

    /**
     * Note: This Memtable's contiguous commit log region does NOT actually start here
     * See {@link #commitLogContiguousUpperBound}
     */
    ReplayPosition prevCommitLogContiguousUpperBound()
    {
        return prevCommitLogContiguousUpperBound;
    }

    /**
     * Note: This Memtable's contents extend past this point in the commit log
     * See {@link #commitLogContiguousUpperBound}
     */
    @VisibleForTesting
    public ReplayPosition commitLogContiguousUpperBound()
    {
        return commitLogContiguousUpperBound;
    }

    public void setPrevCommitLogContiguousUpperBound(ReplayPosition lowerBound)
    {
        assert prevCommitLogContiguousUpperBound == null;
        assert lowerBound.compareTo(beforeCommitLogLowerBound) >= 0;
        this.prevCommitLogContiguousUpperBound = lowerBound;
    }

    public void setCommitLogContiguousUpperBound(ReplayPosition upperBound)
    {
        assert commitLogContiguousUpperBound == null;
        this.commitLogContiguousUpperBound = upperBound;
    }

    void setDiscarded()
    {
        allocator.setDiscarded();
    }

    /**
     * Decide if this Memtable should take the write, or if it should go to a later {@link Memtable}.
     *
     * This decision is based <i>exclusively</i> on {@code opGroup} and {@link #writeBarrier}.
     *
     * If {@link #writeBarrier} is unset, this {@link Memtable} is not flushing and can accept all writes,
     * so {@code opGroup} is not relevant to the decision (so long as this method is invoked on
     * earlier {@link Memtable} first).
     *
     * If {@link #writeBarrier} is set, this {@link Memtable} is flushing and will wait until all operations
     * earlier than {@link #writeBarrier} have completed before doing so.
     * All operations older than {@link #writeBarrier} will be marked such that {@link OpOrder.Group#isBlocking()}
     * returns {@code true}.  This will permit them to ignore memory limits, so that they may proceed unhindered,
     * permitting flush to reclaim their associated memory.
     *
     * It was previously permitted for some of these operations to fall through to later {@link Memtable}
     * in order to ensure a <i>precise</i> upper bound to the {@link ReplayPosition} for mutations
     * occurring in this {@link Memtable}, however:
     * If any of these deferred operations mixed with a future {@link OpOrder.Group} in the later {@link Memtable},
     * memory limits that apply to this future {@link OpOrder.Group} may transitively affect to the operations
     * that block the flush of this {@link Memtable}, by blocking an operation in the later {@link OpOrder.Group}
     * while it holds some shared mutually exclusive resource (specifically, the {@link AtomicBTreePartition}
     * contended update lock) that the deferred operations need to execute.
     *
     * This is no longer possible because this method now considers only {@link Memtable#writeBarrier}
     * and the provided {@link OpOrder.Group}, such that a {@link Memtable} contains _only_ those operations that
     * were started before its {@link Memtable#writeBarrier} was issued, and after the prior {@link Memtable}'s
     * {@link Memtable#writeBarrier} was issued.
     *
     * @return true if the operation should be written to this Memtable
     */
    public boolean accepts(OpOrder.Group opGroup)
    {
        // if the barrier hasn't been set yet, then this memtable is still taking ALL writes
        OpOrder.Barrier barrier = this.writeBarrier;
        if (barrier == null)
            return true;
        // if the barrier has been set, but is in the past, we are destined for a future memtable
        return barrier.isAfter(opGroup);
    }

    public boolean isLive()
    {
        return allocator.isLive();
    }

    public boolean isClean()
    {
        return partitions.isEmpty();
    }

    public boolean mayContainDataBefore(ReplayPosition position)
    {
        return beforeCommitLogLowerBound.compareTo(position) < 0;
    }

    /**
     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
     */
    public boolean isExpired()
    {
        int period = cfs.metadata.params.memtableFlushPeriodInMs;
        return period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period));
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * replayPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        AtomicBTreePartition previous = partitions.get(update.partitionKey());

        long initialSize = 0;
        if (previous == null)
        {
            final DecoratedKey cloneKey = allocator.clone(update.partitionKey(), opGroup);
            AtomicBTreePartition empty = new AtomicBTreePartition(cfs.metadata, cloneKey, allocator);
            // We'll add the columns later. This avoids wasting works if we get beaten in the putIfAbsent
            previous = partitions.putIfAbsent(cloneKey, empty);
            if (previous == null)
            {
                previous = empty;
                // allocate the row overhead after the fact; this saves over allocating and having to free after, but
                // means we can overshoot our declared limit.
                int overhead = (int) (cloneKey.getToken().getHeapSize() + ROW_OVERHEAD_HEAP_SIZE);
                allocator.onHeap().allocate(overhead, opGroup);
                initialSize = 8;
            }
        }

        long[] pair = previous.addAllWithSizeDelta(update, opGroup, indexer);
        minTimestamp = Math.min(minTimestamp, previous.stats().minTimestamp);
        liveDataSize.addAndGet(initialSize + pair[0]);
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        return pair[1];
    }

    public int partitionCount()
    {
        return partitions.size();
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             cfs.name, hashCode(), FBUtilities.prettyPrintMemory(liveDataSize.get()), currentOperations,
                             100 * allocator.onHeap().ownershipRatio(), 100 * allocator.offHeap().ownershipRatio());
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange, final boolean isForThrift)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean startIsMin = keyRange.left.isMinimum();
        boolean stopIsMin = keyRange.right.isMinimum();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;
        Map<PartitionPosition, AtomicBTreePartition> subMap;
        if (startIsMin)
            subMap = stopIsMin ? partitions : partitions.headMap(keyRange.right, includeStop);
        else
            subMap = stopIsMin
                   ? partitions.tailMap(keyRange.left, includeStart)
                   : partitions.subMap(keyRange.left, includeStart, keyRange.right, includeStop);

        int minLocalDeletionTime = Integer.MAX_VALUE;

        // avoid iterating over the memtable if we purge all tombstones
        if (cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones())
            minLocalDeletionTime = findMinLocalDeletionTime(subMap.entrySet().iterator());

        final Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter = subMap.entrySet().iterator();

        return new MemtableUnfilteredPartitionIterator(cfs, iter, isForThrift, minLocalDeletionTime, columnFilter, dataRange);
    }

    private int findMinLocalDeletionTime(Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iterator)
    {
        int minLocalDeletionTime = Integer.MAX_VALUE;
        while (iterator.hasNext())
        {
            Map.Entry<PartitionPosition, AtomicBTreePartition> entry = iterator.next();
            minLocalDeletionTime = Math.min(minLocalDeletionTime, entry.getValue().stats().minLocalDeletionTime);
        }
        return minLocalDeletionTime;
    }

    public Partition getPartition(DecoratedKey key)
    {
        return partitions.get(key);
    }

    public Collection<SSTableReader> flush()
    {
        long estimatedSize = estimatedSize();
        Directories.DataDirectory dataDirectory = cfs.getDirectories().getWriteableLocation(estimatedSize);
        if (dataDirectory == null)
            throw new RuntimeException("Insufficient disk space to write " + estimatedSize + " bytes");
        File sstableDirectory = cfs.getDirectories().getLocationForDisk(dataDirectory);
        assert sstableDirectory != null : "Flush task is not bound to any disk";
        return writeSortedContents(sstableDirectory);
    }

    public long getMinTimestamp()
    {
        return minTimestamp;
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1L * 1024 * 1024 * 1024 * 1024 * 1024);
    }

    private long estimatedSize()
    {
        long keySize = 0;
        for (PartitionPosition key : partitions.keySet())
        {
            //  make sure we don't write non-sensical keys
            assert key instanceof DecoratedKey;
            keySize += ((DecoratedKey)key).getKey().remaining();
        }
        return (long) ((keySize // index entries
                        + keySize // keys in data file
                        + liveDataSize.get()) // data
                       * 1.2); // bloom filter and row index overhead
    }

    private Collection<SSTableReader> writeSortedContents(File sstableDirectory)
    {
        boolean isBatchLogTable = cfs.name.equals(SystemKeyspace.BATCHES) && cfs.keyspace.getName().equals(SystemKeyspace.NAME);

        logger.debug("Writing {}", Memtable.this.toString());

        Collection<SSTableReader> ssTables;
        try (SSTableTxnWriter writer = createFlushWriter(cfs.getSSTablePath(sstableDirectory), columnsCollector.get(), statsCollector.get()))
        {
            boolean trackContention = logger.isTraceEnabled();
            int heavilyContendedRowCount = 0;
            // (we can't clear out the map as-we-go to free up memory,
            //  since the memtable is being used for queries in the "pending flush" category)
            for (AtomicBTreePartition partition : partitions.values())
            {
                // Each batchlog partition is a separate entry in the log. And for an entry, we only do 2
                // operations: 1) we insert the entry and 2) we delete it. Further, BL data is strictly local,
                // we don't need to preserve tombstones for repair. So if both operation are in this
                // memtable (which will almost always be the case if there is no ongoing failure), we can
                // just skip the entry (CASSANDRA-4667).
                if (isBatchLogTable && !partition.partitionLevelDeletion().isLive() && partition.hasRows())
                    continue;

                if (trackContention && partition.usePessimisticLocking())
                    heavilyContendedRowCount++;

                if (!partition.isEmpty())
                {
                    try (UnfilteredRowIterator iter = partition.unfilteredIterator())
                    {
                        writer.append(iter);
                    }
                }
            }

            if (writer.getFilePointer() > 0)
            {
                logger.debug(String.format("Completed flushing %s (%s) for commitlog position %s",
                                           writer.getFilename(),
                                           FBUtilities.prettyPrintMemory(writer.getFilePointer()),
                                          commitLogContiguousUpperBound));

                // sstables should contain non-repaired data.
                ssTables = writer.finish(true);
            }
            else
            {
                logger.debug("Completed flushing {}; nothing needed to be retained.  Commitlog position was {}",
                            writer.getFilename(), commitLogContiguousUpperBound);
                writer.abort();
                ssTables = Collections.emptyList();
            }

            if (heavilyContendedRowCount > 0)
                logger.trace(String.format("High update contention in %d/%d partitions of %s ", heavilyContendedRowCount, partitions.size(), Memtable.this.toString()));

            return ssTables;
        }
    }

    @SuppressWarnings("resource") // log and writer closed by SSTableTxnWriter
    public SSTableTxnWriter createFlushWriter(String filename,
                                              PartitionColumns columns,
                                              EncodingStats stats)
    {
        // we operate "offline" here, as we expose the resulting reader consciously when done
        // (although we may want to modify this behaviour in future, to encapsulate full flush behaviour in LifecycleTransaction)
        LifecycleTransaction txn = null;
        try
        {
            txn = LifecycleTransaction.offline(OperationType.FLUSH);
            MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.metadata.comparator)
                    .commitLogIntervals(new IntervalSet<>(prevCommitLogContiguousUpperBound, commitLogContiguousUpperBound));

            return new SSTableTxnWriter(txn,
                                        cfs.createSSTableMultiWriter(Descriptor.fromFilename(filename),
                                                                     (long) partitions.size(),
                                                                     ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                     sstableMetadataCollector,
                                                                     new SerializationHeader(true, cfs.metadata, columns, stats),
                                                                     txn));
        }
        catch (Throwable t)
        {
            if (txn != null)
                txn.close();
            throw t;
        }
    }

    private static int estimateRowOverhead(final int count)
    {
        // calculate row overhead
        try (final OpOrder.Group group = new OpOrder().start())
        {
            int rowOverhead;
            MemtableAllocator allocator = MEMORY_POOL.newAllocator();
            ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap<>();
            final Object val = new Object();
            for (int i = 0 ; i < count ; i++)
                partitions.put(allocator.clone(new BufferDecoratedKey(new LongToken(i), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
            double avgSize = ObjectSizes.measureDeep(partitions) / (double) count;
            rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
            rowOverhead -= ObjectSizes.measureDeep(new LongToken(0));
            rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
            allocator.setDiscarding();
            allocator.setDiscarded();
            return rowOverhead;
        }
    }

    public static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final ColumnFamilyStore cfs;
        private final Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter;
        private final boolean isForThrift;
        private final int minLocalDeletionTime;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(ColumnFamilyStore cfs, Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter, boolean isForThrift, int minLocalDeletionTime, ColumnFilter columnFilter, DataRange dataRange)
        {
            this.cfs = cfs;
            this.iter = iter;
            this.isForThrift = isForThrift;
            this.minLocalDeletionTime = minLocalDeletionTime;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public boolean isForThrift()
        {
            return isForThrift;
        }

        public int getMinLocalDeletionTime()
        {
            return minLocalDeletionTime;
        }

        public CFMetaData metadata()
        {
            return cfs.metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            Map.Entry<PartitionPosition, AtomicBTreePartition> entry = iter.next();
            // Actual stored key should be true DecoratedKey
            assert entry.getKey() instanceof DecoratedKey;
            DecoratedKey key = (DecoratedKey)entry.getKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);
            return filter.getUnfilteredRowIterator(columnFilter, entry.getValue());
        }
    }

    private static class ColumnsCollector
    {
        private final HashMap<ColumnDefinition, AtomicBoolean> predefined = new HashMap<>();
        private final ConcurrentSkipListSet<ColumnDefinition> extra = new ConcurrentSkipListSet<>();
        ColumnsCollector(PartitionColumns columns)
        {
            for (ColumnDefinition def : columns.statics)
                predefined.put(def, new AtomicBoolean());
            for (ColumnDefinition def : columns.regulars)
                predefined.put(def, new AtomicBoolean());
        }

        public void update(PartitionColumns columns)
        {
            for (ColumnDefinition s : columns.statics)
                update(s);
            for (ColumnDefinition r : columns.regulars)
                update(r);
        }

        private void update(ColumnDefinition definition)
        {
            AtomicBoolean present = predefined.get(definition);
            if (present != null)
            {
                if (!present.get())
                    present.set(true);
            }
            else
            {
                extra.add(definition);
            }
        }

        public PartitionColumns get()
        {
            PartitionColumns.Builder builder = PartitionColumns.builder();
            for (Map.Entry<ColumnDefinition, AtomicBoolean> e : predefined.entrySet())
                if (e.getValue().get())
                    builder.add(e.getKey());
            return builder.addAll(extra).build();
        }
    }

    private static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);

        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }

    @VisibleForTesting
    public void markContended(DecoratedKey key)
    {
        AtomicBTreePartition partition = partitions.get(key);
        if (partition == null)
        {
            AtomicBTreePartition prev = partitions.putIfAbsent(key, partition = new AtomicBTreePartition(cfs.metadata, key, allocator));
            if (prev != null)
                partition = prev;
        }
        partition.markContended();
    }
}
