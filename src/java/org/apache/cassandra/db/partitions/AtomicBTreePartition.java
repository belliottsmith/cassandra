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
package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.Locks;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * A thread-safe and atomic Partition implementation.
 *
 * Operations (in particular addAll) on this implementation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all rows have
 * been added.
 */
public class AtomicBTreePartition extends AbstractBTreePartition
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition(CFMetaData.createFake("keyspace", "table"),
                                                                                       DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.allocate(1)),
                                                                                       null));

    // Reserved values for wasteTracker field. These values must not be consecutive (see avoidReservedValues)
    private static final int TRACKER_NEVER_WASTED = 0;
    private static final int TRACKER_PESSIMISTIC_LOCKING = Integer.MAX_VALUE;

    // The granularity with which we track wasted allocation/work; we round up
    private static final int ALLOCATION_GRANULARITY_BYTES = 1024;
    // The number of bytes we have to waste in excess of our acceptable realtime rate of waste (defined below)
    private static final long EXCESS_WASTE_BYTES = 10 * 1024 * 1024L;
    private static final int EXCESS_WASTE_OFFSET = (int) (EXCESS_WASTE_BYTES / ALLOCATION_GRANULARITY_BYTES);
    // Note this is a shift, because dividing a long time and then picking the low 32 bits doesn't give correct rollover behavior
    private static final int CLOCK_SHIFT = 17;
    // CLOCK_GRANULARITY = 1^9ns >> CLOCK_SHIFT == 132us == (1/7.63)ms

    private static final AtomicIntegerFieldUpdater<AtomicBTreePartition> wasteTrackerUpdater = AtomicIntegerFieldUpdater.newUpdater(AtomicBTreePartition.class, "wasteTracker");
    private static final AtomicReferenceFieldUpdater<AtomicBTreePartition, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreePartition.class, Holder.class, "ref");

    /**
     * (clock + allocation) granularity are combined to give us an acceptable (waste) allocation rate that is defined by
     * the passage of real time of ALLOCATION_GRANULARITY_BYTES/CLOCK_GRANULARITY, or in this case 7.63Kb/ms, or 7.45Mb/s
     *
     * in wasteTracker we maintain within EXCESS_WASTE_OFFSET before the current time; whenever we waste bytes
     * we increment the current value if it is within this window, and set it to the min of the window plus our waste
     * otherwise.
     */
    private volatile int wasteTracker = TRACKER_NEVER_WASTED;

    private final MemtableAllocator allocator;
    private volatile Holder ref;

    public AtomicBTreePartition(CFMetaData metadata, DecoratedKey partitionKey, MemtableAllocator allocator)
    {
        // involved in potential bug? partition columns may be a subset if we alter columns while it's in memtable
        super(metadata, partitionKey);
        this.allocator = allocator;
        this.ref = EMPTY;
    }

    protected Holder holder()
    {
        return ref;
    }

    protected boolean canHaveShadowedData()
    {
        return true;
    }

    /**
     * Extracts the updated versions of any datum inserted from {@code addTree} into {@code newTree} that
     * were resolved to remain in {@code newTree}.
     *
     * Implementation note: if we could mutate addTree, this could be done much more efficiently by performing
     * an in-situ replace/remove, then rebalancing.  However since our typical use case involves rewriting the
     * whole tree, and since we cannot safely perform it in-situ, there's nothing to be gained by being clever.
     *
     * @param addTree the tree we were originally inserting
     * @param newTree the tree that emerged from inserting {@code addTree}
     * @return the remaining parts of {@code addTree}, as taken from {@code newTree}
     */
    @VisibleForTesting
    static Object[] extractUnshadowed(CFMetaData metadata, Object[] addTree, Object[] newTree)
    {
        return BTree.transformAndFilter(addTree, (Row addRow, SearchIterator<Clustering, Row> newIter) -> {
            Row newRow = newIter.next(addRow.clustering());
            if (newRow == null)
                return null;

            Row retainRow = extractUnshadowed(addRow, newRow);
            if (retainRow == null || retainRow.isEmpty())
                return null;

            return retainRow;
        }, BTree.slice(newTree, metadata.comparator, BTree.Dir.ASC));
    }

    /**
     * Extracts the updated versions of any datum inserted from {@code addRow} into {@code newRow} that
     * were resolved to remain in {@code newRow}.
     *
     * Implementation note: if we could mutate addRow, this could be done much more efficiently by performing
     * an in-situ replace/remove, then rebalancing.  However since our typical use case involves rewriting the
     * whole tree, and since we cannot safely perform it in-situ, there's nothing to be gained by being clever.
     *
     * @param addRow the row we were originally inserting
     * @param newRow the row that emerged from inserting {@code addRow}
     * @return the remaining parts of {@code addRow}, as taken from {@code newRow}
     */
    @VisibleForTesting
    static Row extractUnshadowed(Row addRow, Row newRow)
    {
        return addRow.transformAndFilter((ColumnData addData, SearchIterator<ColumnDefinition, ColumnData> newIter) -> {
            ColumnData newData = newIter.next(addData.column());
            if (newData == null)
                return null;

            ColumnDefinition column = newData.column();
            if (column.isComplex())
                return extractUnshadowed((ComplexColumnData) addData, (ComplexColumnData) newData);

            if (column.isCounterColumn())
                return newData;

            if (newData.equals(addData))
                return newData;

            return null;

        }, newRow.searchIterator());
    }

    /**
     * Extracts the updated versions of any datum inserted from {@code addCd} into {@code newCd} that
     * were resolved to remain in {@code newCd}.
     *
     * @param addCd the complex column data we were originally inserting
     * @param newCd the complex column data that emerged from inserting {@code addCd}
     * @return the remaining parts of {@code addRow}, as taken from {@code newCd}
     */
    @VisibleForTesting
    static ComplexColumnData extractUnshadowed(ComplexColumnData addCd, ComplexColumnData newCd)
    {
        return addCd.transformAndFilter((Cell addCell, SearchIterator<CellPath, Cell> newIter) -> {
            Cell newCell = newIter.next(addCell.path());
            return newCell != null && newCell.equals(addCell) ? newCell : null;
        }, newCd.searchIterator());
    }

    private static Row updateStaticRow(RowUpdater updater, Row current, Row add)
    {
        return add.isEmpty() ? current : (current.isEmpty() ? updater.apply(add) : updater.apply(current, add));
    }

    /**
     * Adds a given update to this in-memtable partition.
     *
     * @return an array containing first the difference in size seen after merging the updates, and second the minimum
     * time detla between updates.
     */
    public long[] addAllWithSizeDelta(PartitionUpdate update, OpOrder.Group writeOp, UpdateTransaction indexer)
    {
        boolean monitorOwned = false;
        try
        {
            PartitionColumns newColumns;
            Row copiedStaticRow;
            DeletionInfo copiedDeletionInfo;
            Object[] copiedTree;
            if (usePessimisticLocking())
            {
                // copy everything upfront - this might mean we waste memtable space on shadowed data,
                // but we have to pick either that or permitting the first CAS for each modification to
                // compete with a lock owner
                Row.Builder builder = allocator.rowBuilder(writeOp);

                newColumns = update.columns();
                copiedStaticRow = Rows.copy(update.staticRow(), builder).build();
                copiedDeletionInfo = update.deletionInfo().copy(HeapAllocator.instance);
                copiedTree = BTree.transformAndFilter(update.holder().tree, (Row row) -> Rows.copy(row, builder).build());

                Locks.monitorEnterUnsafe(this);
                monitorOwned = true;
            }
            else
            {
                indexer.start();

                RowUpdater updater = RowUpdater.cloning(allocator, writeOp, indexer);
                Holder current = ref;

                newColumns = update.columns().mergeTo(current.columns);

                Row addStaticRow = update.staticRow();
                Row newStaticRow = updateStaticRow(updater, current.staticRow, addStaticRow);

                copiedDeletionInfo = update.deletionInfo().copy(HeapAllocator.instance);
                DeletionInfo newDeletionInfo = current.deletionInfo.add(copiedDeletionInfo);

                EncodingStats addStats = update.stats();
                EncodingStats newStats = current.stats.mergeWith(addStats);

                Object[] newTree = BTree.update(current.tree, update.metadata().comparator, update, update.rowCount(), updater);

                Holder newHolder = new Holder(newColumns, newTree, newDeletionInfo, newStaticRow, newStats);

                if (refUpdater.compareAndSet(this, current, newHolder))
                    return finishAddAllWithSizeDelta(update, indexer, updater, newHolder, current);

                copiedStaticRow = current.staticRow.isEmpty() ? newStaticRow : extractUnshadowed(addStaticRow, newStaticRow);
                copiedTree = BTree.isEmpty(current.tree) ? newTree : extractUnshadowed(metadata, update.holder().tree, newTree);

                monitorOwned = maybeUsePessimisticLocking(updater.heapSize);
            }

            RowUpdater updater = RowUpdater.counting(this, allocator, writeOp, indexer);
            while (true)
            {
                indexer.start();

                Holder current = ref;
                updater.ref = current;

                newColumns = newColumns.mergeTo(current.columns);
                Row newStaticRow = updateStaticRow(updater, current.staticRow, copiedStaticRow);
                DeletionInfo newDeletionInfo = current.deletionInfo.add(copiedDeletionInfo);
                EncodingStats newStats = current.stats.mergeWith(update.stats());

                Object[] newTree = BTree.update(current.tree, update.metadata().comparator, BTree.iterable(copiedTree), BTree.size(copiedTree), updater);
                Holder newHolder = new Holder(newColumns, newTree, newDeletionInfo, newStaticRow, newStats);

                if (refUpdater.compareAndSet(this, current, newHolder))
                    return finishAddAllWithSizeDelta(update, indexer, updater, newHolder, current);

                updater.reset();
                if (!monitorOwned)
                    monitorOwned = maybeUsePessimisticLocking(updater.heapSize);
            }
        }
        finally
        {
            indexer.commit();
            if (monitorOwned)
                Locks.monitorExitUnsafe(this);
        }
    }

    private boolean maybeUsePessimisticLocking(long allocatedHeapSize)
    {
        boolean shouldLock = usePessimisticLocking();
        if (!shouldLock)
        {
            shouldLock = updateWastedAllocationTracker(allocatedHeapSize);
        }
        if (!shouldLock)
            return false;

        Locks.monitorEnterUnsafe(this);
        return true;
    }

    public boolean usePessimisticLocking()
    {
        return wasteTracker == TRACKER_PESSIMISTIC_LOCKING;
    }

    /**
     * Update the wasted allocation tracker state based on newly wasted allocation information
     *
     * @param wastedBytes the number of bytes wasted by this thread
     * @return true if the caller should now proceed with pessimistic locking because the waste limit has been reached
     */
    private boolean updateWastedAllocationTracker(long wastedBytes)
    {
        // Early check for huge allocation that exceeds the limit
        if (wastedBytes < EXCESS_WASTE_BYTES)
        {
            // We round up to ensure work < granularity are still accounted for
            int wastedAllocation = ((int) (wastedBytes + ALLOCATION_GRANULARITY_BYTES - 1)) / ALLOCATION_GRANULARITY_BYTES;

            int oldTrackerValue;
            while (TRACKER_PESSIMISTIC_LOCKING != (oldTrackerValue = wasteTracker))
            {
                // Note this time value has an arbitrary offset, but is a constant rate 32 bit counter (that may wrap)
                int time = (int) (System.nanoTime() >>> CLOCK_SHIFT);
                int delta = oldTrackerValue - time;
                if (oldTrackerValue == TRACKER_NEVER_WASTED || delta >= 0 || delta < -EXCESS_WASTE_OFFSET)
                    delta = -EXCESS_WASTE_OFFSET;
                delta += wastedAllocation;
                if (delta >= 0)
                    break;
                if (wasteTrackerUpdater.compareAndSet(this, oldTrackerValue, avoidReservedValues(time + delta)))
                    return false;
            }
        }
        // We have definitely reached our waste limit so set the state if it isn't already
        wasteTrackerUpdater.set(this, TRACKER_PESSIMISTIC_LOCKING);
        // And tell the caller to proceed with pessimistic locking
        return true;
    }

    private static int avoidReservedValues(int wasteTracker)
    {
        if (wasteTracker == TRACKER_NEVER_WASTED || wasteTracker == TRACKER_PESSIMISTIC_LOCKING)
            return wasteTracker + 1;
        return wasteTracker;
    }

    /**
     * Subroutine of addAllWithSizeDelta to complete any book-keeping after successfully updating {@link #holder}
     */
    private static long[] finishAddAllWithSizeDelta(PartitionUpdate update, UpdateTransaction indexer, RowUpdater updater, Holder newHolder, Holder oldHolder)
    {
        if (indexer != UpdateTransaction.NO_OP)
        {
            DeletionInfo addDeletionInfo = update.deletionInfo();

            if (!addDeletionInfo.partitionDeletion().isLive())
                indexer.onPartitionDeletion(addDeletionInfo.partitionDeletion());

            if (addDeletionInfo.hasRanges())
                addDeletionInfo.rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);
        }

        updater.allocated(newHolder.deletionInfo.unsharedHeapSize() - oldHolder.deletionInfo.unsharedHeapSize());
        updater.finish();
        return new long[] { updater.dataSize, updater.colUpdateTimeDelta };
    }

    // the function we provide to the btree utilities to perform any column replacements
    private static final class RowUpdater implements UpdateFunction<Row, Row>
    {
        final AtomicBTreePartition partition;
        final MemtableAllocator allocator;
        final Row.Builder builder;
        final OpOrder.Group writeOp;
        final UpdateTransaction indexer;
        final int nowInSec;
        final boolean clone;
        long dataSize;
        long heapSize;
        long colUpdateTimeDelta = Long.MAX_VALUE;
        Holder ref;

        private RowUpdater(AtomicBTreePartition partition, MemtableAllocator allocator, boolean clone, Row.Builder builder, OpOrder.Group writeOp, UpdateTransaction indexer)
        {
            this.partition = partition;
            this.allocator = allocator;
            this.builder = builder;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.nowInSec = FBUtilities.nowInSeconds();
            this.clone = clone;
        }

        public Row apply(Row insert)
        {
            if (clone)
                insert = Rows.copy(insert, builder).build();

            indexer.onInserted(insert);

            this.dataSize += insert.dataSize();
            this.heapSize += insert.unsharedHeapSizeExcludingData();
            return insert;
        }

        public Row apply(Row existing, Row update)
        {
            colUpdateTimeDelta = Math.min(colUpdateTimeDelta, Rows.merge(existing, update, builder, nowInSec));

            Row reconciled = builder.build();
            indexer.onUpdated(existing, reconciled);

            dataSize += reconciled.dataSize() - existing.dataSize();
            heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();

            return reconciled;
        }

        public boolean abortEarly()
        {
            return ref != null && ref != partition.ref;
        }

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
        }

        public void allocated(long heapSize)
        {
            this.heapSize += heapSize;
        }

        protected void finish()
        {
            allocator.onHeap().adjust(heapSize, writeOp);
        }

        /**
         * Construct a RowUpdater that copies all new contents with the provided allocator
         */
        public static RowUpdater cloning(MemtableAllocator allocator, OpOrder.Group writeOp, UpdateTransaction indexer)
        {
            return new RowUpdater(null, allocator, true, allocator.rowBuilder(writeOp), writeOp, indexer);
        }

        /**
         * Construct a RowUpdater that does not copy anything, only performs necessary book keeping
         */
        public static RowUpdater counting(AtomicBTreePartition partition, MemtableAllocator allocator, OpOrder.Group writeOp, UpdateTransaction indexer)
        {
            return new RowUpdater(partition, allocator, true, BTreeRow.sortedBuilder(), writeOp, indexer);
        }
    }

    @VisibleForTesting
    public void unsafeSetHolder(Holder holder)
    {
        ref = holder;
    }

    @VisibleForTesting
    public Holder unsafeGetHolder()
    {
        return ref;
    }
}
