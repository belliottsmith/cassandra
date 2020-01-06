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
package org.apache.cassandra.db.rows;

import java.security.MessageDigest;
import java.util.Comparator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.caching.TinyThreadLocalPool;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Generic interface for the data of a given column (inside a row).
 *
 * In practice, there is only 2 implementations of this: either {@link Cell} for simple columns
 * or {@code ComplexColumnData} for complex columns.
 */
public abstract class ColumnData
{
    public static final Comparator<ColumnData> comparator = (cd1, cd2) -> cd1.column().compareTo(cd2.column());

    /**
     * Construct an UpdateFunction for reconciling normal ColumnData
     * (i.e. not suitable for ComplexColumnDeletion sentinels, but suitable ComplexColumnData or Cell)
     *
     * @param updateF a consumer receiving all pairs of reconciled cells
     * @param maxDeletion the row or partition deletion time to use for purging
     */
    public static Reconciler reconciler(ReconcileUpdateFunction updateF, DeletionTime maxDeletion, int nowInSec)
    {
        TinyThreadLocalPool.TinyPool<Reconciler> pool = Reconciler.POOL.get();
        Reconciler reconciler = pool.poll();
        if (reconciler == null)
            reconciler = new Reconciler();
        reconciler.init(updateF, maxDeletion, nowInSec);
        reconciler.pool = pool;
        return reconciler;
    }

    public static ReconcileUpdateFunction noOp = new ReconcileUpdateFunction()
    {
        public Cell apply(Cell previous, Cell insert)
        {
            return insert;
        }

        public ColumnData apply(ColumnData insert)
        {
            return insert;
        }

        public void onAllocated(long delta)
        {
        }
    };

    public interface ReconcileUpdateFunction
    {
        Cell apply(Cell previous, Cell insert);
        ColumnData apply(ColumnData insert);
        void onAllocated(long delta);
    }

    public static class Reconciler implements UpdateFunction<ColumnData, ColumnData>, AutoCloseable
    {
        private static final TinyThreadLocalPool<Reconciler> POOL = new TinyThreadLocalPool<>();
        private ReconcileUpdateFunction modifier;
        private DeletionTime maxDeletion;
        private int nowInSec;
        private TinyThreadLocalPool.TinyPool<Reconciler> pool;

        private void init(ReconcileUpdateFunction modifier, DeletionTime maxDeletion, int nowInSec)
        {
            this.modifier = modifier;
            this.maxDeletion = maxDeletion;
            this.nowInSec = nowInSec;
        }

        public ColumnData apply(ColumnData existing, ColumnData update)
        {
            if (!(existing instanceof ComplexColumnData))
            {
                Cell existingCell = (Cell) existing, updateCell = (Cell) update;
                boolean isExistingShadowed = maxDeletion.deletes(existingCell);
                boolean isUpdateShadowed = maxDeletion.deletes(updateCell);

                Cell result = isExistingShadowed || isUpdateShadowed
                              ? isUpdateShadowed ? existingCell : updateCell
                              : Cells.reconcile(existingCell, updateCell, nowInSec);

                return modifier.apply(existingCell, result);
            }
            else
            {
                ComplexColumnData existingComplex = (ComplexColumnData) existing;
                ComplexColumnData updateComplex = (ComplexColumnData) update;

                DeletionTime existingDeletion = existingComplex.complexDeletion();
                DeletionTime updateDeletion = updateComplex.complexDeletion();
                DeletionTime maxComplexDeletion = existingDeletion.supersedes(updateDeletion) ? existingDeletion : updateDeletion;

                Reconciler reconciler = Reconciler.this;
                DeletionTime complexDeletion = DeletionTime.LIVE;
                if (maxComplexDeletion.supersedes(maxDeletion))
                {
                    complexDeletion = maxComplexDeletion;
                    reconciler = reconciler(modifier, complexDeletion, nowInSec);
                }

                Object[] cells = BTree.<Cell, Cell, Cell>update(existingComplex.tree(), updateComplex.tree(), existingComplex.column.cellComparator(), (UpdateFunction) reconciler);
                ComplexColumnData result = new ComplexColumnData(existingComplex.column, cells, complexDeletion);
                if (reconciler != this)
                    reconciler.close();
                return result;
            }
        }

        public void onAllocated(long heapSize)
        {
            modifier.onAllocated(heapSize);
        }

        public ColumnData apply(ColumnData insert)
        {
            return modifier.apply(insert);
        }

        public void close()
        {
            pool.offer(this);
            modifier = null;
            pool = null;
        }
    }

    protected final ColumnDefinition column;
    protected ColumnData(ColumnDefinition column)
    {
        this.column = column;
    }

    /**
     * The column this is data for.
     *
     * @return the column this is a data for.
     */
    public final ColumnDefinition column() { return column; }

    /**
     * The size of the data hold by this {@code ColumnData}.
     *
     * @return the size used by the data of this {@code ColumnData}.
     */
    public abstract int dataSize();

    public abstract long unsharedHeapSizeExcludingData();

    /**
     * Validate the column data.
     *
     * @throws MarshalException if the data is not valid.
     */
    public abstract void validate();

    /**
     * Adds the data to the provided digest.
     *
     * @param digest the {@code MessageDigest} to add the data to.
     */
    public abstract void digest(MessageDigest digest);

    public abstract ColumnData clone(AbstractAllocator allocator);

    /**
     * Returns a copy of the data where all timestamps for live data have replaced by {@code newTimestamp} and
     * all deletion timestamp by {@code newTimestamp - 1}.
     *
     * This exists for the Paxos path, see {@link PartitionUpdate#updateAllTimestamp} for additional details.
     */
    public abstract ColumnData updateAllTimestamp(long newTimestamp);

    public abstract ColumnData markCounterLocalToBeCleared();

    public abstract ColumnData purge(DeletionPurger purger, int nowInSec);

    public abstract long maxTimestamp();
}
