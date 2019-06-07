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
package com.apple.cie.db.marshal;

import java.nio.ByteBuffer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.cie.cql3.functions.CappedSortedMap;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.rows.AbstractCell;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Abstract base class for capped map cell resolvers, handles detecting and decoding optional cap cells in the map and
 * calls onRegularCell/endColumn to handle process the cells in the collection.
 *
 * The capped map cell resolver limits the number of cells resolved to an optional cap cell (holding N) and N elements.
 * Note that not all of the SSTables/memtables containing this column are guaranteed to be present (for example when
 * compacting), so the upper bound for a column is the number of overlapping sstables plus number of overlapping
 * memtables.  For leveled compaction strategy this should be bounded by number of L0 tables + number of levels.
 *
 * cap cells are supplied by the user as tombstones @{literal UPDATE table SET csm[cap(?)] = null, csm[now()] = ?},
 * with the cap encoded in the UUID clockseq.  To prevent the tombstone storing cap data being removed after
 * {@literal gc_grace_seconds}, the resolver adjusts the local deletion timestamp so that it should not be deleted
 * during normal operations by setting to {@literal CAP_LOCAL_DELETION_TIME}
 *
 * cap cells are dropped by the cells resolver when there are no live or tombstone cells left in the map.
 * Without the timestamp adjustment, any CSMs which set cap less than the system-wide default may expose
 * older values that were previously hidden.
 *
 * If cap is set for the column, it should be supplied on every update as it will be dropped if
 * all cells are garbage collected (which is desirable so that cap cells for TTLd elements will disappear with them).
 *
 * During compaction, the cells resolver executes before the deletion purger.  The cells resolver
 * has to make the decision on whether to output the cap cell as the soon-to-be-deleted cells
 * are added, so will output a cap cell with no element cells.  The next time it runs through compaction,
 * if the map is still empty the cap cell will be removed.
 *
 * Both default and provided caps are limited by the minimum / maximum effective capacity
 * length hot properties which can be used to limit all capped maps node-wide in the event of a production incident.
 * Limiting by effective size does not affect the user-supplied limit for cap tombstones written to disk.  The
 * cap cells are passed through unmodified, and the effective value is recalculated on each use.
 *
 * The size-bounding logic can be completely disabled/enabled with the setCellResolverEnabled(false)
 * hot property.  While the cells resolver is disabled, updated cap tombstones will be treated as normal
 * tombstone cells and will survive as long as they would normally in a map.
 */
abstract class AbstractCappedMapCellsResolver implements AbstractCappedMapCellsResolverMBean
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractCappedMapCellsResolver.class);

    protected volatile int defaultCap;
    protected volatile int minEffectiveCap = 0;
    protected volatile int maxEffectiveCap = CappedSortedMap.CAP_MAX;
    protected volatile boolean enabled = true;

    public AbstractCappedMapCellsResolver(String mbeanName, int defaultCap)
    {
        this.defaultCap = defaultCap;
        MBeanWrapper mbs = MBeanWrapper.instance;
        try
        {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    abstract String typeName();

    protected static int bound(int min, int value, int max)
    {
        assert(min <= max);
        return Integer.min(max, Integer.max(min, value));
    }

    abstract class WrapperBuilder implements Cells.Builder
    {
        protected int regularsIn = 0;
        protected int regularsOut = 0;

        protected final int resetCap;
        protected int cap;
        int capCellLocalDeletionTime = 1;
        protected CapCell<?> capCell = null;

        protected final Cells.Builder builder;
        protected WrapperBuilder(Cells.Builder builder, int defaultCap)
        {
            this.builder = builder;
            this.resetCap = defaultCap;
            this.cap = effectiveCap(defaultCap);
        }

        protected int effectiveCap(int cap)
        {
            return bound(minEffectiveCap, cap, maxEffectiveCap);
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            int cap = cap(cell);
            if (cap < 0)
                addRegularCell(cell);
            else
                addCapCell(cell, cap);
        }

        private CapCell<?> capCell(Cell<?> cell)
        {
            return new CapCell<>(cell, cell.localDeletionTime());
        }

        protected void addCapCell(Cell<?> cell, int cellCap)
        {
            if (regularsIn != 0)
                logger.error("Capped map cells must appear before all regular cells, ignoring cap {}", cellCap);

            if (capCell == null || cell.timestamp() > capCell.timestamp())
            {
                // Adjust the provided limit by the effective cap.  The user-specified
                // limit will be written through.  This permits controlling runtime behavior
                // if there are issues from longer caps, without losing the user intent.
                cap = effectiveCap(cellCap);
                capCell = capCell(cell);
            }

            onCapCell(cell, cellCap);
        }

        protected void onCapCell(Cell<?> cell, int cellCap) { }

        protected void addRegularCell(Cell<?> cell)
        {
            regularsIn++;

            // emit the cap cell before any regular values
            if (regularsOut == 0 && capCell != null)
            {
                // override the local deletion timestamp to prevent the cap (which is a tombstone) from being
                // compacted out when there may be live cells available in the queue.  If no regular cells
                // added to the resolver, then this will not be called and the cap cell will be dropped then.
                // Cap cell will be dropped together with the last live cell.
                builder.addCell(capCell);
            }

            if (cell != capCell && cell.localDeletionTime() >= capCellLocalDeletionTime)
                capCellLocalDeletionTime = Math.min(cell.localDeletionTime(), Cell.MAX_DELETION_TIME);

            onRegularCell(cell);
        }

        // Called for each regular cell
        abstract void onRegularCell(Cell<?> cell);

        // For use by onRegularCell
        protected void keepCell(Cell<?> cell)
        {
            builder.addCell(cell);
            regularsOut++;
        }

        @Override
        public void endColumn()
        {
            if (capCell != null)
                capCell.localDeletionTime = capCellLocalDeletionTime;

            if (regularsOut == 0)
                builder.addCell(capCell);

            capCell = null;
            regularsIn = 0;
            regularsOut = 0;
            cap = effectiveCap(resetCap);
            builder.endColumn();
        }

        protected int cap(Cell<?> cell)
        {
            return cell.isTombstone() ? CappedSortedMap.getCap(TimeUUIDType.instance.compose(cell.path().get(0))) : -1;
        }
    }


    // JMX controlled properties
    public boolean getCellResolverEnabled()
    {
        return enabled;
    }

    public void setCellResolverEnabled(boolean enable)
    {
        logger.info("Setting {} cell resolver enabled from {} to {}", typeName(), enabled, enable);
        enabled = enable;
    }

    public int getMinEffectiveCap()
    {
        return minEffectiveCap;
    }

    public void setMinEffectiveCap(int minCap)
    {
        int updatedMin = bound(0, minCap, CappedSortedMap.CAP_MAX);
        logger.info("Setting {} minimum effective cap from {} to {}", typeName(), maxEffectiveCap, updatedMin);
        minEffectiveCap = updatedMin;
    }

    public int getMaxEffectiveCap()
    {
        return maxEffectiveCap;
    }

    public void setMaxEffectiveCap(int maxCap)
    {
        int updatedMax = bound(0, maxCap, CappedSortedMap.CAP_MAX);

        logger.info("Setting {} maximum effective cap from {} to {}", typeName(), maxEffectiveCap, updatedMax);
        maxEffectiveCap = updatedMax;

        int updatedMin = Integer.min(minEffectiveCap, maxEffectiveCap);
        if (updatedMin != minEffectiveCap)
        {
            logger.info("Moving {} minimum effective cap from {} to {}", typeName(), minEffectiveCap, updatedMin);
            minEffectiveCap = updatedMin;
        }
    }

    public int getDefaultCap()
    {
        return defaultCap;
    }

    public void setDefaultCap(int cap)
    {
        logger.info("Setting {} default cap from {} to {}", typeName(), this.defaultCap, cap);
        this.defaultCap = cap;
    }

    private static class CapCell<T> extends AbstractCell<T>
    {
        private final Cell<T> delegate;
        private int localDeletionTime;

        private CapCell(Cell<T> delegate, int localDeletionTime)
        {
            super(delegate.column());
            this.delegate = delegate;
            this.localDeletionTime = localDeletionTime;
        }

        public T value() { return delegate.value(); }
        public ValueAccessor<T> accessor() { return delegate.accessor(); }
        public long timestamp() { return delegate.timestamp(); }
        public int ttl() { return delegate.ttl(); }
        public CellPath path() { return delegate.path(); }
        public long unsharedHeapSizeExcludingData() { return delegate.unsharedHeapSizeExcludingData(); }

        public int localDeletionTime() { return localDeletionTime; }

        public Cell<?> withUpdatedColumn(ColumnMetadata newColumn)
        {
            return new CapCell(delegate.withUpdatedColumn(newColumn), localDeletionTime);
        }

        public Cell<?> withUpdatedValue(ByteBuffer newValue)
        {
            return new CapCell(delegate.withUpdatedValue(newValue), localDeletionTime);
        }

        public Cell<?> withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime)
        {
            return new CapCell(delegate.withUpdatedTimestampAndLocalDeletionTime(newTimestamp, newLocalDeletionTime), localDeletionTime);
        }

        public Cell withSkippedValue()
        {
            return new CapCell(delegate.withUpdatedValue(ByteBufferUtil.EMPTY_BYTE_BUFFER), localDeletionTime);
        }
    }
}
