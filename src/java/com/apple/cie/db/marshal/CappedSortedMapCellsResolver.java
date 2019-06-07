/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */
package com.apple.cie.db.marshal;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.cie.cql3.functions.CappedSortedMap;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;

/**
 * Cell resolver for CappedSortedMapType.  Limits the number of cells resolved for CappedSortedMapType
 * to an optional cap cell (holding N) and N elements.  Note that not all of the SSTables/memtables containing
 * this column are guaranteed to be present (for example when compacting), so the upper bound for a column
 * is the number of overlapping sstables plus number of overlapping memtables.  For leveled compaction strategy
 * this should be bounded by number of L0 tables + number of levels.
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
 * length hot properties which can be used to limit all capped sorted maps node-wide in the event of a production incident.
 * Limiting by effective size does not affect the user-supplied limit for cap tombstones written to disk.  The
 * cap cells are passed through unmodified, and the effective value is recalculated on each use.
 *
 * The size-bounding logic can be completely disabled/enabled with the setCellResolverEnabled(false)
 * hot property.  While the cells resolver is disabled, updated cap tombstones will be treated as normal
 * tombstone cells and will survive as long as they would normally in a map.
 */
public class CappedSortedMapCellsResolver implements CappedSortedMapCellsResolverMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CappedSortedMapCellsResolver.class);

    private static final int CAP_LOCAL_DELETION_TIMESTAMP = Cell.MAX_DELETION_TIME - 1;

    private volatile int defaultCap = Integer.getInteger("com.apple.cie.db.marshal.cappedsortedmap.defaultcap", 10);
    private int minEffectiveCap = 0;
    private int maxEffectiveCap = CappedSortedMap.CAP_MAX;
    private boolean enabled = true;

    private static final String MBEAN_NAME = "com.apple.cie.db:type=CappedSortedMapType";

    static final CappedSortedMapCellsResolver instance = new CappedSortedMapCellsResolver();
    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static int bound(int min, int value, int max)
    {
        assert(min <= max);
        return Integer.min(max, Integer.max(min, value));
    }

    private int effectiveCap(int cap)
    {
        return bound(minEffectiveCap, cap, maxEffectiveCap);
    }


    Cells.Builder wrapCellsBuilder(Cells.Builder resolver)
    {
        return enabled ? new WrapperBuilder(resolver) : resolver;
    }

    private class WrapperBuilder implements Cells.Builder
    {
        private int regularsIn = 0;
        private int regularsOut = 0;

        private int cap = effectiveCap(defaultCap);
        private Cell capCell = null;

        private final Cells.Builder builder;
        private WrapperBuilder(Cells.Builder builder)
        {
            this.builder = builder;
        }

        @Override
        public void addCell(Cell cell)
        {
            int cap = cap(cell);
            if (cap < 0)
                onRegularCell(cell);
            else
                onCapCell(cell, cap);
        }

        private void onCapCell(Cell cell, int cellCap)
        {
            if (regularsIn != 0)
                logger.error("CappedSortedMapType cap cells must appear before all regular cells, ignoring cap {}", cellCap);

            if (capCell == null || cell.timestamp() > capCell.timestamp())
            {
                // Adjust the provided limit by the effective cap.  The user-specified
                // limit will be written through.  This permits controlling runtime behavior
                // if there are issues from longer caps, without losing the user intent.
                cap = effectiveCap(cellCap);
                capCell = cell;
            }
        }

        private void onRegularCell(Cell cell)
        {
            regularsIn++;

            // emit the cap cell before any regular values
            if (regularsOut == 0 && capCell != null)
            {
                // override the local deletion timestamp to prevent the cap (which is a tombstone) from being
                // compacted out when there may be live cells available in the queue.  If no regular cells
                // added to the resolver, then this will not be called and the cap cell will be dropped then.
                if (capCell.localDeletionTime() != CAP_LOCAL_DELETION_TIMESTAMP)
                    capCell = capCell.withUpdatedTimestampAndLocalDeletionTime(capCell.timestamp(), CAP_LOCAL_DELETION_TIMESTAMP);
                builder.addCell(capCell);
            }

            if (regularsOut < cap)
            {
                // Then add the capacity entries
                builder.addCell(cell);
                regularsOut++;
            }
        }

        @Override
        public void endColumn()
        {
            capCell = null;
            regularsIn = 0;
            regularsOut = 0;
            cap = effectiveCap(defaultCap);
            builder.endColumn();
        }

        private int cap(Cell cell)
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
        logger.info("Setting cell resolver enabled from {} to {}", enabled, enable);
        enabled = enable;
    }

    public int getMinEffectiveCap()
    {
        return minEffectiveCap;
    }

    public void setMinEffectiveCap(int minCap)
    {
        int updatedMin = bound(0, minCap, CappedSortedMap.CAP_MAX);
        logger.info("Setting minimum effective cap from {} to {}", maxEffectiveCap, updatedMin);
        minEffectiveCap = updatedMin;
    }

    public int getMaxEffectiveCap()
    {
        return maxEffectiveCap;
    }

    public void setMaxEffectiveCap(int maxCap)
    {
        int updatedMax = bound(0, maxCap, CappedSortedMap.CAP_MAX);

        logger.info("Setting maximum effective cap from {} to {}", maxEffectiveCap, updatedMax);
        maxEffectiveCap = updatedMax;

        int updatedMin = Integer.min(minEffectiveCap, maxEffectiveCap);
        if (updatedMin != minEffectiveCap)
        {
            logger.info("Moving minimum effective cap from {} to {}", minEffectiveCap, updatedMin);
            minEffectiveCap = updatedMin;
        }
    }

    public int getDefaultCap()
    {
        return defaultCap;
    }

    public void setDefaultCap(int cap)
    {
        logger.info("Setting default cap from {} to {}", this.defaultCap, cap);
        this.defaultCap = cap;
    }
}
