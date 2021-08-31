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

package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/**
 * Tests CompactionStrategyManager's handling of pending repair sstables
 */
public class CompactionStrategyManagerPendingRepairTest extends AbstractPendingRepairTest
{
    /**
     * Pending repair strategy should be created when we encounter a new pending id
     */
    @Test
    public void sstableAdded()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        Assert.assertTrue(csm.pendingRepairs().isEmpty());

        SSTableReader sstable = makeSSTable(true);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());

        mutateRepaired(sstable, repairID);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertNull(csm.getForPendingRepair(repairID));

        // add the sstable
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable)), cfs.getTracker());
        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable));
        Assert.assertNotNull(csm.getForPendingRepair(repairID));
        Assert.assertTrue(csm.getForPendingRepair(repairID).getSSTables().contains(sstable));
    }

    @Test
    public void sstableListChangedAddAndRemove()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable1 = makeSSTable(true);
        mutateRepaired(sstable1, repairID);

        SSTableReader sstable2 = makeSSTable(true);
        mutateRepaired(sstable2, repairID);

        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable1));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable1));
        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable2));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable2));
        Assert.assertNull(csm.getForPendingRepair(repairID));

        // add only
        SSTableListChangedNotification notification;
        notification = new SSTableListChangedNotification(Collections.singleton(sstable1),
                                                          Collections.emptyList(),
                                                          OperationType.COMPACTION);
        csm.handleNotification(notification, cfs.getTracker());

        Assert.assertNotNull(csm.getForPendingRepair(repairID));
        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable1));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable1));
        Assert.assertTrue(csm.getForPendingRepair(repairID).getSSTables().contains(sstable1));
        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable2));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable2));
        Assert.assertFalse(csm.getForPendingRepair(repairID).getSSTables().contains(sstable2));

        // remove and add
        notification = new SSTableListChangedNotification(Collections.singleton(sstable2),
                                                          Collections.singleton(sstable1),
                                                          OperationType.COMPACTION);
        csm.handleNotification(notification, cfs.getTracker());

        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable1));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable1));
        Assert.assertFalse(csm.getForPendingRepair(repairID).getSSTables().contains(sstable1));
        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable2));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable2));
        Assert.assertTrue(csm.getForPendingRepair(repairID).getSSTables().contains(sstable2));
    }

    @Test
    public void sstableRepairStatusChanged()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        // add as unrepaired
        SSTableReader sstable = makeSSTable(false);
        Assert.assertTrue(csm.getUnrepaired().getSSTables().contains(sstable));
        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable));
        Assert.assertNull(csm.getForPendingRepair(repairID));

        SSTableRepairStatusChanged notification;

        // change to pending repaired
        mutateRepaired(sstable, repairID);
        notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable));
        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable));
        Assert.assertNotNull(csm.getForPendingRepair(repairID));
        Assert.assertTrue(csm.getForPendingRepair(repairID).getSSTables().contains(sstable));

        // change to repaired
        mutateRepaired(sstable, System.currentTimeMillis());
        notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable));
        Assert.assertTrue(csm.getRepaired().getSSTables().contains(sstable));
        Assert.assertNull(csm.getForPendingRepair(repairID));
    }

    @Test
    public void sstableDeleted()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable)), cfs.getTracker());
        Assert.assertTrue(csm.getForPendingRepair(repairID).getSSTables().contains(sstable));

        // delete sstable
        SSTableDeletingNotification notification = new SSTableDeletingNotification(sstable);
        csm.handleNotification(notification, cfs.getTracker());
        Assert.assertNull(csm.getForPendingRepair(repairID));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable));
        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable));
    }

    /**
     * CompactionStrategyManager.getStrategies should include
     * pending repair strategies when appropriate
     */
    @Test
    public void getStrategies()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        List<AbstractCompactionStrategy> strategies = csm.getStrategies();
        assertEquals(2, strategies.size());
        Assert.assertTrue(strategies.contains(csm.getRepaired()));
        Assert.assertTrue(strategies.contains(csm.getUnrepaired()));

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable)), cfs.getTracker());

        strategies = csm.getStrategies();
        assertEquals(3, strategies.size());
        Assert.assertTrue(strategies.contains(csm.getRepaired()));
        Assert.assertTrue(strategies.contains(csm.getUnrepaired()));
        Assert.assertTrue(strategies.contains(csm.getForPendingRepair(repairID)));
    }

    /**
     * Tests that finalized repairs result in cleanup compaction tasks
     * which reclassify the sstables as repaired
     */
    @Test
    public void cleanupCompactionFinalized() throws NoSuchRepairSessionException
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable)), cfs.getTracker());
        LocalSessionAccessor.finalizeUnsafe(repairID);
        Assert.assertNotNull(csm.getForPendingRepair(repairID));
        Assert.assertTrue(csm.getForPendingRepair(repairID).getSSTables().contains(sstable));
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());

        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute(ActiveCompactionsTracker.NOOP);

        Assert.assertTrue(csm.getRepaired().getSSTables().contains(sstable));
        Assert.assertFalse(csm.getUnrepaired().getSSTables().contains(sstable));
        Assert.assertNull(csm.getForPendingRepair(repairID));

        // sstable should have pendingRepair cleared, and repairedAt set correctly
        long expectedRepairedAt = ActiveRepairService.instance.getParentRepairSession(repairID).repairedAt;
        Assert.assertFalse(sstable.isPendingRepair());
        Assert.assertTrue(sstable.isRepaired());
        assertEquals(expectedRepairedAt, sstable.getSSTableMetadata().repairedAt);
    }

    @Test
    public void testMultipleReloads() throws IOException
    {
        cfs.setCompactionParameters(ImmutableMap.of("class", "LeveledCompactionStrategy"));
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        SSTableReader sstable2 = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        mutateRepaired(sstable2, repairID);

        // change level to confuse LCS when getting the same sstable multiple times
        sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 2);
        sstable.reloadSSTableMetadata();
        csm.handleNotification(new SSTableAddedNotification(Sets.newHashSet(sstable, sstable2)), cfs.getTracker());

        csm.reloadWithWriteLock(cfs.metadata);
        csm.reloadWithWriteLock(cfs.metadata);
        csm.reloadWithWriteLock(cfs.metadata);

        int foundStrategies = 0;
        for (AbstractCompactionStrategy strat : csm.getPendingRepairManager().getStrategies())
        {
            foundStrategies++;
            if (strat.getSSTables().size() == 2)
                assertEquals(ImmutableSet.of(sstable, sstable2), strat.getSSTables());
            else if (strat.getSSTables().size() != 0)
                Assert.fail("there should only be 2 sstables in total in the strategies, not "+strat.getSSTables());
        }
        assertEquals(1, foundStrategies);
        LocalSessionAccessor.finalizeUnsafe(repairID);
        int compactionCount = 0;

        while (csm.getForPendingRepair(repairID) != null)
        {
            AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
            Assert.assertNotNull(compactionTask);
            Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());
            // run the compaction
            compactionTask.execute(ActiveCompactionsTracker.NOOP);
            compactionCount++;
        }
        assertEquals(1, compactionCount);
    }

    /**
     * Tests that failed repairs result in cleanup compaction tasks
     * which reclassify the sstables as unrepaired
     */
    @Test
    public void cleanupCompactionFailed()
    {
        UUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID);
        csm.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable)), cfs.getTracker());
        LocalSessionAccessor.failUnsafe(repairID);

        Assert.assertNotNull(csm.getForPendingRepair(repairID));
        Assert.assertNotNull(csm.getForPendingRepair(repairID).getSSTables().contains(sstable));
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());

        AbstractCompactionTask compactionTask = csm.getNextBackgroundTask(FBUtilities.nowInSeconds());
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(PendingRepairManager.RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute(ActiveCompactionsTracker.NOOP);

        Assert.assertFalse(csm.getRepaired().getSSTables().contains(sstable));
        Assert.assertTrue(csm.getUnrepaired().getSSTables().contains(sstable));
        Assert.assertNull(csm.getForPendingRepair(repairID));

        // sstable should have pendingRepair cleared, and repairedAt set correctly
        Assert.assertFalse(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());
        assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().repairedAt);
    }
}
