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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.consistent.PendingAntiCompaction;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CancelCompactionsTest
{
    /**
     * makes sure we only cancel compactions if the precidate says we have overlapping sstables
     */
    @Test
    public void cancelTest() throws InterruptedException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<SSTableReader> sstables = createSSTables(cfs, 10, 0);
        Set<SSTableReader> toMarkCompacting = new HashSet<>(sstables.subList(0, 3));

        TestCompactionTask tct = new TestCompactionTask(cfs, toMarkCompacting);
        try
        {
            tct.start();

            List<CompactionInfo.Holder> activeCompactions = CompactionManager.instance.active.getCompactions();
            assertEquals(1, activeCompactions.size());
            assertEquals(activeCompactions.get(0).getCompactionInfo().getSSTables(), toMarkCompacting);
            // predicate requires the non-compacting sstables, should not cancel the one currently compacting:
            cfs.runWithCompactionsDisabled(() -> null, (sstable) -> !toMarkCompacting.contains(sstable), false, false);
            assertEquals(1, activeCompactions.size());
            assertFalse(activeCompactions.get(0).isStopRequested());

            // predicate requires the compacting ones - make sure stop is requested and that when we abort that
            // compaction we actually run the callable (countdown the latch)
            CountDownLatch cdl = new CountDownLatch(1);
            Thread t = new Thread(() -> cfs.runWithCompactionsDisabled(() -> { cdl.countDown(); return null; }, toMarkCompacting::contains, false, false));
            t.start();
            while (!activeCompactions.get(0).isStopRequested())
                Thread.sleep(100);

            // cdl.countDown will not get executed until we have aborted all compactions for the sstables in toMarkCompacting
            assertFalse(cdl.await(2, TimeUnit.SECONDS));
            tct.abort();
            // now the compactions are aborted and we can successfully wait for the latch
            t.join();
            assertTrue(cdl.await(2, TimeUnit.SECONDS));
        }
        finally
        {
            tct.abort();
        }
    }

    /**
     * make sure we only cancel relevant compactions when there are multiple ongoing compactions
     */
    @Test
    public void multipleCompactionsCancelTest() throws InterruptedException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<SSTableReader> sstables = createSSTables(cfs, 10, 0);

        List<TestCompactionTask> tcts = new ArrayList<>();
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(0, 3))));
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(6, 9))));

        try
        {
            tcts.forEach(TestCompactionTask::start);

            List<CompactionInfo.Holder> activeCompactions = CompactionManager.instance.active.getCompactions();
            assertEquals(2, activeCompactions.size());

            Set<Set<SSTableReader>> compactingSSTables = new HashSet<>();
            compactingSSTables.add(activeCompactions.get(0).getCompactionInfo().getSSTables());
            compactingSSTables.add(activeCompactions.get(1).getCompactionInfo().getSSTables());
            Set<Set<SSTableReader>> expectedSSTables = new HashSet<>();
            expectedSSTables.add(new HashSet<>(sstables.subList(0, 3)));
            expectedSSTables.add(new HashSet<>(sstables.subList(6, 9)));
            assertEquals(compactingSSTables, expectedSSTables);

            cfs.runWithCompactionsDisabled(() -> null, (sstable) -> false, false, false);
            assertEquals(2, activeCompactions.size());
            assertTrue(activeCompactions.stream().noneMatch(CompactionInfo.Holder::isStopRequested));

            CountDownLatch cdl = new CountDownLatch(1);
            // start a compaction which only needs the sstables where first token is > 50 - these are the sstables compacted by tcts.get(1)
            Thread t = new Thread(() -> cfs.runWithCompactionsDisabled(() -> { cdl.countDown(); return null; }, (sstable) -> first(sstable) > 50, false, false));
            t.start();
            activeCompactions = CompactionManager.instance.active.getCompactions();
            assertEquals(2, activeCompactions.size());
            Thread.sleep(500);
            for (CompactionInfo.Holder holder : activeCompactions)
            {
                if (holder.getCompactionInfo().getSSTables().containsAll(sstables.subList(6, 9)))
                    assertTrue(holder.isStopRequested());
                else
                    assertFalse(holder.isStopRequested());
            }
            tcts.get(1).abort();
            assertEquals(1, CompactionManager.instance.active.getCompactions().size());
            cdl.await();
            t.join();
        }
        finally
        {
            tcts.forEach(TestCompactionTask::abort);
        }
    }

    /**
     * Makes sure sub range compaction now only cancels the relevant compactions, not all of them
     */
    @Test
    public void testSubrangeCompaction() throws InterruptedException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<SSTableReader> sstables = createSSTables(cfs, 10, 0);

        List<TestCompactionTask> tcts = new ArrayList<>();
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(0, 2))));
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(3, 4))));
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(5, 7))));
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(8, 9))));
        try
        {
            tcts.forEach(TestCompactionTask::start);

            List<CompactionInfo.Holder> activeCompactions = CompactionManager.instance.active.getCompactions();
            assertEquals(4, activeCompactions.size());
            Range<Token> range = new Range<>(token(0), token(49));
            Thread t = new Thread(() -> {
                try
                {
                    cfs.forceCompactionForTokenRange(Collections.singleton(range));
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });

            t.start();

            Thread.sleep(500);
            assertEquals(4, CompactionManager.instance.active.getCompactions().size());
            List<TestCompactionTask> toAbort = new ArrayList<>();
            for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
            {
                if (holder.getCompactionInfo().getSSTables().stream().anyMatch(sstable -> sstable.intersects(Collections.singleton(range))))
                {
                    assertTrue(holder.isStopRequested());
                    for (TestCompactionTask tct : tcts)
                        if (tct.sstables.equals(holder.getCompactionInfo().getSSTables()))
                            toAbort.add(tct);
                }
                else
                    assertFalse(holder.isStopRequested());
            }
            assertEquals(2, toAbort.size());
            toAbort.forEach(TestCompactionTask::abort);
            t.join();

        }
        finally
        {
            tcts.forEach(TestCompactionTask::abort);
        }
    }

    @Test
    public void testAnticompaction() throws InterruptedException, ExecutionException
    {
        // MockSchema flushing does not work in 3.0 it seems;
        ColumnFamilyStore cfs = MockSchema.newCFSOverrideFlush();

        List<SSTableReader> sstables = createSSTables(cfs, 10, 0);
        List<SSTableReader> alreadyRepairedSSTables = createSSTables(cfs, 10, 10);
        for (SSTableReader sstable : alreadyRepairedSSTables)
            AbstractPendingRepairTest.mutateRepaired(sstable, System.currentTimeMillis());
        assertEquals(20, cfs.getLiveSSTables().size());
        List<TestCompactionTask> tcts = new ArrayList<>();
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(0, 2))));
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(3, 4))));
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(5, 7))));
        tcts.add(new TestCompactionTask(cfs, new HashSet<>(sstables.subList(8, 9))));

        List<TestCompactionTask> nonAffectedTcts = new ArrayList<>();
        nonAffectedTcts.add(new TestCompactionTask(cfs, new HashSet<>(alreadyRepairedSSTables)));

        try
        {
            tcts.forEach(TestCompactionTask::start);
            nonAffectedTcts.forEach(TestCompactionTask::start);
            List<CompactionInfo.Holder> activeCompactions = CompactionManager.instance.active.getCompactions();
            assertEquals(5, activeCompactions.size());
            // make sure that sstables are fully contained so that the metadata gets mutated
            Range<Token> range = new Range<>(token(-1), token(49));

            UUID prsid = UUID.randomUUID();
            ActiveRepairService.instance.registerParentRepairSession(prsid, FBUtilities.getBroadcastAddress(), Collections.singletonList(cfs), Collections.singleton(range), true, 1, true, PreviewKind.NONE);

            PendingAntiCompaction pac = new PendingAntiCompaction(prsid, Collections.singleton(range), Executors.newSingleThreadExecutor());
            Future<?> fut = pac.run();
            Thread.sleep(600);
            List<TestCompactionTask> toAbort = new ArrayList<>();
            for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
            {
                if (holder.getCompactionInfo().getSSTables().stream().anyMatch(sstable -> sstable.intersects(Collections.singleton(range)) && !sstable.isRepaired() && !sstable.isPendingRepair()))
                {
                    assertTrue(holder.isStopRequested());
                    for (TestCompactionTask tct : tcts)
                        if (tct.sstables.equals(holder.getCompactionInfo().getSSTables()))
                            toAbort.add(tct);
                }
                else
                    assertFalse(holder.isStopRequested());
            }
            assertEquals(2, toAbort.size());
            toAbort.forEach(TestCompactionTask::abort);
            fut.get();
            for (SSTableReader sstable : sstables)
                assertTrue(!sstable.intersects(Collections.singleton(range)) || sstable.isPendingRepair());
        }
        finally
        {
            tcts.forEach(TestCompactionTask::abort);
            nonAffectedTcts.forEach(TestCompactionTask::abort);
        }
    }

    /**
     * Make sure index rebuilds get cancelled
     */
    @Test
    public void testIndexRebuild() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<SSTableReader> sstables = createSSTables(cfs, 5, 0);
        Index idx = new StubIndex(cfs, null);
        CountDownLatch indexBuildStarted = new CountDownLatch(1);
        CountDownLatch indexBuildRunning = new CountDownLatch(1);
        CountDownLatch compactionsStopped = new CountDownLatch(1);
        ReducingKeyIterator reducingKeyIterator = new ReducingKeyIterator(sstables)
        {
            @Override
            public boolean hasNext()
            {
                indexBuildStarted.countDown();
                try
                {
                    indexBuildRunning.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException();
                }
                return false;
            }
        };
        Future<?> f = CompactionManager.instance.submitIndexBuild(new SecondaryIndexBuilder(cfs, Collections.singleton(idx), reducingKeyIterator, ImmutableSet.copyOf(sstables)));
        // wait for hasNext to get called
        indexBuildStarted.await();
        assertEquals(1, CompactionManager.instance.active.getCompactions().size());
        boolean foundCompaction = false;
        for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
        {
            if (holder.getCompactionInfo().getSSTables().equals(new HashSet<>(sstables)))
            {
                assertFalse(holder.isStopRequested());
                foundCompaction = true;
            }
        }
        assertTrue(foundCompaction);
        cfs.runWithCompactionsDisabled(() -> {compactionsStopped.countDown(); return null;}, (sstable) -> true, false, false);
        // wait for the runWithCompactionsDisabled callable
        compactionsStopped.await();
        assertEquals(1, CompactionManager.instance.active.getCompactions().size());
        foundCompaction = false;
        for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
        {
            if (holder.getCompactionInfo().getSSTables().equals(new HashSet<>(sstables)))
            {
                assertTrue(holder.isStopRequested());
                foundCompaction = true;
            }
        }
        assertTrue(foundCompaction);
        // signal that the index build should be finished
        indexBuildRunning.countDown();
        f.get();
        assertTrue(CompactionManager.instance.active.getCompactions().isEmpty());
    }

    long first(SSTableReader sstable)
    {
        return (long)sstable.first.getToken().getTokenValue();
    }

    Token token(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private List<SSTableReader> createSSTables(ColumnFamilyStore cfs, int count, int startGeneration)
    {
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            long first = i * 10;
            long last  = (i + 1) * 10 - 1;
            sstables.add(MockSchema.sstable(startGeneration + i, 0, true, cfs, MockSchema.readerBounds(first), MockSchema.readerBounds(last)));
        }
        cfs.disableAutoCompaction();
        cfs.addSSTables(sstables);
        return sstables;
    }

    private static class TestCompactionTask
    {
        private ColumnFamilyStore cfs;
        private final Set<SSTableReader> sstables;
        private LifecycleTransaction txn;
        private CompactionController controller;
        private CompactionIterator ci;
        private List<ISSTableScanner> scanners;

        public TestCompactionTask(ColumnFamilyStore cfs, Set<SSTableReader> sstables)
        {
            this.cfs = cfs;
            this.sstables = sstables;
        }

        public void start()
        {
            scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
            txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
            assertNotNull(txn);
            controller = new CompactionController(cfs, sstables, Integer.MIN_VALUE);
            ci = new CompactionIterator(txn.opType(), scanners, controller, FBUtilities.nowInSeconds(), UUID.randomUUID());
            CompactionManager.instance.active.beginCompaction(ci);
        }

        public void abort()
        {
            if (controller != null)
                controller.close();
            if (ci != null)
                ci.close();
            if (txn != null)
                txn.abort();
            if (scanners != null)
                scanners.forEach(ISSTableScanner::close);
            CompactionManager.instance.active.finishCompaction(ci);

        }
    }
}
