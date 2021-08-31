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

package org.apache.cassandra.repair.consistent;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.AbstractPendingRepairTest;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PendingAntiCompactionTest extends AbstractPendingAntiCompactionTest
{
    private static final Logger logger = LoggerFactory.getLogger(PendingAntiCompactionTest.class);
    private static final Collection<Range<Token>> FULL_RANGE;
    static
    {
        DatabaseDescriptor.forceStaticInitialization();
        Token minToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        FULL_RANGE = Collections.singleton(new Range<>(minToken, minToken));
    }

    private static class InstrumentedAcquisitionCallback extends PendingAntiCompaction.AcquisitionCallback
    {
        public InstrumentedAcquisitionCallback(UUID parentRepairSession, Collection<Range<Token>> ranges, BooleanSupplier isCancelled)
        {
            super(parentRepairSession, ranges, isCancelled);
        }

        Set<UUID> submittedCompactions = new HashSet<>();

        ListenableFuture<?> submitPendingAntiCompaction(PendingAntiCompaction.AcquireResult result)
        {
            submittedCompactions.add(result.cfs.metadata.cfId);
            result.abort();  // prevent ref leak complaints
            return ListenableFutureTask.create(() -> {}, null);
        }
    }

    /**
     * verify the pending anti compaction happy path
     */
    @Test
    public void successCase() throws Exception
    {
        Assert.assertSame(ByteOrderedPartitioner.class, DatabaseDescriptor.getPartitioner().getClass());
        cfs.disableAutoCompaction();

        // create 2 sstables, one that will be split, and another that will be moved
        for (int i = 0; i < 8; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, tbl), i, i);
        }
        cfs.forceBlockingFlush();
        for (int i = 8; i < 12; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, tbl), i, i);
        }
        cfs.forceBlockingFlush();
        Assert.assertEquals(2, cfs.getLiveSSTables().size());

        Token left = ByteOrderedPartitioner.instance.getToken(ByteBufferUtil.bytes((int) 6));
        Token right = ByteOrderedPartitioner.instance.getToken(ByteBufferUtil.bytes((int) 16));
        Collection<Range<Token>> ranges = Collections.singleton(new Range<>(left, right));

        // create a session so the anti compaction can fine it
        UUID sessionID = UUIDGen.getTimeUUID();
        ActiveRepairService.instance.registerParentRepairSession(sessionID, InetAddress.getLocalHost(), Lists.newArrayList(cfs), ranges, true, 1, true, PreviewKind.NONE);

        PendingAntiCompaction pac;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            pac = new PendingAntiCompaction(sessionID, ranges, executor, () -> false);
            pac.run().get();
        }
        finally
        {
            executor.shutdown();
        }

        Assert.assertEquals(3, cfs.getLiveSSTables().size());
        int pendingRepair = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.isPendingRepair())
                pendingRepair++;
        }
        Assert.assertEquals(2, pendingRepair);
    }

    @Test
    public void acquisitionSuccess() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(6);
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        List<SSTableReader> expected = sstables.subList(0, 3);
        Collection<Range<Token>> ranges = new HashSet<>();
        for (SSTableReader sstable : expected)
        {
            ranges.add(new Range<>(sstable.first.getToken(), sstable.last.getToken()));
        }

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, ranges, UUIDGen.getTimeUUID(), 0, 0);

        logger.info("SSTables: {}", sstables);
        logger.info("Expected: {}", expected);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);
        logger.info("Originals: {}", result.txn.originals());
        Assert.assertEquals(3, result.txn.originals().size());
        for (SSTableReader sstable : expected)
        {
            logger.info("Checking {}", sstable);
            Assert.assertTrue(result.txn.originals().contains(sstable));
        }

        Assert.assertEquals(Transactional.AbstractTransactional.State.IN_PROGRESS, result.txn.state());
        result.abort();
    }

    @Test
    public void repairedSSTablesAreNotAcquired() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(2, sstables.size());
        SSTableReader repaired = sstables.get(0);
        SSTableReader unrepaired = sstables.get(1);
        Assert.assertTrue(repaired.intersects(FULL_RANGE));
        Assert.assertTrue(unrepaired.intersects(FULL_RANGE));

        repaired.descriptor.getMetadataSerializer().mutateRepaired(repaired.descriptor, 1, null);
        repaired.reloadSSTableMetadata();

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        logger.info("Originals: {}", result.txn.originals());
        Assert.assertEquals(1, result.txn.originals().size());
        Assert.assertTrue(result.txn.originals().contains(unrepaired));
        result.abort(); // release sstable refs
    }

    @Test
    public void finalizedPendingRepairSSTablesAreNotAcquired() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(2, sstables.size());
        SSTableReader repaired = sstables.get(0);
        SSTableReader unrepaired = sstables.get(1);
        Assert.assertTrue(repaired.intersects(FULL_RANGE));
        Assert.assertTrue(unrepaired.intersects(FULL_RANGE));

        UUID sessionId = prepareSession();
        LocalSessionAccessor.finalizeUnsafe(sessionId);
        repaired.descriptor.getMetadataSerializer().mutateRepaired(repaired.descriptor, 0, sessionId);
        repaired.reloadSSTableMetadata();
        Assert.assertTrue(repaired.isPendingRepair());

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        logger.info("Originals: {}", result.txn.originals());
        Assert.assertEquals(1, result.txn.originals().size());
        Assert.assertTrue(result.txn.originals().contains(unrepaired));
        result.abort();  // releases sstable refs
    }


    @Test
    public void conflictingSessionAcquisitionFailure() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        Assert.assertEquals(2, sstables.size());
        SSTableReader repaired = sstables.get(0);
        SSTableReader unrepaired = sstables.get(1);
        Assert.assertTrue(repaired.intersects(FULL_RANGE));
        Assert.assertTrue(unrepaired.intersects(FULL_RANGE));

        UUID sessionId = prepareSession();
        repaired.descriptor.getMetadataSerializer().mutateRepaired(repaired.descriptor, 0, sessionId);
        repaired.reloadSSTableMetadata();
        Assert.assertTrue(repaired.isPendingRepair());

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNull(result);
    }

    @Test
    public void pendingRepairNoSSTablesExist() throws Exception
    {
        cfs.disableAutoCompaction();

        Assert.assertEquals(0, cfs.getLiveSSTables().size());


        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        result.abort();  // There's nothing to release, but we should exit cleanly
    }

    /**
     * anti compaction task should be submitted if everything is ok
     */
    @Test
    public void callbackSuccess() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), FULL_RANGE, () -> false);
        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        cb.apply(Lists.newArrayList(result));

        Assert.assertEquals(1, cb.submittedCompactions.size());
        Assert.assertTrue(cb.submittedCompactions.contains(cfm.cfId));
    }

    /**
     * If one of the supplied AcquireResults is null, either an Exception was thrown, or
     * we couldn't get a transaction for the sstables. In either case we need to cancel the repair, and release
     * any sstables acquired for other tables
     */
    @Test
    public void callbackNullResult() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);
        Assert.assertEquals(Transactional.AbstractTransactional.State.IN_PROGRESS, result.txn.state());

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), FULL_RANGE, () -> false);
        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        cb.apply(Lists.newArrayList(result, null));

        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        Assert.assertEquals(Transactional.AbstractTransactional.State.ABORTED, result.txn.state());
    }

    /**
     * If an AcquireResult has a null txn, there were no sstables to acquire references
     * for, so no anti compaction should have been submitted.
     */
    @Test
    public void callbackNullTxn() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        Assert.assertNotNull(result);

        ColumnFamilyStore cfs2 = Schema.instance.getColumnFamilyStoreIncludingIndexes(Pair.create("system", "peers"));
        PendingAntiCompaction.AcquireResult fakeResult = new PendingAntiCompaction.AcquireResult(cfs2, null, null);

        InstrumentedAcquisitionCallback cb = new InstrumentedAcquisitionCallback(UUIDGen.getTimeUUID(), FULL_RANGE, () -> false);
        Assert.assertTrue(cb.submittedCompactions.isEmpty());
        cb.apply(Lists.newArrayList(result, fakeResult));

        Assert.assertEquals(1, cb.submittedCompactions.size());
        Assert.assertTrue(cb.submittedCompactions.contains(cfm.cfId));
        Assert.assertFalse(cb.submittedCompactions.contains(cfs2.metadata.cfId));
    }


    @Test
    public void singleAnticompaction() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        UUID sessionID = UUIDGen.getTimeUUID();
        ActiveRepairService.instance.registerParentRepairSession(sessionID,
                                                                 InetAddress.getByName("127.0.0.1"),
                                                                 Lists.newArrayList(cfs),
                                                                 FULL_RANGE,
                                                                 true,0,
                                                                 true,
                                                                 PreviewKind.NONE);
        CompactionManager.instance.performAnticompaction(result.cfs, FULL_RANGE, result.refs, result.txn,
                                                         ActiveRepairService.UNREPAIRED_SSTABLE, sessionID, sessionID, () -> false);

    }

    @Test (expected = CompactionInterruptedException.class)
    public void cancelledAntiCompaction() throws Exception
    {
        cfs.disableAutoCompaction();
        makeSSTables(1);

        PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, FULL_RANGE, UUIDGen.getTimeUUID(), 0, 0);
        PendingAntiCompaction.AcquireResult result = acquisitionCallable.call();
        UUID sessionID = UUIDGen.getTimeUUID();
        ActiveRepairService.instance.registerParentRepairSession(sessionID,
                                                                 InetAddress.getByName("127.0.0.1"),
                                                                 Lists.newArrayList(cfs),
                                                                 FULL_RANGE,
                                                                 true,0,
                                                                 true,
                                                                 PreviewKind.NONE);

        // attempt to anti-compact the sstable in half
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        Token left = cfs.getPartitioner().midpoint(sstable.first.getToken(), sstable.last.getToken());
        Token right = sstable.last.getToken();
        CompactionManager.instance.performAnticompaction(result.cfs, Collections.singleton(new Range<>(left, right)),
                                                         result.refs, result.txn, ActiveRepairService.UNREPAIRED_SSTABLE,
                                                         sessionID, sessionID, () -> true);
    }

    /**
     * Makes sure that PendingAntiCompaction fails when anticompaction throws exception
     */
    @Test
    public void antiCompactionException() throws NoSuchRepairSessionException
    {
        cfs.disableAutoCompaction();
        makeSSTables(2);
        UUID prsid = prepareSession();
        ListeningExecutorService es = MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService());
        PendingAntiCompaction pac = new PendingAntiCompaction(prsid, FULL_RANGE, es, () -> false) {
            @Override
            protected AcquisitionCallback getAcquisitionCallback(UUID prsId, Collection<Range<Token>> tokenRanges)
            {
                return new AcquisitionCallback(prsid, tokenRanges, () -> false)
                {
                    @Override
                    ListenableFuture<?> submitPendingAntiCompaction(AcquireResult result)
                    {
                        Runnable r = new WrappedRunnable()
                        {
                            protected void runMayThrow()
                            {
                                throw new CompactionInterruptedException("test anticompaction exception");
                            }
                        };
                        return es.submit(r);
                    }
                };
            }
        };
        ListenableFuture<?> fut = pac.run();
        try
        {
            fut.get();
            Assert.fail("Should throw exception");
        }
        catch(Throwable t)
        {
        }
    }


    @Test
    public void testBlockedAcquisition() throws ExecutionException, InterruptedException, TimeoutException, NoSuchRepairSessionException
    {
        cfs.disableAutoCompaction();
        ExecutorService es = Executors.newFixedThreadPool(1);

        makeSSTables(2);
        UUID prsid = prepareSession();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        List<ISSTableScanner> scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
        try
        {
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                 CompactionController controller = new CompactionController(cfs, sstables, 0);
                 CompactionIterator ci = CompactionManager.getAntiCompactionIterator(scanners, controller, 0, UUID.randomUUID(), CompactionManager.instance.active, () -> false))
            {
                // `ci` is our imaginary ongoing anticompaction which makes no progress until after 30s
                // now we try to start a new AC, which will try to cancel all ongoing compactions

                CompactionManager.instance.active.beginCompaction(ci);
                PendingAntiCompaction pac = new PendingAntiCompaction(prsid, FULL_RANGE, 0, 0, es, () -> false);
                ListenableFuture fut = pac.run();
                try
                {
                    fut.get(30, TimeUnit.SECONDS);
                    fail("the future should throw exception since we try to start a new anticompaction when one is already running");
                }
                catch (ExecutionException e)
                {
                    assertTrue(e.getCause() instanceof PendingAntiCompaction.SSTableAcquisitionException);
                }

                assertEquals(1, getCompactionsFor(cfs).size());
                for (CompactionInfo.Holder holder : getCompactionsFor(cfs))
                    assertFalse(holder.isStopRequested());
            }
        }
        finally
        {
            es.shutdown();
            ISSTableScanner.closeAllAndPropagate(scanners, null);
        }
    }


    private List<CompactionInfo.Holder> getCompactionsFor(ColumnFamilyStore cfs)
    {
        List<CompactionInfo.Holder> compactions = new ArrayList<>();
        for (CompactionInfo.Holder holder : CompactionManager.instance.active.getCompactions())
        {
            if (holder.getCompactionInfo().getCFMetaData().equals(cfs.metadata))
                compactions.add(holder);
        }
        return compactions;
    }


    @Test
    public void testUnblockedAcquisition() throws ExecutionException, InterruptedException, NoSuchRepairSessionException
    {
        cfs.disableAutoCompaction();
        ExecutorService es = Executors.newFixedThreadPool(1);
        makeSSTables(2);
        UUID prsid = prepareSession();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        List<ISSTableScanner> scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
        try
        {
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                 CompactionController controller = new CompactionController(cfs, sstables, 0);
                 CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners, controller, 0, UUID.randomUUID()))
            {
                // `ci` is our imaginary ongoing anticompaction which makes no progress until after 5s
                // now we try to start a new AC, which will try to cancel all ongoing compactions

                CompactionManager.instance.active.beginCompaction(ci);
                PendingAntiCompaction pac = new PendingAntiCompaction(prsid, FULL_RANGE, es, () -> false);
                ListenableFuture fut = pac.run();
                try
                {
                    fut.get(5, TimeUnit.SECONDS);
                }
                catch (TimeoutException e)
                {
                    // expected, we wait 1 minute for compactions to get cancelled in runWithCompactionsDisabled, but we are not iterating
                    // CompactionIterator so the compaction is not actually cancelled
                }
                try
                {
                    assertTrue(ci.hasNext());
                    ci.next();
                    fail("CompactionIterator should be abortable");
                }
                catch (CompactionInterruptedException e)
                {
                    CompactionManager.instance.active.finishCompaction(ci);
                    txn.abort();
                    // expected
                }
                CountDownLatch cdl = new CountDownLatch(1);
                Futures.addCallback(fut, new FutureCallback<Object>()
                {
                    public void onSuccess(@Nullable Object o)
                    {
                        cdl.countDown();
                    }

                    public void onFailure(Throwable throwable)
                    {
                    }
                });
                assertTrue(cdl.await(1, TimeUnit.MINUTES));
            }
        }
        finally
        {
            es.shutdown();
        }
    }

    @Test
    public void testSSTablePredicateOngoingAntiCompaction()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        cfs.disableAutoCompaction();
        List<SSTableReader> sstables = new ArrayList<>();
        List<SSTableReader> repairedSSTables = new ArrayList<>();
        List<SSTableReader> pendingSSTables = new ArrayList<>();
        for (int i = 1; i <= 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(i, i * 10, i * 10 + 9, cfs);
            sstables.add(sstable);
        }
        for (int i = 1; i <= 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(i + 10, i * 10, i * 10 + 9, cfs);
            AbstractPendingRepairTest.mutateRepaired(sstable, System.currentTimeMillis());
            repairedSSTables.add(sstable);
        }
        for (int i = 1; i <= 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(i + 20, i * 10, i * 10 + 9, cfs);
            AbstractPendingRepairTest.mutateRepaired(sstable, UUID.randomUUID());
            pendingSSTables.add(sstable);
        }

        cfs.addSSTables(sstables);
        cfs.addSSTables(repairedSSTables);

        // if we are compacting the non-repaired non-pending sstables, we should get an error
        tryPredicate(cfs, sstables, null, true);
        // make sure we don't try to grab pending or repaired sstables;
        tryPredicate(cfs, repairedSSTables, sstables, false);
        tryPredicate(cfs, pendingSSTables, sstables, false);
    }

    private void tryPredicate(ColumnFamilyStore cfs, List<SSTableReader> compacting, List<SSTableReader> expectedLive, boolean shouldFail)
    {
        CompactionInfo.Holder holder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata, OperationType.ANTICOMPACTION, 0, 1000, UUID.randomUUID(), compacting);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };
        CompactionManager.instance.active.beginCompaction(holder);
        try
        {
            PendingAntiCompaction.AntiCompactionPredicate predicate =
            new PendingAntiCompaction.AntiCompactionPredicate(Collections.singleton(new Range<>(new Murmur3Partitioner.LongToken(0), new Murmur3Partitioner.LongToken(100))),
                                                              UUID.randomUUID());
            Set<SSTableReader> live = Sets.newHashSet(Iterables.filter(cfs.getLiveSSTables(), predicate));
            if (shouldFail)
                fail("should fail - we try to grab already anticompacting sstables for anticompaction");
            assertEquals(live, new HashSet<>(expectedLive));
        }
        catch (PendingAntiCompaction.SSTableAcquisitionException e)
        {
            if (!shouldFail)
                fail("We should not fail filtering sstables");
        }
        finally
        {
            CompactionManager.instance.active.finishCompaction(holder);
        }
    }

    @Test
    public void testRetries() throws InterruptedException, ExecutionException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        cfs.addSSTable(MockSchema.sstable(1, true, cfs));
        CountDownLatch cdl = new CountDownLatch(5);
        ExecutorService es = Executors.newFixedThreadPool(1);
        CompactionInfo.Holder holder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata, OperationType.ANTICOMPACTION, 0, 0, UUID.randomUUID(), cfs.getLiveSSTables());
            }

            public boolean isGlobal()
            {
                return false;
            }
        };
        try
        {
            PendingAntiCompaction.AntiCompactionPredicate acp = new PendingAntiCompaction.AntiCompactionPredicate(FULL_RANGE, UUID.randomUUID())
            {
                @Override
                public boolean apply(SSTableReader sstable)
                {
                    cdl.countDown();
                    if (cdl.getCount() > 0)
                        throw new PendingAntiCompaction.SSTableAcquisitionException("blah");
                    return true;
                }
            };
            CompactionManager.instance.active.beginCompaction(holder);
            PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, UUID.randomUUID(), 10, 1, acp);
            Future f = es.submit(acquisitionCallable);
            cdl.await();
            assertNotNull(f.get());
        }
        finally
        {
            es.shutdown();
            CompactionManager.instance.active.finishCompaction(holder);
        }
    }

    @Test
    public void testRetriesTimeout() throws InterruptedException, ExecutionException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        cfs.addSSTable(MockSchema.sstable(1, true, cfs));
        ExecutorService es = Executors.newFixedThreadPool(1);
        CompactionInfo.Holder holder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata, OperationType.ANTICOMPACTION, 0, 0, UUID.randomUUID(), cfs.getLiveSSTables());
            }

            public boolean isGlobal()
            {
                return false;
            }
        };
        try
        {
            PendingAntiCompaction.AntiCompactionPredicate acp = new PendingAntiCompaction.AntiCompactionPredicate(FULL_RANGE, UUID.randomUUID())
            {
                @Override
                public boolean apply(SSTableReader sstable)
                {
                    throw new PendingAntiCompaction.SSTableAcquisitionException("blah");
                }
            };
            CompactionManager.instance.active.beginCompaction(holder);
            PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, UUID.randomUUID(), 2, 1000, acp);
            Future fut = es.submit(acquisitionCallable);
            assertNull(fut.get());
        }
        finally
        {
            es.shutdown();
            CompactionManager.instance.active.finishCompaction(holder);
        }
    }

    @Test
    public void testWith2i() throws ExecutionException, InterruptedException, NoSuchRepairSessionException
    {
        cfs2.disableAutoCompaction();
        makeSSTables(2, cfs2, 100);
        cfs2.forceBlockingFlush();
        ColumnFamilyStore idx = cfs2.indexManager.getAllIndexColumnFamilyStores().iterator().next();
        ExecutorService es = Executors.newFixedThreadPool(1);
        try
        {
            UUID prsid = prepareSession(cfs2);
            for (SSTableReader sstable : cfs2.getLiveSSTables())
                assertFalse(sstable.isPendingRepair());

            // mark the sstables pending, with a 2i compaction going, which should be untouched;
            try (LifecycleTransaction txn = idx.getTracker().tryModify(idx.getLiveSSTables(), OperationType.COMPACTION))
            {
                PendingAntiCompaction pac = new PendingAntiCompaction(prsid, FULL_RANGE, es, () -> false);
                pac.run().get();
            }
            // and make sure it succeeded;
            for (SSTableReader sstable : cfs2.getLiveSSTables())
                assertTrue(sstable.isPendingRepair());
        }
        finally
        {
            es.shutdown();
        }
    }

}
