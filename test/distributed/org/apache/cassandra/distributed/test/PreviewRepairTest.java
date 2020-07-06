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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.IInvokableInstance;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PreviewRepairTest extends DistributedTestBase
{
    /**
     * makes sure that the repaired sstables are not matching on the two
     * nodes by disabling autocompaction on node2 and then running an
     * incremental repair
     */
    @Test
    public void testWithMismatchingPending() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            Thread.sleep(1000);
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).callOnInstance(repair(options(false, false)));
            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // make sure that all sstables have moved to repaired by triggering a compaction
            // also disables autocompaction on the nodes
            cluster.forEach((node) -> node.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
                cfs.disableAutoCompaction();
            }));
            cluster.get(1).callOnInstance(repair(options(false, false)));
            // now re-enable autocompaction on node1, this moves the sstables for the new repair to repaired
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                cfs.enableAutoCompaction();
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
            });
            Pair<Boolean, Boolean> rs = cluster.get(1).callOnInstance(repair(options(true, false)));
            assertTrue(rs.left); // preview repair should succeed
            assertFalse(rs.right); // and we should see no mismatches
        }
    }

    /**
     * another case where the repaired datasets could mismatch is if an incremental repair finishes just as the preview
     * repair is starting up.
     *
     * This tests this case:
     * 1. we start a preview repair
     * 2. pause the validation requests from node1 -> node2
     * 3. node1 starts its validation
     * 4. run an incremental repair which completes fine
     * 5. node2 resumes its validation
     *
     * Now we will include sstables from the second incremental repair on node2 but not on node1
     * This should fail since we fail any preview repair which is ongoing when an incremental repair finishes (step 4 above)
     */
    @Test
    public void testFinishingIncRepairDuringPreview() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            Thread.sleep(2000);
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).callOnInstance(repair(options(false, false)));

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            SimpleCondition previewRepairStarted = new SimpleCondition();
            SimpleCondition continuePreviewRepair = new SimpleCondition();
            DelayFirstRepairTypeMessageFilter filter = DelayFirstRepairTypeMessageFilter.validationRequest(previewRepairStarted, continuePreviewRepair);
            // this pauses the validation request sent from node1 to node2 until the inc repair below has completed
            cluster.filters().verbs(MessagingService.Verb.REPAIR_MESSAGE.ordinal()).from(1).to(2).messagesMatching(filter).drop();

            Future<Pair<Boolean, Boolean>> rsFuture = es.submit(() -> cluster.get(1).callOnInstance(repair(options(true, false))));
            previewRepairStarted.await();
            // this needs to finish before the preview repair is unpaused on node2
            cluster.get(1).callOnInstance(repair(options(false, false)));
            continuePreviewRepair.signalAll();
            Pair<Boolean, Boolean> rs = rsFuture.get();
            assertFalse(rs.left); // preview repair should have failed
            assertFalse(rs.right); // and no mismatches should have been reported
        }
        finally
        {
            es.shutdown();
        }
    }

    /**
     * Tests that a IR is running, but not completed before validation compaction starts
     */
    @Test
    public void testConcurrentIncRepairDuringPreview() throws IOException, InterruptedException, ExecutionException
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            Thread.sleep(2000);
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).callOnInstance(repair(options(false, false)));

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            SimpleCondition previewRepairStarted = new SimpleCondition();
            SimpleCondition continuePreviewRepair = new SimpleCondition();
            // this pauses the validation request sent from node1 to node2 until the inc repair below has run
            cluster.filters()
                   .verbs(MessagingService.Verb.REPAIR_MESSAGE.ordinal())
                   .from(1).to(2)
                   .messagesMatching(DelayFirstRepairTypeMessageFilter.validationRequest(previewRepairStarted, continuePreviewRepair))
                   .drop();

            SimpleCondition irRepairStarted = new SimpleCondition();
            SimpleCondition continueIrRepair = new SimpleCondition();
            // this blocks the IR from committing, so we can reenale the preview
            cluster.filters()
                   .verbs(MessagingService.Verb.REPAIR_MESSAGE.ordinal())
                   .from(1).to(2)
                   .messagesMatching(DelayFirstRepairTypeMessageFilter.finalizePropose(irRepairStarted, continueIrRepair))
                   .drop();

            Future<Pair<Boolean, Boolean>> previewResult = cluster.get(1).asyncCallsOnInstance(repair(options(true, false))).call();
            previewRepairStarted.await();

            // trigger IR and wait till its ready to commit
            Future<Pair<Boolean, Boolean>> irResult = cluster.get(1).asyncCallsOnInstance(repair(options(false, false))).call();
            irRepairStarted.await();

            // unblock preview repair and wait for it to complete
            continuePreviewRepair.signalAll();

            Pair<Boolean, Boolean> rs = previewResult.get();
            assertFalse(rs.left); // preview repair should have failed
            assertFalse(rs.right); // and no mismatches should have been reported

            continueIrRepair.signalAll();
            Pair<Boolean, Boolean> ir = irResult.get();
            assertTrue(ir.left); // success
            assertFalse(ir.right); // not preview, so we don't care about preview notification
        }
    }

    /**
     * Same as testFinishingIncRepairDuringPreview but the previewed range does not intersect the incremental repair
     * so both preview and incremental repair should finish fine (without any mismatches)
     */

    @Test
    public void testFinishingNonIntersectingIncRepairDuringPreview() throws IOException, InterruptedException, ExecutionException
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .set("enable_christmas_patch", true)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            Thread.sleep(1000);
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            assertTrue(cluster.get(1).callOnInstance(repair(options(false, false))).left);

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // pause preview repair validation messages on node2 until node1 has finished
            SimpleCondition previewRepairStarted = new SimpleCondition();
            SimpleCondition continuePreviewRepair = new SimpleCondition();
            DelayFirstRepairTypeMessageFilter filter = DelayFirstRepairTypeMessageFilter.validationRequest(previewRepairStarted, continuePreviewRepair);
            cluster.filters().verbs(MessagingService.Verb.REPAIR_MESSAGE.ordinal()).from(1).to(2).messagesMatching(filter).drop();

            // get local ranges to repair two separate ranges:
            List<String> localRanges = cluster.get(1).callOnInstance(() -> {
                List<String> res = new ArrayList<>();
                for (Range<Token> r : StorageService.instance.getLocalRanges(KEYSPACE))
                    res.add(r.left.getTokenValue()+ ":"+ r.right.getTokenValue());
                return res;
            });

            assertEquals(2, localRanges.size());
            String previewedRange = localRanges.get(0);
            String repairedRange = localRanges.get(1);
            Future<Pair<Boolean, Boolean>> repairStatusFuture = cluster.get(1).asyncCallsOnInstance(repair(options(true, false, previewedRange))).call();
            previewRepairStarted.await(); // wait for node1 to start validation compaction
            // this needs to finish before the preview repair is unpaused on node2
            assertTrue(cluster.get(1).callOnInstance(repair(options(false, false, repairedRange))).left);

            continuePreviewRepair.signalAll();
            Pair<Boolean, Boolean> rs = repairStatusFuture.get();
            assertTrue(rs.left); // repair should succeed
            assertFalse(rs.right); // and no mismatches
        }
    }

    @Test
    public void snapshotTest() throws IOException, InterruptedException
    {
        try(Cluster cluster = init(Cluster.build(3).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .set("snapshot_on_repaired_data_mismatch", true)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            cluster.schemaChange("create table " + KEYSPACE + ".tbl2 (id int primary key, t int)");
            Thread.sleep(1000);

            // populate 2 tables
            insert(cluster.coordinator(1), 0, 100, "tbl");
            insert(cluster.coordinator(1), 0, 100, "tbl2");
            cluster.forEach((n) -> n.flush(KEYSPACE));

            // make sure everything is marked repaired
            cluster.get(1).callOnInstance(repair(options(false, false)));
            waitMarkedRepaired(cluster);
            // make node2 mismatch
            unmarkRepaired(cluster.get(2), "tbl");
            verifySnapshots(cluster, "tbl", true);
            verifySnapshots(cluster, "tbl2", true);

            AtomicInteger snapshotMessageCounter = new AtomicInteger();
            cluster.filters().verbs(MessagingService.Verb.SNAPSHOT.getId()).messagesMatching((from, to, message) -> {
                snapshotMessageCounter.incrementAndGet();
                return false;
            }).drop();
            cluster.get(1).callOnInstance(repair(options(true, true)));
            verifySnapshots(cluster, "tbl", false);
            // tbl2 should not have a mismatch, so the snapshots should be empty here
            verifySnapshots(cluster, "tbl2", true);
            assertEquals(3, snapshotMessageCounter.get());

            // and make sure that we don't try to snapshot again
            snapshotMessageCounter.set(0);
            cluster.get(3).callOnInstance(repair(options(true, true)));
            assertEquals(0, snapshotMessageCounter.get());
        }
    }

    private void waitMarkedRepaired(Cluster cluster)
    {
        cluster.forEach(node -> node.runOnInstance(() -> {
            for (String table : Arrays.asList("tbl", "tbl2"))
            {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                while (true)
                {
                    if (cfs.getLiveSSTables().stream().allMatch(SSTableReader::isRepaired))
                        return;
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
            }
        }));
    }

    private void unmarkRepaired(IInvokableInstance instance, String table)
    {
        instance.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            try
            {
                cfs.getCompactionStrategyManager().mutateRepaired(cfs.getLiveSSTables(), ActiveRepairService.UNREPAIRED_SSTABLE, null);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private void verifySnapshots(Cluster cluster, String table, boolean shouldBeEmpty)
    {
        cluster.forEach(node -> node.runOnInstance(() -> {
            for (ColumnFamilyStore cfs : Arrays.asList(Keyspace.open(KEYSPACE).getColumnFamilyStore(table),
                                                       Keyspace.open("system").getColumnFamilyStore("repair_history"),
                                                       Keyspace.open("system").getColumnFamilyStore("repair_history_invalidations")))
            {
                if(shouldBeEmpty)
                {
                    assertTrue(cfs.getSnapshotDetails().isEmpty());
                    // first table should be empty but we might still have snapshots in repair_history
                    return;
                }

                while (cfs.getSnapshotDetails().isEmpty())
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        }));
    }

    static abstract class DelayFirstRepairMessageFilter implements IMessageFilters.Matcher
    {
        private final SimpleCondition pause;
        private final SimpleCondition resume;
        private final AtomicBoolean waitForRepair = new AtomicBoolean(true);

        protected DelayFirstRepairMessageFilter(SimpleCondition pause, SimpleCondition resume)
        {
            this.pause = pause;
            this.resume = resume;
        }

        protected abstract boolean matchesMessage(RepairMessage message);

        public final boolean matches(int from, int to, IMessage message)
        {
            try
            {
                Pair<MessageIn<Object>, Integer> msg = Instance.deserializeMessage(message);
                RepairMessage repairMessage = (RepairMessage) msg.left.payload;
                // only the first message should be delayed:
                if (matchesMessage(repairMessage) && waitForRepair.compareAndSet(true, false))
                {
                    pause.signalAll();
                    resume.await();
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return false; // don't drop the message
        }
    }

    static class DelayFirstRepairTypeMessageFilter extends DelayFirstRepairMessageFilter
    {
        private final RepairMessage.Type type;

        public DelayFirstRepairTypeMessageFilter(SimpleCondition pause, SimpleCondition resume, RepairMessage.Type type)
        {
            super(pause, resume);
            this.type = type;
        }

        public static DelayFirstRepairTypeMessageFilter validationRequest(SimpleCondition pause, SimpleCondition resume)
        {
            return new DelayFirstRepairTypeMessageFilter(pause, resume, RepairMessage.Type.VALIDATION_REQUEST);
        }

        public static DelayFirstRepairTypeMessageFilter finalizePropose(SimpleCondition pause, SimpleCondition resume)
        {
            return new DelayFirstRepairTypeMessageFilter(pause, resume, RepairMessage.Type.FINALIZE_PROPOSE);
        }

        protected boolean matchesMessage(RepairMessage repairMessage)
        {
            return repairMessage.messageType == type;
        }
    }

    static void insert(ICoordinator coordinator, int start, int count)
    {
        insert(coordinator, start, count, "tbl");
    }

    static void insert(ICoordinator coordinator, int start, int count, String table)
    {
        for (int i = start; i < start + count; i++)
            coordinator.execute("insert into " + KEYSPACE + "." + table + " (id, t) values (?, ?)", ConsistencyLevel.ALL, i, i);
    }

    /**
     * returns a pair with [repair success, was inconsistent]
     */
    static IIsolatedExecutor.SerializableCallable<Pair<Boolean, Boolean>> repair(Map<String, String> options)
    {
        return () -> {
            SimpleCondition await = new SimpleCondition();
            AtomicBoolean success = new AtomicBoolean(true);
            AtomicBoolean wasInconsistent = new AtomicBoolean(false);
            StorageService.instance.repairAsync(KEYSPACE, options, ImmutableList.of((tag, event) -> {
                if (event.getType() == ProgressEventType.ERROR)
                {
                    success.set(false);
                    await.signalAll();
                }
                else if (event.getType() == ProgressEventType.NOTIFICATION && event.getMessage().contains("Repaired data is inconsistent"))
                {
                    wasInconsistent.set(true);
                }
                else if (event.getType() == ProgressEventType.COMPLETE)
                    await.signalAll();
            }));
            try
            {
                await.await(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return Pair.create(success.get(), wasInconsistent.get());
        };
    }

    static Map<String, String> options(boolean preview, boolean full)
    {
        Map<String, String> config = new HashMap<>();
        config.put(RepairOption.PARALLELISM_KEY, RepairParallelism.PARALLEL.toString());
        if (preview)
            config.put(RepairOption.PREVIEW, PreviewKind.REPAIRED.toString());
        config.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(!full));
        return config;
    }

    static Map<String, String> options(boolean preview, boolean full, String range)
    {
        Map<String, String> options = options(preview, full);
        options.put(RepairOption.RANGES_KEY, range);
        return options;
    }
}
