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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.*;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanup;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_CLEANUP_FINISH_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_COMMIT_AND_PREPARE_REQ;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_COMMIT;

public class PaxosRepairTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosRepairTest.class);
    private static final String TABLE = "tbl";

    private static int getUncommitted(IInvokableInstance instance, String keyspace, String table)
    {
        if (instance.isShutdown())
            return 0;
        int uncommitted = instance.callsOnInstance(() -> {
            CFMetaData cfm = Schema.instance.getCFMetaData(keyspace, table);
            return Iterators.size(PaxosState.uncommittedTracker().uncommittedKeyIterator(cfm.cfId, null));
        }).call();
        logger.info("{} has {} uncommitted instances", instance, uncommitted);
        return uncommitted;
    }

    private static void assertAllAlive(Cluster cluster)
    {
        Set<InetAddress> allEndpoints = cluster.stream().map(i -> i.broadcastAddress().getAddress()).collect(Collectors.toSet());
        cluster.stream().forEach(instance -> {
            instance.runOnInstance(() -> {
                ImmutableSet<InetAddress> endpoints = Gossiper.instance.getEndpoints();
                Assert.assertEquals(allEndpoints, endpoints);
                for (InetAddress endpoint : endpoints)
                    Assert.assertTrue(FailureDetector.instance.isAlive(endpoint));
            });
        });
    }

    private static void assertUncommitted(IInvokableInstance instance, String ks, String table, int expected)
    {
        Assert.assertEquals(expected, getUncommitted(instance, ks, table));
    }

    private static boolean hasUncommitted(Cluster cluster, String ks, String table)
    {
        return cluster.stream().map(instance -> getUncommitted(instance, ks, table)).reduce((a, b) -> a + b).get() > 0;
    }

    private static void repair(Cluster cluster, String keyspace, String table, boolean force)
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, RepairParallelism.SEQUENTIAL.getName());
        options.put(RepairOption.PRIMARY_RANGE_KEY, Boolean.toString(false));
        options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(false));
        options.put(RepairOption.JOB_THREADS_KEY, Integer.toString(1));
        options.put(RepairOption.TRACE_KEY, Boolean.toString(false));
        options.put(RepairOption.COLUMNFAMILIES_KEY, "");
        options.put(RepairOption.PULL_REPAIR_KEY, Boolean.toString(false));
        options.put(RepairOption.FORCE_REPAIR_KEY, Boolean.toString(force));
        options.put(RepairOption.PREVIEW, PreviewKind.NONE.toString());
        options.put(RepairOption.IGNORE_UNREPLICATED_KS, Boolean.toString(false));
        options.put(RepairOption.REPAIR_PAXOS, Boolean.toString(true));
        options.put(RepairOption.PAXOS_ONLY, Boolean.toString(true));

        cluster.get(1).runOnInstance(() -> {
            int cmd = StorageService.instance.repairAsync(keyspace, options);

            while (true)
            {
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
                Pair<ActiveRepairService.ParentRepairStatus, List<String>> status = ActiveRepairService.instance.getRepairStatus(cmd);
                if (status == null)
                    continue;

                switch (status.left)
                {
                    case IN_PROGRESS:
                        continue;
                    case COMPLETED:
                        return;
                    default:
                        throw new AssertionError("Repair failed with errors: " + status.right);
                }
            }
        });
    }

    private static void repair(Cluster cluster, String keyspace, String table)
    {
        repair(cluster, keyspace, table, false);
    }

    private static final Consumer<IInstanceConfig> CONFIG_CONSUMER = cfg -> {
        cfg.with(Feature.NETWORK);
        cfg.with(Feature.GOSSIP);
        cfg.set("paxos_variant", "apple_norrl");
        cfg.set("truncate_request_timeout_in_ms", 1000L);
        cfg.set("partitioner", "ByteOrderedPartitioner");
        cfg.set("initial_token", ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(cfg.num() * 100)));
    };

    @Test
    public void paxosRepairTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, CONFIG_CONSUMER)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            Assert.assertFalse(hasUncommitted(cluster, KEYSPACE, TABLE));

            assertAllAlive(cluster);
            cluster.verbs(PAXOS_COMMIT).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (400, 2, 2) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }

            Assert.assertTrue(hasUncommitted(cluster, KEYSPACE, TABLE));

            cluster.filters().reset();

            assertAllAlive(cluster);
            repair(cluster, KEYSPACE, TABLE);

            assertUncommitted(cluster.get(1), KEYSPACE, TABLE, 0);
            assertUncommitted(cluster.get(2), KEYSPACE, TABLE, 0);
            assertUncommitted(cluster.get(3), KEYSPACE, TABLE, 0);
        }
    }

    @Ignore
    @Test
    public void topologyChangePaxosTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build(4).withConfig(CONFIG_CONSUMER).createWithoutStarting())
        {
            for (int i=1; i<=3; i++)
                cluster.get(i).startup();

            init(cluster);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);

            cluster.verbs(PAXOS_COMMIT).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (350, 2, 2) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }
            Assert.assertTrue(hasUncommitted(cluster, KEYSPACE, TABLE));

            cluster.filters().reset();

            // node 4 starting should repair paxos and inform the other nodes of its gossip state
            cluster.get(4).startup();
            Assert.assertFalse(hasUncommitted(cluster, KEYSPACE, TABLE));
        }
    }

    @Test
    public void paxosCleanupWithReproposal() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, cfg -> cfg
                .set("paxos_variant", "apple_rrl")
                .set("truncate_request_timeout_in_ms", 1000L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.verbs(PAXOS_COMMIT).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }
            Assert.assertTrue(hasUncommitted(cluster, KEYSPACE, TABLE));
            cluster.forEach(i -> i.runOnInstance(() -> Keyspace.open("system").getColumnFamilyStore("paxos").forceBlockingFlush()));

            CountDownLatch haveFetchedLowBound = new CountDownLatch(1);
            CountDownLatch haveReproposed = new CountDownLatch(1);
            cluster.verbs(APPLE_PAXOS_CLEANUP_FINISH_PREPARE).inbound().messagesMatching((from, to, verb) -> {
                haveFetchedLowBound.countDown();
                Uninterruptibles.awaitUninterruptibly(haveReproposed);
                return false;
            }).drop();

            ExecutorService executor = Executors.newCachedThreadPool();
            List<InetAddress> endpoints = cluster.stream().map(i -> i.broadcastAddress().getAddress()).collect(Collectors.toList());
            Future<?> cleanup = cluster.get(1).appliesOnInstance((List<InetAddress> es, ExecutorService exec)-> {
                CFMetaData metadata = Keyspace.open(KEYSPACE).getMetadata().getTableOrViewNullable(TABLE);
                return PaxosCleanup.cleanup(es, metadata.cfId, StorageService.instance.getLocalRanges(KEYSPACE), false, exec);
            }).apply(endpoints, executor);

            Uninterruptibles.awaitUninterruptibly(haveFetchedLowBound);
            IMessageFilters.Filter filter2 = cluster.verbs(PAXOS_COMMIT, APPLE_PAXOS_COMMIT_AND_PREPARE_REQ).drop();
            try
            {
                cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1", ConsistencyLevel.SERIAL);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }
            filter2.off();
            haveReproposed.countDown();
            cluster.filters().reset();

            cleanup.get();
            ExecutorUtils.shutdownNowAndWait(1L, TimeUnit.MINUTES, executor);
            for (int i = 1 ; i <= 3 ; ++i)
                assertRows(cluster.get(i).executeInternal("SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1"), row(1, 1, 1));
            Assert.assertFalse(hasUncommitted(cluster, KEYSPACE, TABLE));
        }
    }

    @Test
    public void paxosCleanupWithDelayedProposal() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, cfg -> cfg
                .set("paxos_variant", "apple_rrl")
                .set("truncate_request_timeout_in_ms", 1000L)))
        )
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            CountDownLatch haveFinishedRepair = new CountDownLatch(1);
            cluster.verbs(APPLE_PAXOS_PREPARE_REQ).messagesMatching((from, to, verb) -> {
                Uninterruptibles.awaitUninterruptibly(haveFinishedRepair);
                return false;
            }).drop();
            cluster.verbs(PAXOS_COMMIT).drop();
            Future<?> insert = cluster.get(1).async(() -> {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }).call();
            cluster.verbs(APPLE_PAXOS_CLEANUP_FINISH_PREPARE).messagesMatching((from, to, verb) -> {
                haveFinishedRepair.countDown();
                try { insert.get(); } catch (Throwable t) {}
                cluster.filters().reset();
                return false;
            }).drop();

            ExecutorService executor = Executors.newCachedThreadPool();

            Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);

            List<InetAddress> endpoints = cluster.stream().map(i -> i.broadcastAddress().getAddress()).collect(Collectors.toList());
            Future<?> cleanup = cluster.get(1).appliesOnInstance((List<InetAddress> es, ExecutorService exec)-> {
                CFMetaData metadata = Keyspace.open(KEYSPACE).getMetadata().getTableOrViewNullable(TABLE);
                return PaxosCleanup.cleanup(es, metadata.cfId, StorageService.instance.getLocalRanges(KEYSPACE), false, exec);
            }).apply(endpoints, executor);

            cleanup.get();
            try
            {
                insert.get();
            }
            catch (Throwable t)
            {
            }
            ExecutorUtils.shutdownNowAndWait(1L, TimeUnit.MINUTES, executor);
            Assert.assertFalse(hasUncommitted(cluster, KEYSPACE, TABLE));
        }
    }

    @Test
    public void paxosRepairPreventsStaleReproposal() throws Throwable
    {
        UUID staleBallot = Paxos.newBallot(Ballot.none(), org.apache.cassandra.db.ConsistencyLevel.SERIAL);
        try (Cluster cluster = init(Cluster.create(3, cfg -> cfg
                                                             .set("paxos_variant", "apple_rrl")
                                                             .set("truncate_request_timeout_in_ms", 1000L)))
        )
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (k int primary key, v int)");
            repair(cluster, KEYSPACE, TABLE);

            // stop and start node 2 to test loading paxos repair history from disk
            cluster.get(2).shutdown();
            cluster.get(2).startup();

            for (int i=0; i<cluster.size(); i++)
            {
                cluster.get(i+1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
                    DecoratedKey key = cfs.decorateKey(ByteBufferUtil.bytes(1));
                    Assert.assertFalse(FBUtilities.getBroadcastAddress().toString(), Commit.isAfter(staleBallot, cfs.getPaxosRepairLowBound(key)));
                });
            }

            // add in the stale proposal
            cluster.get(1).runOnInstance(() -> {
                CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
                DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
                ColumnDefinition cdef = cfm.getColumnDefinition(new ColumnIdentifier("v", false));
                Cell cell = BufferCell.live(cfm, cdef, UUIDGen.microsTimestamp(staleBallot), ByteBufferUtil.bytes(1));
                Row row = BTreeRow.singleCellRow(Clustering.EMPTY, cell);
                PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, key, row);
                Commit.Proposal proposal = Commit.Proposal.from(staleBallot, update);
                SystemKeyspace.savePaxosProposal(proposal);
            });

            // shutdown node 3 so we're guaranteed to see the stale proposal
            cluster.get(3).shutdown();

            // the stale inflight proposal should be ignored and the query should succeed
            String query = "INSERT INTO " + KEYSPACE + '.' + TABLE + " (k, v) VALUES (1, 2) IF NOT EXISTS";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM);
            Assert.assertEquals(new Object[][]{new Object[]{ true }}, result);
        }
    }

    @Test
    public void paxosRepairHistoryIsntUpdatedInForcedRepair() throws Throwable
    {
        UUID staleBallot = Paxos.newBallot(Ballot.none(), org.apache.cassandra.db.ConsistencyLevel.SERIAL);
        try (Cluster cluster = init(Cluster.create(3, cfg -> cfg.with(Feature.GOSSIP, Feature.NETWORK)
                                                             .set("paxos_variant", "apple_rrl")
                                                             .set("truncate_request_timeout_in_ms", 1000L)))
        )
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (k int primary key, v int)");
            cluster.get(3).shutdown();
            InetAddress node3 = cluster.get(3).broadcastAddress().getAddress();

            for (int i=0; i<10; i++)
            {
                if (!cluster.get(1).callOnInstance(() -> FailureDetector.instance.isAlive(node3)))
                    break;
            }

            repair(cluster, KEYSPACE, TABLE, true);
            for (int i=0; i<cluster.size() -1; i++)
            {
                cluster.get(i+1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
                    DecoratedKey key = cfs.decorateKey(ByteBufferUtil.bytes(1));
                    Assert.assertTrue(FBUtilities.getBroadcastAddress().toString(), Commit.isAfter(staleBallot, cfs.getPaxosRepairLowBound(key)));
                });
            }
        }
    }
}
