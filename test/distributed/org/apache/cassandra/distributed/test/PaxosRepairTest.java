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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

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
            return Iterators.size(PaxosState.uncommittedTracker().uncommittedKeyIterator(cfm.cfId, null, null));
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

    private static void repair(Cluster cluster, String keyspace, String table)
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, RepairParallelism.SEQUENTIAL.getName());
        options.put(RepairOption.PRIMARY_RANGE_KEY, Boolean.toString(false));
        options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(false));
        options.put(RepairOption.JOB_THREADS_KEY, Integer.toString(1));
        options.put(RepairOption.TRACE_KEY, Boolean.toString(false));
        options.put(RepairOption.COLUMNFAMILIES_KEY, "");
        options.put(RepairOption.PULL_REPAIR_KEY, Boolean.toString(false));
        options.put(RepairOption.FORCE_REPAIR_KEY, Boolean.toString(false));
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

    private static final Consumer<IInstanceConfig> CONFIG_CONSUMER = cfg -> {
        cfg.with(Feature.NETWORK);
        cfg.with(Feature.GOSSIP);
        cfg.set("paxos_variant", "apple_norrl");
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
            cluster.verbs(MessagingService.Verb.PAXOS_COMMIT).drop();
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

            cluster.verbs(MessagingService.Verb.PAXOS_COMMIT).drop();
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
}
