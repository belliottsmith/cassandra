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

package org.apache.cassandra.distributed.test.ring;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import static java.util.Arrays.asList;
import static org.apache.cassandra.distributed.action.GossipHelper.bootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.pullSchemaFrom;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class BootstrapTest extends TestBaseImpl
{
    @Test
    public void bootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster,0, 100);

            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", false,
                         () -> newInstance.startup(cluster));

            cluster.forEach(statusToBootstrap(newInstance));

            cluster.run(asList(pullSchemaFrom(cluster.get(1)),
                               bootstrap()),
                        newInstance.config().num());

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state",
                                    100L,
                                    e.getValue().longValue());
        }
    }

    @Test
    public void autoBootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster,0, 100);
            bootstrapAndJoinNode(cluster);

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state", e.getValue().longValue(), 100L);
        }
    }

    /**
     * Apple-internal
     *
     * The test creates a 2 nodes cluster. Node1 is the seed. Only node2 enters the bootstrap mode.
     * A message filter is added to delay the APPLE_REPAIRED_RANGES message (so fetching repaired range cannot complete)
     * until the insepctor has verified the node2 is still in bootstrap mode.
     * Once the blocking cluster.startup() proceeds to the next line, all nodes should be _not_ in the bootstrap mode.
     */
    @Test
    public void testBootstrapBlocksUntilAllStreamingCompletes() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(config -> config.set("enable_christmas_patch", true)
                                                                  .set("auto_bootstrap", true)
                                                                  .with(GOSSIP)
                                                                  .with(NETWORK))
                                      .createWithoutStarting();
             Closeable executorCloser = executor::shutdown)
        {

            final CountDownLatch messageGuard = new CountDownLatch(1);
            final CountDownLatch inspectorReady = new CountDownLatch(1);
            final IMessageFilters.Matcher delayMessage = (from, to, msg) ->
            {
                inspectorReady.countDown();
                Uninterruptibles.awaitUninterruptibly(messageGuard);
                // additional wait time that blocks the bootstrap process and defer the completion of cluster.startup()/
                Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
                return false; // do not drop
            };
            cluster.filters()
                   .verbs(Verb.APPLE_REPAIRED_RANGES_REQ.id)
                   .messagesMatching(delayMessage)
                   .drop();

            final Runnable inspector = () ->
            {
                Uninterruptibles.awaitUninterruptibly(inspectorReady);
                cluster.get(2).runOnInstance(() -> {
                    Assert.assertTrue("Node 2 should be still bootstrapping while streaming repaired ranges",
                                      StorageService.instance.isBootstrapMode());
                });
                messageGuard.countDown();
            };
            executor.submit(inspector);

            cluster.startup();
            cluster.forEach(node -> {
                int nodeId = node.config().num();
                node.runOnInstance(() -> {
                    String errorFormat = "All nodes should not be in bootstrap mode after cluster start up. But node %d is still in bootstrap mode";
                    Assert.assertFalse(String.format(errorFormat, nodeId), StorageService.instance.isBootstrapMode());
                });
            });
        }
    }


    public static void populate(ICluster cluster, int from, int to)
    {
        populate(cluster, from, to, 1, 3, ConsistencyLevel.QUORUM);
    }

    public static void populate(ICluster cluster, int from, int to, int coord, int rf, ConsistencyLevel cl)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                               cl,
                                               i, i, i);
        }
    }

    public static Map<Integer, Long> count(ICluster cluster)
    {
        return IntStream.rangeClosed(1, cluster.size())
                        .boxed()
                        .collect(Collectors.toMap(nodeId -> nodeId,
                                                  nodeId -> (Long) cluster.get(nodeId).executeInternal("SELECT count(*) FROM " + KEYSPACE + ".tbl")[0][0]));
    }
}
