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
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.service.ActiveRepairService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class LocalSessionsCleanupTest extends TestBaseImpl
{
    @Test
    public void testCleanup() throws IOException, InterruptedException, TimeoutException
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;
        System.setProperty("cassandra.repair_delete_timeout_seconds", "1");

        try (Cluster cluster = init(builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);
            cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
            bootstrapAndJoinNode(cluster);

            cluster.get(1).logs().watchFor("New node /127.0.0.3:7012 at token");
            cluster.get(2).logs().watchFor("New node /127.0.0.3:7012 at token");
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);

            cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
            cluster.get(1).runOnInstance(() -> assertEquals(2, ActiveRepairService.instance.consistent.local.getNumSessions()));
            Thread.sleep(2000); // wait at least repair_delete_timeout_seconds

            cluster.get(1).runOnInstance(() -> {
                ActiveRepairService.instance.consistent.local.cleanup();
                assertEquals(1, ActiveRepairService.instance.consistent.local.getNumSessions());
            });
        }
    }

    @Test
    public void testCleanupDroppedTable() throws IOException, InterruptedException
    {
        System.setProperty("cassandra.repair_delete_timeout_seconds", "1");

        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);
            cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();

            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);

            Thread.sleep(2000); // wait at least repair_delete_timeout_seconds

            cluster.get(1).runOnInstance(() -> {
                ActiveRepairService.instance.consistent.local.cleanup();
                assertEquals(1, ActiveRepairService.instance.consistent.local.getNumSessions());
            });

            cluster.schemaChange(withKeyspace("drop table %s.tbl"));

            Thread.sleep(2000); // wait at least repair_delete_timeout_seconds

            cluster.get(1).runOnInstance(() -> {
                ActiveRepairService.instance.consistent.local.cleanup();
                assertEquals(0, ActiveRepairService.instance.consistent.local.getNumSessions());
            });
        }
    }
}
