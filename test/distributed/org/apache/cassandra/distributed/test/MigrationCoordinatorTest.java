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
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.schema.MigrationCoordinator;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.hamcrest.Matchers.*;

public class MigrationCoordinatorTest extends TestBaseImpl
{

    @Before
    public void setUp()
    {
        System.clearProperty("cassandra.replace_address");
        System.clearProperty("cassandra.consistent.rangemovement");
        System.clearProperty(MigrationCoordinator.IGNORED_ENDPOINTS_PROP);
        System.clearProperty(MigrationCoordinator.IGNORED_VERSIONS_PROP);
    }
    /**
     * We shouldn't wait on versions only available from a node being replaced
     * see CASSANDRA-
     */
    @Test
    public void replaceNode() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks with replication={'class':'SimpleStrategy', 'replication_factor':2}");
            InetAddress replacementAddress = cluster.get(2).broadcastAddress().getAddress();
            cluster.get(2).shutdown().get();
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE ks.tbl (k int primary key, v int)");

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            System.setProperty("cassandra.replace_address", replacementAddress.getHostAddress());
            cluster.bootstrap(config).startup();

            List<String> schemaPullFailureLogs = cluster.get(3).logs().grep("Can't send schema pull request: node.* is down.").getResult();

            // Not perfectly precise, but want to make sure we're not retrying failures ASAP (every ~millisecond)
            double maxRetriesInSchemaDelay = cluster.get(3).callOnInstance(() -> StorageService.SCHEMA_DELAY_MILLIS / MigrationCoordinator.SCHEMA_PULL_FAILURE_RETRY_DELAY_MILLIS);
            int approxUpperBoundNumSchemaFailures = (int) Math.ceil(1.25 * maxRetriesInSchemaDelay);
            Assert.assertThat(schemaPullFailureLogs, hasSize(lessThanOrEqualTo(approxUpperBoundNumSchemaFailures)));
        }
    }

    @Test
    public void explicitEndpointIgnore() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks with replication={'class':'SimpleStrategy', 'replication_factor':2}");
            InetAddress ignoredEndpoint = cluster.get(2).broadcastAddress().getAddress();
            cluster.get(2).shutdown(false);
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE ks.tbl (k int primary key, v int)");

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            System.setProperty(MigrationCoordinator.IGNORED_ENDPOINTS_PROP, ignoredEndpoint.getHostAddress());
            System.setProperty("cassandra.consistent.rangemovement", "false");
            cluster.bootstrap(config).startup();
        }
    }

    @Test
    public void explicitVersionIgnore() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            UUID initialVersion = cluster.get(2).callsOnInstance(() -> Schema.instance.getVersion()).call();
            cluster.schemaChange("CREATE KEYSPACE ks with replication={'class':'SimpleStrategy', 'replication_factor':2}");
            UUID oldVersion;
            do
            {
                oldVersion = cluster.get(2).callsOnInstance(() -> Schema.instance.getVersion()).call();
            } while (oldVersion.equals(initialVersion));
            cluster.get(2).shutdown(false);
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE ks.tbl (k int primary key, v int)");

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            System.setProperty(MigrationCoordinator.IGNORED_VERSIONS_PROP, initialVersion.toString() + ',' + oldVersion.toString());
            System.setProperty("cassandra.consistent.rangemovement", "false");
            cluster.bootstrap(config).startup();
        }
    }
}
