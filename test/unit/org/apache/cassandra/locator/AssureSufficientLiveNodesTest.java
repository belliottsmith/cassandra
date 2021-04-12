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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AssureSufficientLiveNodesTest
{
    private static List<InetAddress> instances;
    private static final AtomicInteger testIdGen = new AtomicInteger(0);
    private static final Supplier<String> keyspaceNameGen = () -> "race_" + testIdGen.getAndIncrement();

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddress endpoint)
            {
                byte[] address = endpoint.getAddress();
                return "rake" + address[1];
            }

            public String getDatacenter(InetAddress endpoint)
            {
                byte[] address = endpoint.getAddress();
                return "datacenter" + address[1];
            }

            public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress)
            {
                return null;
            }

            public void sortByProximity(InetAddress address, List<InetAddress> addresses)
            {
            }

            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }

            public void gossiperStarting()
            {
            }

            public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2)
            {
                return false;
            }
        });

        instances = ImmutableList.of(
            // datacenter 1
            InetAddress.getByName("127.1.0.255"), InetAddress.getByName("127.1.0.254"), InetAddress.getByName("127.1.0.253"),
            // datacenter 2
            InetAddress.getByName("127.2.0.255"), InetAddress.getByName("127.2.0.254"), InetAddress.getByName("127.2.0.253"),
            // datacenter 3
            InetAddress.getByName("127.3.0.255"), InetAddress.getByName("127.3.0.254"), InetAddress.getByName("127.3.0.253"));

        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);
        for (int i = 0; i < instances.size(); i++)
        {
            InetAddress ip = instances.get(i);
            Token token = new Murmur3Partitioner.LongToken(i);
            UUID hostId = UUID.randomUUID();
            Gossiper.runInGossipStageBlocking(() -> {
                                                  Gossiper.instance.initializeNodeUnsafe(ip, hostId, 1);
                                                  Gossiper.instance.injectApplicationState(ip, ApplicationState.STATUS, valueFactory.normal(Collections.singleton(token)));
                                                  Gossiper.instance.realMarkAlive(ip, Gossiper.instance.getEndpointStateForEndpoint(ip));
                                              });
            metadata.updateHostId(hostId, ip);
            metadata.updateNormalToken(token, ip);
        }
    }

    @Test
    public void insufficientLiveNodesTest()
    {
        final KeyspaceParams largeRF = KeyspaceParams.nts("datacenter1", 6);
        // Not a race in fact, cause using the same keyspace params at the both places.
        assertThatThrownBy(() -> raceOfReplicationStrategyWithWriteResponseHanderTest(largeRF, largeRF, QUORUM))
            .as("Unavailable should be thrown given 3 live nodes is less than a quorum of 6")
            .isInstanceOf(UnavailableException.class)
            .hasMessageContaining("Cannot achieve consistency level QUORUM");
    }

    @Test
    public void raceOnAddDatacenterNotCausesUnavailable()
    {
        raceOfReplicationStrategyWithWriteResponseHanderTest(
            KeyspaceParams.nts("datacenter1", 3), // init
            KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), // alter to
            EACH_QUORUM);

        raceOfReplicationStrategyWithGetReadExecutorTest(
            KeyspaceParams.nts("datacenter1", 3), // init
            KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), // alter to
            EACH_QUORUM);
    }

    @Test
    public void raceOnAddDatacenterViolateQuorumNotCausesUnavailable()
    {
        raceOfReplicationStrategyWithWriteResponseHanderTest(
            KeyspaceParams.nts("datacenter1", 3), // init. The # of live endpoints is 3.
            KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), // alter to. (3 + 3) / 2 + 1 > 3
            QUORUM);
        raceOfReplicationStrategyWithWriteResponseHanderTest(
            KeyspaceParams.nts("datacenter1", 2, "datacenter2", 1), // init. The # of live endpoints is 2 + 1 = 3
            KeyspaceParams.nts("datacenter1", 2, "datacenter2", 1, "datacenter3", 3), // alter to. (3 + 3) / 2 + 1 > 3
            QUORUM);

        raceOfReplicationStrategyWithGetReadExecutorTest(
            KeyspaceParams.nts("datacenter1", 3), // init. The # of live endpoints is 3.
            KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), // alter to. (3 + 3) / 2 + 1 > 3
            QUORUM);
        raceOfReplicationStrategyWithGetReadExecutorTest(
            KeyspaceParams.nts("datacenter1", 2, "datacenter2", 1), // init. The # of live endpoints is 2 + 1 = 3
            KeyspaceParams.nts("datacenter1", 2, "datacenter2", 1, "datacenter3", 3), // alter to. (3 + 3) / 2 + 1 > 3
            QUORUM);
    }

    @Test
    public void raceOnRemoveDatacenterNotCausesUnavailable()
    {
        raceOfReplicationStrategyWithWriteResponseHanderTest(
            KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), // init
            KeyspaceParams.nts("datacenter1", 3), // alter to
            EACH_QUORUM);

        raceOfReplicationStrategyWithGetReadExecutorTest(
            KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), // init
            KeyspaceParams.nts("datacenter1", 3), // alter to
            EACH_QUORUM);
    }

    @Test
    public void raceOnIncreaseReplicationFactorNotCausesUnavailable()
    {
        raceOfReplicationStrategyWithWriteResponseHanderTest(
            KeyspaceParams.nts("datacenter1", 1),
            KeyspaceParams.nts("datacenter1", 3),
            LOCAL_QUORUM);
        raceOfReplicationStrategyWithWriteResponseHanderTest(
            KeyspaceParams.nts("datacenter1", 1),
            KeyspaceParams.nts("datacenter1", 3),
            QUORUM);

        raceOfReplicationStrategyWithGetReadExecutorTest(
            KeyspaceParams.nts("datacenter1", 1),
            KeyspaceParams.nts("datacenter1", 3),
            LOCAL_QUORUM);
        raceOfReplicationStrategyWithGetReadExecutorTest(
            KeyspaceParams.nts("datacenter1", 1),
            KeyspaceParams.nts("datacenter1", 3),
            QUORUM);
    }

    private void raceOfReplicationStrategyWithGetReadExecutorTest(KeyspaceParams init, KeyspaceParams alterTo, ConsistencyLevel consistencyLevel)
    {
        raceOfReplicationStrategyTest(
            init, alterTo,
            (rs) ->
            {
                Token tk = new Murmur3Partitioner.LongToken(0);
                List<InetAddress> all = StorageProxy.getLiveSortedEndpoints(rs, tk);
                return consistencyLevel.filterForQuery(rs, all);
            },
            consistencyLevel::assureSufficientLiveNodes);
    }

    private void raceOfReplicationStrategyWithWriteResponseHanderTest(KeyspaceParams init, KeyspaceParams alterTo, ConsistencyLevel consistencyLevel)
    {
        raceOfReplicationStrategyTest(
            init, alterTo,
            (rs) ->
            {
                Token tk = new Murmur3Partitioner.LongToken(0);
                return rs.getNaturalEndpoints(tk);
            },
            (rs, replicas) ->
            {
                AbstractWriteResponseHandler<?> responseHandler = rs.getWriteResponseHandler(
                    replicas,
                    ImmutableList.of(),
                    consistencyLevel,
                    () -> {},
                    WriteType.SIMPLE,
                    ip -> true);
                // exit early if we can't fulfill the CL at this time
                responseHandler.assureSufficientLiveNodes();
        });
    }

    private void raceOfReplicationStrategyTest(KeyspaceParams init, KeyspaceParams alterTo,
                                               Function<AbstractReplicationStrategy, List<InetAddress>> targetReplicaSelector,
                                               BiConsumer<AbstractReplicationStrategy, List<InetAddress>> test)
    {
        String keyspaceName = keyspaceNameGen.get();
        KeyspaceMetadata ksMeta = KeyspaceMetadata.create(keyspaceName, init, Tables.of(SchemaLoader.standardCFMD("Foo", "Bar")));
        MigrationManager.announceNewKeyspace(ksMeta, true);
        Keyspace racedKs = Keyspace.open(keyspaceName);
        AbstractReplicationStrategy rs = racedKs.getReplicationStrategy();
        List<InetAddress> targetReplica = targetReplicaSelector.apply(rs);
        // Update replication strategy
        racedKs.setMetadata(ksMeta.withSwapped(alterTo));
        test.accept(rs, targetReplica);
    }
}
