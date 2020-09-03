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
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.db.ConsistencyLevel.*;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PROPOSE_REQ;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_REPAIR_REQ;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_COMMIT;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_PROPOSE;
import static org.apache.cassandra.net.MessagingService.Verb.READ;

public abstract class CASTestBase extends TestBaseImpl
{
    static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);

    static String tableName()
    {
        return tableName("tbl");
    }

    static String tableName(String prefix)
    {
        return prefix + TABLE_COUNTER.getAndIncrement();
    }

    static void repair(Cluster cluster, String tableName, int pk, int repairWith, int repairWithout)
    {
        IMessageFilters.Filter filter = cluster.filters().verbs(
                APPLE_PAXOS_REPAIR_REQ.ordinal(),
                APPLE_PAXOS_PREPARE_REQ.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(repairWith).to(repairWithout).drop();
        cluster.get(repairWith).runOnInstance(() -> {
            CFMetaData schema = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).metadata;
            DecoratedKey key = schema.decorateKey(Int32Type.instance.decompose(pk));
            try
            {
                PaxosRepair.async(SERIAL, key, schema, null).await();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        });
        filter.off();
    }

    static int pk(Cluster cluster, int lb, int ub)
    {
        return pk(cluster.get(lb), cluster.get(ub));
    }

    static int pk(IInstance lb, IInstance ub)
    {
        return pk(Murmur3Partitioner.instance.getTokenFactory().fromString(lb.config().getString("initial_token")),
                Murmur3Partitioner.instance.getTokenFactory().fromString(ub.config().getString("initial_token")));
    }

    static int pk(Token lb, Token ub)
    {
        int pk = 0;
        Token pkt;
        while (lb.compareTo(pkt = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk))) >= 0 || ub.compareTo(pkt) < 0)
            ++pk;
        return pk;
    }

    int[] to(int ... nodes)
    {
        return nodes;
    }

    AutoCloseable drop(Cluster cluster, int from, int[] toPrepareAndRead, int[] toPropose, int[] toCommit)
    {
        IMessageFilters.Filter filter1 = cluster.filters().verbs(APPLE_PAXOS_PREPARE_REQ.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(from).to(toPrepareAndRead).drop();
        IMessageFilters.Filter filter2 = cluster.filters().verbs(APPLE_PAXOS_PROPOSE_REQ.ordinal(), PAXOS_PROPOSE.ordinal()).from(from).to(toPropose).drop();
        IMessageFilters.Filter filter3 = cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(from).to(toCommit).drop();
        return () -> {
            filter1.off();
            filter2.off();
            filter3.off();
        };
    }

    AutoCloseable drop(Cluster cluster, int from, int[] toPrepare, int[] toRead, int[] toPropose, int[] toCommit)
    {
        IMessageFilters.Filter filter1 = cluster.filters().verbs(APPLE_PAXOS_PREPARE_REQ.ordinal(), PAXOS_PREPARE.ordinal()).from(from).to(toPrepare).drop();
        IMessageFilters.Filter filter2 = cluster.filters().verbs(READ.ordinal()).from(from).to(toRead).drop();
        IMessageFilters.Filter filter3 = cluster.filters().verbs(APPLE_PAXOS_PROPOSE_REQ.ordinal(), PAXOS_PROPOSE.ordinal()).from(from).to(toPropose).drop();
        IMessageFilters.Filter filter4 = cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(from).to(toCommit).drop();
        return () -> {
            filter1.off();
            filter2.off();
            filter3.off();
            filter4.off();
        };
    }

    static void debugOwnership(Cluster cluster, int pk)
    {
        for (int i = 1 ; i <= cluster.size() ; ++i)
            System.out.println(i + ": " + cluster.get(i).appliesOnInstance((Integer v) -> StorageService.instance.getNaturalAndPendingEndpoints(KEYSPACE, Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(v))))
                    .apply(pk));
    }

    static void debugPaxosState(Cluster cluster, int pk)
    {
        debugPaxosState(cluster, "tbl", pk);
    }
    static void debugPaxosState(Cluster cluster, String table, int pk)
    {
        UUID cfid = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metadata.cfId);
        for (int i = 1 ; i <= cluster.size() ; ++i)
            for (Object[] row : cluster.get(i).executeInternal("select in_progress_ballot, proposal_ballot, most_recent_commit_at from system.paxos where row_key = ? and cf_id = ?", Int32Type.instance.decompose(pk), cfid))
                System.out.println(i + ": " + (row[0] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[0])) + ", " + (row[1] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[1])) + ", " + (row[2] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[2])));
    }

    public static void addToRing(boolean bootstrapping, IInstance peer)
    {
        try
        {
            IInstanceConfig config = peer.config();
            IPartitioner partitioner = FBUtilities.newPartitioner(config.getString("partitioner"));
            Token token = partitioner.getTokenFactory().fromString(config.getString("initial_token"));
            InetAddress address = peer.broadcastAddress().getAddress();

            UUID hostId = config.hostId();
            Gossiper.runInGossipStageBlocking(() -> {
                Gossiper.instance.initializeNodeUnsafe(address, hostId, 1);
                Gossiper.instance.injectApplicationState(address,
                                                         ApplicationState.TOKENS,
                                                         new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(token)));
                VersionedValue status = bootstrapping
                                        ? new VersionedValue.VersionedValueFactory(partitioner).bootstrapping(Collections.singleton(token))
                                        : new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(token));
                Gossiper.instance.injectApplicationState(address, ApplicationState.STATUS, status);
                StorageService.instance.onChange(address, ApplicationState.STATUS, status);
                Gossiper.instance.realMarkAlive(address, Gossiper.instance.getEndpointStateForEndpoint(address));
            });
            int version = Math.min(MessagingService.current_version, peer.getMessagingVersion());
            MessagingService.instance().setVersion(address, version);

            if (!bootstrapping)
                assert StorageService.instance.getTokenMetadata().isMember(address);
            PendingRangeCalculatorService.instance.blockUntilFinished();
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    // reset gossip state so we know of the node being alive only
    public static void removeFromRing(IInstance peer)
    {
        try
        {
            IInstanceConfig config = peer.config();
            IPartitioner partitioner = FBUtilities.newPartitioner(config.getString("partitioner"));
            Token token = partitioner.getTokenFactory().fromString(config.getString("initial_token"));
            InetAddress address = config.broadcastAddress().getAddress();

            Gossiper.runInGossipStageBlocking(() -> {
                StorageService.instance.onChange(address,
                                                 ApplicationState.STATUS,
                                                 new VersionedValue.VersionedValueFactory(partitioner).left(Collections.singleton(token), 0L, 0));
                Gossiper.instance.unsafeAnulEndpoint(address);
                Gossiper.instance.realMarkAlive(address, new EndpointState(new HeartBeatState(0, 0)));
            });
            PendingRangeCalculatorService.instance.blockUntilFinished();
        }
        catch (Throwable e) // UnknownHostException
        {
            throw new RuntimeException(e);
        }
    }

    public static void addToRingNormal(IInstance peer)
    {
        addToRing(false, peer);
        assert StorageService.instance.getTokenMetadata().isMember(peer.broadcastAddress().getAddress());
    }

    public static void addToRingBootstrapping(IInstance peer)
    {
        addToRing(true, peer);
    }
}
