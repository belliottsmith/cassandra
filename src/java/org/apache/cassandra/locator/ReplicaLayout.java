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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.ReplicaCollection.Mutable.Conflict;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.AlwaysSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReplicaLayout<E extends Endpoints<E>, L extends ReplicaLayout<E, L>>
{
    protected volatile E all;
    protected final E natural;
    protected final E pending;
    protected final E selected;

    protected final Keyspace keyspace;
    protected final ConsistencyLevel consistencyLevel;

    private ReplicaLayout(Keyspace keyspace, ConsistencyLevel consistencyLevel, E natural, E pending, E selected)
    {
        this(keyspace, consistencyLevel, natural, pending, selected, null);
    }
    private ReplicaLayout(Keyspace keyspace, ConsistencyLevel consistencyLevel, E natural, E pending, E selected, E all)
    {
        if (selected == null) throw new RuntimeException();
        this.keyspace = keyspace;
        this.consistencyLevel = consistencyLevel;
        this.natural = natural;
        this.pending = pending;
        this.selected = selected;
        if (all == null && pending == null)
            all = natural;
        this.all = all;
    }

    public Replica getReplicaFor(InetAddressAndPort endpoint)
    {
        return natural.byEndpoint().get(endpoint);
    }

    public E naturalReplicas()
    {
        return natural;
    }

    public E allReplicas()
    {
        E result = all;
        if (result == null)
            all = result = Endpoints.concat(natural, pending, Conflict.ALL);
        return result;
    }

    public E selectedReplicas()
    {
        return selected;
    }

    public E pendingReplicas()
    {
        return pending;
    }

    public Keyspace keyspace()
    {
        return keyspace;
    }

    public ConsistencyLevel consistencyLevel()
    {
        return consistencyLevel;
    }

    abstract public L withSelected(E replicas);

    public L withSelected(List<Replica> replicas)
    {
        return withSelected(selected.snapshot(replicas));
    }

    abstract public L withConsistencyLevel(ConsistencyLevel cl);

    public L forResponded(Iterable<InetAddressAndPort> endpoints)
    {
        // Preserve insertion order
        Set<InetAddressAndPort> endpointSet = new HashSet<>(Iterables.size(endpoints));
        Iterables.addAll(endpointSet, endpoints);
        return withSelected(selected.filter(replica -> endpointSet.contains(replica.endpoint())));
    }

    public L forAllUncontacted()
    {
        // TODO: should this also be DC local
        return withSelected(allReplicas().filter(r -> !selected.contains(r)));
    }

    public L forNaturalUncontacted()
    {
        E more;
        if (consistencyLevel.isDatacenterLocal() && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
        {
            IEndpointSnitch snitch = keyspace.getReplicationStrategy().snitch;
            String localDC = DatabaseDescriptor.getLocalDataCenter();

            more = natural.filter(replica -> !selected.contains(replica) &&
                                             snitch.getDatacenter(replica).equals(localDC));
        }
        else
        {
            more = natural.filter(replica -> !selected.contains(replica));
        }

        return withSelected(more);
    }

    public static class ForRange extends ReplicaLayout<EndpointsForRange, ForRange>
    {
        public final AbstractBounds<PartitionPosition> range;

        @VisibleForTesting
        public ForRange(Keyspace keyspace, ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> range, EndpointsForRange natural, EndpointsForRange selected)
        {
            // Range queries do not contact pending replicas
            super(keyspace, consistencyLevel, natural, null, selected);
            this.range = range;
        }

        @Override
        public ForRange withSelected(EndpointsForRange newSelected)
        {
            return new ForRange(keyspace, consistencyLevel, range, natural, newSelected);
        }

        @Override
        public ForRange withConsistencyLevel(ConsistencyLevel cl)
        {
            return new ForRange(keyspace, cl, range, natural, selected);
        }
    }

    public static class ForToken extends ReplicaLayout<EndpointsForToken, ForToken>
    {
        public final Token token;

        @VisibleForTesting
        public ForToken(Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, EndpointsForToken natural, EndpointsForToken pending, EndpointsForToken selected)
        {
            super(keyspace, consistencyLevel, natural, pending, selected);
            this.token = token;
        }
        public ForToken(Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, EndpointsForToken natural, EndpointsForToken pending, EndpointsForToken selected, EndpointsForToken all)
        {
            super(keyspace, consistencyLevel, natural, pending, selected, all);
            this.token = token;
        }

        public ForToken withSelected(EndpointsForToken newSelected)
        {
            return new ForToken(keyspace, consistencyLevel,  token, natural, pending, newSelected);
        }

        @Override
        public ForToken withConsistencyLevel(ConsistencyLevel cl)
        {
            return new ForToken(keyspace, cl, token, natural, pending, selected);
        }

        public ForToken withoutLocal()
        {
            return new ForToken(keyspace, consistencyLevel, token, natural, pending, selected.filter((replica -> !replica.isLocal())));
        }
    }

    public static class ForPaxos extends ForToken
    {
        private final int requiredParticipants;

        private ForPaxos(Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, int requiredParticipants, EndpointsForToken natural, EndpointsForToken pending, EndpointsForToken selected, EndpointsForToken all)
        {
            super(keyspace, consistencyLevel, token, natural, pending, selected, all);
            this.requiredParticipants = requiredParticipants;
        }

        public int getRequiredParticipants()
        {
            return requiredParticipants;
        }
    }

    public static ForToken forSingleReplica(Keyspace keyspace, Token token, Replica replica)
    {
        EndpointsForToken singleReplica = EndpointsForToken.of(token, replica);
        return new ForToken(keyspace, ConsistencyLevel.ONE, token, singleReplica, EndpointsForToken.empty(token), singleReplica);
    }

    public static ForRange forSingleReplica(Keyspace keyspace, AbstractBounds<PartitionPosition> range, Replica replica)
    {
        EndpointsForRange singleReplica = EndpointsForRange.of(replica);
        return new ForRange(keyspace, ConsistencyLevel.ONE, range, singleReplica, singleReplica);
    }

    public static ForToken forCounterWrite(Keyspace keyspace, Token token, Replica replica)
    {
        EndpointsForToken replicas = EndpointsForToken.of(token, replica);
        return ReplicaLayout.forWrite(keyspace, ConsistencyLevel.ONE, token, replicas, EndpointsForToken.empty(token), replicas);
    }

    public static ForToken forBatchlogWrite(Keyspace keyspace, Collection<InetAddressAndPort> endpoints) throws UnavailableException
    {
        // A single case we write not for range or token, but multiple mutations to many tokens
        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
        EndpointsForToken natural = EndpointsForToken.copyOf(token, SystemReplicas.getSystemReplicas(endpoints));
        EndpointsForToken pending = EndpointsForToken.empty(token);

        ConsistencyLevel consistencyLevel = natural.size() == 1 ? ConsistencyLevel.ONE : ConsistencyLevel.TWO;
        return new ForToken(keyspace, consistencyLevel, token, natural, pending, natural);
    }

    @VisibleForTesting
    public static ForToken forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, EndpointsForToken natural, EndpointsForToken pending, EndpointsForToken selected) throws UnavailableException
    {
        return new ForToken(keyspace, consistencyLevel, token, natural, pending, selected);
    }

    public static ForToken forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, Predicate<InetAddressAndPort> isAlive) throws UnavailableException
    {
        EndpointsForToken natural = StorageService.getNaturalReplicasForToken(keyspace.getName(), token);
        EndpointsForToken pending = StorageService.instance.getTokenMetadata().pendingEndpointsForToken(token, keyspace.getName());

        if (!keyspace.getReplicationStrategy().hasTransientReplicas())
        {
            return new ForToken(keyspace, consistencyLevel, token, natural, pending, natural);
        }

        return forWrite(keyspace, consistencyLevel, token, consistencyLevel.blockFor(keyspace), natural, pending, isAlive);
    }

    public static ReplicaLayout.ForPaxos forPaxos(Keyspace keyspace, DecoratedKey key, ConsistencyLevel consistencyForPaxos) throws UnavailableException
    {
        Token tk = key.getToken();
        EndpointsForToken natural = StorageService.getNaturalReplicasForToken(keyspace.getName(), tk);
        EndpointsForToken pending = StorageService.instance.getTokenMetadata().pendingEndpointsForToken(tk, keyspace.getName());
        // TODO: test LWTs
        Replicas.assertFull(natural);
        Replicas.assertFull(pending);
        if (consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL)
        {
            // Restrict naturalReplicas and pendingReplicas to node in the local DC only
            String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            Predicate<Replica> isLocalDc = replica -> localDc.equals(snitch.getDatacenter(replica));

            natural = natural.filter(isLocalDc);
            pending = pending.filter(isLocalDc);
        }
        int participants = pending.size() + natural.size();
        int requiredParticipants = participants / 2 + 1; // See CASSANDRA-8346, CASSANDRA-833

        EndpointsForToken all = Endpoints.concat(natural, pending, Conflict.ALL);
        EndpointsForToken selected = all.filter(IAsyncCallback.isReplicaAlive);
        if (selected.size() < requiredParticipants)
            throw new UnavailableException(consistencyForPaxos, requiredParticipants, selected.size());

        // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
        // Note that we fake an impossible number of required nodes in the unavailable exception
        // to nail home the point that it's an impossible operation no matter how many nodes are live.
        if (pending.size() > 1)
            throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", pending.size()),
                                           consistencyForPaxos,
                                           participants + 1,
                                           selected.size());

        return new ReplicaLayout.ForPaxos(keyspace, consistencyForPaxos, key.getToken(), requiredParticipants, natural, pending, selected, all);
    }

    /**
     * We want to send mutations to as many full replicas as we can, and just as many transient replicas
     * as we need to meet blockFor.
     */
    @VisibleForTesting
    public static ForToken forWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, Token token, int blockFor, EndpointsForToken natural, EndpointsForToken pending, Predicate<InetAddressAndPort> livePredicate) throws UnavailableException
    {
        EndpointsForToken all = Endpoints.concat(natural, pending, Conflict.ALL);
        EndpointsForToken selected = all
                .select()
                .add(r -> r.isFull() && livePredicate.test(r.endpoint()))
                .add(r -> r.isTransient() && livePredicate.test(r.endpoint()), blockFor)
                .get();

        if (selected.size() < blockFor)
            throw new UnavailableException(consistencyLevel, blockFor, selected.size());

        if (selected.isEmpty() || selected.get(0).isTransient())
            throw new UnavailableException("At least one full replica required for writes", consistencyLevel, blockFor, 0);

        return new ForToken(keyspace, consistencyLevel, token, natural, pending, selected, all);
    }

    public static ForToken forRead(Keyspace keyspace, Token token, ConsistencyLevel consistencyLevel, SpeculativeRetryPolicy retry)
    {
        EndpointsForToken natural = StorageProxy.getLiveSortedReplicasForToken(keyspace, token);
        EndpointsForToken selected = consistencyLevel.filterForQuery(keyspace, natural, retry.equals(AlwaysSpeculativeRetryPolicy.INSTANCE));

        // Throw UAE early if we don't have enough replicas.
        consistencyLevel.assureSufficientLiveNodes(keyspace, selected);

        return new ForToken(keyspace, consistencyLevel, token, natural, null, selected);
    }

    public static ForRange forRangeRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> range, EndpointsForRange natural, EndpointsForRange selected)
    {
        return new ForRange(keyspace, consistencyLevel, range, natural, selected);
    }

    public static ForToken forSpeculation(ForToken original)
    {
        EndpointsForToken newSelection = original.allReplicas().subList(0, original.selectedReplicas().size() + 1);
        return new ForToken(original.keyspace, ConsistencyLevel.ALL, original.token, original.natural, original.pending, newSelection);
    }

    public String toString()
    {
        return "ReplicaLayout [ CL: " + consistencyLevel + " keyspace: " + keyspace + " natural: " + natural + " selected: " + selected + " ]";
    }
}

