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
package org.apache.cassandra.db;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import org.apache.cassandra.locator.SameDCTester;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.Replicas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.transport.ProtocolException;

public enum ConsistencyLevel
{
    ANY         (0),
    ONE         (1),
    TWO         (2),
    THREE       (3),
    QUORUM      (4),
    ALL         (5),
    LOCAL_QUORUM(6, true),
    EACH_QUORUM (7),
    SERIAL      (8),
    LOCAL_SERIAL(9),
    LOCAL_ONE   (10, true),
    NODE_LOCAL  (11, true);

    private static final Logger logger = LoggerFactory.getLogger(ConsistencyLevel.class);

    // Used by the binary protocol
    public final int code;
    private final boolean isDCLocal;
    private static final ConsistencyLevel[] codeIdx;
    static
    {
        int maxCode = -1;
        for (ConsistencyLevel cl : ConsistencyLevel.values())
            maxCode = Math.max(maxCode, cl.code);
        codeIdx = new ConsistencyLevel[maxCode + 1];
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            if (codeIdx[cl.code] != null)
                throw new IllegalStateException("Duplicate code");
            codeIdx[cl.code] = cl;
        }
    }

    private ConsistencyLevel(int code)
    {
        this(code, false);
    }

    private ConsistencyLevel(int code, boolean isDCLocal)
    {
        this.code = code;
        this.isDCLocal = isDCLocal;
    }

    public static ConsistencyLevel fromCode(int code)
    {
        if (code < 0 || code >= codeIdx.length)
            throw new ProtocolException(String.format("Unknown code %d for a consistency level", code));
        return codeIdx[code];
    }

    public static int quorumFor(Keyspace keyspace)
    {
        return (keyspace.getReplicationStrategy().getReplicationFactor().allReplicas / 2) + 1;
    }

    public static int localQuorumFor(Keyspace keyspace, String dc)
    {
        return (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
             ? (((NetworkTopologyStrategy) keyspace.getReplicationStrategy()).getReplicationFactor(dc).allReplicas / 2) + 1
             : quorumFor(keyspace);
    }

    public int blockFor(Keyspace keyspace)
    {
        switch (this)
        {
            case ONE:
            case LOCAL_ONE:
                return 1;
            case ANY:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
            case SERIAL:
                return quorumFor(keyspace);
            case ALL:
                return keyspace.getReplicationStrategy().getReplicationFactor().allReplicas;
            case LOCAL_QUORUM:
            case LOCAL_SERIAL:
                return localQuorumFor(keyspace, DatabaseDescriptor.getLocalDataCenter());
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();
                    int n = 0;
                    for (String dc : strategy.getDatacenters())
                        n += localQuorumFor(keyspace, dc);
                    return n;
                }
                else
                {
                    return quorumFor(keyspace);
                }
            default:
                throw new UnsupportedOperationException("Invalid consistency level: " + toString());
        }
    }

    public int blockForWrite(Keyspace keyspace, Endpoints<?> pending)
    {
        assert pending != null;

        int blockFor = blockFor(keyspace);
        switch (this)
        {
            case ANY:
                break;
            case LOCAL_ONE: case LOCAL_QUORUM: case LOCAL_SERIAL:
                // we will only count local replicas towards our response count, as these queries only care about local guarantees
                blockFor += countDCLocalReplicas(pending).allReplicas();
                break;
            case ONE: case TWO: case THREE:
            case QUORUM: case EACH_QUORUM:
            case SERIAL:
            case ALL:
                blockFor += pending.size();
        }
        return blockFor;
    }

    /**
     * Determine if this consistency level meets or exceeds the consistency requirements of the given cl for the given keyspace
     */
    public boolean satisfies(ConsistencyLevel other, Keyspace keyspace)
    {
        return blockFor(keyspace) >= other.blockFor(keyspace);
    }

    public boolean isDatacenterLocal()
    {
        return isDCLocal;
    }

    private static ReplicaCount countDCLocalReplicas(ReplicaCollection<?> liveReplicas)
    {
        Predicate<Replica> isDCSameAsSelf = SameDCTester.replicas();
        ReplicaCount count = new ReplicaCount();
        for (Replica replica : liveReplicas)
            if (isDCSameAsSelf.test(replica))
                count.increment(replica);
        return count;
    }

    private static class ReplicaCount
    {
        int fullReplicas;
        int transientReplicas;

        int allReplicas()
        {
            return fullReplicas + transientReplicas;
        }

        void increment(Replica replica)
        {
            if (replica.isFull()) ++fullReplicas;
            else ++transientReplicas;
        }

        boolean isSufficient(int allReplicas, int fullReplicas)
        {
            return this.fullReplicas >= fullReplicas
                    && this.allReplicas() >= allReplicas;
        }
    }

    private static Map<String, ReplicaCount> countPerDCEndpoints(Keyspace keyspace, Iterable<Replica> liveReplicas)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();

        Map<String, ReplicaCount> dcEndpoints = new HashMap<>();
        for (String dc: strategy.getDatacenters())
            dcEndpoints.put(dc, new ReplicaCount());

        for (Replica replica : liveReplicas)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(replica);
            dcEndpoints.get(dc).increment(replica);
        }
        return dcEndpoints;
    }

    public boolean isSufficientReplicasForRead(Keyspace keyspace, Endpoints<?> liveReplicas)
    {
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                return true;
            case LOCAL_ONE:
                return countDCLocalReplicas(liveReplicas).isSufficient(1, 1);
            case LOCAL_QUORUM:
                return countDCLocalReplicas(liveReplicas).isSufficient(blockFor(keyspace), 1);
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    int fullCount = 0;
                    for (Map.Entry<String, ReplicaCount> entry : countPerDCEndpoints(keyspace, liveReplicas).entrySet())
                    {
                        ReplicaCount count = entry.getValue();
                        if (!count.isSufficient(localQuorumFor(keyspace, entry.getKey()), 0))
                            return false;
                        fullCount += count.fullReplicas;
                    }
                    return fullCount > 0;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                return liveReplicas.size() >= blockFor(keyspace)
                        && Replicas.countFull(liveReplicas) > 0;
        }
    }

    public void assureSufficientReplicasForRead(Keyspace keyspace, Endpoints<?> liveReplicas) throws UnavailableException
    {
        assureSufficientReplicas(keyspace, liveReplicas, blockFor(keyspace), 1);
    }
    public void assureSufficientReplicasForWrite(Keyspace keyspace, Endpoints<?> allLive, Endpoints<?> pendingWithDown) throws UnavailableException
    {
        assureSufficientReplicas(keyspace, allLive, blockForWrite(keyspace, pendingWithDown), 0);
    }
    void assureSufficientReplicas(Keyspace keyspace, Endpoints<?> allLive, int blockFor, int blockForFullReplicas) throws UnavailableException
    {
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                break;
            case LOCAL_ONE:
            {
                ReplicaCount localLive = countDCLocalReplicas(allLive);
                if (!localLive.isSufficient(blockFor, blockForFullReplicas))
                    throw UnavailableException.create(this, 1, blockForFullReplicas, localLive.allReplicas(), localLive.fullReplicas);
                break;
            }
            case LOCAL_QUORUM:
            {
                ReplicaCount localLive = countDCLocalReplicas(allLive);
                if (!localLive.isSufficient(blockFor, blockForFullReplicas))
                {
                    if (logger.isTraceEnabled())
                    {
                        logger.trace(String.format("Local replicas %s are insufficient to satisfy LOCAL_QUORUM requirement of %d live replicas and %d full replicas in '%s'",
                                allLive.filter(SameDCTester.replicas()), blockFor, blockForFullReplicas, DatabaseDescriptor.getLocalDataCenter()));
                    }
                    throw UnavailableException.create(this, blockFor, blockForFullReplicas, localLive.allReplicas(), localLive.fullReplicas);
                }
                break;
            }
            case EACH_QUORUM:
                if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                {
                    int total = 0;
                    int totalFull = 0;
                    for (Map.Entry<String, ReplicaCount> entry : countPerDCEndpoints(keyspace, allLive).entrySet())
                    {
                        int dcBlockFor = localQuorumFor(keyspace, entry.getKey());
                        ReplicaCount dcCount = entry.getValue();
                        if (!dcCount.isSufficient(dcBlockFor, 0))
                            throw UnavailableException.create(this, entry.getKey(), dcBlockFor, dcCount.allReplicas(), 0, dcCount.fullReplicas);
                        totalFull += dcCount.fullReplicas;
                        total += dcCount.allReplicas();
                    }
                    if (totalFull < blockForFullReplicas)
                        throw UnavailableException.create(this, blockFor, total, blockForFullReplicas, totalFull);
                    break;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                int live = allLive.size();
                int full = Replicas.countFull(allLive);
                if (live < blockFor || full < blockForFullReplicas)
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Live nodes {} do not satisfy ConsistencyLevel ({} required)", Iterables.toString(allLive), blockFor);
                    throw UnavailableException.create(this, blockFor, blockForFullReplicas, live, full);
                }
                break;
        }
    }

    public void validateForRead(String keyspaceName) throws InvalidRequestException
    {
        switch (this)
        {
            case ANY:
                throw new InvalidRequestException("ANY ConsistencyLevel is only supported for writes");
        }
    }

    public void validateForWrite(String keyspaceName) throws InvalidRequestException
    {
        switch (this)
        {
            case SERIAL:
            case LOCAL_SERIAL:
                throw new InvalidRequestException("You must use conditional updates for serializable writes");
        }
    }

    // This is the same than validateForWrite really, but we include a slightly different error message for SERIAL/LOCAL_SERIAL
    public void validateForCasCommit(String keyspaceName) throws InvalidRequestException
    {
        switch (this)
        {
            case EACH_QUORUM:
                requireNetworkTopologyStrategy(keyspaceName);
                break;
            case SERIAL:
            case LOCAL_SERIAL:
                throw new InvalidRequestException(this + " is not supported as conditional update commit consistency. Use ANY if you mean \"make sure it is accepted but I don't care how many replicas commit it for non-SERIAL reads\"");
        }
    }

    public void validateForCas() throws InvalidRequestException
    {
        if (!isSerialConsistency())
            throw new InvalidRequestException("Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL");
    }

    public boolean isSerialConsistency()
    {
        return this == SERIAL || this == LOCAL_SERIAL;
    }

    public void validateCounterForWrite(TableMetadata metadata) throws InvalidRequestException
    {
        if (this == ConsistencyLevel.ANY)
            throw new InvalidRequestException("Consistency level ANY is not yet supported for counter table " + metadata.name);

        if (isSerialConsistency())
            throw new InvalidRequestException("Counter operations are inherently non-serializable");
    }

    private void requireNetworkTopologyStrategy(String keyspaceName) throws InvalidRequestException
    {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
        if (!(strategy instanceof NetworkTopologyStrategy))
            throw new InvalidRequestException(String.format("consistency level %s not compatible with replication strategy (%s)", this, strategy.getClass().getName()));
    }
}
