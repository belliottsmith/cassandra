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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.transport.ProtocolException;

import static com.google.common.collect.Iterables.*;

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
    UNSAFE_DELAY_QUORUM(99, false),
    UNSAFE_DELAY_SERIAL(100, false),
    UNSAFE_DELAY_LOCAL_QUORUM(101, true),
    UNSAFE_DELAY_LOCAL_SERIAL(102, true);

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

    public static interface ResponseTracker
    {
        int blockFor();

        /**
         *
         * @return
         */
        int waitingOn();

        /**
         * @return true iff sufficient responses have been received, i.e. waitingOn() == 0
         */
        boolean await(long timeout, TimeUnit units) throws InterruptedException;

        /**
         * @return true iff we were waiting for the response
         */
        boolean receive(InetAddress address);

        /**
         * @return true iff we are waiting for a response from this node
         */
        boolean waitsOn(InetAddress address);
    }

    public static class SimpleResponseTracker implements ResponseTracker
    {
        final int blockFor;
        final CountDownLatch latch;
        @VisibleForTesting
        protected SimpleResponseTracker(int blockFor)
        {
            this.blockFor = blockFor;
            this.latch = new CountDownLatch(blockFor);
        }

        public int blockFor()
        {
            return blockFor;
        }

        public int waitingOn()
        {
            return (int) latch.getCount();
        }

        public boolean await(long timeout, TimeUnit units) throws InterruptedException
        {
            return latch.await(timeout, units);
        }

        public boolean receive(InetAddress address)
        {
            if (!waitsOn(address))
                return false;

            latch.countDown();
            return true;
        }

        public boolean waitsOn(InetAddress address)
        {
            return true;
        }
    }

    public static class LocalResponseTracker extends SimpleResponseTracker
    {
        private LocalResponseTracker(int blockFor)
        {
            super(blockFor);
        }

        public boolean waitsOn(InetAddress address)
        {
            return isLocal(address);
        }
    }

    public static class EachQuorumResponseTracker extends SimpleResponseTracker
    {
        final Map<String, AtomicInteger> perDc;
        private EachQuorumResponseTracker(int blockFor, Map<String, AtomicInteger> perDc)
        {
            super(blockFor);
            this.perDc = perDc;
        }

        public boolean receive(InetAddress address)
        {
            if (0 <= perDc.get(DatabaseDescriptor.getEndpointSnitch().getDatacenter(address)).decrementAndGet())
                latch.countDown();
            return true;
        }
    }

    public ResponseTracker trackWrite(AbstractReplicationStrategy replicationStrategy, Collection<InetAddress> live, Collection<InetAddress> allPending) throws UnavailableException
    {
        switch (this)
        {
            case LOCAL_ONE:
            case UNSAFE_DELAY_LOCAL_QUORUM:
            case LOCAL_QUORUM:
            case UNSAFE_DELAY_LOCAL_SERIAL:
            case LOCAL_SERIAL:
            {
                int blockFor = blockFor(replicationStrategy) + size(filter(allPending, ConsistencyLevel::isLocal));
                assureSufficientLiveNodes(replicationStrategy, live, blockFor);
                return new LocalResponseTracker(blockFor);
            }
            case EACH_QUORUM:
            {
                if ((replicationStrategy instanceof NetworkTopologyStrategy))
                {
                    NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicationStrategy;

                    Map<String, Integer> pendingByDc = ConsistencyLevel.countPerDCEndpoints(replicationStrategy, allPending);
                    Map<String, Integer> endpointsByDc = ConsistencyLevel.countPerDCEndpoints(replicationStrategy, live);
                    Map<String, AtomicInteger> blockForByDc = new HashMap<>();

                    int totalBlockFor = 0;
                    for (String dc : strategy.getDatacenters())
                    {
                        int dcBlockFor = ConsistencyLevel.localQuorumFor(replicationStrategy, dc)
                                         + pendingByDc.getOrDefault(dc, 0);

                        int liveForDc = endpointsByDc.getOrDefault(dc, 0);
                        if (liveForDc < dcBlockFor)
                            throw new UnavailableException(ConsistencyLevel.EACH_QUORUM, dc, dcBlockFor, liveForDc);

                        blockForByDc.put(dc, new AtomicInteger(dcBlockFor));
                        totalBlockFor += dcBlockFor;
                    }

                    return new EachQuorumResponseTracker(totalBlockFor, blockForByDc);
                }
            }
            default:
            {
                int blockFor = blockFor(replicationStrategy) + allPending.size();
                assureSufficientLiveNodes(replicationStrategy, live, blockFor);
                return new SimpleResponseTracker(blockFor);
            }
        }
    }

    public static Map<String, Integer> countPerDCEndpoints(AbstractReplicationStrategy replicationStrategy, Iterable<InetAddress> liveEndpoints)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicationStrategy;

        Map<String, Integer> dcEndpoints = new HashMap<>();
        for (String dc: strategy.getDatacenters())
            dcEndpoints.put(dc, 0);

        for (InetAddress endpoint : liveEndpoints)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            dcEndpoints.put(dc, dcEndpoints.get(dc) + 1);
        }
        return dcEndpoints;
    }

    private static int quorumFor(AbstractReplicationStrategy replicationStrategy)
    {
        return (replicationStrategy.getReplicationFactor() / 2) + 1;
    }

    private static int localQuorumFor(AbstractReplicationStrategy replicationStrategy, String dc)
    {
        return (replicationStrategy instanceof NetworkTopologyStrategy)
               ? (((NetworkTopologyStrategy) replicationStrategy).getReplicationFactor(dc) / 2) + 1
               : quorumFor(replicationStrategy);
    }

    public int blockFor(Keyspace keyspace)
    {
        return blockFor(keyspace.getReplicationStrategy());
    }

    public int blockFor(AbstractReplicationStrategy replicationStrategy)
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
            case UNSAFE_DELAY_QUORUM:
            case SERIAL:
            case UNSAFE_DELAY_SERIAL:
                return quorumFor(replicationStrategy);
            case ALL:
                return replicationStrategy.getReplicationFactor();
            case UNSAFE_DELAY_LOCAL_QUORUM:
            case LOCAL_QUORUM:
            case UNSAFE_DELAY_LOCAL_SERIAL:
            case LOCAL_SERIAL:
                return localQuorumFor(replicationStrategy, DatabaseDescriptor.getLocalDataCenter());
            case EACH_QUORUM:
                if (replicationStrategy instanceof NetworkTopologyStrategy)
                {
                    NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicationStrategy;
                    int n = 0;
                    for (String dc : strategy.getDatacenters())
                        n += localQuorumFor(replicationStrategy, dc);
                    return n;
                }
                else
                {
                    return quorumFor(replicationStrategy);
                }
            default:
                throw new UnsupportedOperationException("Invalid consistency level: " + toString());
        }
    }

    /**
     * Determine if this consistency level meets or exceeds the consistency requirements of the given cl for the given keyspace
     */
    public boolean satisfies(ConsistencyLevel other, Keyspace keyspace)
    {
        AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
        return blockFor(rs) >= other.blockFor(rs);
    }

    public boolean isDatacenterLocal()
    {
        return isDCLocal;
    }

    public static boolean isLocal(InetAddress endpoint)
    {
        return DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint));
    }

    public int countLocalEndpoints(Iterable<InetAddress> liveEndpoints)
    {
        int count = 0;
        for (InetAddress endpoint : liveEndpoints)
            if (isLocal(endpoint))
                count++;
        return count;
    }

    public List<InetAddress> filterForQuery(AbstractReplicationStrategy replicationStrategy, List<InetAddress> liveEndpoints)
    {
        return filterForQuery(replicationStrategy, liveEndpoints, ReadRepairDecision.NONE);
    }

    public List<InetAddress> filterForQuery(Keyspace keyspace, List<InetAddress> liveEndpoints, ReadRepairDecision readRepair)
    {
        return filterForQuery(keyspace.getReplicationStrategy(), liveEndpoints, readRepair);
    }

    public List<InetAddress> filterForQuery(AbstractReplicationStrategy replicationStrategy, List<InetAddress> liveEndpoints, ReadRepairDecision readRepair)
    {
        /*
         * If we are doing an each quorum query, we have to make sure that the endpoints we select
         * provide a quorum for each data center. If we are not using a NetworkTopologyStrategy,
         * we should fall through and grab a quorum in the replication strategy.
         */
        if (this == EACH_QUORUM && replicationStrategy instanceof NetworkTopologyStrategy)
            return filterForEachQuorum(replicationStrategy, liveEndpoints, readRepair);

        /*
         * Endpoints are expected to be restricted to live replicas, sorted by snitch preference.
         * For LOCAL_QUORUM, move local-DC replicas in front first as we need them there whether
         * we do read repair (since the first replica gets the data read) or not (since we'll take
         * the blockFor first ones).
         */
        if (isDCLocal)
            Collections.sort(liveEndpoints, DatabaseDescriptor.getLocalComparator());

        switch (readRepair)
        {
            case NONE:
                return liveEndpoints.subList(0, Math.min(liveEndpoints.size(), blockFor(replicationStrategy)));
            case GLOBAL:
                return liveEndpoints;
            case DC_LOCAL:
                List<InetAddress> local = new ArrayList<InetAddress>();
                List<InetAddress> other = new ArrayList<InetAddress>();
                for (InetAddress add : liveEndpoints)
                {
                    if (isLocal(add))
                        local.add(add);
                    else
                        other.add(add);
                }
                // check if blockfor more than we have localep's
                int blockFor = blockFor(replicationStrategy);
                if (local.size() < blockFor)
                    local.addAll(other.subList(0, Math.min(blockFor - local.size(), other.size())));
                return local;
            default:
                throw new AssertionError();
        }
    }

    public static List<InetAddress> filterForEachQuorum(Keyspace keyspace, List<InetAddress> liveEndpoints, ReadRepairDecision readRepair)
    {
        return filterForEachQuorum(keyspace.getReplicationStrategy(), liveEndpoints, readRepair);
    }

    public static List<InetAddress> filterForEachQuorum(AbstractReplicationStrategy replicationStrategy, List<InetAddress> liveEndpoints, ReadRepairDecision readRepair)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicationStrategy;

        // quickly drop out if read repair is GLOBAL, since we just use all of the live endpoints
        if (readRepair == ReadRepairDecision.GLOBAL)
            return liveEndpoints;

        Map<String, List<InetAddress>> dcsEndpoints = new HashMap<>();
        for (String dc: strategy.getDatacenters())
            dcsEndpoints.put(dc, new ArrayList<>());

        for (InetAddress add : liveEndpoints)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(add);
            dcsEndpoints.get(dc).add(add);
        }

        List<InetAddress> waitSet = new ArrayList<>();
        for (Map.Entry<String, List<InetAddress>> dcEndpoints : dcsEndpoints.entrySet())
        {
            List<InetAddress> dcEndpoint = dcEndpoints.getValue();
            if (readRepair == ReadRepairDecision.DC_LOCAL && dcEndpoints.getKey().equals(DatabaseDescriptor.getLocalDataCenter()))
                waitSet.addAll(dcEndpoint);
            else
                waitSet.addAll(dcEndpoint.subList(0, Math.min(localQuorumFor(replicationStrategy, dcEndpoints.getKey()), dcEndpoint.size())));
        }

        return waitSet;
    }

    public boolean isSufficientLiveNodes(Keyspace keyspace, Iterable<InetAddress> liveEndpoints)
    {
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                return true;
            case LOCAL_ONE:
                return countLocalEndpoints(liveEndpoints) >= 1;
            case UNSAFE_DELAY_LOCAL_QUORUM:
            case LOCAL_QUORUM:
                return countLocalEndpoints(liveEndpoints) >= blockFor(replicationStrategy);
            case EACH_QUORUM:
                if (replicationStrategy instanceof NetworkTopologyStrategy)
                {
                    for (Map.Entry<String, Integer> entry : countPerDCEndpoints(replicationStrategy, liveEndpoints).entrySet())
                    {
                        if (entry.getValue() < localQuorumFor(replicationStrategy, entry.getKey()))
                            return false;
                    }
                    return true;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                return size(liveEndpoints) >= blockFor(replicationStrategy);
        }
    }

    public void assureSufficientLiveNodes(AbstractReplicationStrategy replicationStrategy, Iterable<InetAddress> liveEndpoints) throws UnavailableException
    {
        assureSufficientLiveNodes(replicationStrategy, liveEndpoints, blockFor(replicationStrategy));
    }

    public void assureSufficientLiveNodes(AbstractReplicationStrategy replicationStrategy, Iterable<InetAddress> liveEndpoints, int blockFor) throws UnavailableException
    {
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                break;
            case LOCAL_ONE:
                if (countLocalEndpoints(liveEndpoints) < blockFor)
                    throw new UnavailableException(this, 1, 0);
                break;
            case UNSAFE_DELAY_LOCAL_QUORUM:
            case LOCAL_QUORUM:
                int localLive = countLocalEndpoints(liveEndpoints);
                if (localLive < blockFor)
                {
                    if (logger.isTraceEnabled())
                    {
                        StringBuilder builder = new StringBuilder("Local replicas [");
                        for (InetAddress endpoint : liveEndpoints)
                        {
                            if (isLocal(endpoint))
                                builder.append(endpoint).append(",");
                        }
                        builder.append("] are insufficient to satisfy LOCAL_QUORUM requirement of ").append(blockFor).append(" live nodes in '").append(DatabaseDescriptor.getLocalDataCenter()).append("'");
                        logger.trace(builder.toString());
                    }
                    throw new UnavailableException(this, blockFor, localLive);
                }
                break;
            case EACH_QUORUM:
                if (replicationStrategy instanceof NetworkTopologyStrategy)
                {
                    for (Map.Entry<String, Integer> entry : countPerDCEndpoints(replicationStrategy, liveEndpoints).entrySet())
                    {
                        int dcBlockFor = localQuorumFor(replicationStrategy, entry.getKey());
                        int dcLive = entry.getValue();
                        if (dcLive < dcBlockFor)
                            throw new UnavailableException(this, entry.getKey(), dcBlockFor, dcLive);
                    }
                    break;
                }
                // Fallthough on purpose for SimpleStrategy
            default:
                int live = size(liveEndpoints);
                if (live < blockFor)
                {
                    logger.trace("Live nodes {} do not satisfy ConsistencyLevel ({} required)", Iterables.toString(liveEndpoints), blockFor);
                    throw new UnavailableException(this, blockFor, live);
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
            case UNSAFE_DELAY_SERIAL:
            case LOCAL_SERIAL:
            case UNSAFE_DELAY_LOCAL_SERIAL:
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
            case UNSAFE_DELAY_SERIAL:
            case LOCAL_SERIAL:
            case UNSAFE_DELAY_LOCAL_SERIAL:
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
        switch (this)
        {
            case SERIAL:
            case UNSAFE_DELAY_SERIAL:
            case LOCAL_SERIAL:
            case UNSAFE_DELAY_LOCAL_SERIAL:
                return true;
            default:
                return false;
        }
    }

    public void validateCounterForWrite(CFMetaData metadata) throws InvalidRequestException
    {
        if (this == ConsistencyLevel.ANY)
            throw new InvalidRequestException("Consistency level ANY is not yet supported for counter table " + metadata.cfName);

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
