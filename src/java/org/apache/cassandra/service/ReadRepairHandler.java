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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

public class ReadRepairHandler implements IAsyncCallback
{
    private final Keyspace keyspace;
    private final DecoratedKey key;
    private final ConsistencyLevel consistency;
    private final InetAddress[] participants;
    private final Map<InetAddress, Mutation> pendingRepairs;
    private final CountDownLatch latch;

    private volatile long mutationsSentTime;

    public ReadRepairHandler(Keyspace keyspace, DecoratedKey key, ConsistencyLevel consistency, Map<InetAddress, Mutation> repairs, int maxBlockFor, InetAddress[] participants)
    {
        this.keyspace = keyspace;
        this.key = key;
        this.consistency = consistency;
        this.pendingRepairs = new ConcurrentHashMap<>(repairs);
        this.participants = participants;

        // here we remove empty repair mutations from the block for total, since
        // we're not sending them mutations
        int blockFor = maxBlockFor;
        for (InetAddress participant: participants)
        {
            // remote dcs can sometimes get involved in dc-local reads. We want to repair
            // them if they do, but they shouldn't interfere with blocking the client read.
            if (!repairs.containsKey(participant) && shouldBlockOn(participant))
                blockFor--;
        }

        // there are some cases where logically identical data can return different digests
        // For read repair, this would result in ReadRepairHandler being called with a map of
        // empty mutations. If we'd also speculated on either of the read stages, the number
        // of empty mutations would be greater than blockFor, causing the latch ctor to throw
        // an illegal argument exception due to a negative start value. So here we clamp it 0
        latch = new CountDownLatch(Math.max(blockFor, 0));
    }

    @VisibleForTesting
    long waitingOn()
    {
        return latch.getCount();
    }

    @VisibleForTesting
    boolean isLocal(InetAddress endpoint)
    {
        return ConsistencyLevel.isLocal(endpoint);
    }

    private boolean shouldBlockOn(InetAddress endpoint)
    {
        return !consistency.isDatacenterLocal() || isLocal(endpoint);
    }

    @VisibleForTesting
    int numUnackedRepairs()
    {
        return pendingRepairs.size();
    }

    @VisibleForTesting
    void ack(InetAddress from)
    {
        if (shouldBlockOn(from))
        {
            pendingRepairs.remove(from);
            latch.countDown();
        }
    }

    public void response(MessageIn msg)
    {
        ack(msg.from);
    }

    private static PartitionUpdate extractUpdate(Mutation mutation)
    {
        return Iterables.getOnlyElement(mutation.getPartitionUpdates());
    }

    /**
     * Combine the contents of any unacked repair into a single update
     */
    private PartitionUpdate mergeUnackedUpdates()
    {
        // recombinate the updates
        List<PartitionUpdate> updates = Lists.newArrayList(Iterables.transform(pendingRepairs.values(), ReadRepairHandler::extractUpdate));
        return updates.isEmpty() ? null : PartitionUpdate.merge(updates);
    }

    @VisibleForTesting
    protected void sendRR(MessageOut<Mutation> message, InetAddress endpoint)
    {
        MessagingService.instance().sendRR(message, endpoint, this);
    }

    public void sendInitialRepairs()
    {
        mutationsSentTime = System.nanoTime();
        for (Map.Entry<InetAddress, Mutation> entry: pendingRepairs.entrySet())
        {
            InetAddress destination = entry.getKey();
            Mutation mutation = entry.getValue();
            UUID cfId = extractUpdate(mutation).metadata().cfId;

            Tracing.trace("Sending read-repair-mutation to {}", destination);
            // use a separate verb here to avoid writing hints on timeouts
            sendRR(mutation.createMessage(MessagingService.Verb.READ_REPAIR), destination);
            ColumnFamilyStore.metricsFor(cfId).readRepairRequests.mark();

            if (!shouldBlockOn(destination))
                pendingRepairs.remove(destination);
        }
    }

    public boolean awaitRepairs(long timeout, TimeUnit timeoutUnit)
    {
        long elapsed = System.nanoTime() - mutationsSentTime;
        long remaining = timeoutUnit.toNanos(timeout) - elapsed;

        try
        {
            return latch.await(remaining, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static int msgVersionIdx(int version)
    {
        return version - MessagingService.min_version;
    }

    /**
     * If it looks like we might not receive acks for all the repair mutations we sent out, combine all
     * the unacked mutations and send them to the minority of nodes not involved in the read repair data
     * read / write cycle. We will accept acks from them in lieu of acks from the initial mutations sent
     * out, so long as we receive the same number of acks as repair mutations transmitted. This prevents
     * misbehaving nodes from killing a quorum read, while continuing to guarantee monotonic quorum reads
     */
    public void maybeSendAdditionalRepairs(long timeout, TimeUnit timeoutUnit)
    {
        if (awaitRepairs(timeout, timeoutUnit))
            return;

        ReadRepairMetrics.speculatedDataRepair.mark();

        Set<InetAddress> exclude = Sets.newHashSet(participants);
        Iterable<InetAddress> candidates = Iterables.filter(getCandidateEndpoints(), e -> !exclude.contains(e));
        if (Iterables.isEmpty(candidates))
            return;

        PartitionUpdate update = mergeUnackedUpdates();
        if (update == null)
            // final response was received between speculate
            // timeout and call to get unacked mutation.
            return;

        Mutation[] versionedMutations = new Mutation[msgVersionIdx(MessagingService.current_version) + 1];

        for (InetAddress endpoint: candidates)
        {
            int versionIdx = msgVersionIdx(MessagingService.instance().getVersion(endpoint));

            Mutation mutation = versionedMutations[versionIdx];

            if (mutation == null)
            {
                mutation = DataResolver.createRepairMutation(update, consistency, endpoint, true);
                versionedMutations[versionIdx] = mutation;
            }

            if (mutation == null)
            {
                // the mutation is too large to send.
                continue;
            }

            Tracing.trace("Sending speculative read-repair-mutation to {}", endpoint);
            sendRR(mutation.createMessage(MessagingService.Verb.READ_REPAIR), endpoint);
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    @VisibleForTesting
    protected Iterable<InetAddress> getCandidateEndpoints()
    {
        return getCandidateEndpoints(keyspace, key, consistency);
    }

    /**
     * Returns all of the endpoints that are replicas for the given key. If the consistency level is datacenter
     * local, only the endpoints in the local dc will be returned.
     */
    private static Iterable<InetAddress> getCandidateEndpoints(Keyspace keyspace, DecoratedKey key, ConsistencyLevel consistency)
    {
        List<InetAddress> endpoints = StorageProxy.getLiveSortedEndpoints(keyspace, key);
        return consistency.isDatacenterLocal() && keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy
               ? Iterables.filter(endpoints, ConsistencyLevel::isLocal)
               : endpoints;
    }

    static Iterable<InetAddress> getCandidateEndpoints(AbstractReadExecutor executor)
    {
        return getCandidateEndpoints(executor.keyspace, executor.key(), executor.consistency);
    }
}

