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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
    private final ConsistencyLevel consistency;
    private final ConsistencyLevel.ResponseTracker blockFor;
    private final List<InetAddress> additional;
    private final Map<InetAddress, Mutation> pendingRepairs;

    private volatile long mutationsSentTime;

    public ReadRepairHandler(ConsistencyLevel consistency,
                             Map<InetAddress, Mutation> repairs,
                             ConsistencyLevel.ResponseTracker blockFor,
                             List<InetAddress> initial,
                             List<InetAddress> additional)
    {
        this.consistency = consistency;
        this.pendingRepairs = new ConcurrentHashMap<>(repairs);
        this.additional = additional;

        this.blockFor = blockFor;
        // here we remove empty repair mutations from the block for total, since
        // we're not sending them mutations
        for (InetAddress participant : initial)
        {
            // remote dcs can sometimes get involved in dc-local reads. We want to repair
            // them if they do, but they shouldn't interfere with blocking the client read.
            if (!repairs.containsKey(participant))
                this.blockFor.receive(participant);
        }
    }

    @VisibleForTesting
    int waitingOn()
    {
        return blockFor.waitingOn();
    }

    @VisibleForTesting
    int numUnackedRepairs()
    {
        return pendingRepairs.size();
    }

    int blockFor()
    {
        return blockFor.blockFor();
    }

    @VisibleForTesting
    void ack(InetAddress from)
    {
        pendingRepairs.remove(from);
        blockFor.receive(from);
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

            if (!blockFor.waitsOn(destination))
                pendingRepairs.remove(destination);
        }
    }

    public boolean awaitRepairs(long timeout, TimeUnit timeoutUnit)
    {
        long elapsed = System.nanoTime() - mutationsSentTime;
        long remaining = timeoutUnit.toNanos(timeout) - elapsed;

        try
        {
            return blockFor.await(remaining, TimeUnit.NANOSECONDS);
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

        if (additional.isEmpty())
            return;

        PartitionUpdate update = mergeUnackedUpdates();
        if (update == null)
            // final response was received between speculate
            // timeout and call to get unacked mutation.
            return;

        Mutation[] versionedMutations = new Mutation[msgVersionIdx(MessagingService.current_version) + 1];

        for (InetAddress endpoint : additional)
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

