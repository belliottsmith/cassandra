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
package org.apache.cassandra.service.reads.repair;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.locator.Endpoints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ReplicaPlan;
import org.apache.cassandra.service.reads.DigestResolver;

public interface ReadRepair
{
    public interface Factory
    {
        ReadRepair create(ReadCommand command, ReplicaPlan replicaPlan, long queryStartNanoTime);
    }

    /**
     * Used by DataResolver to generate corrections as the partition iterator is consumed
     */
    UnfilteredPartitionIterators.MergeListener getMergeListener(Endpoints<?> replicas);

    /**
     * Called when the digests from the initial read don't match. Reads may block on the
     * repair started by this method.
     * @param digestResolver supplied so we can get the original data response
     * @param resultConsumer hook for the repair to set it's result on completion
     */
    public void startRepair(DigestResolver digestResolver, Consumer<PartitionIterator> resultConsumer);

    /**
     * Block on the reads (or timeout) sent out in {@link ReadRepair#startRepair}
     */
    public void awaitReads() throws ReadTimeoutException;

    /**
     * if it looks like we might not receive data requests from everyone in time, send additional requests
     * to additional replicas not contacted in the initial full data read. If the collection of nodes that
     * end up responding in time end up agreeing on the data, and we don't consider the response from the
     * disagreeing replica that triggered the read repair, that's ok, since the disagreeing data would not
     * have been successfully written and won't be included in the response the the client, preserving the
     * expectation of monotonic quorum reads
     */
    public void maybeSendAdditionalReads();

    /**
     * If it looks like we might not receive acks for all the repair mutations we sent out, combine all
     * the unacked mutations and send them to the minority of nodes not involved in the read repair data
     * read / write cycle. We will accept acks from them in lieu of acks from the initial mutations sent
     * out, so long as we receive the same number of acks as repair mutations transmitted. This prevents
     * misbehaving nodes from killing a quorum read, while continuing to guarantee monotonic quorum reads
     */
    public void maybeSendAdditionalWrites();

    /**
     * Block on any mutations (or timeout) we sent out to repair replicas in {@link ReadRepair#repairPartition}
     */
    public void awaitWrites();

    /**
     * Repairs a partition _after_ receiving data responses. This method receives replica list, since
     * we will block repair only on the replicas that have responded.
     */
    void repairPartition(Map<Replica, Mutation> mutations, Endpoints<?> targets);

    static ReadRepair create(ReadCommand command, ReplicaPlan replicaPlan, long queryStartNanoTime)
    {
        return command.metadata().params.readRepair.create(command, replicaPlan, queryStartNanoTime);
    }
}
