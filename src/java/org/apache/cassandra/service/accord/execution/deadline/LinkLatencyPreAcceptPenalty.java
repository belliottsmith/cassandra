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

package org.apache.cassandra.service.accord.execution.deadline;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.primitives.TxnId;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

/**
 * If the maximum one-way latency from any DC to us is Y, we wait until txnId.hlc() + Y; i.e. until any PreAccept with
 * a lower txnId should have arrived, from whichever DC it was sent (assuming synchronised clocks). Equivalently, if the
 * latency from the sender's DC is X, we wait Y-X after the (expected) receipt. Since Y is the same for every PreAccept
 * we receive, the deadlines are in txnId order, as the deferred tasks are released.
 *
 * The one-way latency from each DC is estimated as half the round-trip between this node and that DC measured by
 * {@link LinkLatencies}, so {@code link_latency_probe_interval} must be configured. A DC with no estimate (yet) is
 * ignored, so no penalty is applied until some estimate is available.
 *
 * Note that every DC we know of is considered, as any node may coordinate a transaction.
 *
 * Takes no parameters.
 */
public class LinkLatencyPreAcceptPenalty implements PreAcceptPenalty
{
    private static final Logger logger = LoggerFactory.getLogger(LinkLatencyPreAcceptPenalty.class);

    static final class Snapshot
    {
        final Epoch epoch;
        final long version;
        final long maxInbound; // Y

        Snapshot(Epoch epoch, long version, long maxInbound)
        {
            this.epoch = epoch;
            this.version = version;
            this.maxInbound = maxInbound;
        }
    }

    private final Supplier<LinkLatencies.Snapshot> latencies;
    private volatile Snapshot snapshot;

    public LinkLatencyPreAcceptPenalty(Map<String, String> parameters)
    {
        this(LinkLatencies.instance::snapshot);
        if (!parameters.isEmpty())
            throw new ConfigurationException(getClass().getSimpleName() + " takes no parameters (link latencies are measured by LinkLatencies); found " + parameters.keySet());
        if (DatabaseDescriptor.getLinkLatencyProbeInterval() == null)
            logger.warn("{} is configured but link_latency_probe_interval is not, so no link latencies will be measured and no PreAccept penalty will be applied",
                        getClass().getSimpleName());
    }

    @VisibleForTesting
    LinkLatencyPreAcceptPenalty(Supplier<LinkLatencies.Snapshot> latencies)
    {
        this.latencies = latencies;
    }

    @Override
    public long penaltyMicros(Node.Id local, Node.Id coordinator, TxnId txnId)
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null)
            return 0;

        return snapshot(metadata).maxInbound;
    }

    private Snapshot snapshot(ClusterMetadata metadata)
    {
        Snapshot snapshot = this.snapshot;
        LinkLatencies.Snapshot latencies = this.latencies.get();
        if (snapshot != null && snapshot.epoch.equals(metadata.epoch) && snapshot.version == latencies.version)
            return snapshot;

        this.snapshot = snapshot = new Snapshot(metadata.epoch, latencies.version, maxInbound(metadata.directory.knownDatacenters(), latencies));
        return snapshot;
    }

    @VisibleForTesting
    static long maxInbound(Collection<String> dcs, LinkLatencies.Snapshot latencies)
    {
        long max = 0;
        for (String dc : dcs)
        {
            long roundTrip = latencies.roundTripMicros(dc);
            if (roundTrip != LinkLatencies.Snapshot.UNKNOWN)
                max = Math.max(max, roundTrip / 2);
        }
        return max;
    }
}
