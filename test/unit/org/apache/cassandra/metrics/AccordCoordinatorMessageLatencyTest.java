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

package org.apache.cassandra.metrics;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics.MessageLatency;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.TimeoutStrategy;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.assertj.core.api.Assertions.assertThat;

public class AccordCoordinatorMessageLatencyTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRequestName()
    {
        assertThat(MessageLatency.requestName(Verb.ACCORD_PRE_ACCEPT_REQ)).isEqualTo("PreAccept");
        assertThat(MessageLatency.requestName(Verb.ACCORD_STABLE_THEN_READ_REQ)).isEqualTo("StableThenRead");
        assertThat(MessageLatency.requestName(Verb.ACCORD_INTEROP_READ_REQ)).isEqualTo("InteropRead");
    }

    @Test
    public void testScopeAndContext()
    {
        TxnId write = new TxnId(1, 1, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
        MessageLatency writes = AccordCoordinatorMetrics.messageLatency(Verb.ACCORD_ACCEPT_REQ, write);
        assertThat(writes).isSameAs(AccordCoordinatorMetrics.writeMetrics.messageLatency(Verb.ACCORD_ACCEPT_REQ));
        assertThat(AccordCoordinatorMetrics.messageLatency(null, write)).isNull();
        assertThat(AccordCoordinatorMetrics.messageLatency(Verb.ACCORD_ACCEPT_REQ, null)).isNull();

        // anything other than a MessageLatency is ignored
        AccordCoordinatorMetrics.recordMeasured(null, "dc", 1, 0);
        AccordCoordinatorMetrics.recordInferred("unrelated", "dc", 1, 0);
    }

    @Test
    public void testRecordMeasured()
    {
        MessageLatency metrics = AccordCoordinatorMetrics.syncPointMetrics.messageLatency(Verb.ACCORD_APPLY_REQ);

        AccordCoordinatorMetrics.recordMeasured(metrics, "dc1", ms(70), ms(10));
        // reported time fractionally exceeding the (micro precision) round-trip is capped at the round-trip
        AccordCoordinatorMetrics.recordMeasured(metrics, LinkLatencies.SELF, ms(5), ms(5) + 500);
        // with an unknown destination, recorded only in the total
        AccordCoordinatorMetrics.recordMeasured(metrics, null, ms(5), ms(1));
        // negative values are discarded
        metrics.recordMeasured("dc1", -1, 0);
        metrics.recordMeasured("dc1", ms(1), -1);

        assertThat(metrics.roundTrip.getCount()).isEqualTo(3);
        assertThat(metrics.roundTrip("dc1").getCount()).isEqualTo(1);
        assertThat(metrics.roundTrip(LinkLatencies.SELF).getCount()).isEqualTo(1);
        assertThat(metrics.atReplica.getCount()).isEqualTo(3);
        assertThat(metrics.atReplica.getSnapshot().getMax()).isLessThanOrEqualTo(metrics.roundTrip.getSnapshot().getMax());
        assertThat(metrics.inferred.getCount()).isEqualTo(0);
        assertThat(Metrics.getTimers()).containsKey("org.apache.cassandra.metrics.AccordCoordinator.ApplyMessage-dc1.rx")
                                       .containsKey("org.apache.cassandra.metrics.AccordCoordinator.ApplyMessage.rx")
                                       .containsKey("org.apache.cassandra.metrics.AccordCoordinator.ApplyReplicaTime.rx");
    }

    @Test
    public void testRecordInferred()
    {
        MessageLatency metrics = AccordCoordinatorMetrics.syncPointMetrics.messageLatency(Verb.ACCORD_COMMIT_REQ);

        // not reported by the replica, with a known link: the link estimate is subtracted
        AccordCoordinatorMetrics.recordInferred(metrics, "dc1", ms(70), ms(60));
        // with an unknown link: only the round-trip is known
        AccordCoordinatorMetrics.recordInferred(metrics, "dc1", ms(80), -1);
        // faster than the link estimate: replica time is clamped at zero
        AccordCoordinatorMetrics.recordInferred(metrics, "dc1", ms(50), ms(60));
        metrics.recordInferred("dc1", -1, 0);

        assertThat(metrics.roundTrip.getCount()).isEqualTo(3);
        assertThat(metrics.roundTrip("dc1").getCount()).isEqualTo(3);
        assertThat(metrics.atReplica.getCount()).isEqualTo(2);
        assertThat(metrics.inferred.getCount()).isEqualTo(2);
        assertThat(metrics.atReplica.getSnapshot().getMin()).isEqualTo(0);
    }

    @Test
    public void testLatencySources()
    {
        Verb verb = Verb.ACCORD_GET_LATEST_DEPS_REQ;
        MessageLatency reads = AccordCoordinatorMetrics.readMetrics.messageLatency(verb);
        MessageLatency writes = AccordCoordinatorMetrics.writeMetrics.messageLatency(verb);
        for (int i = 0 ; i < 100 ; ++i)
        {
            reads.recordMeasured("near", ms(2), 0);
            writes.recordMeasured("near", ms(4), 0);
            writes.recordMeasured("far", ms(100), 0);
        }

        TimeoutStrategy.LatencySourceFactory near = AccordCoordinatorMetrics.latencies(verb, "near");
        TimeoutStrategy.LatencySourceFactory far = AccordCoordinatorMetrics.latencies(verb, "far");
        TimeoutStrategy.LatencySourceFactory unknown = AccordCoordinatorMetrics.latencies(verb, "unknown");
        TimeoutStrategy.LatencySourceFactory any = AccordCoordinatorMetrics.latencies(verb, null);

        // histogram buckets are ~20% wide; values are in micros
        assertThat(near.source("r").get(0.5)).isBetween(1600L, 2400L);
        assertThat(near.source("w").get(0.5)).isBetween(3200L, 4800L);
        assertThat(near.source("rw").get(0.5)).isBetween(3200L, 4800L);
        assertThat(far.source("w").get(0.5)).isBetween(80_000L, 120_000L);
        // reads have no samples to "far", so fall back to reads to any destination
        assertThat(far.source("r").get(0.5)).isBetween(1600L, 2400L);
        // destinations without samples fall back to all destinations, which for writes are half near, half far
        assertThat(unknown.source("w").get(0.99)).isBetween(80_000L, 120_000L);
        assertThat(any.source("w").get(0.25)).isBetween(3200L, 4800L);
    }

    private static long ms(long millis)
    {
        return MILLISECONDS.toNanos(millis);
    }
}
