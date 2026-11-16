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

package org.apache.cassandra.net;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.LinkLatencies.Snapshot;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.assertj.core.api.Assertions.assertThat;

public class LinkLatenciesTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testWindowMedian()
    {
        LinkLatencies.Window window = new LinkLatencies.Window(5);
        assertThat(window.add(10)).isEqualTo(10);
        // lower median: an outlier in a window of two is not averaged in
        assertThat(window.add(1000)).isEqualTo(10);
        assertThat(window.add(20)).isEqualTo(20);
        assertThat(window.add(30)).isEqualTo(20);
        assertThat(window.add(40)).isEqualTo(30); // {10, 1000, 20, 30, 40}
        // evicts the oldest samples (10, then 1000)
        assertThat(window.add(50)).isEqualTo(40); // {50, 1000, 20, 30, 40}
        assertThat(window.add(60)).isEqualTo(40); // {50, 60, 20, 30, 40}
        assertThat(window.add(5)).isEqualTo(40);  // {50, 60, 5, 30, 40}
    }

    @Test
    public void testEstimateIsMedianRoundTrip()
    {
        LinkLatencies latencies = new LinkLatencies();
        assertThat(latencies.snapshot().roundTripMicros("dcA")).isEqualTo(Snapshot.UNKNOWN);

        latencies.addRoundTrip("dcA", 80, MILLISECONDS);
        Snapshot first = latencies.snapshot();
        assertThat(first.roundTripMicros("dcA")).isEqualTo(80_000);
        assertThat(first.roundTripMicros("dcB")).isEqualTo(Snapshot.UNKNOWN);
        assertThat(first.roundTripMicros(null)).isEqualTo(Snapshot.UNKNOWN);

        // a single delayed probe does not move the estimate
        latencies.addRoundTrip("dcA", 82, MILLISECONDS);
        latencies.addRoundTrip("dcA", 5000, MILLISECONDS);
        assertThat(latencies.snapshot().roundTripMicros("dcA")).isEqualTo(82_000);

        // negative samples are discarded
        latencies.addRoundTrip("dcA", -1, MILLISECONDS);
        assertThat(latencies.snapshot().roundTripMicros("dcA")).isEqualTo(82_000);
    }

    @Test
    public void testSnapshotVersioning()
    {
        LinkLatencies latencies = new LinkLatencies();
        Snapshot empty = latencies.snapshot();

        latencies.addRoundTrip("dcA", 1000, MICROSECONDS);
        Snapshot a = latencies.snapshot();
        assertThat(a.version).isGreaterThan(empty.version);
        assertThat(a.asMap()).containsEntry("dcA", 1000L);

        // an unchanged estimate publishes nothing new
        latencies.addRoundTrip("dcA", 1000, MICROSECONDS);
        assertThat(latencies.snapshot()).isSameAs(a);

        latencies.addRoundTrip("dcB", 2000, MICROSECONDS);
        Snapshot ab = latencies.snapshot();
        assertThat(ab.version).isGreaterThan(a.version);
        assertThat(ab.asMap()).containsEntry("dcA", 1000L).containsEntry("dcB", 2000L);
        // earlier snapshots are immutable
        assertThat(a.asMap()).doesNotContainKey("dcB");

        latencies.unsafeSetRoundTripMicros("dcB", -1);
        Snapshot removed = latencies.snapshot();
        assertThat(removed.version).isGreaterThan(ab.version);
        assertThat(removed.roundTripMicros("dcB")).isEqualTo(Snapshot.UNKNOWN);
        assertThat(removed.roundTripMicros("dcA")).isEqualTo(1000);
    }

    @Test
    public void testPeerLookup() throws Exception
    {
        LinkLatencies latencies = new LinkLatencies();
        latencies.unsafeSetRoundTripMicros(DatabaseDescriptor.getLocator().local().datacenter, 250);
        // there is no link to ourselves, even with an estimate for our own datacenter
        assertThat(latencies.roundTripNanos(FBUtilities.getBroadcastAddressAndPort())).isEqualTo(0);
        // peers whose datacenter is unknown have no estimate
        assertThat(latencies.roundTripNanos(InetAddressAndPort.getByName("127.0.0.99"))).isEqualTo(Snapshot.UNKNOWN);
    }

    @Test
    public void testMetrics()
    {
        LinkLatencies latencies = new LinkLatencies();
        latencies.addRoundTrip("metricsDc", 10, MILLISECONDS);
        String prefix = "org.apache.cassandra.metrics.Messaging.metricsDc";
        assertThat(Metrics.getTimers().get(prefix + LinkLatencies.PROBE_ROUND_TRIP_METRIC).getCount()).isEqualTo(1);
        assertThat(Metrics.getTimers().get(prefix + LinkLatencies.OBSERVED_ROUND_TRIP_METRIC).getCount()).isEqualTo(0);
        assertThat(Metrics.getTimers().get(prefix + LinkLatencies.SEND_QUEUE_METRIC).getCount()).isEqualTo(0);
        assertThat(Metrics.getGauges().get(prefix + LinkLatencies.PROBE_ESTIMATE_METRIC).getValue()).isEqualTo(10_000L);
        assertThat(latencies.links()).containsKey("metricsDc");

        latencies.unsafeSetRoundTripMicros("metricsDc", -1);
        assertThat(latencies.links()).doesNotContainKey("metricsDc");
        for (String metric : new String[]{ LinkLatencies.PROBE_ROUND_TRIP_METRIC, LinkLatencies.OBSERVED_ROUND_TRIP_METRIC, LinkLatencies.SEND_QUEUE_METRIC })
            assertThat(Metrics.getTimers()).doesNotContainKey(prefix + metric);
        assertThat(Metrics.getGauges()).doesNotContainKey(prefix + LinkLatencies.PROBE_ESTIMATE_METRIC);
    }

    @Test
    public void testObserved() throws Exception
    {
        LinkLatencies latencies = new LinkLatencies();
        // ignored: ourselves, and peers with no known datacenter
        latencies.addObserved(FBUtilities.getBroadcastAddressAndPort(), 0, 10, 100, 5);
        latencies.addObserved(InetAddressAndPort.getByName("127.0.0.99"), 0, 10, 100, 5);
        assertThat(latencies.links()).isEmpty();
    }

    @Test
    public void testSupportsProbe()
    {
        assertThat(LinkLatencies.supportsProbe(null)).isFalse();
        assertThat(LinkLatencies.supportsProbe(version("5.0.4"))).isFalse();
        assertThat(LinkLatencies.supportsProbe(version("6.0.0-alpha2"))).isFalse();
        assertThat(LinkLatencies.supportsProbe(version("6.0-alpha2-SNAPSHOT"))).isFalse();
        // snapshots are treated as the version they are building towards
        assertThat(LinkLatencies.supportsProbe(version("6.0-alpha3-SNAPSHOT"))).isTrue();
        assertThat(LinkLatencies.supportsProbe(version("6.0.0-alpha3"))).isTrue();
        assertThat(LinkLatencies.supportsProbe(version("6.0.0-beta1"))).isTrue();
        assertThat(LinkLatencies.supportsProbe(version("6.0.0"))).isTrue();
        assertThat(LinkLatencies.supportsProbe(version("6.0-SNAPSHOT"))).isTrue();
        assertThat(LinkLatencies.supportsProbe(version("6.1.0"))).isTrue();
    }

    private static NodeVersion version(String version)
    {
        return new NodeVersion(new CassandraVersion(version), NodeVersion.CURRENT_METADATA_VERSION);
    }
}
