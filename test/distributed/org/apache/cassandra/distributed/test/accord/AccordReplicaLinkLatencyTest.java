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

package org.apache.cassandra.distributed.test.accord;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Snapshot;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.net.ArtificialLatency;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * With artificial latency between two datacenters, checks that {@link LinkLatencies} estimates the link round-trip
 * using {@link Verb#LINK_PROBE_REQ}, and that the coordinator's per-replica round-trip metrics separate the link from
 * the time at the replica (as reported by the replica), for both remote replicas and local delivery.
 */
public class AccordReplicaLinkLatencyTest extends TestBaseImpl
{
    private static final long ONE_WAY_MILLIS = 30;
    private static final long ROUND_TRIP_MICROS = TimeUnit.MILLISECONDS.toMicros(2 * ONE_WAY_MILLIS);
    private static final int TXNS = 20;

    @Test
    public void replicaTimeExcludesLinkLatency() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDCs(2)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)
                                                             .set("write_request_timeout", "10s")
                                                             .set("link_latency_probe_interval", "20ms")
                                                             .set("link_latency_probe_window", 16))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication={'class':'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 1}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, v int, primary key (k)) WITH " + TransactionalMode.full.asCqlParam());

            cluster.get(1).runOnInstance(() -> setLatency("datacenter2"));
            cluster.get(2).runOnInstance(() -> setLatency("datacenter1"));
            try
            {
                test(cluster);
            }
            finally
            {
                cluster.forEach(i -> i.runOnInstance(() -> ArtificialLatency.setEnabled(false)));
            }
        }
    }

    private static void test(Cluster cluster) throws Throwable
    {
        // each node should converge on the artificial round-trip to the other datacenter
        long node1Estimate = awaitEstimate(cluster.get(1), "datacenter2");
        long node2Estimate = awaitEstimate(cluster.get(2), "datacenter1");
        System.out.println("### round-trip estimates: node1->datacenter2=" + node1Estimate + "us, node2->datacenter1=" + node2Estimate + "us");

        for (int i = 0 ; i < TXNS ; ++i)
            cluster.coordinator(1).execute("BEGIN TRANSACTION\n  UPDATE ks.tbl SET v = ? WHERE k = ?;\nCOMMIT TRANSACTION", ConsistencyLevel.ANY, i, i);

        // the coordinator pre-accepts locally without delivering a message, so its PreAccept round-trips are all to node2
        double[] preAccept = awaitStats(cluster.get(1), Verb.ACCORD_PRE_ACCEPT_REQ, TXNS);
        System.out.println("### node1 PreAccept " + STATS + ": " + Arrays.toString(preAccept));
        // every reply reported the time spent at the replica, so nothing was inferred
        assertThat(preAccept[1]).isEqualTo(preAccept[0]);
        assertThat(preAccept[2]).isEqualTo(preAccept[0]);
        assertThat(preAccept[9]).isEqualTo(0);
        // every link used was to datacenter2, and (with an idle connection) its measured link time agrees with the
        // probe estimate; the histogram buckets are ~20% wide
        assertThat(preAccept[6]).isBetween(0.8 * node1Estimate, 1.25 * node1Estimate);
        assertThat(preAccept[7]).isBetween(0.8 * node1Estimate, 1.25 * node1Estimate);
        // total = replica + link, on average
        assertThat(preAccept[3] - preAccept[4]).isBetween(0.7 * preAccept[5], 1.3 * preAccept[5]);
        // and the time at the replica is much less than the link round-trip
        assertThat(preAccept[8]).isLessThan(0.5 * node1Estimate);

        // Apply is sent both to node2, and to ourselves by local delivery (with no link)
        double[] apply = awaitStats(cluster.get(1), Verb.ACCORD_APPLY_REQ, 2 * TXNS);
        System.out.println("### node1 Apply " + STATS + ": " + Arrays.toString(apply));
        assertThat(apply[1]).isEqualTo(apply[0]);
        assertThat(apply[2]).isEqualTo(apply[0]);
        assertThat(apply[9]).isEqualTo(0);
        assertThat(apply[6]).isEqualTo(0);
        assertThat(apply[7]).isBetween(0.8 * node1Estimate, 1.25 * node1Estimate);
        // half of the replies were from each, so the mean link is about half the remote link
        assertThat(apply[5]).isBetween(0.35 * node1Estimate, 0.65 * node1Estimate);
        assertThat(apply[3] - apply[4]).isBetween(0.7 * apply[5], 1.3 * apply[5]);
    }

    private static final String STATS = "{count: total, replica, link; mean: total, replica, link; link min, max; replica median; inferred}";

    /**
     * @return {@link #STATS} of node's rw round-trip metrics for {@code verb}, once at least {@code count} are recorded
     */
    private static double[] awaitStats(IInvokableInstance instance, Verb verb, int count) throws InterruptedException
    {
        long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true)
        {
            double[] stats = instance.callOnInstance(() -> {
                AccordCoordinatorMetrics.ReplicaRoundTrip metrics = AccordCoordinatorMetrics.writeMetrics.replicaRoundTrip(verb);
                Snapshot roundTrip = metrics.roundTrip.getSnapshot(), atReplica = metrics.atReplica.getSnapshot(), link = metrics.link.getSnapshot();
                return new double[]{ metrics.roundTrip.getCount(), metrics.atReplica.getCount(), metrics.link.getCount(),
                                     roundTrip.getMean(), atReplica.getMean(), link.getMean(),
                                     link.getMin(), link.getMax(), atReplica.getMedian(), metrics.inferred.getCount() };
            });
            if (stats[0] >= count)
                return stats;
            if (nanoTime() > deadline)
                throw new AssertionError("Timed out waiting for " + count + ' ' + verb + " round-trips: " + Arrays.toString(stats));
            Thread.sleep(100);
        }
    }

    private static void setLatency(String remoteDc)
    {
        ArtificialLatency.setArtificialLatencies(remoteDc + ':' + ONE_WAY_MILLIS + "ms,unused:0ms");
        ArtificialLatency.setArtificialLatencyOnlyPermittedConsistencyLevels(false);
        ArtificialLatency.setArtificialLatencyVerbs(ArtificialLatency.recommendedVerbs() + ",LINK_PROBE_REQ,LINK_PROBE_RSP");
        ArtificialLatency.setEnabled(true);
    }

    private static long awaitEstimate(IInvokableInstance instance, String dc) throws InterruptedException
    {
        long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(30);
        long estimate = -1;
        while (nanoTime() < deadline)
        {
            estimate = instance.callOnInstance(() -> {
                long micros = LinkLatencies.instance.snapshot().roundTripMicros(dc);
                Gauge<?> gauge = Metrics.getGauges().get("org.apache.cassandra.metrics.Messaging." + dc + LinkLatencies.ESTIMATE_METRIC);
                // the gauge should agree with the snapshot
                return gauge == null || !Long.valueOf(micros).equals(gauge.getValue()) ? -1L : micros;
            });
            if (estimate >= ROUND_TRIP_MICROS && estimate < 1.5 * ROUND_TRIP_MICROS)
                return estimate;
            Thread.sleep(100);
        }
        throw new AssertionError("Estimate for " + dc + " did not converge: " + estimate + "us");
    }
}
