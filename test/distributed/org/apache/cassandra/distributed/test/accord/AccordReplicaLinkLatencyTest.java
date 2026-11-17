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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Snapshot;

import accord.local.Node;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.net.ArtificialLatency;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.api.AccordWaitStrategies;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * With latency added between two datacenters, checks that {@link LinkLatencies} probes estimate the link round-trip,
 * and that the coordinator separates each Accord round-trip into time queued to be sent, on the network, and at the
 * replica (as reported by the replica), attributing the added latency correctly depending on where it occurs:
 * <ul>
 *   <li>on receipt (i.e. after serialization), modelling the network: to the link</li>
 *   <li>before serialization ({@link ArtificialLatency}), modelling queueing to be sent: to the sender's send queue
 *       for requests, and to the replica's time for responses</li>
 * </ul>
 */
public class AccordReplicaLinkLatencyTest extends TestBaseImpl
{
    private static final long ONE_WAY_MILLIS = 30;
    private static final long ROUND_TRIP_MICROS = MILLISECONDS.toMicros(2 * ONE_WAY_MILLIS);
    private static final long ONE_WAY_MICROS = MILLISECONDS.toMicros(ONE_WAY_MILLIS);
    private static final int TXNS = 20;

    @Test
    public void networkLatencyIsAttributedToLink() throws Throwable
    {
        withCluster(true, (cluster, estimate) -> {
            // the coordinator pre-accepts for itself directly (reported via AccordAgent.onLocalRequest), and sends PreAccept to node2
            double[] preAccept = awaitStats(cluster.get(1), Verb.ACCORD_PRE_ACCEPT_REQ, 2 * TXNS);
            System.out.println("### node1 PreAccept " + STATS + ": " + Arrays.toString(preAccept));
            // every reply reported the time spent at the replica, so nothing was inferred
            assertThat(preAccept[1]).isEqualTo(preAccept[0]);
            assertThat(preAccept[4]).isEqualTo(0);
            // the time at the replica is much less than the link round-trip, which accounts for the rest of the half of
            // round-trips that were remote
            assertThat(preAccept[3]).isLessThan(0.5 * estimate);
            assertThat(preAccept[2] - preAccept[3]).isBetween(0.35 * estimate, 0.65 * estimate);

            long[] preAcceptByDestination = awaitPerDestination(cluster.get(1), Verb.ACCORD_PRE_ACCEPT_REQ);
            System.out.println("### node1 PreAccept " + PER_DESTINATION + ": " + Arrays.toString(preAcceptByDestination));
            assertPerDestination(preAcceptByDestination, estimate);

            // Apply is sent both to node2, and to ourselves by local delivery
            double[] apply = awaitStats(cluster.get(1), Verb.ACCORD_APPLY_REQ, 2 * TXNS);
            System.out.println("### node1 Apply " + STATS + ": " + Arrays.toString(apply));
            assertThat(apply[1]).isEqualTo(apply[0]);
            assertThat(apply[4]).isEqualTo(0);

            // Apply round-trips are tracked per destination too
            long[] applyByDestination = awaitPerDestination(cluster.get(1), Verb.ACCORD_APPLY_REQ);
            System.out.println("### node1 Apply " + PER_DESTINATION + ": " + Arrays.toString(applyByDestination));
            assertPerDestination(applyByDestination, estimate);

            // the round-trips observed on the network to datacenter2 agree with the probes, and spent little time queued
            // to be sent; there are none to datacenter1, as we were its only replica
            long[] observed = observed(cluster.get(1));
            assertThat(observed[0]).isGreaterThanOrEqualTo(2 * TXNS);
            assertThat(observed[4]).isEqualTo(observed[0]);
            // the histogram buckets are ~20% wide
            assertThat(observed[1]).isBetween((long) (0.8 * estimate), (long) (1.25 * estimate));
            assertThat(observed[2]).isGreaterThanOrEqualTo((long) (0.8 * estimate));
            assertThat(observed[5]).isLessThan((long) (0.1 * estimate));
            assertThat(observed[6]).isEqualTo(0);

            // the same is presented by the debug view
            Object[][] rows = cluster.get(1).executeInternal("SELECT datacenter, probe_estimate_micros, probe_count, observed_count, send_queue_count FROM system_views_debug.link_latencies");
            System.out.println("### node1 link_latencies: " + Arrays.deepToString(rows));
            assertThat(rows.length).isEqualTo(1);
            assertThat(rows[0][0]).isEqualTo("datacenter2");
            assertThat((Long) rows[0][1]).isGreaterThan(0);
            assertThat((Long) rows[0][2]).isGreaterThan(0);
            assertThat((Long) rows[0][3]).isGreaterThanOrEqualTo(observed[0]);
            assertThat((Long) rows[0][4]).isGreaterThanOrEqualTo(observed[4]);
        });
    }

    @Test
    public void sendLatencyIsAttributedToQueues() throws Throwable
    {
        withCluster(false, (cluster, estimate) -> {
            // requests wait ONE_WAY before serialization at node1 (its send queue), and responses wait ONE_WAY before
            // serialization at node2 (the replica's time); the network itself is fast
            double[] preAccept = awaitStats(cluster.get(1), Verb.ACCORD_PRE_ACCEPT_REQ, 2 * TXNS);
            System.out.println("### node1 PreAccept " + STATS + ": " + Arrays.toString(preAccept));
            assertThat(preAccept[4]).isEqualTo(0);
            // half of the PreAccepts were our own (with no added delay), and half to node2
            assertThat(preAccept[3]).isBetween(0.4 * ONE_WAY_MICROS, 0.75 * ONE_WAY_MICROS);

            long[] observed = observed(cluster.get(1));
            assertThat(observed[0]).isGreaterThanOrEqualTo(TXNS);
            assertThat(observed[1]).isLessThan((long) (0.25 * ONE_WAY_MICROS));
            assertThat(observed[5]).isBetween((long) (0.8 * ONE_WAY_MICROS), (long) (1.25 * ONE_WAY_MICROS));
        });
    }

    private interface ClusterTest
    {
        void test(Cluster cluster, long node1Estimate) throws Throwable;
    }

    /**
     * @param network if true, delay messages between datacenters on receipt; otherwise before they are sent
     */
    private static void withCluster(boolean network, ClusterTest test) throws Throwable
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

            if (network)
            {
                cluster.get(1).runOnInstance(() -> delayOnReceipt("datacenter2"));
                cluster.get(2).runOnInstance(() -> delayOnReceipt("datacenter1"));
            }
            else
            {
                cluster.get(1).runOnInstance(() -> delayBeforeSend("datacenter2"));
                cluster.get(2).runOnInstance(() -> delayBeforeSend("datacenter1"));
            }

            try
            {
                // each node should converge on the added round-trip to the other datacenter
                long node1Estimate = awaitEstimate(cluster.get(1), "datacenter2");
                long node2Estimate = awaitEstimate(cluster.get(2), "datacenter1");
                System.out.println("### round-trip estimates: node1->datacenter2=" + node1Estimate + "us, node2->datacenter1=" + node2Estimate + "us");

                for (int i = 0 ; i < TXNS ; ++i)
                    cluster.coordinator(1).execute("BEGIN TRANSACTION\n  UPDATE ks.tbl SET v = ? WHERE k = ?;\nCOMMIT TRANSACTION", ConsistencyLevel.ANY, i, i);

                test.test(cluster, node1Estimate);
            }
            finally
            {
                if (!network)
                    cluster.forEach(i -> i.runOnInstance(() -> ArtificialLatency.setEnabled(false)));
            }
        }
    }

    /**
     * Delay (re)delivery of IMMEDIATE stage messages from {@code remoteDc} by {@link #ONE_WAY_MILLIS}, i.e. after
     * they were serialized, as network latency would be. Only IMMEDIATE stage messages are delayed, as they are
     * redelivered on another thread; this includes the link probes and nearly all Accord messages.
     */
    private static void delayOnReceipt(String remoteDc)
    {
        Set<Message<?>> delayed = ConcurrentHashMap.newKeySet();
        MessagingService.instance().inboundSink.add(message -> {
            if (delayed.remove(message))
                return true;
            if (message.verb().stage != Stage.IMMEDIATE || !remoteDc.equals(LinkLatencies.datacenter(message.from())))
                return true;
            delayed.add(message);
            ScheduledExecutors.scheduledFastTasks.schedule(() -> MessagingService.instance().inboundSink.accept(message), ONE_WAY_MILLIS, MILLISECONDS);
            return false;
        });
    }

    /**
     * Delay sending messages to {@code remoteDc} by {@link #ONE_WAY_MILLIS}, before they are serialized
     */
    private static void delayBeforeSend(String remoteDc)
    {
        ArtificialLatency.setArtificialLatencies(remoteDc + ':' + ONE_WAY_MILLIS + "ms,unused:0ms");
        ArtificialLatency.setArtificialLatencyOnlyPermittedConsistencyLevels(false);
        ArtificialLatency.setArtificialLatencyVerbs(ArtificialLatency.recommendedVerbs() + ",LINK_PROBE_REQ,LINK_PROBE_RSP");
        ArtificialLatency.setEnabled(true);
    }

    /**
     * @return node's observed link to datacenter2 {count, median, min, max; send queue count, median; datacenter1 count}
     */
    private static long[] observed(IInvokableInstance instance)
    {
        long[] observed = instance.callOnInstance(() -> {
            LinkLatencies.Link dc2 = LinkLatencies.instance.links().get("datacenter2");
            LinkLatencies.Link dc1 = LinkLatencies.instance.links().get("datacenter1");
            Snapshot link = dc2.observedRoundTrip.getSnapshot(), queue = dc2.sendQueue.getSnapshot();
            return new long[]{ dc2.observedRoundTrip.getCount(), (long) link.getMedian(), link.getMin(), link.getMax(),
                               dc2.sendQueue.getCount(), (long) queue.getMedian(),
                               dc1 == null ? 0 : dc1.observedRoundTrip.getCount() };
        });
        System.out.println("### observed datacenter2 {count, median, min, max; send queue count, median; datacenter1 count}: " + Arrays.toString(observed));
        return observed;
    }

    private static final String PER_DESTINATION = "{self count, median; datacenter2 count, median; slow wait self, datacenter2}";

    /**
     * @return {@link #PER_DESTINATION} for node's rw round-trips of {@code verb}, where the slow waits are those of
     *         slow_txn_preaccept, judged against the same message to the same destination
     */
    private static long[] perDestination(IInvokableInstance instance, Verb verb)
    {
        return instance.callOnInstance(() -> {
            AccordCoordinatorMetrics.MessageLatency metrics = AccordCoordinatorMetrics.writeMetrics.messageLatency(verb);
            TxnId txnId = new TxnId(1, 1, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
            return new long[]{ metrics.roundTrip(LinkLatencies.SELF).getCount(), (long) metrics.roundTrip(LinkLatencies.SELF).getSnapshot().getMedian(),
                               metrics.roundTrip("datacenter2").getCount(), (long) metrics.roundTrip("datacenter2").getSnapshot().getMedian(),
                               AccordWaitStrategies.slowPreaccept(txnId, verb, LinkLatencies.SELF).computeWait(1, TimeUnit.MICROSECONDS),
                               AccordWaitStrategies.slowPreaccept(txnId, verb, "datacenter2").computeWait(1, TimeUnit.MICROSECONDS) };
        });
    }

    /**
     * Wait for the slow wait to datacenter2 to reflect the recent round-trips, as the latencies a strategy is based on
     * are cached for up to 10s (and the strategy may have been first used before there were any samples)
     */
    private static long[] awaitPerDestination(IInvokableInstance instance, Verb verb) throws InterruptedException
    {
        long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(20);
        long[] result = perDestination(instance, verb);
        while (result[5] < 2 * result[3] * 8 / 10 && nanoTime() < deadline)
        {
            Thread.sleep(500);
            result = perDestination(instance, verb);
        }
        return result;
    }

    private static void assertPerDestination(long[] perDestination, long estimate)
    {
        assertThat(perDestination[0]).isGreaterThanOrEqualTo(TXNS);
        assertThat(perDestination[2]).isGreaterThanOrEqualTo(TXNS);
        // round-trips to ourselves are short, and to datacenter2 at least the link
        assertThat(perDestination[1]).isLessThan((long) (0.25 * estimate));
        assertThat(perDestination[3]).isBetween((long) (0.8 * estimate), (long) (1.5 * estimate));
        // slow_txn_preaccept defaults to 30ms <= p50*2 <= 1000ms
        assertThat(perDestination[4]).isLessThan(perDestination[5]);
        assertThat(perDestination[5]).isBetween(2 * perDestination[3] * 8 / 10, 2 * perDestination[3] * 12 / 10);
    }

    private static final String STATS = "{count: total, replica; mean: total, replica; inferred}";

    /**
     * @return {@link #STATS} of node's rw round-trip metrics for {@code verb}, once at least {@code count} are recorded
     */
    private static double[] awaitStats(IInvokableInstance instance, Verb verb, int count) throws InterruptedException
    {
        long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true)
        {
            double[] stats = instance.callOnInstance(() -> {
                AccordCoordinatorMetrics.MessageLatency metrics = AccordCoordinatorMetrics.writeMetrics.messageLatency(verb);
                return new double[]{ metrics.roundTrip.getCount(), metrics.atReplica.getCount(),
                                     metrics.roundTrip.getSnapshot().getMean(), metrics.atReplica.getSnapshot().getMean(),
                                     metrics.inferred.getCount() };
            });
            if (stats[0] >= count)
                return stats;
            if (nanoTime() > deadline)
                throw new AssertionError("Timed out waiting for " + count + ' ' + verb + " round-trips: " + Arrays.toString(stats) + "; by destination " + Arrays.toString(perDestination(instance, verb)));
            Thread.sleep(100);
        }
    }

    private static long awaitEstimate(IInvokableInstance instance, String dc) throws InterruptedException
    {
        long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(30);
        long estimate = -1;
        while (nanoTime() < deadline)
        {
            estimate = instance.callOnInstance(() -> {
                long micros = LinkLatencies.instance.snapshot().roundTripMicros(dc);
                Gauge<?> gauge = Metrics.getGauges().get("org.apache.cassandra.metrics.Messaging." + dc + LinkLatencies.PROBE_ESTIMATE_METRIC);
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
