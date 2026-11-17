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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.net.Verb.LINK_PROBE_REQ;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Tracks the round-trip latency of the network link between this node and each datacenter (including our own),
 * independently of the time taken by peers to process requests, so that it may be separated out of other latency
 * measurements, by two means:
 *
 * <h3>Probes</h3>
 * Every {@link DatabaseDescriptor#getLinkLatencyProbeInterval() probe interval} we send a {@link Verb#LINK_PROBE_REQ}
 * on the urgent connection to one live peer chosen at random from each datacenter, and time its round-trip with the
 * monotonic clock. Both the probe and its response are handled on the {@link org.apache.cassandra.concurrent.Stage#IMMEDIATE
 * IMMEDIATE} stage, i.e. on the network event loop as they are received, so this measures (as far as possible) the
 * network itself, excluding queueing behind other messages or on any stage. Each datacenter keeps the most recent
 * {@link DatabaseDescriptor#getLinkLatencyProbeWindow() window} of samples, and its estimate is their median, which is
 * published as an immutable {@link Snapshot} (whose {@link Snapshot#version} changes whenever any estimate does, so
 * that derived values may be cached), e.g. to infer the time spent at a replica that does not report it.
 *
 * Probing is disabled unless {@code link_latency_probe_interval} is configured, and peers are only probed if their
 * version supports {@link Verb#LINK_PROBE_REQ} ({@link #MIN_PROBE_VERSION}).
 *
 * <h3>Observation</h3>
 * Requests that carry {@link ParamType#SENT_AT_NANOS}, whose responder reports {@link ParamType#RECEIPT_TO_REPLY_NANOS}
 * (and echoes the former), yield the {@link #addObserved observed} round-trip on the network of real traffic (from
 * our serialization of the request to receipt of the response, excluding the responder's time up to its
 * serialization of the response), and the time each request spent queued here before serialization.
 *
 * <h3>Metrics</h3>
 * In the {@code Messaging} group, per datacenter:
 * <ul>
 *   <li>{@code <dc>-LinkProbeRoundTrip}: the probe round-trip samples</li>
 *   <li>{@code <dc>-LinkProbeEstimate}: the current probe estimate, in microseconds</li>
 *   <li>{@code <dc>-LinkObservedRoundTrip}: the observed round-trips of real traffic, which unlike probes include
 *       queueing behind other messages on the connections and on receipt</li>
 *   <li>{@code <dc>-LinkSendQueue}: the time observed requests spent queued here before being serialized</li>
 * </ul>
 * These are also presented by {@code system_views_debug.link_latencies}.
 */
public class LinkLatencies
{
    private static final Logger logger = LoggerFactory.getLogger(LinkLatencies.class);
    private static final MetricNameFactory factory = new DefaultNameFactory(MessagingMetrics.TYPE_NAME);

    public static final String PROBE_ROUND_TRIP_METRIC = "-LinkProbeRoundTrip";
    public static final String PROBE_ESTIMATE_METRIC = "-LinkProbeEstimate";
    public static final String OBSERVED_ROUND_TRIP_METRIC = "-LinkObservedRoundTrip";
    public static final String SEND_QUEUE_METRIC = "-LinkSendQueue";
    private static final String[] METRICS = { PROBE_ROUND_TRIP_METRIC, PROBE_ESTIMATE_METRIC, OBSERVED_ROUND_TRIP_METRIC, SEND_QUEUE_METRIC };
    public static final int DEFAULT_WINDOW = 64;

    /**
     * The destination of messages delivered to ourselves (see {@link #destination})
     */
    public static final String SELF = "self";

    /**
     * The earliest version supporting {@link Verb#LINK_PROBE_REQ}
     */
    public static final CassandraVersion MIN_PROBE_VERSION = new CassandraVersion("6.0.0-alpha3");

    public static final LinkLatencies instance = new LinkLatencies();

    /**
     * Responds immediately, on the network event loop
     */
    static final IVerbHandler<NoPayload> probeHandler = message -> MessagingService.instance().send(message.emptyResponse(), message.from());

    /**
     * An immutable view of the estimated round-trip latencies between this node and each datacenter.
     */
    public static final class Snapshot
    {
        public static final long UNKNOWN = -1;
        public static final Snapshot EMPTY = new Snapshot(0, ImmutableMap.of());

        /**
         * Changes whenever any estimate changes
         */
        public final long version;
        private final Map<String, Long> roundTripMicros;

        private Snapshot(long version, Map<String, Long> roundTripMicros)
        {
            this.version = version;
            this.roundTripMicros = roundTripMicros;
        }

        /**
         * @return the estimated round-trip latency in micros between this node and {@code dc}, or {@link #UNKNOWN}
         */
        public long roundTripMicros(@Nullable String dc)
        {
            Long micros = dc == null ? null : roundTripMicros.get(dc);
            return micros == null ? UNKNOWN : micros;
        }

        public Map<String, Long> asMap()
        {
            return roundTripMicros;
        }

        @Override
        public String toString()
        {
            return "LinkLatencies{v" + version + ':' + roundTripMicros + '}';
        }
    }

    /**
     * The samples for one datacenter
     */
    @VisibleForTesting
    static final class Window
    {
        private final long[] samples;
        private int count, next;

        Window(int size)
        {
            this.samples = new long[Math.max(1, size)];
        }

        /**
         * @return the median of the window after adding {@code sample}
         */
        synchronized long add(long sample)
        {
            samples[next] = sample;
            next = (next + 1) % samples.length;
            if (count < samples.length)
                ++count;

            long[] sorted = Arrays.copyOf(samples, count);
            Arrays.sort(sorted);
            // lower median, so a window of two samples does not average in an outlier
            return sorted[(count - 1) / 2];
        }
    }

    /**
     * The metrics for one datacenter
     */
    public static final class Link
    {
        private final Window window;
        public final Timer probeRoundTrip, observedRoundTrip, sendQueue;

        private Link(String dc, int windowSize, Gauge<Long> probeEstimate)
        {
            this.window = new Window(windowSize);
            this.probeRoundTrip = Metrics.timer(factory.createMetricName(dc + PROBE_ROUND_TRIP_METRIC));
            this.observedRoundTrip = Metrics.timer(factory.createMetricName(dc + OBSERVED_ROUND_TRIP_METRIC));
            this.sendQueue = Metrics.timer(factory.createMetricName(dc + SEND_QUEUE_METRIC));
            Metrics.register(factory.createMetricName(dc + PROBE_ESTIMATE_METRIC), probeEstimate);
        }
    }

    private final Map<String, Link> links = new ConcurrentHashMap<>();
    private volatile Snapshot snapshot = Snapshot.EMPTY;
    private volatile boolean started;
    private volatile int windowSize = DEFAULT_WINDOW;

    @VisibleForTesting
    LinkLatencies()
    {
    }

    public Snapshot snapshot()
    {
        return snapshot;
    }

    /**
     * @return the metrics for each datacenter we have measured
     */
    public Map<String, Link> links()
    {
        return Collections.unmodifiableMap(links);
    }

    private Link link(String dc)
    {
        Link link = links.get(dc);
        if (link == null)
            link = links.computeIfAbsent(dc, d -> new Link(d, windowSize, () -> snapshot.roundTripMicros(d)));
        return link;
    }

    /**
     * @return true iff a node of {@code version} supports {@link Verb#LINK_PROBE_REQ}
     */
    @VisibleForTesting
    static boolean supportsProbe(@Nullable NodeVersion version)
    {
        if (version == null || version.cassandraVersion == null)
            return false;

        // treat snapshot builds as the version they are building towards
        String release = version.cassandraVersion.toString().replaceFirst("(?i)[.-]SNAPSHOT(?=\\+|$)", "");
        return new CassandraVersion(release).compareTo(MIN_PROBE_VERSION) >= 0;
    }

    /**
     * @return the datacenter of {@code endpoint}, or null if unknown
     */
    @Nullable
    public static String datacenter(InetAddressAndPort endpoint)
    {
        Location location = DatabaseDescriptor.getLocator().location(endpoint);
        return location == null || Location.UNKNOWN.equals(location) ? null : location.datacenter;
    }

    /**
     * @return {@link #SELF} if {@code endpoint} is this node, otherwise its datacenter (or null if unknown); i.e.
     *         the distinct destinations whose latencies are worth tracking separately
     */
    @Nullable
    public static String destination(InetAddressAndPort endpoint)
    {
        return endpoint.equals(FBUtilities.getBroadcastAddressAndPort()) ? SELF : datacenter(endpoint);
    }

    /**
     * @return the estimated round-trip in nanos to {@code peer}'s datacenter, zero if {@code peer} is this node,
     *         or {@link Snapshot#UNKNOWN} if unknown
     */
    public long roundTripNanos(InetAddressAndPort peer)
    {
        if (peer.equals(FBUtilities.getBroadcastAddressAndPort()))
            return 0;
        long micros = snapshot.roundTripMicros(datacenter(peer));
        return micros < 0 ? Snapshot.UNKNOWN : TimeUnit.MICROSECONDS.toNanos(micros);
    }

    /**
     * Begin probing peers, if configured; idempotent
     */
    public synchronized void start()
    {
        if (started)
            return;

        DurationSpec.IntMillisecondsBound interval = DatabaseDescriptor.getLinkLatencyProbeInterval();
        if (interval == null || interval.toMilliseconds() <= 0)
            return;

        started = true;
        windowSize = Math.max(1, DatabaseDescriptor.getLinkLatencyProbeWindow());
        long intervalMillis = interval.toMilliseconds();
        logger.info("Probing link latency to each datacenter every {}ms, estimating from the median of the last {} samples", intervalMillis, windowSize);
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::probe, intervalMillis, intervalMillis, MILLISECONDS);
    }

    private void probe()
    {
        try
        {
            MessagingService messaging = MessagingService.instance();
            if (messaging.isShuttingDown())
                return;

            ClusterMetadata metadata = ClusterMetadata.currentNullable();
            if (metadata == null)
                return;

            InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
            Directory directory = metadata.directory;
            Map<String, Collection<InetAddressAndPort>> byDc = directory.allDatacenterEndpoints().asMap();
            for (Map.Entry<String, Collection<InetAddressAndPort>> e : byDc.entrySet())
            {
                InetAddressAndPort peer = pickLivePeer(directory, e.getValue(), self);
                if (peer != null)
                    probe(messaging, e.getKey(), peer);
            }
            forgetDatacenters(byDc.keySet());
        }
        catch (Throwable t)
        {
            logger.warn("Failed to probe link latencies", t);
        }
    }

    private void probe(MessagingService messaging, String dc, InetAddressAndPort peer)
    {
        long start = nanoTime();
        messaging.sendWithCallback(Message.out(LINK_PROBE_REQ, NoPayload.noPayload), peer, new RequestCallback<NoPayload>()
        {
            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                addRoundTrip(dc, nanoTime() - start, NANOSECONDS);
            }
        });
    }

    private static InetAddressAndPort pickLivePeer(Directory directory, Collection<InetAddressAndPort> endpoints, InetAddressAndPort self)
    {
        // reservoir sample of size one
        InetAddressAndPort pick = null;
        int live = 0;
        for (InetAddressAndPort endpoint : endpoints)
        {
            if (endpoint.equals(self) || !FailureDetector.instance.isAlive(endpoint))
                continue;
            NodeId id = directory.peerId(endpoint);
            if (id == null || !supportsProbe(directory.version(id)))
                continue;
            if (ThreadLocalRandom.current().nextInt(++live) == 0)
                pick = endpoint;
        }
        return pick;
    }

    /**
     * Record a round-trip sample for {@code dc}, updating its estimate
     */
    @VisibleForTesting
    public void addRoundTrip(String dc, long roundTrip, TimeUnit unit)
    {
        if (roundTrip < 0)
            return;

        Link link = link(dc);
        link.probeRoundTrip.update(roundTrip, unit);
        long medianNanos = link.window.add(unit.toNanos(roundTrip));
        publish(dc, NANOSECONDS.toMicros(medianNanos));
    }

    /**
     * Record the observed timing of a request to a peer in {@code dc} and its response, all by our monotonic clock,
     * except {@code responderNanos}, which the responder measured by its own
     *
     * @param registeredAtNanos when the request was submitted to be sent
     * @param sentAtNanos when the request was serialized
     * @param receivedAtNanos when the response was received
     * @param responderNanos the time between the responder's receipt of the request and serialization of the response
     */
    public void addObserved(String dc, long registeredAtNanos, long sentAtNanos, long receivedAtNanos, long responderNanos)
    {
        Link link = link(dc);
        // registeredAt may be measured with only micro precision, so may fractionally exceed sentAt
        link.sendQueue.update(Math.max(0, sentAtNanos - registeredAtNanos), NANOSECONDS);
        link.observedRoundTrip.update(Math.max(0, receivedAtNanos - sentAtNanos - responderNanos), NANOSECONDS);
    }

    /**
     * Override the estimate for {@code dc}, for testing; a negative value removes it
     */
    @VisibleForTesting
    public void unsafeSetRoundTripMicros(String dc, long micros)
    {
        if (micros < 0) remove(Collections.singleton(dc));
        else publish(dc, micros);
    }

    private synchronized void publish(String dc, long roundTripMicros)
    {
        Snapshot prev = snapshot;
        if (prev.roundTripMicros(dc) == roundTripMicros)
            return;

        Map<String, Long> next = new HashMap<>(prev.roundTripMicros);
        next.put(dc, roundTripMicros);
        snapshot = new Snapshot(prev.version + 1, ImmutableMap.copyOf(next));
    }

    private void forgetDatacenters(Set<String> known)
    {
        Set<String> remove = new HashSet<>(snapshot.roundTripMicros.keySet());
        remove.addAll(links.keySet());
        remove.removeAll(known);
        if (!remove.isEmpty())
            remove(remove);
    }

    private synchronized void remove(Set<String> dcs)
    {
        for (String dc : dcs)
        {
            if (links.remove(dc) != null)
            {
                for (String metric : METRICS)
                    Metrics.remove(factory.createMetricName(dc + metric));
            }
        }

        Snapshot prev = snapshot;
        Map<String, Long> next = new HashMap<>(prev.roundTripMicros);
        if (next.keySet().removeAll(dcs))
            snapshot = new Snapshot(prev.version + 1, ImmutableMap.copyOf(next));
    }
}
