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
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.net.Verb.LINK_PROBE_REQ;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Estimates the round-trip latency of the network link between this node and each datacenter (including our own),
 * independently of the time taken by peers to process requests, so that it may be separated out of other latency
 * measurements, e.g. to infer the time a request spent at a replica that does not report it.
 *
 * Every {@link DatabaseDescriptor#getLinkLatencyProbeInterval() probe interval} we send a {@link Verb#LINK_PROBE_REQ}
 * to one live peer chosen at random from each datacenter, and time its round-trip with the monotonic clock. Both the
 * probe and its response are handled on the {@link org.apache.cassandra.concurrent.Stage#IMMEDIATE IMMEDIATE} stage,
 * i.e. on the network event loop as they are received, so the measurement excludes any stage queueing on either node.
 * Probes and their responses are sent on the urgent connection, so the estimate excludes (as far as possible)
 * queueing behind other messages: it estimates the network itself. By contrast, round-trips of real requests whose
 * responder reports its own time ({@link ParamType#RECEIPT_TO_REPLY_NANOS}) yield the link time including queueing
 * on their connections, so the two may be compared to see how much of a link's latency is due to messaging queues.
 *
 * Each datacenter keeps the most recent {@link DatabaseDescriptor#getLinkLatencyProbeWindow() window} of samples,
 * and its estimate is their median, discarding transient congestion. Only round-trips are estimated, so no
 * assumptions are made about clock synchronisation or link symmetry.
 *
 * Estimates are published as an immutable {@link Snapshot}, whose {@link Snapshot#version} changes whenever any
 * estimate does, so that derived values may be cached. They are also exported as metrics in the {@code Messaging}
 * group:
 * <ul>
 *   <li>{@code <dc>-LinkRoundTripLatency}: a timer of the raw round-trip samples</li>
 *   <li>{@code <dc>-LinkRoundTripEstimate}: the current round-trip estimate, in microseconds</li>
 * </ul>
 *
 * Probing is disabled unless {@code link_latency_probe_interval} is configured, as nodes that predate
 * {@link Verb#LINK_PROBE_REQ} cannot deserialize it; it must only be configured once every node supports it.
 * While disabled every datacenter is {@link Snapshot#UNKNOWN unknown}. Estimates may also be
 * {@link #unsafeSetRoundTripMicros set directly} for testing.
 */
public class LinkLatencies
{
    private static final Logger logger = LoggerFactory.getLogger(LinkLatencies.class);
    private static final MetricNameFactory factory = new DefaultNameFactory(MessagingMetrics.TYPE_NAME);

    public static final String ROUND_TRIP_METRIC = "-LinkRoundTripLatency";
    public static final String ESTIMATE_METRIC = "-LinkRoundTripEstimate";
    public static final int DEFAULT_WINDOW = 64;

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

    private final class Link
    {
        final Window window;
        final Timer roundTrip;

        Link(String dc, int windowSize)
        {
            this.window = new Window(windowSize);
            this.roundTrip = Metrics.timer(factory.createMetricName(dc + ROUND_TRIP_METRIC));
            Metrics.register(factory.createMetricName(dc + ESTIMATE_METRIC), (Gauge<Long>) () -> snapshot.roundTripMicros(dc));
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
     * @return the datacenter of {@code endpoint}, or null if unknown
     */
    @Nullable
    public static String datacenter(InetAddressAndPort endpoint)
    {
        Location location = DatabaseDescriptor.getLocator().location(endpoint);
        return location == null || Location.UNKNOWN.equals(location) ? null : location.datacenter;
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
            Map<String, Collection<InetAddressAndPort>> byDc = metadata.directory.allDatacenterEndpoints().asMap();
            for (Map.Entry<String, Collection<InetAddressAndPort>> e : byDc.entrySet())
            {
                InetAddressAndPort peer = pickLivePeer(e.getValue(), self);
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

    private static InetAddressAndPort pickLivePeer(Collection<InetAddressAndPort> endpoints, InetAddressAndPort self)
    {
        // reservoir sample of size one
        InetAddressAndPort pick = null;
        int live = 0;
        for (InetAddressAndPort endpoint : endpoints)
        {
            if (endpoint.equals(self) || !FailureDetector.instance.isAlive(endpoint))
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

        Link link = links.computeIfAbsent(dc, d -> new Link(d, windowSize));
        link.roundTrip.update(roundTrip, unit);
        long medianNanos = link.window.add(unit.toNanos(roundTrip));
        publish(dc, NANOSECONDS.toMicros(medianNanos));
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
        Snapshot prev = snapshot;
        if (!known.containsAll(prev.roundTripMicros.keySet()))
        {
            Set<String> remove = new HashSet<>(prev.roundTripMicros.keySet());
            remove.removeAll(known);
            remove(remove);
        }
    }

    private synchronized void remove(Set<String> dcs)
    {
        Snapshot prev = snapshot;
        Map<String, Long> next = new HashMap<>(prev.roundTripMicros);
        if (!next.keySet().removeAll(dcs))
            return;

        for (String dc : dcs)
        {
            if (links.remove(dc) != null)
            {
                Metrics.remove(factory.createMetricName(dc + ROUND_TRIP_METRIC));
                Metrics.remove(factory.createMetricName(dc + ESTIMATE_METRIC));
            }
        }
        snapshot = new Snapshot(prev.version + 1, ImmutableMap.copyOf(next));
    }
}
