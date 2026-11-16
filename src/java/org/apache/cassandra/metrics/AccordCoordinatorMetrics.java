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

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import accord.api.CoordinatorEventListener;
import accord.api.Result;
import accord.coordinate.ExecutePath;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.TxnId;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordCoordinatorMetrics
{
    public final static AccordCoordinatorMetrics readMetrics = new AccordCoordinatorMetrics("ro");
    public final static AccordCoordinatorMetrics writeMetrics = new AccordCoordinatorMetrics("rw");
    public final static AccordCoordinatorMetrics syncPointMetrics = new AccordCoordinatorMetrics("rx");

    public static final String ACCORD_COORDINATOR = "AccordCoordinator";
    public static final String COORDINATOR_EPOCHS = "Epochs";
    public static final String COORDINATOR_KEYS = "Keys";
    public static final String COORDINATOR_TABLES = "Tables";
    public static final String COORDINATOR_DEPENDENCIES = "Dependencies";
    public static final String COORDINATOR_PREACCEPT_LATENCY = "PreAcceptLatency";
    public static final String COORDINATOR_COMMIT_LATENCY = "CommitLatency";
    public static final String COORDINATOR_EXECUTE_LATENCY = "ExecuteLatency";
    public static final String COORDINATOR_APPLY_LATENCY = "ApplyLatency";
    public static final String EPHEMERAL = "Ephemeral";
    public static final String FAST_PATHS = "FastPaths";
    public static final String MEDIUM_PATHS = "MediumPaths";
    public static final String SLOW_PATHS = "SlowPaths";
    public static final String PREEMPTED = "Preempted";
    public static final String REJECTED = "Rejected";
    public static final String TIMEOUTS = "Timeouts";
    public static final String INVALIDATIONS = "Invalidations";
    public static final String RECOVERY_DELAY = "RecoveryDelay";
    public static final String RECOVERY_TIME = "RecoveryTime";
    public static final String FAST_PATH_TO_TOTAL = "FastPathToTotal";
    public static final String REPLICA_ROUND_TRIP = "ReplicaRoundTrip";
    public static final String REPLICA_TIME = "ReplicaTime";
    public static final String REPLICA_LINK = "ReplicaLink";
    public static final String REPLICA_TIME_INFERRED = "ReplicaTimeInferred";

    /**
     * The round-trips of one kind of request (by request verb) sent by this node to replicas, for one kind of
     * transaction, measured with the monotonic clock from registering the request's callback to receipt of its final
     * reply, and split into the time spent at the replica and the time spent on the link:
     * <ul>
     *   <li>{@code ReplicaRoundTrip.<Request>}: the total round-trip</li>
     *   <li>{@code ReplicaTime.<Request>}: the time spent at the replica, i.e. from its receipt of the request to its
     *       reply, as reported by the replica ({@link org.apache.cassandra.net.ParamType#RECEIPT_TO_REPLY_NANOS}); for
     *       requests delivered to ourselves this is the whole round-trip</li>
     *   <li>{@code ReplicaLink.<Request>}: the remainder of the round-trip, i.e. the time on the network including
     *       messaging queues in both directions (zero for ourselves)</li>
     *   <li>{@code ReplicaTimeInferred.<Request>}: the number of samples for which the replica did not report its
     *       time (e.g. it runs an older version), so that the split was instead inferred by subtracting the estimated
     *       {@link org.apache.cassandra.net.LinkLatencies link round-trip} to its datacenter; if there is no
     *       estimate either, only the total round-trip is recorded</li>
     * </ul>
     */
    public static final class ReplicaRoundTrip
    {
        public final Timer roundTrip, atReplica, link;
        public final Counter inferred;

        private ReplicaRoundTrip(DefaultNameFactory factory, String request)
        {
            roundTrip = Metrics.timer(factory.createMetricName(REPLICA_ROUND_TRIP + '.' + request));
            atReplica = Metrics.timer(factory.createMetricName(REPLICA_TIME + '.' + request));
            link = Metrics.timer(factory.createMetricName(REPLICA_LINK + '.' + request));
            inferred = Metrics.counter(factory.createMetricName(REPLICA_TIME_INFERRED + '.' + request));
        }

        /**
         * @param replicaNanos the time spent at the replica, as reported by the replica
         */
        public void recordMeasured(long roundTripNanos, long replicaNanos)
        {
            if (roundTripNanos < 0 || replicaNanos < 0)
                return;

            // the round-trip is measured with micro precision, so may be fractionally less than the replica's time
            replicaNanos = Math.min(replicaNanos, roundTripNanos);
            roundTrip.update(roundTripNanos, NANOSECONDS);
            atReplica.update(replicaNanos, NANOSECONDS);
            link.update(roundTripNanos - replicaNanos, NANOSECONDS);
        }

        /**
         * @param linkNanos the estimated link round-trip, or negative if unknown
         */
        public void recordInferred(long roundTripNanos, long linkNanos)
        {
            if (roundTripNanos < 0)
                return;

            roundTrip.update(roundTripNanos, NANOSECONDS);
            if (linkNanos >= 0)
            {
                inferred.inc();
                atReplica.update(Math.max(0, roundTripNanos - linkNanos), NANOSECONDS);
                link.update(linkNanos, NANOSECONDS);
            }
        }

        /**
         * e.g. ACCORD_PRE_ACCEPT_REQ -> PreAccept
         */
        static String requestName(Verb verb)
        {
            String name = verb.name();
            if (name.startsWith("ACCORD_")) name = name.substring("ACCORD_".length());
            if (name.endsWith("_REQ")) name = name.substring(0, name.length() - "_REQ".length());
            StringBuilder result = new StringBuilder();
            for (String part : name.split("_"))
            {
                if (part.isEmpty()) continue;
                result.append(part.charAt(0)).append(part.substring(1).toLowerCase());
            }
            return result.toString();
        }
    }

    private final DefaultNameFactory nameFactory;
    private final ConcurrentHashMap<Verb, ReplicaRoundTrip> replicaRoundTrips = new ConcurrentHashMap<>();

    /**
     * A histogram of the number of dependencies per transaction at this coordinator.
     */
    public final Histogram dependencies;

    /**
     * A histogram of the time to preaccept on this coordinator
     */
    public final Timer preacceptLatency;

    /**
     * A histogram of the time to decide a transaction's timestamp on this coordinator.
     * This is time to complete preaccept for the fast and medium path, and time to accept on slow path.
     */
    public final Timer commitLatency;

    /**
     * A histogram of the time to begin execution on this coordinator
     */
    public final Timer executeLatency;

    /**
     * A histogram of the time to complete execution on this coordinator
     */
    public final Timer applyLatency;

    /**
     * The number of epochs used to coordinate the transaction
     */
    public final Histogram epochs;

    /**
     * The number of keys involved in a transaction
     */
    public final Histogram keys;

    /**
     * The number of tables involved in a transaction
     */
    public final Histogram tables;

    /**
     * The number of ephemeral transactions executed on this coordinator.
     */
    public final Meter ephemeral;

    /**
     * The number of fast path transactions executed on this coordinator.
     */
    public final Meter fastPaths;

    /**
     * The number of medium path transactions executed on this coordinator.
     */
    public final Meter mediumPaths;

    /**
     * The number of slow path transactions executed on this coordinator.
     */
    public final Meter slowPaths;

    /**
     * The number of preempted transactions on this coordinator.
     */
    public final Meter preempted;

    /**
     * The number of preempted transactions on this coordinator.
     */
    public final Meter rejected;

    /**
     * The number of timed out transactions on this coordinator.
     */
    public final Meter timeouts;

    /**
     * The number of invalidated transactions on this coordinator.
     */
    public final Meter invalidations;

    /**
     * The time between the start of the transaction and the start of the recovery, if the transaction is recovered.
     */
    public final Timer recoveryDelay;

    /**
     * The time between the start of the recovery and the execution of the transaction, if the transaction is recovered.
     */
    public final Timer recoveryDuration;

    /**
     * The ratio of the number of fast path transactions to the total number of transactions.
     */
    public final RatioGaugeSet fastPathToTotal;

    private AccordCoordinatorMetrics(String scope)
    {
        DefaultNameFactory coordinator = nameFactory = new DefaultNameFactory(ACCORD_COORDINATOR, scope);
        dependencies = Metrics.histogram(coordinator.createMetricName(COORDINATOR_DEPENDENCIES), true);
        preacceptLatency = Metrics.timer(coordinator.createMetricName(COORDINATOR_PREACCEPT_LATENCY));
        commitLatency = Metrics.timer(coordinator.createMetricName(COORDINATOR_COMMIT_LATENCY));
        executeLatency = Metrics.timer(coordinator.createMetricName(COORDINATOR_EXECUTE_LATENCY));
        applyLatency = Metrics.timer(coordinator.createMetricName(COORDINATOR_APPLY_LATENCY));
        epochs = Metrics.histogram(coordinator.createMetricName(COORDINATOR_EPOCHS), true);
        keys = Metrics.histogram(coordinator.createMetricName(COORDINATOR_KEYS), true);
        tables = Metrics.histogram(coordinator.createMetricName(COORDINATOR_TABLES), true);

        ephemeral = Metrics.meter(coordinator.createMetricName(EPHEMERAL));
        fastPaths = Metrics.meter(coordinator.createMetricName(FAST_PATHS));
        mediumPaths = Metrics.meter(coordinator.createMetricName(MEDIUM_PATHS));
        slowPaths = Metrics.meter(coordinator.createMetricName(SLOW_PATHS));
        preempted = Metrics.meter(coordinator.createMetricName(PREEMPTED));
        rejected = Metrics.meter(coordinator.createMetricName(REJECTED));
        timeouts = Metrics.meter(coordinator.createMetricName(TIMEOUTS));
        invalidations = Metrics.meter(coordinator.createMetricName(INVALIDATIONS));
        recoveryDelay = Metrics.timer(coordinator.createMetricName(RECOVERY_DELAY));
        recoveryDuration = Metrics.timer(coordinator.createMetricName(RECOVERY_TIME));
        fastPathToTotal = new RatioGaugeSet(fastPaths, RatioGaugeSet.sum(ephemeral, fastPaths, mediumPaths, slowPaths), coordinator, FAST_PATH_TO_TOTAL + ".%s");
    }

    public ReplicaRoundTrip replicaRoundTrip(Verb requestVerb)
    {
        ReplicaRoundTrip result = replicaRoundTrips.get(requestVerb);
        if (result == null)
            result = replicaRoundTrips.computeIfAbsent(requestVerb, v -> new ReplicaRoundTrip(nameFactory, ReplicaRoundTrip.requestName(v)));
        return result;
    }

    /**
     * @return the metrics to record round-trips of {@code requestVerb} for {@code txnId} into, or null if none
     */
    @Nullable
    public static ReplicaRoundTrip replicaRoundTrip(@Nullable Verb requestVerb, @Nullable TxnId txnId)
    {
        if (requestVerb == null) return null;
        AccordCoordinatorMetrics metrics = forTransaction(txnId);
        return metrics == null ? null : metrics.replicaRoundTrip(requestVerb);
    }

    @Nullable
    static AccordCoordinatorMetrics forTransaction(@Nullable TxnId txnId)
    {
        if (txnId != null)
        {
            if (txnId.isWrite())
                return writeMetrics;
            else if (txnId.isSomeRead())
                return readMetrics;
            else if (txnId.isSyncPoint())
                return syncPointMetrics;
        }
        return null;
    }

    /**
     * Record a round-trip of a request registered with {@code context} (if it is a {@link ReplicaRoundTrip}), for
     * which the replica reported its time
     */
    public static void recordMeasured(@Nullable Object context, long roundTripNanos, long replicaNanos)
    {
        if (context instanceof ReplicaRoundTrip)
            ((ReplicaRoundTrip) context).recordMeasured(roundTripNanos, replicaNanos);
    }

    /**
     * Record a round-trip of a request registered with {@code context} (if it is a {@link ReplicaRoundTrip}), for
     * which the replica did not report its time, given the estimated link round-trip (negative if unknown)
     */
    public static void recordInferred(@Nullable Object context, long roundTripNanos, long linkNanos)
    {
        if (context instanceof ReplicaRoundTrip)
            ((ReplicaRoundTrip) context).recordInferred(roundTripNanos, linkNanos);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("AccordCoordinatorMetrics [");

        try
        {
            for (Field f : getClass().getDeclaredFields())
            {
                f.setAccessible(true);
                if (Counting.class.isAssignableFrom(f.getType()))
                {
                    Counting metric = (Counting) f.get(this);
                    builder.append(String.format("%s: count=%d, ", f.getName(), metric.getCount()));
                }
            }
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        builder.append(']');
        return builder.toString();
    }

    public static class Listener implements CoordinatorEventListener
    {
        @Override
        public void onPreAccepted(TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordTimeService.nowMicros();
                metrics.preacceptLatency.update(Math.max(0, now - txnId.hlc()), MICROSECONDS);
            }
        }

        @Override
        public void onAccepted(TxnId txnId, Ballot ballot, ExecutePath path)
        {
            if (path == ExecutePath.SLOW)
            {
                AccordCoordinatorMetrics metrics = forTransaction(txnId);
                if (metrics != null)
                {
                    long now = AccordTimeService.nowMicros();
                    long latency = MICROSECONDS.toNanos(Math.max(0, now - txnId.hlc()));
                    metrics.commitLatency.update(latency, NANOSECONDS);
                }
            }
        }

        @Override
        public void onExecuting(TxnId txnId, @Nullable Ballot ballot, Deps deps, @Nullable ExecutePath path)
        {
            Tracing.trace("{} agreed {}", path, txnId);
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                metrics.dependencies.update(deps.txnIdCount());
                long now = AccordTimeService.nowMicros();
                long latency = MICROSECONDS.toNanos(Math.max(0, now - txnId.hlc()));
                metrics.executeLatency.update(latency, NANOSECONDS);
                if (path != null)
                {
                    switch (path)
                    {
                        default: throw new UnhandledEnum(path);
                        case EPHEMERAL: metrics.ephemeral.mark(); break;
                        case FAST: metrics.fastPaths.mark(); break;
                        case MEDIUM:
                            metrics.commitLatency.update(latency, NANOSECONDS);
                            metrics.mediumPaths.mark(); break;
                        case SLOW: metrics.slowPaths.mark(); break;
                        case BACKLOG:
                        case RECOVER:
                            break;
                    }
                }
            }
        }

        @Override
        public void onExecuted(TxnId txnId, Ballot ballot)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordTimeService.nowMicros();
                metrics.applyLatency.update(Math.max(0, now - txnId.hlc()), MICROSECONDS);
            }
        }

        @Override
        public void onRecoveryStopped(Node node, TxnId txnId, Ballot ballot, Result result, Throwable failure)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordTimeService.nowMicros();

                metrics.recoveryDuration.update(Math.max(0, now - ballot.hlc()), MICROSECONDS);
                metrics.recoveryDelay.update(Math.max(0, ballot.hlc() - txnId.hlc()), MICROSECONDS);
            }
        }

        @Override
        public void onInvalidated(TxnId txnId)
        {
            Tracing.trace("Invalidated {}", txnId);
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.invalidations.mark();
        }

        @Override
        public void onTimeout(@Nullable TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.timeouts.mark();
        }

        @Override
        public void onRejected(TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.rejected.mark();
        }

        @Override
        public void onExhausted(@Nullable TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.timeouts.mark();
            CoordinatorEventListener.super.onExhausted(txnId);
        }

        @Override
        public void onPreempted(@Nullable TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.preempted.mark();
        }
    }
}
