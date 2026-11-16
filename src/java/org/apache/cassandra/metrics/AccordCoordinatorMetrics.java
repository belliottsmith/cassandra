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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import accord.api.CoordinatorEventListener;
import accord.api.Result;
import accord.coordinate.ExecutePath;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.TxnId;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.TimeoutStrategy;
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
    public static final String COORDINATOR_BACKLOG_APPLIED_LATENCY = "BacklogAppliedLatency";
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
    public static final String MESSAGE = "Message";
    public static final String REPLICA_TIME = "ReplicaTime";
    public static final String REPLICA_TIME_INFERRED = "ReplicaTimeInferred";

    /**
     * The latency of one kind of message (by request verb) sent by this node to replicas, for one kind of transaction,
     * i.e. its round-trip, measured with the monotonic clock from submitting the request to receipt of its final reply:
     * <ul>
     *   <li>{@code <Request>Message}: the round-trip, to any replica (e.g. {@code PreAcceptMessage})</li>
     *   <li>{@code <Request>Message-<destination>}: the round-trip to replicas in datacenter {@code <destination>},
     *       or to ourselves ({@link LinkLatencies#SELF}) by local delivery or loopback. These inform decisions about
     *       whether a message to a given replica is slow ({@link #latencies}).</li>
     *   <li>{@code <Request>ReplicaTime}: the time spent at the replica, i.e. from its receipt of the request to its
     *       serialization of the reply, as reported by the replica ({@link org.apache.cassandra.net.ParamType#RECEIPT_TO_REPLY_NANOS});
     *       for requests delivered to ourselves this is the whole round-trip</li>
     *   <li>{@code <Request>ReplicaTimeInferred}: the number of samples for which the replica did not report its
     *       time (e.g. it runs an older version), so that it was instead inferred by subtracting the estimated
     *       {@link LinkLatencies link round-trip} to its datacenter; if there is no estimate either, the replica
     *       time is not recorded</li>
     * </ul>
     * The remainder of each round-trip (time queued here to be sent, and on the network) is recorded per datacenter by
     * {@link LinkLatencies}.
     */
    public static final class MessageLatency
    {
        private final DefaultNameFactory factory;
        private final String name;
        public final Timer roundTrip, atReplica;
        public final Counter inferred;
        private final ConcurrentHashMap<String, Timer> roundTripByDestination = new ConcurrentHashMap<>();

        private MessageLatency(DefaultNameFactory factory, String request)
        {
            this.factory = factory;
            this.name = request + MESSAGE;
            roundTrip = Metrics.timer(factory.createMetricName(name));
            atReplica = Metrics.timer(factory.createMetricName(request + REPLICA_TIME));
            inferred = Metrics.counter(factory.createMetricName(request + REPLICA_TIME_INFERRED));
        }

        /**
         * @param destination a datacenter, or {@link LinkLatencies#SELF}
         * @return the round-trips to {@code destination}
         */
        public Timer roundTrip(String destination)
        {
            Timer timer = roundTripByDestination.get(destination);
            if (timer == null)
                timer = roundTripByDestination.computeIfAbsent(destination, d -> Metrics.timer(factory.createMetricName(name + '-' + d)));
            return timer;
        }

        /**
         * @return the recent distribution of round-trips to {@code destination} (or to any replica, if we have no
         *         samples for {@code destination}, or it is null)
         */
        public Snapshot roundTripSnapshot(@Nullable String destination)
        {
            Timer timer = destination == null ? null : roundTripByDestination.get(destination);
            return timer != null && timer.getCount() > 0 ? timer.getSnapshot() : roundTrip.getSnapshot();
        }

        /**
         * @param destination a datacenter, {@link LinkLatencies#SELF}, or null if unknown
         * @param replicaNanos the time spent at the replica, as reported by the replica
         */
        public void recordMeasured(@Nullable String destination, long roundTripNanos, long replicaNanos)
        {
            if (roundTripNanos < 0 || replicaNanos < 0)
                return;

            recordRoundTrip(destination, roundTripNanos);
            // the round-trip may be measured with only micro precision, so may be fractionally less than the replica's time
            atReplica.update(Math.min(replicaNanos, roundTripNanos), NANOSECONDS);
        }

        /**
         * @param destination a datacenter, {@link LinkLatencies#SELF}, or null if unknown
         * @param linkNanos the estimated link round-trip, or negative if unknown
         */
        public void recordInferred(@Nullable String destination, long roundTripNanos, long linkNanos)
        {
            if (roundTripNanos < 0)
                return;

            recordRoundTrip(destination, roundTripNanos);
            if (linkNanos >= 0)
            {
                inferred.inc();
                atReplica.update(Math.max(0, roundTripNanos - linkNanos), NANOSECONDS);
            }
        }

        private void recordRoundTrip(@Nullable String destination, long roundTripNanos)
        {
            roundTrip.update(roundTripNanos, NANOSECONDS);
            if (destination != null)
                roundTrip(destination).update(roundTripNanos, NANOSECONDS);
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
    private final ConcurrentHashMap<Verb, MessageLatency> messageLatencies = new ConcurrentHashMap<>();

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
     * With accord.coordinator_backlog_execution_persist_locally, the time between this (original) coordinator beginning
     * to persist a transaction and receiving a report that a designated replica in another DC has applied it there
     */
    public final Timer backlogAppliedLatency;

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
        backlogAppliedLatency = Metrics.timer(coordinator.createMetricName(COORDINATOR_BACKLOG_APPLIED_LATENCY));
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

    public MessageLatency messageLatency(Verb requestVerb)
    {
        MessageLatency result = messageLatencies.get(requestVerb);
        if (result == null)
            result = messageLatencies.computeIfAbsent(requestVerb, v -> new MessageLatency(nameFactory, MessageLatency.requestName(v)));
        return result;
    }

    /**
     * @return the metrics to record round-trips of {@code requestVerb} for {@code txnId} into, or null if none
     */
    @Nullable
    public static MessageLatency messageLatency(@Nullable Verb requestVerb, @Nullable TxnId txnId)
    {
        if (requestVerb == null) return null;
        AccordCoordinatorMetrics metrics = forTransaction(txnId);
        return metrics == null ? null : metrics.messageLatency(requestVerb);
    }

    /**
     * @return the recent round-trip latencies of {@code requestVerb} to {@code destination}, for use by a
     *         {@link TimeoutStrategy}: {@code r} selects those of read transactions, {@code w} those of writes,
     *         and {@code rw} the larger of the two
     */
    public static TimeoutStrategy.LatencySourceFactory latencies(Verb requestVerb, @Nullable String destination)
    {
        MessageLatency reads = readMetrics.messageLatency(requestVerb), writes = writeMetrics.messageLatency(requestVerb);
        return new TimeoutStrategy.ReadWriteLatencySourceFactory(() -> reads.roundTripSnapshot(destination),
                                                                 () -> writes.roundTripSnapshot(destination));
    }

    /**
     * @return the recent round-trip latencies of {@code requestVerb} to {@code destination} for sync points, for use
     *         by a {@link TimeoutStrategy} (whatever the read/write selection, as sync points are neither)
     */
    public static TimeoutStrategy.LatencySourceFactory syncPointLatencies(Verb requestVerb, @Nullable String destination)
    {
        MessageLatency syncPoints = syncPointMetrics.messageLatency(requestVerb);
        Supplier<Snapshot> snapshot = () -> syncPoints.roundTripSnapshot(destination);
        return new TimeoutStrategy.ReadWriteLatencySourceFactory(snapshot, snapshot);
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
     * Record a round-trip of a request registered with {@code context} (if it is a {@link MessageLatency}), for
     * which the replica reported its time
     */
    public static void recordMeasured(@Nullable Object context, @Nullable String destination, long roundTripNanos, long replicaNanos)
    {
        if (context instanceof MessageLatency)
            ((MessageLatency) context).recordMeasured(destination, roundTripNanos, replicaNanos);
    }

    /**
     * Record a round-trip of a request registered with {@code context} (if it is a {@link MessageLatency}), for
     * which the replica did not report its time, given the estimated link round-trip (negative if unknown)
     */
    public static void recordInferred(@Nullable Object context, @Nullable String destination, long roundTripNanos, long linkNanos)
    {
        if (context instanceof MessageLatency)
            ((MessageLatency) context).recordInferred(destination, roundTripNanos, linkNanos);
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
        public void onBacklogApplied(TxnId txnId, long elapsed, TimeUnit units)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.backlogAppliedLatency.update(elapsed, units);
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
