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

package org.apache.cassandra.service.accord.api;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.primitives.TxnId;

import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StringRetryStrategy;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteMetrics;
import static org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory.none;
import static org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory.rw;

public class AccordWaitStrategies
{
    /**
     * A {@link TimeoutStrategy} specification that is evaluated separately for each kind of message and destination,
     * so that its percentiles ({@code pX}) refer to the recent round-trips of that message to that destination
     * (see {@link AccordCoordinatorMetrics#latencies}): e.g. {@code p50*2} is twice the median round-trip of a
     * PreAccept to the datacenter it is being sent to (or of local delivery). For a sync point specification, these
     * are the round-trips of sync points; otherwise {@code (r)} selects those of read transactions, {@code (w)} writes,
     * and {@code (rw)} (the default) the larger.
     */
    public static final class MessageTimeoutStrategy
    {
        private static final String ANY_DESTINATION = "";

        final String spec;
        final boolean syncPoints;
        // for requests whose verb is unknown
        private final TimeoutStrategy fallback;
        private final ConcurrentHashMap<Verb, ConcurrentHashMap<String, TimeoutStrategy>> byVerb = new ConcurrentHashMap<>();

        @VisibleForTesting
        MessageTimeoutStrategy(String spec, boolean syncPoints)
        {
            this.spec = spec;
            this.syncPoints = syncPoints;
            // for sync points we have no fallback latencies (and validate the specification for both)
            this.fallback = TimeoutStrategy.parse(spec, syncPoints ? none() : rw(accordReadMetrics, accordWriteMetrics));
        }

        /**
         * @param verb the request's verb, or null if unknown
         * @param destination the request's destination: a datacenter, {@link LinkLatencies#SELF}, or null if unknown
         */
        public TimeoutStrategy get(@Nullable Verb verb, @Nullable String destination)
        {
            if (verb == null)
                return fallback;

            ConcurrentHashMap<String, TimeoutStrategy> byDestination = byVerb.get(verb);
            if (byDestination == null)
                byDestination = byVerb.computeIfAbsent(verb, ignore -> new ConcurrentHashMap<>());

            String key = destination == null ? ANY_DESTINATION : destination;
            TimeoutStrategy result = byDestination.get(key);
            if (result == null)
                result = byDestination.computeIfAbsent(key, ignore -> TimeoutStrategy.parse(spec, syncPoints ? AccordCoordinatorMetrics.syncPointLatencies(verb, destination)
                                                                                                             : AccordCoordinatorMetrics.latencies(verb, destination)));
            return result;
        }

        @Override
        public String toString()
        {
            return spec;
        }
    }

    static MessageTimeoutStrategy slowTxnPreaccept, slowRead, slowStatusCheck;
    static MessageTimeoutStrategy slowSyncPointPreaccept, slowSyncPointWaitUntilApplied;
    static MessageTimeoutStrategy expireTxn, expireSyncPoint, expireSyncPointWaitUntilApplied;
    static TimeoutStrategy expireEpochWait;
    static TimeoutStrategy fetchTxn, fetchSyncPoint;
    static RetryStrategy recoverTxn, recoverSyncPoint, retrySyncPoint, retryBackgroundSyncPoint, retryDurability, retryBootstrap, retryJoinBootstrap;
    static RetryStrategy retryFetchMinEpoch, retryFetchTopology;
    static TimeoutStrategy backlogRemoteApplyDelay;

    /*
     * Each of the following strategies concerns a request of {@code verb} for {@code txnId} sent to {@code destination}
     * (a datacenter, {@link LinkLatencies#SELF}, or null if unknown), and is evaluated against the recent round-trips
     * of the same kind of message to the same destination (see {@link MessageTimeoutStrategy})
     */

    public static TimeoutStrategy expire(@Nullable TxnId txnId, @Nullable Verb verb, @Nullable String destination)
    {
        if (verb == Verb.ACCORD_CHECK_STATUS_REQ || verb == Verb.ACCORD_AWAIT_REQ)
            return expireTxn.get(verb, destination); // status checks are cheap, no reason to give sync points longer to timeout

        if (txnId == null || !txnId.isSyncPoint())
            return expireTxn.get(verb, destination);
        return (isWaitUntilApplied(verb) ? expireSyncPointWaitUntilApplied : expireSyncPoint).get(verb, destination);
    }

    public static @Nullable TimeoutStrategy slowRead(@Nullable TxnId txnId, Verb verb, @Nullable String destination)
    {
        if (txnId != null && txnId.isSyncPoint())
            return null;
        return slowRead.get(verb, destination);
    }

    public static TimeoutStrategy slowStatusCheck(Verb verb, @Nullable String destination)
    {
        return slowStatusCheck.get(verb, destination);
    }

    public static TimeoutStrategy slowPreaccept(TxnId txnId, Verb verb, @Nullable String destination)
    {
        return (txnId.isSyncPoint() ? slowSyncPointPreaccept : slowTxnPreaccept).get(verb, destination);
    }

    /**
     * WaitUntilApplied (and ApplyThenWaitUntilApplied) are only sent for sync points
     */
    public static @Nullable TimeoutStrategy slowWaitUntilApplied(TxnId txnId, Verb verb, @Nullable String destination)
    {
        return txnId.isSyncPoint() ? slowSyncPointWaitUntilApplied.get(verb, destination) : null;
    }

    private static boolean isWaitUntilApplied(@Nullable Verb verb)
    {
        return verb == Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ || verb == Verb.ACCORD_APPLY_AND_WAIT_REQ;
    }

    public static TimeoutStrategy fetch(@Nullable TxnId txnId)
    {
        if (txnId == null || !txnId.isSyncPoint())
            return fetchTxn;
        return fetchSyncPoint;
    }

    public static RetryStrategy retryFetchWatermarks()
    {
        return retryFetchMinEpoch;
    }

    public static RetryStrategy retryFetchTopology()
    {
        return retryFetchTopology;
    }

    static
    {
        AccordConfig config = DatabaseDescriptor.getAccord();
        setSlowRead(config.slow_read);
        setSlowStatusCheck(config.slow_status_check);
        setSlowTxnPreaccept(config.slow_txn_preaccept);
        setSlowSyncPointPreaccept(config.slow_syncpoint_preaccept);
        setSlowSyncPointWaitUntilApplied(config.slow_syncpoint_wait_until_applied);
        setExpireTxn(config.expire_txn);
        setExpireSyncPoint(config.expire_syncpoint);
        setExpireSyncPointWaitUntilApplied(config.expire_syncpoint_wait_until_applied);
        setExpireEpochWait(config.expire_epoch_wait);
        setFetchTxn(config.fetch_txn);
        setFetchSyncPoint(config.fetch_syncpoint);
        setRecoverTxn(config.recover_txn);
        setRecoverSyncPoint(config.recover_syncpoint);
        setRetrySyncPoint(config.retry_syncpoint);
        setRetryBackgroundSyncPoint(config.retry_background_syncpoint);
        setRetryDurability(config.retry_durability);
        setRetryBootstrap(config.retry_bootstrap);
        setRetryJoinBootstrap(config.retry_join_bootstrap);
        setRetryFetchMinEpoch(config.retry_fetch_min_epoch);
        setRetryFetchTopology(config.retry_fetch_topology);
        setBacklogRemoteApplyDelay(config.coordinator_backlog_execution_remote_apply_delay);
    }

    public static void setBacklogRemoteApplyDelay(String spec)
    {
        // TODO (expected): a better basis, e.g. inter-DC link latencies; for now PreAccept latency is a proxy for a round-trip to remote DCs
        backlogRemoteApplyDelay = TimeoutStrategy.parse(spec, new TimeoutStrategy.ReadWriteLatencySourceFactory(AccordCoordinatorMetrics.readMetrics.preacceptLatency::getSnapshot,
                                                                                                             AccordCoordinatorMetrics.writeMetrics.preacceptLatency::getSnapshot));
    }

    public static void setSlowRead(String spec)
    {
        slowRead = new MessageTimeoutStrategy(spec, false);
    }

    public static void setSlowStatusCheck(String spec)
    {
        slowStatusCheck = new MessageTimeoutStrategy(spec, false);
    }

    public static void setSlowTxnPreaccept(String spec)
    {
        slowTxnPreaccept = new MessageTimeoutStrategy(spec, false);
    }

    public static void setSlowSyncPointPreaccept(String spec)
    {
        slowSyncPointPreaccept = new MessageTimeoutStrategy(spec, true);
    }

    public static void setSlowSyncPointWaitUntilApplied(String spec)
    {
        slowSyncPointWaitUntilApplied = new MessageTimeoutStrategy(spec, true);
    }

    public static void setExpireTxn(String spec)
    {
        expireTxn = new MessageTimeoutStrategy(spec, false);
    }

    public static void setExpireSyncPoint(String spec)
    {
        expireSyncPoint = new MessageTimeoutStrategy(spec, true);
    }

    public static void setExpireSyncPointWaitUntilApplied(String spec)
    {
        expireSyncPointWaitUntilApplied = new MessageTimeoutStrategy(spec, true);
    }

    public static void setExpireEpochWait(String spec)
    {
        expireEpochWait = TimeoutStrategy.parse(spec, none());
    }

    public static void setFetchTxn(String spec)
    {
        fetchTxn = TimeoutStrategy.parse(spec, rw(accordReadMetrics, accordWriteMetrics));
    }

    public static void setFetchSyncPoint(String spec)
    {
        fetchSyncPoint = TimeoutStrategy.parse(spec, none());
    }

    public static void setRecoverTxn(String spec)
    {
        recoverTxn = RetryStrategy.parse(spec, rw(accordReadMetrics, accordWriteMetrics));
    }

    public static void setRecoverSyncPoint(StringRetryStrategy spec)
    {
        recoverSyncPoint = spec.retry();
    }

    public static void setRetrySyncPoint(StringRetryStrategy spec)
    {
        retrySyncPoint = spec.retry();
    }

    public static void setRetryBackgroundSyncPoint(StringRetryStrategy spec)
    {
        retryBackgroundSyncPoint = spec.retry();
    }

    public static void setRetryDurability(StringRetryStrategy spec)
    {
        retryDurability = spec.retry();
    }

    public static void setRetryBootstrap(StringRetryStrategy spec)
    {
        retryBootstrap = spec.retry();
    }

    public static void setRetryJoinBootstrap(StringRetryStrategy spec)
    {
        retryJoinBootstrap = spec.retry();
    }

    public static void setRetryFetchMinEpoch(StringRetryStrategy spec)
    {
        retryFetchMinEpoch = spec.retry();
    }

    public static void setRetryFetchTopology(StringRetryStrategy spec)
    {
        retryFetchTopology = spec.retry();
    }

    static RetryStrategy recover(TxnId txnId)
    {
        if (txnId.is(ExclusiveSyncPoint))
            return recoverSyncPoint;
        return recoverTxn;
    }
}
