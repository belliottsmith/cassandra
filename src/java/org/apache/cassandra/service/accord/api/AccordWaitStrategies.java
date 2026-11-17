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
     * PreAccept to the datacenter it is being sent to (or of local delivery).
     */
    public static final class MessageTimeoutStrategy
    {
        private static final String ANY_DESTINATION = "";

        final String spec;
        private final ConcurrentHashMap<Verb, ConcurrentHashMap<String, TimeoutStrategy>> byVerb = new ConcurrentHashMap<>();

        MessageTimeoutStrategy(String spec)
        {
            this.spec = spec;
            TimeoutStrategy.parse(spec, none()); // validate
        }

        /**
         * @param verb the request's verb
         * @param destination the request's destination: a datacenter, {@link LinkLatencies#SELF}, or null if unknown
         */
        public TimeoutStrategy get(Verb verb, @Nullable String destination)
        {
            ConcurrentHashMap<String, TimeoutStrategy> byDestination = byVerb.get(verb);
            if (byDestination == null)
                byDestination = byVerb.computeIfAbsent(verb, ignore -> new ConcurrentHashMap<>());

            String key = destination == null ? ANY_DESTINATION : destination;
            TimeoutStrategy result = byDestination.get(key);
            if (result == null)
                result = byDestination.computeIfAbsent(key, ignore -> TimeoutStrategy.parse(spec, AccordCoordinatorMetrics.latencies(verb, destination)));
            return result;
        }

        @Override
        public String toString()
        {
            return spec;
        }
    }

    static MessageTimeoutStrategy slowTxnPreaccept, slowRead, slowStatusCheck;
    static TimeoutStrategy slowSyncPointPreaccept;
    static TimeoutStrategy expireTxn, expireSyncPoint, expireDurability, expireEpochWait;
    static TimeoutStrategy fetchTxn, fetchSyncPoint;
    static RetryStrategy recoverTxn, recoverSyncPoint, retrySyncPoint, retryBackgroundSyncPoint, retryDurability, retryBootstrap, retryJoinBootstrap;
    static RetryStrategy retryFetchMinEpoch, retryFetchTopology;

    public static @Nullable TimeoutStrategy slowRead(@Nullable TxnId txnId, Verb verb, @Nullable String destination)
    {
        if (txnId != null && txnId.isSyncPoint())
            return null;
        return slowRead.get(verb, destination);
    }

    public static TimeoutStrategy expire(@Nullable TxnId txnId, Verb verb)
    {
        switch (verb)
        {
            case ACCORD_CHECK_STATUS_REQ:
            case ACCORD_AWAIT_REQ:
                // status checks are cheap, no reason to give sync points longer to timeout
                return expireTxn;
        }

        if (txnId == null || !txnId.isSyncPoint())
            return expireTxn;
        return verb == Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ ? expireDurability : expireSyncPoint;
    }

    public static @Nullable TimeoutStrategy slowStatusCheck(Verb verb, @Nullable String destination)
    {
        return slowStatusCheck.get(verb, destination);
    }

    public static TimeoutStrategy fetch(@Nullable TxnId txnId)
    {
        if (txnId == null || !txnId.isSyncPoint())
            return fetchTxn;
        return fetchSyncPoint;
    }

    public static TimeoutStrategy slowPreaccept(TxnId txnId, Verb verb, @Nullable String destination)
    {
        if (txnId.isSyncPoint())
            return expireSyncPoint;
        return slowTxnPreaccept.get(verb, destination);
    }

    static TimeoutStrategy slowTxnPreaccept(Verb verb, @Nullable String destination)
    {
        return slowTxnPreaccept.get(verb, destination);
    }

    static TimeoutStrategy slowRead(Verb verb, @Nullable String destination)
    {
        return slowRead.get(verb, destination);
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
        setExpireTxn(config.expire_txn);
        setExpireSyncPoint(config.expire_syncpoint);
        setExpireDurability(config.expire_durability);
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
    }

    public static void setSlowRead(String spec)
    {
        slowRead = new MessageTimeoutStrategy(spec);
    }

    public static void setSlowStatusCheck(String spec)
    {
        slowStatusCheck = new MessageTimeoutStrategy(spec);
    }

    public static void setSlowTxnPreaccept(String spec)
    {
        slowTxnPreaccept = new MessageTimeoutStrategy(spec);
    }

    public static void setSlowSyncPointPreaccept(String spec)
    {
        slowSyncPointPreaccept = TimeoutStrategy.parse(spec, none());
    }

    public static void setExpireTxn(String spec)
    {
        expireTxn = TimeoutStrategy.parse(spec, rw(accordReadMetrics, accordWriteMetrics));
    }

    public static void setExpireSyncPoint(String spec)
    {
        expireSyncPoint = TimeoutStrategy.parse(spec, none());
    }

    public static void setExpireDurability(String spec)
    {
        expireDurability = TimeoutStrategy.parse(spec, none());
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
