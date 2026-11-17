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

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics.MessageLatency;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.api.AccordWaitStrategies.MessageTimeoutStrategy;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AccordMessageTimeoutStrategyTest
{
    private static final TxnId WRITE = new TxnId(1, 1, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
    private static final TxnId SYNC_POINT = new TxnId(1, 1, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, new Node.Id(1));

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testPerMessageAndDestination()
    {
        MessageLatency preAccepts = AccordCoordinatorMetrics.writeMetrics.messageLatency(Verb.ACCORD_PRE_ACCEPT_REQ);
        MessageLatency applies = AccordCoordinatorMetrics.writeMetrics.messageLatency(Verb.ACCORD_APPLY_REQ);
        for (int i = 0 ; i < 100 ; ++i)
        {
            preAccepts.recordMeasured("remote", MILLISECONDS.toNanos(100), 0);
            preAccepts.recordMeasured(LinkLatencies.SELF, MILLISECONDS.toNanos(1), 0);
            applies.recordMeasured("remote", MILLISECONDS.toNanos(300), 0);
        }

        MessageTimeoutStrategy slow = new MessageTimeoutStrategy("10ms <= p50*2 <= 1000ms", false);
        // ~2x the median of the same message to the same destination (histogram buckets are ~20% wide)
        assertThat(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, "remote").computeWait(1, MILLISECONDS)).isBetween(160L, 240L);
        assertThat(slow.get(Verb.ACCORD_APPLY_REQ, "remote").computeWait(1, MILLISECONDS)).isBetween(480L, 720L);
        // bounded below
        assertThat(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, LinkLatencies.SELF).computeWait(1, MILLISECONDS)).isEqualTo(10L);
        // strategies are cached per message and destination
        assertThat(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, "remote")).isSameAs(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, "remote"));
        assertThat(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, null)).isSameAs(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, null));
        // with no verb, fall back to client latencies (of which there are none here)
        assertThat(slow.get(null, "remote").computeWait(1, MILLISECONDS)).isEqualTo(10L);
    }

    @Test
    public void testSyncPoints()
    {
        MessageLatency syncPoints = AccordCoordinatorMetrics.syncPointMetrics.messageLatency(Verb.ACCORD_ACCEPT_REQ);
        MessageLatency writes = AccordCoordinatorMetrics.writeMetrics.messageLatency(Verb.ACCORD_ACCEPT_REQ);
        for (int i = 0 ; i < 100 ; ++i)
        {
            syncPoints.recordMeasured("remote", MILLISECONDS.toNanos(400), 0);
            writes.recordMeasured("remote", MILLISECONDS.toNanos(4), 0);
        }

        // sync point specifications read sync point latencies, whatever the read/write selection
        for (String spec : new String[]{ "p50*2", "p50(r)*2", "p50(w)*2", "p50(rw)*2" })
            assertThat(new MessageTimeoutStrategy(spec, true).get(Verb.ACCORD_ACCEPT_REQ, "remote").computeWait(1, MILLISECONDS)).isBetween(640L, 960L);
        assertThat(new MessageTimeoutStrategy("p50*2", false).get(Verb.ACCORD_ACCEPT_REQ, "remote").computeWait(1, MILLISECONDS)).isBetween(6L, 10L);
    }

    @Test
    public void testDispatch()
    {
        // configured defaults: slow_syncpoint_preaccept = 10s, slow_syncpoint_wait_until_applied = 1s <= p99*2 <= 10s,
        // expire_txn = 5s*attempts <= 60s, expire_syncpoint = 15s*attempts<=300s,
        // expire_syncpoint_wait_until_applied = 10s*attempts <= 30s
        assertThat(AccordWaitStrategies.slowPreaccept(SYNC_POINT, Verb.ACCORD_PRE_ACCEPT_REQ, "remote").computeWait(1, SECONDS)).isEqualTo(10);
        assertThat(AccordWaitStrategies.slowPreaccept(WRITE, Verb.ACCORD_PRE_ACCEPT_REQ, "remote").computeWait(1, MILLISECONDS)).isBetween(30L, 1000L);
        // with no sync point round-trips of these messages, bounded below
        assertThat(AccordWaitStrategies.slowWaitUntilApplied(SYNC_POINT, Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ, "remote").computeWait(1, SECONDS)).isEqualTo(1);
        // otherwise ~2x the p99 to the same destination, bounded above (histogram buckets are ~20% wide)
        MessageLatency applyAndWait = AccordCoordinatorMetrics.syncPointMetrics.messageLatency(Verb.ACCORD_APPLY_AND_WAIT_REQ);
        for (int i = 0 ; i < 100 ; ++i)
        {
            applyAndWait.recordMeasured("near", MILLISECONDS.toNanos(1000), 0);
            applyAndWait.recordMeasured("far", MILLISECONDS.toNanos(8000), 0);
        }
        assertThat(AccordWaitStrategies.slowWaitUntilApplied(SYNC_POINT, Verb.ACCORD_APPLY_AND_WAIT_REQ, "near").computeWait(1, MILLISECONDS)).isBetween(1600L, 2400L);
        assertThat(AccordWaitStrategies.slowWaitUntilApplied(SYNC_POINT, Verb.ACCORD_APPLY_AND_WAIT_REQ, "far").computeWait(1, SECONDS)).isEqualTo(10);
        // and less than the expiry, so it can take effect
        assertThat(AccordWaitStrategies.slowWaitUntilApplied(SYNC_POINT, Verb.ACCORD_APPLY_AND_WAIT_REQ, "near").computeWait(1, MILLISECONDS))
            .isLessThan(AccordWaitStrategies.expire(SYNC_POINT, Verb.ACCORD_APPLY_AND_WAIT_REQ, "near").computeWait(1, MILLISECONDS));
        assertThat(AccordWaitStrategies.slowWaitUntilApplied(WRITE, Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ, "remote")).isNull();
        assertThat(AccordWaitStrategies.slowRead(SYNC_POINT, Verb.ACCORD_READ_REQ, "remote")).isNull();

        assertThat(AccordWaitStrategies.expire(WRITE, Verb.ACCORD_PRE_ACCEPT_REQ, "remote").computeWait(1, SECONDS)).isEqualTo(5);
        assertThat(AccordWaitStrategies.expire(SYNC_POINT, Verb.ACCORD_PRE_ACCEPT_REQ, "remote").computeWait(1, SECONDS)).isEqualTo(15);
        assertThat(AccordWaitStrategies.expire(SYNC_POINT, Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ, "remote").computeWait(1, SECONDS)).isEqualTo(10);
        assertThat(AccordWaitStrategies.expire(SYNC_POINT, Verb.ACCORD_APPLY_AND_WAIT_REQ, "remote").computeWait(3, SECONDS)).isEqualTo(30);
        assertThat(AccordWaitStrategies.expire(WRITE, Verb.ACCORD_APPLY_AND_WAIT_REQ, "remote").computeWait(1, SECONDS)).isEqualTo(5);
        assertThat(AccordWaitStrategies.expire(SYNC_POINT, Verb.ACCORD_CHECK_STATUS_REQ, "remote").computeWait(1, SECONDS)).isEqualTo(5);
        assertThat(AccordWaitStrategies.expire(null, null, null).computeWait(1, SECONDS)).isEqualTo(5);
    }

    @Test
    public void testDeprecatedExpireDurability()
    {
        Config config = YamlConfigurationLoader.fromMap(ImmutableMap.of("accord", ImmutableMap.of("expire_durability", "7s")), true, Config.class);
        assertThat(config.accord.expire_syncpoint_wait_until_applied).isEqualTo("7s");
        config = YamlConfigurationLoader.fromMap(ImmutableMap.of("accord", ImmutableMap.of("expire_syncpoint_wait_until_applied", "8s")), true, Config.class);
        assertThat(config.accord.expire_syncpoint_wait_until_applied).isEqualTo("8s");
    }

    @Test
    public void testInvalidSpec()
    {
        assertThatThrownBy(() -> new MessageTimeoutStrategy("not a spec", false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MessageTimeoutStrategy("not a spec", true)).isInstanceOf(IllegalArgumentException.class);
    }
}
