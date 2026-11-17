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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics.MessageLatency;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.api.AccordWaitStrategies.MessageTimeoutStrategy;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AccordMessageTimeoutStrategyTest
{
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

        MessageTimeoutStrategy slow = new MessageTimeoutStrategy("10ms <= p50*2 <= 1000ms");
        // ~2x the median of the same message to the same destination (histogram buckets are ~20% wide)
        assertThat(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, "remote").computeWait(1, MILLISECONDS)).isBetween(160L, 240L);
        assertThat(slow.get(Verb.ACCORD_APPLY_REQ, "remote").computeWait(1, MILLISECONDS)).isBetween(480L, 720L);
        // bounded below
        assertThat(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, LinkLatencies.SELF).computeWait(1, MILLISECONDS)).isEqualTo(10L);
        // strategies are cached per message and destination
        assertThat(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, "remote")).isSameAs(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, "remote"));
        assertThat(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, null)).isSameAs(slow.get(Verb.ACCORD_PRE_ACCEPT_REQ, null));
    }

    @Test
    public void testInvalidSpec()
    {
        assertThatThrownBy(() -> new MessageTimeoutStrategy("not a spec")).isInstanceOf(IllegalArgumentException.class);
    }
}
