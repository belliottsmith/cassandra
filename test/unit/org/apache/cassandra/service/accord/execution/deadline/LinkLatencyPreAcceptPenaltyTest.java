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

package org.apache.cassandra.service.accord.execution.deadline;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.LinkLatencies;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class LinkLatencyPreAcceptPenaltyTest
{
    private static final List<String> DCS = Arrays.asList("dc1", "dc2", "dc3", "dc4");

    @After
    public void clear()
    {
        for (String dc : DCS)
            LinkLatencies.instance.unsafeSetRoundTripMicros(dc, -1);
    }

    @Test
    public void penaltyIsMaxInboundLatency()
    {
        // round-trips between this node and each dc; dc4 is unknown
        LinkLatencies.instance.unsafeSetRoundTripMicros("dc1", 2_000);
        LinkLatencies.instance.unsafeSetRoundTripMicros("dc2", 60_000);
        LinkLatencies.instance.unsafeSetRoundTripMicros("dc3", 160_000);
        assertEquals(80_000, LinkLatencyPreAcceptPenalty.maxInbound(DCS, LinkLatencies.instance.snapshot()));
        assertEquals(30_000, LinkLatencyPreAcceptPenalty.maxInbound(Arrays.asList("dc1", "dc2", "dc4"), LinkLatencies.instance.snapshot()));
    }

    @Test
    public void noPenaltyWithoutEstimates()
    {
        assertEquals(0, LinkLatencyPreAcceptPenalty.maxInbound(DCS, LinkLatencies.instance.snapshot()));
    }

    @Test
    public void rejectsParameters()
    {
        assertThatThrownBy(() -> new LinkLatencyPreAcceptPenalty(ImmutableMap.of("latencies", "FileDcLinkLatencies")))
            .isInstanceOf(ConfigurationException.class);
    }
}
