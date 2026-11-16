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

import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics.ReplicaRoundTrip;
import org.apache.cassandra.net.Verb;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class AccordCoordinatorReplicaRoundTripTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRequestName()
    {
        assertThat(ReplicaRoundTrip.requestName(Verb.ACCORD_PRE_ACCEPT_REQ)).isEqualTo("PreAccept");
        assertThat(ReplicaRoundTrip.requestName(Verb.ACCORD_STABLE_THEN_READ_REQ)).isEqualTo("StableThenRead");
        assertThat(ReplicaRoundTrip.requestName(Verb.ACCORD_INTEROP_READ_REQ)).isEqualTo("InteropRead");
    }

    @Test
    public void testScopeAndContext()
    {
        TxnId write = new TxnId(1, 1, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
        ReplicaRoundTrip writes = AccordCoordinatorMetrics.replicaRoundTrip(Verb.ACCORD_ACCEPT_REQ, write);
        assertThat(writes).isSameAs(AccordCoordinatorMetrics.writeMetrics.replicaRoundTrip(Verb.ACCORD_ACCEPT_REQ));
        assertThat(AccordCoordinatorMetrics.replicaRoundTrip(null, write)).isNull();
        assertThat(AccordCoordinatorMetrics.replicaRoundTrip(Verb.ACCORD_ACCEPT_REQ, null)).isNull();

        // anything other than a ReplicaRoundTrip is ignored
        AccordCoordinatorMetrics.recordMeasured(null, 1, 0);
        AccordCoordinatorMetrics.recordInferred("unrelated", 1, 0);
    }

    @Test
    public void testRecordMeasured()
    {
        ReplicaRoundTrip metrics = AccordCoordinatorMetrics.syncPointMetrics.replicaRoundTrip(Verb.ACCORD_APPLY_REQ);
        long roundTrips = metrics.roundTrip.getCount(), atReplica = metrics.atReplica.getCount(), links = metrics.link.getCount();

        // reported by the replica: the remainder of the round-trip is the link
        AccordCoordinatorMetrics.recordMeasured(metrics, ms(70), ms(10));
        // reported time fractionally exceeding the (micro precision) round-trip is capped at the round-trip
        AccordCoordinatorMetrics.recordMeasured(metrics, ms(5), ms(5) + 500);
        // negative values are discarded
        metrics.recordMeasured(-1, 0);
        metrics.recordMeasured(ms(1), -1);

        assertThat(metrics.roundTrip.getCount() - roundTrips).isEqualTo(2);
        assertThat(metrics.atReplica.getCount() - atReplica).isEqualTo(2);
        assertThat(metrics.link.getCount() - links).isEqualTo(2);
        assertThat(metrics.link.getSnapshot().getMin()).isEqualTo(0);
        assertThat(metrics.inferred.getCount()).isEqualTo(0);
    }

    @Test
    public void testRecordInferred()
    {
        ReplicaRoundTrip metrics = AccordCoordinatorMetrics.syncPointMetrics.replicaRoundTrip(Verb.ACCORD_COMMIT_REQ);

        // not reported by the replica, with a known link: the link estimate is subtracted
        AccordCoordinatorMetrics.recordInferred(metrics, ms(70), ms(60));
        // with an unknown link: only the total is known
        AccordCoordinatorMetrics.recordInferred(metrics, ms(80), -1);
        // faster than the link estimate: replica time is clamped at zero
        AccordCoordinatorMetrics.recordInferred(metrics, ms(50), ms(60));
        metrics.recordInferred(-1, 0);

        assertThat(metrics.roundTrip.getCount()).isEqualTo(3);
        assertThat(metrics.atReplica.getCount()).isEqualTo(2);
        assertThat(metrics.link.getCount()).isEqualTo(2);
        assertThat(metrics.inferred.getCount()).isEqualTo(2);
        assertThat(metrics.atReplica.getSnapshot().getMin()).isEqualTo(0);
    }

    private static long ms(long millis)
    {
        return MILLISECONDS.toNanos(millis);
    }
}
