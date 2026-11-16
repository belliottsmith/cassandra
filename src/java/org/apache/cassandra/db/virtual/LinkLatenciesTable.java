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

package org.apache.cassandra.db.virtual;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.schema.TableMetadata;

/**
 * The link latencies between this node and each datacenter, as tracked by {@link LinkLatencies}: the probe estimate
 * and distribution (the network itself), the distribution of round-trips observed on the network by real traffic,
 * and the time that traffic spent queued here to be sent. All times are in microseconds.
 */
final class LinkLatenciesTable extends AbstractVirtualTable
{
    static final String NAME = "link_latencies";

    private static final String DATACENTER = "datacenter";
    private static final String PROBE_ESTIMATE = "probe_estimate_micros";
    private static final String PROBE = "probe";
    private static final String OBSERVED = "observed";
    private static final String SEND_QUEUE = "send_queue";
    private static final String[] TIMERS = { PROBE, OBSERVED, SEND_QUEUE };
    private static final String COUNT = "_count", P50 = "_p50_micros", P99 = "_p99_micros", MAX = "_max_micros";

    LinkLatenciesTable(String keyspace)
    {
        super(metadata(keyspace));
    }

    private static TableMetadata metadata(String keyspace)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, NAME)
                                                     .comment("link latencies between this node and each datacenter")
                                                     .kind(TableMetadata.Kind.VIRTUAL)
                                                     .partitioner(new LocalPartitioner(UTF8Type.instance))
                                                     .addPartitionKeyColumn(DATACENTER, UTF8Type.instance)
                                                     .addRegularColumn(PROBE_ESTIMATE, LongType.instance);
        for (String timer : TIMERS)
        {
            builder.addRegularColumn(timer + COUNT, LongType.instance)
                   .addRegularColumn(timer + P50, LongType.instance)
                   .addRegularColumn(timer + P99, LongType.instance)
                   .addRegularColumn(timer + MAX, LongType.instance);
        }
        return builder.build();
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        LinkLatencies.Snapshot estimates = LinkLatencies.instance.snapshot();
        Map<String, LinkLatencies.Link> links = LinkLatencies.instance.links();

        Set<String> dcs = new HashSet<>(links.keySet());
        dcs.addAll(estimates.asMap().keySet());
        for (String dc : dcs)
        {
            result.row(dc);
            long estimate = estimates.roundTripMicros(dc);
            if (estimate >= 0)
                result.column(PROBE_ESTIMATE, estimate);

            LinkLatencies.Link link = links.get(dc);
            if (link != null)
            {
                addTimer(result, PROBE, link.probeRoundTrip);
                addTimer(result, OBSERVED, link.observedRoundTrip);
                addTimer(result, SEND_QUEUE, link.sendQueue);
            }
        }
        return result;
    }

    // timers record in DEFAULT_TIMER_UNIT, i.e. microseconds
    private static void addTimer(SimpleDataSet result, String name, Timer timer)
    {
        long count = timer.getCount();
        result.column(name + COUNT, count);
        if (count == 0)
            return;

        Snapshot snapshot = timer.getSnapshot();
        result.column(name + P50, (long) snapshot.getMedian())
              .column(name + P99, (long) snapshot.get99thPercentile())
              .column(name + MAX, snapshot.getMax());
    }
}
