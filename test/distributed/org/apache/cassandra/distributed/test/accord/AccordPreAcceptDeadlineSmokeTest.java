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

package org.apache.cassandra.distributed.test.accord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.LinkLatencies;
import org.apache.cassandra.service.accord.AccordCommandStores;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.execution.AccordExecutor;
import org.apache.cassandra.service.consensus.TransactionalMode;

/**
 * Smoke test: contended Accord transactions with deadline-ordered PreAccept delivery, on the SIGNAL executor.
 */
public class AccordPreAcceptDeadlineSmokeTest extends TestBaseImpl
{
    private static final int THREADS_PER_NODE = 4, INCREMENTS = 25;

    @Test
    public void contendedIncrements() throws Throwable
    {
        Map<String, Object> penalty = new HashMap<>();
        penalty.put("class_name", "LinkLatencyPreAcceptPenalty");

        try (Cluster cluster = init(Cluster.build(3)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)
                                                             .set("write_request_timeout", "10s")
                                                             .set("accord.queue_submission_model", "SIGNAL")
                                                             .set("accord.preaccept_deadline_penalty", penalty))
                                           .start()))
        {
            // link latencies are not probed (link_latency_probe_interval is unset), so impose a 40ms round-trip (a 20ms penalty)
            cluster.forEach(i -> i.runOnInstance(() -> LinkLatencies.instance.unsafeSetRoundTripMicros(DatabaseDescriptor.getLocalDataCenter(), 40_000)));
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c)) WITH " + TransactionalMode.full.asCqlParam());
            cluster.coordinator(1).execute("INSERT INTO ks.tbl (k, c, v) VALUES (0, 0, 0)", ConsistencyLevel.ALL);

            String increment = "BEGIN TRANSACTION\n" +
                               "  UPDATE ks.tbl SET v += 1 WHERE k = 0 AND c = 0;\n" +
                               "COMMIT TRANSACTION";

            ExecutorService executor = Executors.newFixedThreadPool(3 * THREADS_PER_NODE);
            List<Future<Integer>> futures = new ArrayList<>();
            for (int node = 1 ; node <= 3 ; ++node)
            {
                int coordinator = node;
                for (int t = 0 ; t < THREADS_PER_NODE ; ++t)
                {
                    futures.add(executor.submit(() -> {
                        int failures = 0;
                        for (int i = 0 ; i < INCREMENTS ; ++i)
                        {
                            while (true)
                            {
                                try { cluster.coordinator(coordinator).execute(increment, ConsistencyLevel.ANY); break; }
                                catch (Throwable t2) { if (++failures > 100) throw t2; }
                            }
                        }
                        return failures;
                    }));
                }
            }
            int failures = 0;
            for (Future<Integer> f : futures)
                failures += f.get(5, TimeUnit.MINUTES);
            executor.shutdown();

            Object[][] rows = cluster.coordinator(1).execute("BEGIN TRANSACTION\n  SELECT v FROM ks.tbl WHERE k = 0 AND c = 0;\nCOMMIT TRANSACTION", ConsistencyLevel.ANY);
            long deferred = 0;
            for (int node = 1 ; node <= 3 ; ++node)
            {
                deferred += cluster.get(node).callOnInstance(() -> {
                    long count = 0;
                    for (AccordExecutor e : ((AccordCommandStores) AccordService.instance().node().commandStores()).executors())
                        count += e.unsafeDeferredCount();
                    return count;
                });
            }
            System.out.println("### deferred=" + deferred + " retriedFailures=" + failures + " v=" + rows[0][0]);
            Assert.assertTrue("no PreAccepts were deferred", deferred > 0);
            // a retried failure may have been applied (e.g. if preempted), so we can only require equality if none failed
            int expected = 3 * THREADS_PER_NODE * INCREMENTS;
            if (failures == 0) Assert.assertEquals(expected, rows[0][0]);
            else Assert.assertTrue((Integer) rows[0][0] >= expected);
        }
    }
}
