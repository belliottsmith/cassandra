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

package org.apache.cassandra.distributed.test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.xmas.SuccessfulRepairTimeHolder;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.RepairResult;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static junit.framework.TestCase.assertTrue;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.options;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.repair;
import static org.junit.Assert.assertEquals;

public class ChristmasPatchTest extends TestBaseImpl
{
    @Test
    public void testPopulateTable() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(Feature.GOSSIP,
                                                                             Feature.NETWORK)
                                                                       .set("enable_christmas_patch", true))
                                           .start()))
        {
            int startTime = FBUtilities.nowInSeconds();

            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, i int)");

            Set<Range<Token>> repairRanges = new HashSet<>();
            repairRanges.add(new Range<>(new Murmur3Partitioner.LongToken(-1000),
                                         new Murmur3Partitioner.LongToken(-1)));
            repairRanges.add(new Range<>(new Murmur3Partitioner.LongToken(-1),
                                         new Murmur3Partitioner.LongToken(1000)));

            String range = toString(repairRanges);
            // no repair history on startup
            cluster.forEach((instance) -> instance.runOnInstance(() -> {
                assertTrue(SystemKeyspace.getLastSuccessfulRepair(KEYSPACE, "tbl").isEmpty());
            }));

            RepairResult res = cluster.get(1).callOnInstance(repair(options(false, false, range)));
            assertTrue(res.success); // repair should succeed
            // and make sure repair history is correct on both nodes
            cluster.forEach((instance) -> instance.runOnInstance(() -> {
                int time = -1;
                Map<Range<Token>, Integer> successfulRepairs = SystemKeyspace.getLastSuccessfulRepair(KEYSPACE, "tbl");
                assertEquals(2, successfulRepairs.size());
                assertEquals(successfulRepairs.keySet(), repairRanges);
                for (int repairTime : successfulRepairs.values())
                {
                    if (time == -1)
                        time = repairTime;
                    assertEquals(repairTime, time);
                    assertTrue(repairTime >= startTime);
                    assertTrue(repairTime <= FBUtilities.nowInSeconds());
                }

                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                SuccessfulRepairTimeHolder holder = cfs.getRepairTimeSnapshot();
                for (Pair<Range<Token>, Integer> repairTimes : holder.successfulRepairs)
                    assertEquals(successfulRepairs.get(repairTimes.left), repairTimes.right);
            }));
        }
    }

    private String toString(Set<Range<Token>> repairRanges)
    {
        return repairRanges.stream()
                           .map((range -> range.left+":"+range.right))
                           .collect(Collectors.joining(","));
    }
}
