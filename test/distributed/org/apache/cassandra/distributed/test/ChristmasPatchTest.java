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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.xmas.SuccessfulRepairTimeHolder;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.RepairResult;
import org.apache.cassandra.service.StorageService;
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

            RepairResult res = cluster.get(1).callOnInstance(repair(options(false, true, range)));
            assertTrue(res.success); // repair should succeed
            // and make sure repair history is correct on both nodes
            cluster.forEach((instance) -> instance.runOnInstance(
                () -> checkLastSuccessfulRepairInfo(startTime, repairRanges)));

            cluster.forEach((instance) -> {
                try
                {
                    instance.shutdown().get();
                }
                catch (Throwable tr)
                {
                    Assert.fail("Shutdown failure: " + tr);
                }
                instance.startup();
                instance.runOnInstance(() -> checkLastSuccessfulRepairInfo(startTime, repairRanges));
            });
        }
    }


    static void checkLastSuccessfulRepairInfo(int startTime, Set<Range<Token>> repairRanges)
    {
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
        assertEquals(2, holder.successfulRepairs.size());
        for (Pair<Range<Token>, Integer> repairTimes : holder.successfulRepairs)
            assertEquals(successfulRepairs.get(repairTimes.left), repairTimes.right);

    }


    @Test
    public void testNTMove() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(Feature.GOSSIP,
                                                                             Feature.NETWORK)
                                                                       .set("enable_christmas_patch", true))
                                           .start()))
        {
            cluster.schemaChange("alter keyspace "+KEYSPACE+" with replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, i int)");

            for (int i = 0; i < 10000; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, i) VALUES (?, ?)", ConsistencyLevel.ALL, i, i);

            RepairResult res = cluster.get(1).callOnInstance(repair(options(false, true)));
            assertTrue(res.success);

            cluster.get(3).runOnInstance(() -> {
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").clearRepairedRangeUnsafes();
            });
            Assert.assertEquals(0, cluster.get(3).executeInternal("select * from system.repair_history where keyspace_name=? and columnfamily_name=?", KEYSPACE, "tbl").length);
            cluster.get(2).runOnInstance(() -> {
                try
                {
                    StorageService.instance.move("-6074457345618258603");
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(cluster.get(3).executeInternal("select * from system.repair_history where keyspace_name=? and columnfamily_name=?", KEYSPACE, "tbl").length > 0);
        }
    }

    private String toString(Set<Range<Token>> repairRanges)
    {
        return repairRanges.stream()
                           .map((range -> range.left+":"+range.right))
                           .collect(Collectors.joining(","));
    }
}
