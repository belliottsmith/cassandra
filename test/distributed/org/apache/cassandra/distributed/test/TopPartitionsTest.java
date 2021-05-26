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

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.serializers.BooleanSerializer;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.psjava.util.AssertStatus.assertTrue;


public class TopPartitionsTest extends TestBaseImpl
{
    @Test
    public void basicPartitionSizeTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("min_tracked_partition_size_bytes", 0)
                                                                     .set("min_tracked_partition_tombstone_count", 0)
                                                                     .set("disable_incremental_repair", false)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck int, t int, primary key (id, ck))");
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < i; j++)
                    cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (id, ck, t) values (?,?,?)", ConsistencyLevel.ALL, i, j, i * j + 100);

            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.forEach(inst -> inst.runOnInstance(() -> {
                // partitions 99 -> 90 are the largest, make sure they are in the map;
                Map<String, Long> sizes = cfs().getTopSizePartitions();
                for (int i = 99; i >= 90; i--)
                    assertTrue(sizes.containsKey(String.valueOf(i)));

                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                assertEquals(10, tombstones.size());
                assertTrue(tombstones.values().stream().allMatch(l -> l == 0));
            }));

            long mark = cluster.get(1).logs().mark();
            // make sure incremental repair doesn't change anything;
            long lastUpdatedBeforeRepair = cluster.get(1).callOnInstance(() -> cfs().getTopSizePartitionsLastUpdate());
            cluster.get(1).nodetool("repair", KEYSPACE);
            long lastUpdatedAfterRepair = cluster.get(1).callOnInstance(() -> cfs().getTopSizePartitionsLastUpdate());
            assertEquals(lastUpdatedBeforeRepair, lastUpdatedAfterRepair);
            // we need to wait for the local repair session to be marked finalized - otherwise we might cancel the -vd repair below
            cluster.get(1).logs().watchFor(mark, "Finalized local repair session");


            // make sure we can change the number of tracked partitions (and that -vd actually tracks);
            cluster.get(1).runOnInstance(() -> DatabaseDescriptor.setMaxTopSizePartitionCount(5));
            cluster.get(1).nodetool("repair", "-vd", KEYSPACE);
            cluster.get(1).runOnInstance(() -> assertEquals(5, cfs().getTopSizePartitions().size()));
            lastUpdatedAfterRepair = cluster.get(1).callOnInstance(() -> cfs().getTopSizePartitionsLastUpdate());
            assertTrue(lastUpdatedBeforeRepair < lastUpdatedAfterRepair);
            cluster.get(2).runOnInstance(() -> assertEquals(10, cfs().getTopSizePartitions().size()));

            cluster.get(1).runOnInstance(() -> DatabaseDescriptor.setMaxTopSizePartitionCount(35));
            cluster.get(1).nodetool("repair", "-vd", KEYSPACE);
            cluster.get(1).runOnInstance(() -> assertEquals(35, cfs().getTopSizePartitions().size()));
            cluster.get(2).runOnInstance(() -> assertEquals(10, cfs().getTopSizePartitions().size()));
        }
    }


    @Test
    public void basicRowTombstonesTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("min_tracked_partition_size_bytes", 0)
                                                                     .set("min_tracked_partition_tombstone_count", 0)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck int, t int, primary key (id, ck)) with gc_grace_seconds = 5");
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < i; j++)
                    cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".tbl where id = ? and ck = ?", ConsistencyLevel.ALL, i, j);
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            // tombstones not purgeable
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                {
                    assertTrue(tombstones.containsKey(String.valueOf(i)));
                    assertEquals(i, (long)tombstones.get(String.valueOf(i)));
                }
            });
            // wait for gcgs
            Thread.sleep(6000);
            // count purgeable tombstones;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                {
                    assertTrue(tombstones.containsKey(String.valueOf(i)));
                    assertEquals(i, (long)tombstones.get(String.valueOf(i)));
                }
            });
            cluster.get(1).forceCompact(KEYSPACE, "tbl");
            // all tombstones actually purged;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                assertTrue(tombstones.values().stream().allMatch( l -> l == 0));
            });
        }
    }

    @Test
    public void basicRegularTombstonesTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("min_tracked_partition_size_bytes", 0)
                                                                     .set("min_tracked_partition_tombstone_count", 0)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck int, t int, primary key (id, ck)) with gc_grace_seconds = 5");
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < i; j++)
                    cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET t = null where id = ? and ck = ?", ConsistencyLevel.ALL, i, j);
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            // tombstones not purgeable
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                {
                    assertTrue(tombstones.containsKey(String.valueOf(i)));
                    assertEquals(i, (long)tombstones.get(String.valueOf(i)));
                }
            });
            // wait for gcgs
            Thread.sleep(6000);
            // count purgeable tombstones;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                {
                    assertTrue(tombstones.containsKey(String.valueOf(i)));
                    assertEquals(i, (long)tombstones.get(String.valueOf(i)));
                }
            });

            cluster.get(1).forceCompact(KEYSPACE, "tbl");
            // all tombstones actually purged;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                assertTrue(tombstones.values().stream().allMatch( l -> l == 0));
            });
        }
    }

    @Test
    public void basicRangeTombstonesTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .set("min_tracked_partition_size_bytes", 0)
                                                                     .set("min_tracked_partition_tombstone_count", 0)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck int, t int, primary key (id, ck)) with gc_grace_seconds = 5");
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < i; j++)
                    cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".tbl WHERE id = ? and ck >= ? and ck <= ?", ConsistencyLevel.ALL, i, j, j);
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            // tombstones not purgeable
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                // note that we count range tombstone markers - so the count will be double the number of deletions we did above
                for (int i = 99; i >= 90; i--)
                    assertEquals(i * 2, (long)tombstones.get(String.valueOf(i)));
            });
            // wait for gc_grace to expire
            Thread.sleep(6000);
            // count purgeable tombstones;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                for (int i = 99; i >= 90; i--)
                    assertEquals(i * 2, (long)tombstones.get(String.valueOf(i)));
            });

            cluster.get(1).forceCompact(KEYSPACE, "tbl");
            // all tombstones actually purged;
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                assertTrue(tombstones.values().stream().allMatch( l -> l == 0));
            });
        }
    }

    @Test
    public void partitionDeletionTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .set("min_tracked_partition_tombstone_count", 0)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck int, t int, primary key (id, ck))");
            cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".tbl WHERE id = 1", ConsistencyLevel.ALL);
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            // make sure we count partition deletions
            cluster.get(1).runOnInstance(() -> {
                Map<String, Long> tombstones = cfs().getTopTombstonePartitions();
                assertEquals(1L, (long)tombstones.get("1"));
            });
        }
    }

    @Test
    public void booleanTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                                config.set("disable_incremental_repair", false)
                                                                      .set("min_tracked_partition_tombstone_count", 0)
                                                                      .set("min_tracked_partition_size_bytes", 0)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                           .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int, ck boolean, t int, primary key ((id, ck)))");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, ck, t) values (?, true, ?)"), ConsistencyLevel.ALL, i, i);
            cluster.get(1).nodetool("repair", "-full", KEYSPACE);
            cluster.forEach(i -> i.runOnInstance(() -> {

                cfs().topPartitions.save();
                SystemKeyspace.getTopPartitions(cfs().metadata.get(), "SIZES");
                assertEquals(0, BooleanSerializer.instance.serialize(true).position());
                assertEquals(0, BooleanSerializer.instance.serialize(false).position());
            }));
        }
    }

    private static ColumnFamilyStore cfs()
    {
        return  Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
    }
}
