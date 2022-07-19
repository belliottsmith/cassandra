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

package org.apache.cassandra.distributed.upgrade;

import java.util.Arrays;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.Verb;

public class SelectSizeUpgradeTest extends UpgradeTestBase
{
    private static void flush(ICluster<?> cluster)
    {
        for (int node = 1; node <= cluster.size(); node++)
            cluster.get(node).flush(KEYSPACE);
    }

    /* Create cluster and write partitions of increasing sizes to all nodes with the properties that
     * 1) nulls values are smallest
     * 2) zero length strings are larger than null
     * 3) reported length is always increasing
     * 4) all nodes return the same value (as written with cl.ALL)
     * 5) absent partitions return zero bytes
     */
    public static void setupPartitions(UpgradeableCluster cluster)
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v text)");

        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (-1)", ConsistencyLevel.ALL);
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (0, '')", ConsistencyLevel.ALL);

        long valSize = 1l;
        for (int pk = 1; pk < 20; pk++)
        {
            String val = StringUtils.repeat("X", (int) valSize);
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (" + pk + ", '" + val+ "')", ConsistencyLevel.ALL);
            valSize *= 2;
        }

        flush(cluster); // SELECT_SIZE measures size according to index entries of all SSTables that contain the partition
    }

    public static void checkNode(UpgradeableCluster cluster, int upgradedNode)
    {
        // When upgrading from pre-CASSANDRA-16619 sstables, the host-id is not present
        // in the SSTable metadata, causing the commitlog to replay on startup
        // and rewrite the data in another sstable, increasing the reported size
        // as SELECT_SIZE does not compact.
        cluster.get(upgradedNode).forceCompact(KEYSPACE, "tbl");

        // After each node upgrade, run the check on each instance to get the
        // full combination on coordinating from old and new versions.
        cluster.forEach(instance -> {
            ICoordinator coord = instance.coordinator();
            Object[][] results;

            results = coord.execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = -2", ConsistencyLevel.ALL); // absent
            long absentSize = (Long) results[0][1];
            for (Object[] cols : results)
            {
                Assert.assertEquals("Difference in size reported: " + Arrays.toString(results), results[0][1], cols[1]);
            }

            results = coord.execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = -1", ConsistencyLevel.ALL); // null
            long nullSize = (Long) results[0][1];
            Assert.assertTrue(nullSize > absentSize);
            for (Object[] cols : results)
            {
                Assert.assertEquals("Difference in size reported: " + Arrays.deepToString(results), results[0][1], cols[1]);
            }

            results = coord.execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = 0", ConsistencyLevel.ALL); // zero length
            long emptySize = (Long) results[0][1];
            Assert.assertTrue(emptySize >= nullSize);
            for (Object[] cols : results)
            {
                Assert.assertEquals("Difference in size reported: " + Arrays.deepToString(results), results[0][1], cols[1]);
            }

            long valSize = 1;
            long previousPartition = emptySize;
            for (int pk = 1; pk < 20; pk++)
            {
                results = coord.execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = ?", ConsistencyLevel.ALL, pk);
                long partitionSize = (Long) results[0][1];
                Assert.assertTrue(partitionSize >= valSize);
                Assert.assertTrue(partitionSize >= previousPartition);
                for (Object[] cols : results)
                {
                    Assert.assertEquals("Difference in size reported: " + Arrays.deepToString(results), results[0][1], cols[1]);
                }

                valSize *= 2;
                previousPartition = partitionSize;
            }
        });
    }

    /* Check the properties of the preloaded data */
    @Test
    public void happyPathTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .withConfig(c -> c.set("autocompaction_on_startup_enabled", false))
        .singleUpgrade(v30)
        .setup(SelectSizeUpgradeTest::setupPartitions)
        .runAfterNodeUpgrade(SelectSizeUpgradeTest::checkNode)
        .run();
     }


    public static void checkTimeout(UpgradeableCluster cluster, int ignoredNode)
    {
        checkTimeout(cluster);
    }

    public static void checkTimeout(UpgradeableCluster cluster)
    {
        // After each node upgrade, run the check on each instance to get the
        // full combination on coordinating from old and new versions.
        try
        {
            cluster.forEach(instance -> {
                // Drop partition size requests to other instances
                cluster.filters().reset();
                cluster.filters().verbs(Verb.CIE3_PARTITION_SIZE_REQ.id, Verb.PARTITION_SIZE_REQ.id).outbound().from(instance.config().num()).drop();

                Object[][] results = null;
                Throwable caught = null;
                try
                {
                    results = instance.coordinator().execute("SELECT_SIZE FROM " + KEYSPACE + ".timeout_test WHERE pk = 0", ConsistencyLevel.ALL);
                    System.out.println("results");
                }
                catch (Throwable tr)
                {
                    caught = tr;
                }

                Assert.assertNotNull("Exception expected for read timeout", caught);
                // caught is wrapped in RuntimeException by dtest, compare by name as under different class loaders.
                Throwable rootCause = ExceptionUtils.getRootCause(caught);
                String canonicalName = (rootCause != null ? rootCause : caught).getClass().getCanonicalName();
                Assert.assertEquals(ReadTimeoutException.class.getCanonicalName(),
                                    canonicalName);
            });
        }
        finally
        {
            cluster.filters().reset();
        }
    }

    @Test
    public void timeoutTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .singleUpgrade(v30)
        .withConfig(c -> c.set("autocompaction_on_startup_enabled", false))
        .setup(cluster -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".timeout_test (pk int PRIMARY KEY, v text)");
            checkTimeout(cluster);
        })
        .runAfterNodeUpgrade(SelectSizeUpgradeTest::checkTimeout)
        .run();
    }
}
