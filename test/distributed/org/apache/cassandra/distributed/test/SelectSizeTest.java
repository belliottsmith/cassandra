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

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.Verb;

public class SelectSizeTest extends TestBaseImpl
{
    private static ICluster cluster = null;

    private static void flush(ICluster cluster)
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
    @BeforeClass
    public static void setupPartitions() throws IOException
    {
        cluster = init(Cluster.build().withNodes(3).start());
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

    @AfterClass
    public static void closeCluster() throws Exception
    {
        if (cluster != null)
        {
            cluster.close();
        }
    }

    /* Check the properties of the preloaded data */
    @Test
    public void happyPathTest() throws Throwable
    {
        Object[][] results;
        long valSize = 1l;

        results = cluster.coordinator(1).execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = -2", ConsistencyLevel.ALL); // absent
        long absentSize = (Long) results[0][1];
        for (Object[] cols : results)
        {
            Assert.assertEquals(results[0][1], cols[1]);
        }

        results = cluster.coordinator(1).execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = -1", ConsistencyLevel.ALL); // null
        long nullSize = (Long) results[0][1];
        Assert.assertTrue(nullSize > absentSize);
        for (Object[] cols : results)
        {
            Assert.assertEquals(results[0][1], cols[1]);
        }

        results = cluster.coordinator(1).execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = 0", ConsistencyLevel.ALL); // zero length
        long emptySize = (Long) results[0][1];
        Assert.assertTrue(emptySize >= nullSize);
        for (Object[] cols : results)
        {
            Assert.assertEquals(results[0][1], cols[1]);
        }

        valSize = 1;
        long previousPartition = emptySize;
        for (int pk = 1; pk < 20; pk++)
        {
            results = cluster.coordinator(1).execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = ?", ConsistencyLevel.ALL, pk);
            long partitionSize = (Long) results[0][1];
            Assert.assertTrue(partitionSize >= valSize);
            Assert.assertTrue(partitionSize >= previousPartition);
            for (Object[] cols : results)
            {
                Assert.assertEquals(results[0][1], cols[1]);
            }

            valSize *= 2;
            previousPartition = partitionSize;
        }
    }

    @Test
    public void timeoutTest()
    {
        try
        {

            Object[][] results;
            results = cluster.coordinator(1).execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = 0", ConsistencyLevel.QUORUM);

            cluster.filters().verbs(Verb.PARTITION_SIZE_REQ.id).to(2).drop();
            cluster.filters().verbs(Verb.PARTITION_SIZE_REQ.id).to(3).drop();
            try
            {
                cluster.coordinator(1).execute("SELECT_SIZE FROM " + KEYSPACE + ".tbl WHERE pk = 0", ConsistencyLevel.QUORUM);
            }
            catch (Throwable tr)
            {
                if (!tr.getClass().getCanonicalName().equals(ReadTimeoutException.class.getCanonicalName()))
                {
                    Assert.fail("Expected ReadTimeoutException, got " + tr);
                }
            }
        }
        finally
        {
            cluster.filters().reset();
        }
    }
}
