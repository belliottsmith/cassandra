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

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.impl.TracingUtil;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

public class CompactStorage2to3UpgradeTest extends UpgradeTestBase
{
    @Test
    public void multiColumn() throws Throwable
    {
        new TestCase()
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            assert cluster.size() == 3;
            int rf = cluster.size() - 1;
            assert rf == 2;
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + (cluster.size() - 1) + "};");
            cluster.schemaChange("CREATE TABLE ks.tbl (pk int, v1 int, v2 text, PRIMARY KEY (pk)) WITH COMPACT STORAGE");
            ICoordinator coordinator = cluster.coordinator(1);
            // these shouldn't be replicated by the 3rd node
            coordinator.execute("INSERT INTO ks.tbl (pk, v1, v2) VALUES (3, 3, '3')", ConsistencyLevel.ALL);
            coordinator.execute("INSERT INTO ks.tbl (pk, v1, v2) VALUES (9, 9, '9')", ConsistencyLevel.ALL);
            for (int i = 0; i < cluster.size(); i++)
            {
                int nodeNum = i + 1;
                System.out.println(String.format("****** node %s: %s", nodeNum, cluster.get(nodeNum).config()));
            }
        })
        .runAfterNodeUpgrade(((cluster, node) -> {
            if (node != 2)
                return;

            Object[][] rows = cluster.coordinator(3).execute("SELECT * FROM ks.tbl LIMIT 2", ConsistencyLevel.ALL);
            Object[][] expected = {
            row(9, 9, "9"),
            row(3, 3, "3")
            };
            assertRows(rows, expected);
        })).run();
    }

    @Test
    public void singleColumn() throws Throwable
    {
        new TestCase()
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            assert cluster.size() == 3;
            int rf = cluster.size() - 1;
            assert rf == 2;
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + (cluster.size() - 1) + "};");
            cluster.schemaChange("CREATE TABLE ks.tbl (pk int, v int, PRIMARY KEY (pk)) WITH COMPACT STORAGE");
            ICoordinator coordinator = cluster.coordinator(1);
            // these shouldn't be replicated by the 3rd node
            coordinator.execute("INSERT INTO ks.tbl (pk, v) VALUES (3, 3)", ConsistencyLevel.ALL);
            coordinator.execute("INSERT INTO ks.tbl (pk, v) VALUES (9, 9)", ConsistencyLevel.ALL);
            for (int i = 0; i < cluster.size(); i++)
            {
                int nodeNum = i + 1;
                System.out.println(String.format("****** node %s: %s", nodeNum, cluster.get(nodeNum).config()));
            }
        })
        .runAfterNodeUpgrade(((cluster, node) -> {

            if (node < 2)
                return;

            Object[][] rows = cluster.coordinator(3).execute("SELECT * FROM ks.tbl LIMIT 2", ConsistencyLevel.ALL);
            Object[][] expected = {
            row(9, 9),
            row(3, 3)
            };
            assertRows(rows, expected);
        })).run();
    }

    @Test
    public void testSSTableTimestampSkipping() throws Throwable
    {
        new TestCase()
        .nodes(1)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
            // Remove compact storage, and both pre and post-upgrade reads will only hit one SSTable.
            cluster.schemaChange("CREATE TABLE ks.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");

            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 1, '1') USING TIMESTAMP 1000000");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 50, '2') USING TIMESTAMP 1000001");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 100, '3') USING TIMESTAMP 1000002");
            cluster.get(1).flush("ks");

            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 2, '4') USING TIMESTAMP 2000000");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 51, '5') USING TIMESTAMP 2000001");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 101, '6') USING TIMESTAMP 2000002");
            cluster.get(1).flush("ks");

            Object[][] expected = { row(1, 51, "5") };
            assertSSTablesRead(cluster, "SELECT * FROM ks.tbl WHERE pk = 1 AND ck = 51", expected, 1L);
        })
        .runAfterNodeUpgrade(((cluster, node) -> {
            IUpgradeableInstance instance = cluster.get(1);
            instance.nodetool("upgradesstables", "ks", "tbl");

            Object[][] expected = { row(1, 51, "5") };
            assertSSTablesRead(cluster, "SELECT * FROM ks.tbl WHERE pk = 1 AND ck = 51", expected, 1L);
        })).run();
    }

    @Test
    public void testSSTableTimestampSkippingPkOnly() throws Throwable
    {
        new TestCase()
        .nodes(1)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
            // Remove compact storage, and both pre and post-upgrade reads will only hit one SSTable.
            cluster.schemaChange("CREATE TABLE ks.tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");

            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck) VALUES (1, 1) USING TIMESTAMP 1000000");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck) VALUES (1, 50) USING TIMESTAMP 1000001");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck) VALUES (1, 100) USING TIMESTAMP 1000002");
            cluster.get(1).flush("ks");

            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck) VALUES (1, 2) USING TIMESTAMP 2000000");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck) VALUES (1, 51) USING TIMESTAMP 2000001");
            cluster.get(1).executeInternal("INSERT INTO ks.tbl (pk, ck) VALUES (1, 101) USING TIMESTAMP 2000002");
            cluster.get(1).flush("ks");

            Object[][] expected = { row(1, 51) };
            assertSSTablesRead(cluster, "SELECT * FROM ks.tbl WHERE pk = 1 AND ck = 51", expected, 1L);
        })
        .runAfterNodeUpgrade(((cluster, node) -> {
            IUpgradeableInstance instance = cluster.get(1);
            instance.nodetool("upgradesstables", "ks", "tbl");

            Object[][] expected = { row(1, 51) };
            assertSSTablesRead(cluster, "SELECT * FROM ks.tbl WHERE pk = 1 AND ck = 51", expected, 1L);
        })).run();
    }

    private void assertSSTablesRead(UpgradeableCluster cluster, String query, Object[][] expected, long ssTablesRead) throws Exception
    {
        String originalTraceTimeout = TracingUtil.setWaitForTracingEventTimeoutSecs("1");

        try
        {
            UUID sessionId = UUIDGen.getTimeUUID();
            Object[][] rows = cluster.coordinator(1)
                                     .asyncExecuteWithTracing(sessionId, query, ConsistencyLevel.ONE).get();
            assertRows(rows, expected);

            List<TracingUtil.TraceEntry> traces = TracingUtil.getTrace(cluster, sessionId, ConsistencyLevel.ONE);
            long sstablesRead = traces.stream().filter(traceEntry -> traceEntry.activity.contains("Merging data from sstable")).count();
            assertEquals(ssTablesRead, sstablesRead);
        }
        finally
        {
            TracingUtil.setWaitForTracingEventTimeoutSecs(originalTraceTimeout);
        }
    }
}