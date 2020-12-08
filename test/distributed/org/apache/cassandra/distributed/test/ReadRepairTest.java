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

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.MessagingService.Verb.READ;
import static org.apache.cassandra.net.MessagingService.Verb.READ_REPAIR;

public class ReadRepairTest extends TestBaseImpl
{
    @Test
    public void readRepairTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            // pk=1 token maps to instance 1, and next in the list is 2; so 3 won't be hit
            // in order to make sure 3 gets read-repaired, need to use it as the coordinator
            assertRows(cluster.coordinator(3).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                      ConsistencyLevel.QUORUM),
                       row(1, 1, 1));

            // Verify that data got repaired to the third node
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }
    }

    @Test
    public void failingReadRepairTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            for (int i = 1 ; i <= 2 ; ++i)
                cluster.get(i).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            cluster.verbs(READ_REPAIR).to(3).drop();
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                      ConsistencyLevel.QUORUM),
                       row(1, 1, 1));

            // Data was not repaired
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));
        }
    }

    @Ignore("This test fails, but also only passed on trunk since the filters were not filtering.  We need to fix either the logic or the test, but defer that to https://issues.apache.org/jira/browse/CASSANDRA-16049")
    @Test
    public void movingTokenReadRepairTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(4), 3))
        {
            List<Token> tokens = cluster.tokens();

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            int i = 0;
            while (true)
            {
                Token t = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(i));
                if (t.compareTo(tokens.get(2 - 1)) < 0 && t.compareTo(tokens.get(1 - 1)) > 0)
                    break;
                ++i;
            }

            // write only to #4
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 2, 2)", i);
            cluster.get(4).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 4, 4)", i);
            // mark #2 as leaving in #4
            cluster.forEach(instance -> instance.acceptsOnInstance((InetSocketAddress endpoint) -> {
                StorageService.instance.getTokenMetadata().addLeavingEndpoint(endpoint.getAddress());
                PendingRangeCalculatorService.instance.update();
                PendingRangeCalculatorService.instance.blockUntilFinished();
            }).accept(cluster.get(2).broadcastAddress()));

            // prevent #4 from reading or writing to #3, so our QUORUM must contain #2 and #4
            // since #1 is taking over the range, this means any read-repair must make it to #1 as well
            cluster.filters().verbs(READ.ordinal()).from(4).to(3).drop();
            cluster.filters().verbs(READ_REPAIR.ordinal()).from(4).to(3).drop();
            assertRows(cluster.coordinator(4).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                      ConsistencyLevel.QUORUM, i),
                       row(i, 2, 2),
                       row(i, 4, 4));

            // verify that all nodes except 3 (which we drop messages to) are repaired
            for (int n : new int[] { 1, 2, 4 })
            {
                assertRows(cluster.get(n).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", i),
                           row(i, 2, 2),
                           row(i, 4, 4));
            }
        }
    }

    @Test
    public void emptyRangeTombstones1() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC)");
            cluster.get(1).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
            cluster.coordinator(2).execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 > ? and column1 <= ?",
                                                 ConsistencyLevel.ALL,
                                                 "test", 10, 10);
            cluster.coordinator(2).execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 > ? and column1 <= ?",
                                                 ConsistencyLevel.ALL,
                                                 "test", 11, 11);
            cluster.get(2).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
    }

    @Test
    public void emptyRangeTombstonesFromPaging() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC)");

            cluster.get(1).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 10 WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);

            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO distributed_test_keyspace.tbl (key, column1) VALUES (?, ?) USING TIMESTAMP 30", ConsistencyLevel.ALL, "test", i);

            consume(cluster.coordinator(2).executeWithPaging("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 >= ? and column1 <= ?",
                                           ConsistencyLevel.ALL, 1,
                                           "test", 8, 12));

            consume(cluster.coordinator(2).executeWithPaging("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 >= ? and column1 <= ?",
                                                             ConsistencyLevel.ALL, 1,
                                                             "test", 16, 20));
            cluster.get(2).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
    }

    @Test
    public void speculativeRetryMergeRTErrorTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC) " +
                                 " AND speculative_retry='ALWAYS'");

            cluster.forEach(i -> i.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").disableAutoCompaction()));

            for (int i = 1; i <= 2; i++)
            {
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 1598413424397000 WHERE key=?;", "test");
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 1598414684094000 WHERE key=? and column1 >= ? and column1 < ?;", "test", 10, 100);
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 1598414676775001 WHERE key=? and column1 = ?;", "test", 30);
                cluster.get(i).flush(KEYSPACE);
            }
            cluster.get(3).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 1598415280715000 WHERE key=?;", "test");
            cluster.get(3).flush(KEYSPACE);
            cluster.coordinator(3).execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key=? and column1 >= ? and column1 <= ?", ConsistencyLevel.QUORUM, "test", 20, 40);
        }
    }

    private void consume(Iterator<Object[]> it)
    {
        while (it.hasNext())
            it.next();
    }
}
