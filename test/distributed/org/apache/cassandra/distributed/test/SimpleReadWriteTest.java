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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Joiner;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.distributed.api.*;
import org.apache.cassandra.net.MessagingService;

import org.junit.*;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.CoalescingStrategies;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.MessagingService.Verb.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class SimpleReadWriteTest extends TestBaseImpl
{
    static
    {
        System.setProperty(CoalescingStrategies.NO_ARTIFICIAL_LATENCY_LIMIT_PROPERTY, "true");
    }

    protected static Cluster withNetwork;
    protected static Cluster withoutNetwork;
    static String KEYSPACE;
    static int counter = 0;

    @BeforeClass
    public static void before() throws IOException
    {
        Consumer<IInstanceConfig> cfg = config -> config.with(Feature.NETWORK).set("otc_coalescing_strategy", "ARTIFICIAL_LATENCY");
        withNetwork = Cluster.build().withNodes(3).withConfig(cfg).start();
        withoutNetwork = Cluster.build().withNodes(3).start();
    }

    static <C extends Cluster> C init(C cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
        return cluster;
    }

    @AfterClass
    public static void after()
    {
        withNetwork.close();
    }

    @Before
    public void beforeEach() throws InterruptedException
    {
        KEYSPACE = "distributed_test_keyspace_" + counter++;
        init(withNetwork);
        init(withoutNetwork);
        Thread.sleep(10);
    }

    @After
    public void afterEach()
    {
        withNetwork.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        withoutNetwork.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    private static void setPaxosVariant(Config.PaxosVariant variant)
    {
        withNetwork.forEach(i -> i.runOnInstance(() -> {
            StorageProxy.instance.setPaxosVariant(variant.toString());
        }));
    }

    private static void setupArtificialLatency(int millis, boolean onlyPermittedCLs, MessagingService.Verb... verbs)
    {
        Arrays.sort(verbs, Comparator.comparingInt(Enum::ordinal));
        String verbString = Joiner.on(',').join(verbs);
        withNetwork.forEach(i -> i.runOnInstance(() -> {
            StorageProxy.instance.setArtificialLatencyMillis(millis);
            StorageProxy.instance.setArtificialLatencyOnlyPermittedConsistencyLevels(onlyPermittedCLs);
            StorageProxy.instance.setArtificialLatencyVerbs(verbString);
        }));
    }

    @Before
    public void setup()
    {
        setupArtificialLatency(0, true);
        withNetwork.forEach(i -> i.runOnInstance(() -> {
            StorageProxy.instance.setArtificialLatencyMillis(0);
            StorageProxy.instance.setArtificialLatencyOnlyPermittedConsistencyLevels(true);
            StorageProxy.instance.setArtificialLatencyVerbs("");
        }));
    }

    @Test
    public void coordinatorReadTest()
    {
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        withNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
        withNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
        withNetwork.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");

        assertRows(withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                  ConsistencyLevel.ALL,
                                                  1),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3));
    }

    @Test
    public void largeMessageTest()
    {
        int largeMessageThreshold = 1024 * 64;
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < largeMessageThreshold; i++)
            builder.append('a');
        String s = builder.toString();
        withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
                                       ConsistencyLevel.ALL,
                                       s);
        assertRows(withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                  ConsistencyLevel.ALL,
                                                  1),
                   row(1, 1, s));
    }

    @Test
    public void coordinatorWriteTest()
    {
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                       ConsistencyLevel.ALL);

        for (int i = 0; i < 3; i++)
        {
            assertRows(withNetwork.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }

        assertRows(withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.QUORUM),
                   row(1, 1, 1));
    }

    private void coordinatorDelaySerialWriteTest(Config.PaxosVariant variant) throws Throwable
    {
        setPaxosVariant(variant);
        setupArtificialLatency(100, false, APPLE_PAXOS_PREPARE_REQ, APPLE_PAXOS_PROPOSE_REQ, APPLE_PAXOS_PREPARE_REFRESH_REQ, APPLE_PAXOS_COMMIT_AND_PREPARE_REQ, PAXOS_PREPARE, PAXOS_PROPOSE, PAXOS_COMMIT, REQUEST_RESPONSE);
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        long start = System.nanoTime();
        withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS",
                                       ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
        long end = System.nanoTime();

        assertTrue(variant.name() + ' ' + TimeUnit.NANOSECONDS.toMillis(end - start), end - start > TimeUnit.MILLISECONDS.toNanos(variant == Config.PaxosVariant.legacy ? 600 : 400));

        start = System.nanoTime();
        assertRows(withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.SERIAL),
                   row(1, 1, 1));
        end = System.nanoTime();
        assertTrue(variant.name() + ' ' + TimeUnit.NANOSECONDS.toMillis(end - start), end - start > TimeUnit.MILLISECONDS.toNanos(variant == Config.PaxosVariant.legacy ? 200 : 100));
    }

    @Test
    public void legacyPaxosCoordinatorDelaySerialWriteTest() throws Throwable
    {
        coordinatorDelaySerialWriteTest(Config.PaxosVariant.legacy);
    }

    @Test
    public void applePaxosCoordinatorDelaySerialWriteTest() throws Throwable
    {
        coordinatorDelaySerialWriteTest(Config.PaxosVariant.apple_rrl);
    }

    private void coordinatorDelayUnsafeXSerialWriteTest(Config.PaxosVariant variant) throws Throwable
    {
        setPaxosVariant(variant);
        setupArtificialLatency(100, true, APPLE_PAXOS_PREPARE_REQ, APPLE_PAXOS_PROPOSE_REQ, APPLE_PAXOS_PREPARE_REFRESH_REQ, APPLE_PAXOS_COMMIT_AND_PREPARE_REQ, PAXOS_PREPARE, PAXOS_PROPOSE, PAXOS_COMMIT, REQUEST_RESPONSE);
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        long start = System.nanoTime();
        withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS",
                                       ConsistencyLevel.UNSAFE_DELAY_SERIAL, ConsistencyLevel.UNSAFE_DELAY_QUORUM);
        long end = System.nanoTime();

        assertTrue(variant.name() + " " + TimeUnit.NANOSECONDS.toMillis(end - start), end - start > TimeUnit.MILLISECONDS.toNanos(variant == Config.PaxosVariant.legacy ? 600 : 400));

        start = System.nanoTime();
        assertRows(withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.UNSAFE_DELAY_SERIAL),
                   row(1, 1, 1));
        end = System.nanoTime();
        assertTrue(variant.name() + " " + TimeUnit.NANOSECONDS.toMillis(end - start), end - start > TimeUnit.MILLISECONDS.toNanos(variant == Config.PaxosVariant.legacy ? 200 : 100));
    }

    @Test
    public void legacyPaxosCoordinatorDelayUnsafeXSerialWriteTest() throws Throwable
    {
        coordinatorDelayUnsafeXSerialWriteTest(Config.PaxosVariant.legacy);
    }

    @Test
    public void applePaxosCoordinatorDelayUnsafeXSerialWriteTest() throws Throwable
    {
        coordinatorDelayUnsafeXSerialWriteTest(Config.PaxosVariant.apple_rrl);
    }
//
    @Test
    public void coordinatorDelayQuorumReadWriteTest() throws Throwable
    {
        setupArtificialLatency(100, true, PAXOS_PREPARE, PAXOS_PROPOSE, PAXOS_COMMIT, MUTATION, READ, READ_REPAIR, REQUEST_RESPONSE);
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        long start = System.nanoTime();
        withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                       ConsistencyLevel.UNSAFE_DELAY_QUORUM);
        long end = System.nanoTime();

        assertTrue(end - start > TimeUnit.MILLISECONDS.toNanos(200));

        start = System.nanoTime();
        assertRows(withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.UNSAFE_DELAY_LOCAL_QUORUM),
                   row(1, 1, 1));
        end = System.nanoTime();
        assertTrue(end - start > TimeUnit.MILLISECONDS.toNanos(200));
    }

    @Test
    public void coordinatorNoDelayQuorumReadWriteTest() throws Throwable
    {
        setupArtificialLatency(1000, true, PAXOS_PREPARE, PAXOS_PROPOSE, PAXOS_COMMIT, MUTATION, READ, READ_REPAIR, REQUEST_RESPONSE);
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        long start = System.nanoTime();
        withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                       ConsistencyLevel.QUORUM);
        long end = System.nanoTime();

        assertTrue(end - start < TimeUnit.MILLISECONDS.toNanos(1000));

        start = System.nanoTime();
        assertRows(withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.QUORUM),
                   row(1, 1, 1));
        end = System.nanoTime();
        assertTrue(end - start < TimeUnit.MILLISECONDS.toNanos(1000));
    }

    @Test
    public void readRepairTest()
    {
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        withNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
        withNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

        assertRows(withNetwork.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

        assertRows(withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.ALL), // ensure node3 in preflist
                   row(1, 1, 1));

        // Verify that data got repaired to the third node
        assertRows(withNetwork.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                   row(1, 1, 1));
    }

    /**
     * If a node receives a mutation for a column it's not aware of, it should fail, since it can't write the data.
     */
    @Test
    public void writeWithSchemaDisagreement() throws Throwable
    {
        withoutNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

        withoutNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        withoutNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        withoutNetwork.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

        // Introduce schema disagreement
        withoutNetwork.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

        Exception thrown = null;
        try
        {
            withoutNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)",
                                                  ConsistencyLevel.ALL);
        }
        catch (RuntimeException e)
        {
            thrown = e;
        }

        Assert.assertTrue(thrown.getMessage().contains("Exception occurred on node"));
    }

    /**
     * If a node receives a mutation for a column it knows has been dropped, the write should succeed
     */
    @Test
    public void writeWithSchemaDisagreement2() throws Throwable
    {
        withoutNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        withoutNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)");
        withoutNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)");
        withoutNetwork.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)");

        for (int i=0; i<withoutNetwork.size(); i++)
            withoutNetwork.get(i+1).flush(KEYSPACE);;

        // Introduce schema disagreement
        withoutNetwork.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl DROP v2", 1);

        // execute a write including the dropped column where the coordinator is not yet aware of the drop
        // all nodes should process this without error
        withoutNetwork.coordinator(2).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)",
                                           ConsistencyLevel.ALL);
        // and flushing should also be fine
        for (int i=0; i<withoutNetwork.size(); i++)
            withoutNetwork.get(i+1).flush(KEYSPACE);;
        // the results of reads will vary depending on whether the coordinator has seen the schema change
        // note: read repairs will propagate the v2 value to node1, but this is safe and handled correctly
        assertRows(withoutNetwork.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL),
                   rows(row(1,1,1,1), row(2,2,2,2)));
        assertRows(withoutNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL),
                   rows(row(1,1,1), row(2,2,2)));
    }

    /**
     * If a node isn't aware of a column, but receives a mutation without that column, the write should succeed
     */
    @Test
    public void writeWithInconsequentialSchemaDisagreement() throws Throwable
    {
        withoutNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

        withoutNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        withoutNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        withoutNetwork.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

        // Introduce schema disagreement
        withoutNetwork.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

        Exception thrown = null;
        try
        {
            assertRows(withoutNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                    ConsistencyLevel.ALL),
                    row(1, 1, 1, null));
        }
        catch (Exception e)
        {
            thrown = e;
        }

        // this write shouldn't cause any problems because it doesn't write to the new column
        withoutNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (2, 2, 2)",
                                       ConsistencyLevel.ALL);
    }

    /**
     * If a node receives a read for a column it's not aware of, it shouldn't complain, since it won't have any data for that column
     */
    @Test
    public void readWithSchemaDisagreement() throws Throwable
    {
        withoutNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

        withoutNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        withoutNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        withoutNetwork.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

        // Introduce schema disagreement
        withoutNetwork.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

        Object[][] expected = new Object[][]{new Object[]{1, 1, 1, null}};
        assertRows(withoutNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL), expected);
    }

    @Test
    public void simplePagedReadsTest()
    {
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        int size = 100;
        Object[][] results = new Object[size][];
        for (int i = 0; i < size; i++)
        {
            withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                           ConsistencyLevel.QUORUM,
                                           i, i);
            results[i] = new Object[]{ 1, i, i };
        }

        // Make sure paged read returns same results with different page sizes
        for (int pageSize : new int[]{ 1, 2, 3, 5, 10, 20, 50 })
        {
            assertRows(withNetwork.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl",
                                                                ConsistencyLevel.QUORUM,
                                                                pageSize),
                       results);
        }
    }

    @Test
    public void pagingWithRepairTest()
    {
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        int size = 100;
        Object[][] results = new Object[size][];
        for (int i = 0; i < size; i++)
        {
            // Make sure that data lands on different nodes and not coordinator
            withNetwork.get(i % 2 == 0 ? 2 : 3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                            i, i);

            results[i] = new Object[]{ 1, i, i };
        }

        // Make sure paged read returns same results with different page sizes
        for (int pageSize : new int[]{ 1, 2, 3, 5, 10, 20, 50 })
        {
            assertRows(withNetwork.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl",
                                                                ConsistencyLevel.ALL,
                                                                pageSize),
                       results);
        }

        assertRows(withNetwork.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl"),
                   results);
    }

    @Test
    public void pagingTests() throws Throwable
    {
        try (ICluster singleNode = init(builder().withNodes(1).withSubnet(1).start()))
        {
            withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            singleNode.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < 10; j++)
                {
                    withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                   ConsistencyLevel.QUORUM,
                                                   i, j, i + i);
                    singleNode.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                      ConsistencyLevel.QUORUM,
                                                      i, j, i + i);
                }
            }

            int[] pageSizes = new int[]{ 1, 2, 3, 5, 10, 20, 50 };
            String[] statements = new String[]{ "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck >= 5",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 LIMIT 3",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck >= 5 LIMIT 2",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 LIMIT 2",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC LIMIT 3",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC LIMIT 2",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC LIMIT 2",
                                                "SELECT DISTINCT pk FROM " + KEYSPACE + ".tbl LIMIT 3",
                                                "SELECT DISTINCT pk FROM " + KEYSPACE + ".tbl WHERE pk IN (3,5,8,10)",
                                                "SELECT DISTINCT pk FROM " + KEYSPACE + ".tbl WHERE pk IN (3,5,8,10) LIMIT 2"
            };
            for (String statement : statements)
            {
                for (int pageSize : pageSizes)
                {
                    assertRows(withNetwork.coordinator(1)
                                      .executeWithPaging(statement,
                                                         ConsistencyLevel.QUORUM, pageSize),
                               singleNode.coordinator(1)
                                         .executeWithPaging(statement,
                                                            ConsistencyLevel.QUORUM, Integer.MAX_VALUE));
                }
            }
        }
    }

    @Test
    public void metricsCountQueriesTest()
    {
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = 0; i < 100; i++)
            withNetwork.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?,?,?)", ConsistencyLevel.ALL, i, i, i);

        long readCount1 = readCount(withNetwork.get(1));
        long readCount2 = readCount(withNetwork.get(2));
        for (int i = 0; i < 100; i++)
            withNetwork.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ? and ck = ?", ConsistencyLevel.ALL, i, i);

        readCount1 = readCount(withNetwork.get(1)) - readCount1;
        readCount2 = readCount(withNetwork.get(2)) - readCount2;
        assertEquals(readCount1, readCount2);
        assertEquals(100, readCount1);
    }


    @Test
    public void skippedSSTableWithPartitionDeletionTest() throws Throwable
    {
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
        // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
        withNetwork.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
        // and a row from a different partition, to provide the sstable's min/max clustering
        withNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 2");
        withNetwork.get(1).flush(KEYSPACE);
        // expect a single sstable, where minTimestamp equals the timestamp of the partition delete
        withNetwork.get(1).acceptsOnInstance((String keyspace) -> {
            Set<SSTableReader> sstables = Keyspace.open(keyspace)
                    .getColumnFamilyStore("tbl")
                    .getLiveSSTables();
            assertEquals(1, sstables.size());
            assertEquals(1, sstables.iterator().next().getMinTimestamp());
        }).accept(KEYSPACE);

        // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
        withNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");


        Object[][] rows = withNetwork.coordinator(1)
                .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                        ConsistencyLevel.ALL);
        assertEquals(0, rows.length);
    }

    @Test
    public void skippedSSTableWithPartitionDeletionShadowingDataOnAnotherNode() throws Throwable
    {
        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
        // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
        withNetwork.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
        // and a row from a different partition, to provide the sstable's min/max clustering
        withNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 1");
        withNetwork.get(1).flush(KEYSPACE);
        // sstable 1 has minTimestamp == maxTimestamp == 1 and is skipped due to its min/max clusterings. Now we
        // insert a row which is not shadowed by the partition delete and flush to a second sstable. Importantly,
        // this sstable's minTimestamp is > than the maxTimestamp of the first sstable. This would cause the first
        // sstable not to be reincluded in the merge input, but we can't really make that decision as we don't
        // know what data and/or tombstones are present on other nodes
        withNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 2");
        withNetwork.get(1).flush(KEYSPACE);

        // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
        withNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");

        Object[][] rows = withNetwork.coordinator(1)
                .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                        ConsistencyLevel.ALL);
        // we expect that the row from node 2 (0, 10, 10) was shadowed by the partition delete, but the row from
        // node 1 (0, 6, 6) was not.
        assertRows(rows, new Object[] {0, 6 ,6});
    }

    @Test
    public void skippedSSTableWithPartitionDeletionShadowingDataOnAnotherNode2() throws Throwable
    {
        // don't not add skipped sstables back just because the partition delete ts is < the local min ts

        withNetwork.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
        // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
        withNetwork.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
        // and a row from a different partition, to provide the sstable's min/max clustering
        withNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 3");
        withNetwork.get(1).flush(KEYSPACE);
        // sstable 1 has minTimestamp == maxTimestamp == 1 and is skipped due to its min/max clusterings. Now we
        // insert a row which is not shadowed by the partition delete and flush to a second sstable. The first sstable
        // has a maxTimestamp > than the min timestamp of all sstables, so it is a candidate for reinclusion to the
        // merge. Hoever, the second sstable's minTimestamp is > than the partition delete. This would  cause the
        // first sstable not to be reincluded in the merge input, but we can't really make that decision as we don't
        // know what data and/or tombstones are present on other nodes
        withNetwork.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 2");
        withNetwork.get(1).flush(KEYSPACE);

        // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
        withNetwork.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");

        Object[][] rows = withNetwork.coordinator(1)
                .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                        ConsistencyLevel.ALL);
        // we expect that the row from node 2 (0, 10, 10) was shadowed by the partition delete, but the row from
        // node 1 (0, 6, 6) was not.
        assertRows(rows, new Object[] {0, 6 ,6});
    }

    private long readCount(IInvokableInstance instance)
    {
        return instance.appliesOnInstance((String keyspace) -> Keyspace.open(keyspace).getColumnFamilyStore("tbl").metric.readLatency.latency.getCount()).apply(KEYSPACE);
    }

    private static Object[][] rows(Object[]...rows)
    {
        Object[][] r = new Object[rows.length][];
        System.arraycopy(rows, 0, r, 0, rows.length);
        return r;
    }

}
