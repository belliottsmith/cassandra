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
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.DiagnosticSnapshotService;

import static junit.framework.TestCase.fail;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

public class RepairDigestTrackingTest extends TestBaseImpl implements Serializable
{

    @Test
    public void testInconsistenciesFound() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k INT, c INT, v INT, PRIMARY KEY (k,c))");
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));

            for (int i = 10; i < 20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl")));
            // Mark everything repaired on node2
            cluster.get(2).runOnInstance(() -> markAllRepaired("tbl"));
            cluster.get(2).runOnInstance(() -> assertRepaired("tbl"));

            // now overwrite on node1 only to generate digest mismatches
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", 5, 55);
            cluster.get(1).runOnInstance(() -> assertNotRepaired("tbl"));

            Object[][] expected = new Object[20][];
            for (int i=0; i<20; i++)
                expected[i] = new Object[] { 0, i, i };
            expected[5] = new Object[] { 0, 5, 55 };

            // Execute a range read and assert inconsistency is detected (as nothing is repaired on node1)
            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL), expected);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should differ by 1 after range read", ccBefore + 1, ccAfter);

            // Re-introduce a mismatch and run again as a single partition read
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", 5, 555);
            expected[5] = new Object[] { 0, 5, 555 };
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k=0", ConsistencyLevel.ALL), expected);
            ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should differ by 1 after partition read", ccBefore + 2, ccAfter);
        }
    }

    @Test
    public void testPurgeableTombstonesAreIgnored() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl2 (k INT, c INT, v1 INT, v2 INT, PRIMARY KEY (k,c)) WITH gc_grace_seconds=0");
            // on node1 only insert some tombstones, then flush
            for (int i = 0; i < 10; i++)
            {
                cluster.get(1).executeInternal("DELETE v1 FROM " + KEYSPACE + ".tbl2 USING TIMESTAMP 0 WHERE k=0 and c=? ", i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));

            // insert data on both nodes and flush
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl2 (k, c, v2) VALUES (0, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));

            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl2")));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(() -> markAllRepaired("tbl2")));
            cluster.forEach(i -> i.runOnInstance(() -> assertRepaired("tbl2")));

            // now overwrite on node2 only to generate digest mismatches, but don't flush so the repaired dataset is not affected
            for (int i = 0; i < 10; i++)
            {
                cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl2 (k, c, v2) VALUES (0, ?, ?) USING TIMESTAMP 2", i, i * 2);
            }

            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl2");
            // Unfortunately we need to sleep here to ensure that nowInSec > the local deletion time of the tombstones
            TimeUnit.SECONDS.sleep(2);
            Object[][] expected = new Object[10][];
            for (int i=0; i<10; i++)
                expected[i] = new Object[] { 0, i, null, i * 2};

            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl2", ConsistencyLevel.ALL), expected);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl2");
            Assert.assertEquals("No repaired data inconsistencies should be detected", ccBefore, ccAfter);
        }
    }

    @Test
    public void testRepairedReadCountNormalizationWithInitialUnderread() throws Throwable
    {
        // Asserts that the amount of repaired data read for digest generation is consistent
        // across replicas where one has to read less repaired data to satisfy the original
        // limits of the read request.
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl3 (k INT, c INT, v1 INT, PRIMARY KEY (k,c)) " +
                                 "WITH CLUSTERING ORDER BY (c DESC)");

            // insert data on both nodes and flush
            for (int i=0; i<20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl3 (k, c, v1) VALUES (0, ?, ?) USING TIMESTAMP 0",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl3 (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl3")));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(() -> markAllRepaired("tbl3")));
            cluster.forEach(i -> i.runOnInstance(() -> assertRepaired("tbl3")));

            // Add some unrepaired data to both nodes
            for (int i=20; i<30; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl3 (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            // And some more unrepaired data to node2 only. This causes node2 to read less repaired data than node1
            // when satisfying the limits of the read. So node2 needs to overread more repaired data than node1 when
            // calculating the repaired data digest.
            cluster.get(2).executeInternal("INSERT INTO "  + KEYSPACE + ".tbl3 (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1", 30, 30);

            // Verify single partition read
            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl3");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl3 WHERE k=1 LIMIT 20", ConsistencyLevel.ALL),
                       rows(1, 30, 11));
            long ccAfterPartitionRead = getConfirmedInconsistencies(cluster.get(1), "tbl3");

            // Recreate a mismatch in unrepaired data and verify partition range read
            cluster.get(2).executeInternal("INSERT INTO "  + KEYSPACE + ".tbl3 (k, c, v1) VALUES (1, ?, ?)", 31, 31);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl3 LIMIT 30", ConsistencyLevel.ALL),
                       rows(1, 31, 2));
            long ccAfterRangeRead = getConfirmedInconsistencies(cluster.get(1), "tbl3");

            if (ccAfterPartitionRead != ccAfterRangeRead)
                if (ccAfterPartitionRead != ccBefore)
                    fail("Both range and partition reads reported data inconsistencies but none were expected");
                else
                    fail("Reported inconsistency during range read but none were expected");
            else if (ccAfterPartitionRead != ccBefore)
                fail("Reported inconsistency during partition read but none were expected");
        }
    }

    @Test
    public void testRepairedReadCountNormalizationWithInitialOverread() throws Throwable
    {
        // Asserts that the amount of repaired data read for digest generation is consistent
        // across replicas where one has to read more repaired data to satisfy the original
        // limits of the read request.
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl4 (k INT, c INT, v1 INT, PRIMARY KEY (k,c)) " +
                                 "WITH CLUSTERING ORDER BY (c DESC)");

            // insert data on both nodes and flush
            for (int i=0; i<10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl4 (k, c, v1) VALUES (0, ?, ?) USING TIMESTAMP 0",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl4 (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl4")));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(() -> markAllRepaired("tbl4")));
            cluster.forEach(i -> i.runOnInstance(() -> assertRepaired("tbl4")));

            // Add some unrepaired data to both nodes
            for (int i=10; i<13; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl4 (k, c, v1) VALUES (0, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl4 (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // And some row deletions on node2 only which cover data in the repaired set
            // This will cause node2 to read more repaired data in satisfying the limit of the read request
            // so it should overread less than node1 (in fact, it should not overread at all) in order to
            // calculate the repaired data digest.
            for (int i=7; i<10; i++)
            {
                cluster.get(2).executeInternal("DELETE FROM " + KEYSPACE + ".tbl4 USING TIMESTAMP 2 WHERE k = 0 AND c = ?", i);
                cluster.get(2).executeInternal("DELETE FROM " + KEYSPACE + ".tbl4 USING TIMESTAMP 2 WHERE k = 1 AND c = ?", i);
            }

            // Verify single partition read
            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl4");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl4 WHERE k=0 LIMIT 5", ConsistencyLevel.ALL),
                       rows(rows(0, 12, 10), rows(0, 6, 5)));
            long ccAfterPartitionRead = getConfirmedInconsistencies(cluster.get(1), "tbl4");

            // Verify partition range read
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl4 LIMIT 11", ConsistencyLevel.ALL),
                       rows(rows(1, 12, 10), rows(1, 6, 0), rows(0, 12, 12)));
            long ccAfterRangeRead = getConfirmedInconsistencies(cluster.get(1), "tbl4");

            if (ccAfterPartitionRead != ccAfterRangeRead)
                if (ccAfterPartitionRead != ccBefore)
                    fail("Both range and partition reads reported data inconsistencies but none were expected");
                else
                    fail("Reported inconsistency during range read but none were expected");
            else if (ccAfterPartitionRead != ccBefore)
                fail("Reported inconsistency during partition read but none were expected");
        }
    }

    @Test
    public void testSnapshottingOnInconsistency() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k INT, c INT, v INT, PRIMARY KEY (k,c))");
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));

            for (int i = 10; i < 20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            cluster.forEach(i -> i.runOnInstance(() -> assertNotRepaired("tbl")));
            // Mark everything repaired on node2
            cluster.get(2).runOnInstance(() -> markAllRepaired("tbl"));
            cluster.get(2).runOnInstance(() -> assertRepaired("tbl"));

            // now overwrite on node1 only to generate digest mismatches
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", 5, 55);
            cluster.get(1).runOnInstance(() -> assertNotRepaired("tbl"));

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", 5, 55);
            // Execute a partition read and assert inconsistency is detected (as nothing is repaired on node1)
            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl");
            cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k=0", ConsistencyLevel.ALL);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should increment by 1 after each partition read", ccBefore + 1, ccAfter);

            String snapshotName = DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX
                                  + DateTimeFormatter.BASIC_ISO_DATE.format(LocalDate.now());

            cluster.get(1).runOnInstance(() -> assertSnapshotNotPresent("tbl", snapshotName));
            cluster.get(2).runOnInstance(() -> assertSnapshotNotPresent("tbl", snapshotName));

            // re-introduce a mismatch, enable snapshotting and try again
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", 5, 555);
            cluster.get(1).runOnInstance(() -> StorageProxy.instance.enableSnapshotOnRepairedDataMismatch());

            cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k=0", ConsistencyLevel.ALL);
            ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should increment by 1 after each partition read", ccBefore + 2, ccAfter);

            cluster.get(1).runOnInstance(() -> assertSnapshotPresent("tbl", snapshotName));
            cluster.get(2).runOnInstance(() -> assertSnapshotPresent("tbl", snapshotName));
        }
    }

    @Test
    public void testDigestExclusions() throws Throwable
    {
        String exclusions = KEYSPACE + ":tbl:666f6f";
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.set("repaired_data_tracking_exclusions", exclusions))
                                           .start()))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.forEach(i -> i.runOnInstance(() -> StorageProxy.instance.disableRepairedDataTrackingExclusions()));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k INT, c TEXT, v INT, PRIMARY KEY (k,c))");
            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < 10; j++)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (?, ?, ?)",
                                                   ConsistencyLevel.ALL, i, j % 2 == 0 ? "foo" + j : "bar" + j, j);
                }
            }
            // overwrite on node1 only to generate digest mismatches then flush and mark everything repaired
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", "foo6", 66);
            cluster.forEach(c -> c.flush(KEYSPACE));
            cluster.forEach(i -> i.runOnInstance(() -> markAllRepaired("tbl")));
            cluster.forEach(i -> i.runOnInstance(() -> assertRepaired("tbl")));

            // Execute a partition read and assert inconsistency is detected
            long ccBefore = getConfirmedInconsistencies(cluster.get(1), "tbl");
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE k=0", ConsistencyLevel.ALL);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should increment by 1 after each partition read", ccBefore + 1, ccAfter);

            // re-introduce a mismatch and execute a range read then assert inconsistency is detected
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", "foo6", 666);
            cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should increment by 1 after each partition read", ccBefore + 2, ccAfter);

            // re-introduce a mismatch and try again after enabling exclusions
            cluster.forEach(i -> i.runOnInstance(() -> StorageProxy.instance.enableRepairedDataTrackingExclusions()));
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", "foo6", 6666);
            cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k=0", ConsistencyLevel.ALL);
            ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should not increase when exlcusions are enabled", ccBefore + 2, ccAfter);

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (0, ?, ?)", "foo6", 66666);
            cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            ccAfter = getConfirmedInconsistencies(cluster.get(1), "tbl");
            Assert.assertEquals("confirmed count should not increase when exlcusions are enabled", ccBefore + 2, ccAfter);
        }
    }

    // tl;dr if responses from remote replicas come back while the local runnable is still executing
    // the fact that ReadCommands are mutable means that the trackRepairedStatus flag on the read command instance
    // can go from false to true in ReadCommand.executeLocally, between setting the RepairedDataInfo/gathering the
    // sstables and calling RDI.extend. If this happens, the RDI is still the stand-in object
    // RDI.NULL_REPAIRED_DATA_INFO which has a null repairedDataCounter and we hit the NPE.

    // There are a number of hoops to jump through to repro this, probably not exactly what's happening
    // in real clusters, but at least this proves the theory.

    // The exclusions are required so that we don't NPE earlier in the read. Without them,
    // the RDI.repairedCounter is null in RDI.withRepairedDataInfo, and the NPE happens there instead.
    // To trigger the NPE in RDI.extend, we some repaired sstables to be present in the InputCollector
    // (otherwise we'll short circuit out of InputCollector.finalizeIterators so repairedDataInfo.metrics is never
    // set, which prevents the NPE in RDI.extend). If there are actual partitions in those sstables though, we'll
    // hit the NPE in withRepairedDataInfo. There's probably some other way to repro these conditions, but I
    // couldn't think of it atm so this was the easy way to do it.

    // There are 2 variations tested here; where the local read is either a data or digest read.

    @Test
    public void testLocalDataAndRemoteRequestConcurrency() throws Exception
    {
        try(Cluster cluster = init(Cluster.build(3)
                                          .withInstanceInitializer(BBHelper::installQueryOnly)
                                          .withConfig(config -> config.set("repaired_data_tracking_for_partition_reads_enabled", true)
                                                                      .set("repaired_data_tracking_exclusions_enabled", true)
                                                                      .set("repaired_data_tracking_exclusions", KEYSPACE)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .start()))
        {
            queryLocalAndRemote(cluster);
        }
    }

    @Test
    public void testLocalDigestAndRemoteRequestConcurrency() throws Exception
    {
        try(Cluster cluster = init(Cluster.build(3)
                                          .withInstanceInitializer(BBHelper::installBoth)
                                          .withConfig(config -> config.set("repaired_data_tracking_for_partition_reads_enabled", true)
                                                                      .set("repaired_data_tracking_exclusions_enabled", true)
                                                                      .set("repaired_data_tracking_exclusions", KEYSPACE)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .start()))
        {
            queryLocalAndRemote(cluster);
        }
    }

    private void queryLocalAndRemote(Cluster cluster) throws InterruptedException
    {
        cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int) WITH speculative_retry = 'ALWAYS'");

        cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (id, t) values (0, 0)");
        cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (id, t) values (0, 0)");
        cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (id, t) values (0, 1)");
        cluster.forEach(c -> c.flush(KEYSPACE));
        cluster.forEach(i -> i.runOnInstance(() -> markAllRepaired("tbl")));
        cluster.forEach(i -> i.runOnInstance(() -> assertRepaired("tbl")));

        cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE id=0", ConsistencyLevel.QUORUM);

        // TODO figure out why the instance log still appears to be async
        TimeUnit.SECONDS.sleep(1);
        List<String> result = cluster.get(1).logs().grep(".*NullPointerException.*").getResult();
        Assert.assertTrue("Did not expect to encounter any NPE", result.isEmpty());
    }

    public static class BBHelper
    {
        public static void installQueryOnly(ClassLoader classLoader, Integer num)
        {
            if (num == 1)
            {
                new ByteBuddy().rebase(SinglePartitionReadCommand.class)
                               .method(named("queryStorage"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void installBoth(ClassLoader classLoader, Integer num)
        {
            if (num == 1)
            {
                new ByteBuddy().rebase(SinglePartitionReadCommand.class)
                               .method(named("queryStorage"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().rebase(StorageProxy.class)
                               .method(named("getLiveSortedEndpoints").and(takesArguments(Keyspace.class, RingPosition.class)))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }


        public static UnfilteredPartitionIterator queryStorage(ColumnFamilyStore cfs,
                                                               ReadOrderGroup orderGroup,
                                                               @SuperCall Callable<UnfilteredPartitionIterator> zuperCall)
        {
            try
            {
                if (cfs.metadata.cfName.equals("tbl"))
                {
                    // using sleep here rather than a more deterministic coordination mechanism as
                    // the interactions are across multiple nodes. We want node1 (which has this
                    // helper installed) to execute its local data read only after it's received
                    // responses from the other nodes and initiated a full data read, with repaired
                    // data tracking
                    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
                }
                return zuperCall.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException("ERROR", e);
            }
        }


        private static List<InetAddress> staticList()
        {
            List<InetAddress> list = new ArrayList<>();
            for (int i=3; i > 0; i--)
            {
                try
                {
                    list.add(InetAddress.getByName("127.0.0." + i));
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return list;
        }

        public static List<InetAddress> getLiveSortedEndpoints(Keyspace keyspace,
                                                               RingPosition pos,
                                                               @SuperCall Callable<List<InetAddress>> zuperCall)
        {
            // This is necessary so that the local node sorts last, making it perform a digest request
            // This ensures that node3 get the data request and 2 & 1 the digest requests
            if(keyspace.getName().equals(KEYSPACE))
                return staticList();

            try
            {
                return zuperCall.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private Object[][] rows(Object[][] head, Object[][]...tail)
    {
        return Stream.concat(Stream.of(head),
                             Stream.of(tail).flatMap(Stream::of))
                     .toArray(Object[][]::new);
    }

    private Object[][] rows(int partitionKey, int start, int end)
    {
        if (start == end)
            return new Object[][] { new Object[] { partitionKey, start, end } };

        IntStream clusterings = start > end
                              ? IntStream.range(end -1, start).map(i -> start - i + end - 1)
                              : IntStream.range(start, end);

        return clusterings.mapToObj(i -> new Object[] {partitionKey, i, i}).toArray(Object[][]::new);
    }

    private void assertNotRepaired(String table)
    {
        Keyspace.open(KEYSPACE)
                .getColumnFamilyStore(table)
                .getLiveSSTables()
                .forEach(reader -> Assert.assertEquals("repaired at is set for sstable: " + reader.descriptor,
                                                       getRepairedAt(reader),
                                                       ActiveRepairService.UNREPAIRED_SSTABLE));
    }

    public static void assertRepaired(String table)
    {
        Keyspace.open(KEYSPACE)
                .getColumnFamilyStore(table)
                .getLiveSSTables()
                .forEach(reader -> Assert.assertTrue("repaired at is not set for sstable: " + reader.descriptor, getRepairedAt(reader) > 0));
    }

    public static void markAllRepaired(String table)
    {
        Keyspace.open(KEYSPACE)
                .getColumnFamilyStore(table)
                .getLiveSSTables()
                .forEach(RepairDigestTrackingTest::markRepaired);
    }

    private void assertSnapshotPresent(String table, String snapshotName)
    {
        // snapshots are taken asynchronously, this is crude but it gives it a chance to happen
        int attempts = 10;
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        while (attempts-- > 0 && !cfs.snapshotExists(snapshotName))
        {
            try
            {
                TimeUnit.SECONDS.sleep(1);
            }
            catch (InterruptedException e)
            {
            }
        }
        if (!cfs.snapshotExists(snapshotName))
            throw new AssertionError(String.format("Snapshot %s not found for for %s.%s", snapshotName, KEYSPACE, table));
    }

    private void assertSnapshotNotPresent(String table, String snapshotName)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        Assert.assertFalse(cfs.snapshotExists(snapshotName));
    }

    public static long getRepairedAt(SSTableReader reader)
    {
        Descriptor descriptor = reader.descriptor;
        try
        {
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer()
                                                                      .deserialize(descriptor, EnumSet.of(MetadataType.STATS));

            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            return stats.repairedAt;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void markRepaired(SSTableReader reader) {
        Descriptor descriptor = reader.descriptor;
        try
        {
            descriptor.getMetadataSerializer().mutateRepaired(descriptor, System.currentTimeMillis(), null);
            reader.reloadSSTableMetadata();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private long getConfirmedInconsistencies(IInvokableInstance instance, String table)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(table)
                                                     .metric
                                                     .confirmedRepairedInconsistencies
                                                     .table
                                                     .getCount());
    }

}
