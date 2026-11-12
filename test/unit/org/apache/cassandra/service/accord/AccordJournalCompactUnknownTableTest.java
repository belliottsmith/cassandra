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

package org.apache.cassandra.service.accord;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Journal;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.journal.AccordJournal;
import org.apache.cassandra.service.accord.journal.CommandChanges;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

/**
 * Journal segment compaction must succeed, and keep the records of live transactions, when a record
 * references a dropped table (including a multi-table record whose first unknown table id must be retained).
 */
public class AccordJournalCompactUnknownTableTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        CassandraRelevantProperties.JUNIT_STORAGE_COMPATIBILITY_MODE.setEnum(StorageCompatibilityMode.NONE);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE live (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"),
                                    parse("CREATE TABLE gone1 (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"),
                                    parse("CREATE TABLE gone2 (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        StorageService.instance.initServer();
    }

    private static Command preaccepted(TxnId txnId, Txn txn)
    {
        Ranges ranges = Ranges.EMPTY;
        for (int i = 0; i < txn.keys().size(); i++)
        {
            PartitionKey key = (PartitionKey) txn.keys().get(i);
            ranges = ranges.with(Ranges.of(TokenRange.fullRange(key.table(), getPartitioner())));
        }
        PartialTxn partial = txn.slice(ranges, true);
        RoutingKey homeKey = txn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = txn.keys().toRoute(homeKey);
        return Command.PreAccepted.preaccepted(txnId, SaveStatus.PreAccepted, Status.Durability.NotDurable,
                                               StoreParticipants.all(route), Ballot.ZERO, txnId, partial, null);
    }

    private static Txn liveTxn()
    {
        return AccordTestUtils.createTxn("BEGIN TRANSACTION\n" +
                                         "  LET row1 = (SELECT * FROM ks.live WHERE k=2 AND c=0);\n" +
                                         "  SELECT row1.v;\n" +
                                         "  IF row1 IS NULL THEN\n" +
                                         "    INSERT INTO ks.live (k, c, v) VALUES (2, 0, 1);\n" +
                                         "  END IF\n" +
                                         "COMMIT TRANSACTION");
    }

    private static AccordJournal newJournal(String name)
    {
        File dir = new File(DatabaseDescriptor.getAccordJournalDirectory(), name);
        dir.createDirectoriesIfNotExists();
        AccordJournal journal = new AccordJournal(TestParams.INSTANCE, dir,
                                                  Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL));
        journal.start(null);
        return journal;
    }

    private static void runCompactionBounded(AccordJournal journal) throws Throwable
    {
        // bound the compaction so a hang fails within seconds
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> f = exec.submit(journal::runCompactorForTesting);
            f.get(30, TimeUnit.SECONDS);
        }
        catch (ExecutionException e)
        {
            throw e.getCause();
        }
        finally
        {
            exec.shutdownNow();
        }
    }

    /**
     * T0 (sorting before live T1) references a table that is
     * then dropped; compaction runs; T1's record must survive.
     */
    private static void dropThenCompact(String name, int storeId, String droppedTable, Txn txn0) throws Throwable
    {
        AccordJournal journal = newJournal(name);
        TxnId t0 = AccordTestUtils.txnId(1, 10, 1);
        TxnId t1 = AccordTestUtils.txnId(1, 20, 1);

        journal.saveCommand(storeId, new Journal.CommandUpdate(null, preaccepted(t0, txn0)), () -> {});
        journal.saveCommand(storeId, new Journal.CommandUpdate(null, preaccepted(t1, liveTxn())), () -> {});
        journal.closeCurrentSegmentForTestingIfNonEmpty();
        Assert.assertEquals("T1 record present before compaction", 1, journal.load(storeId, t1).count());

        TableMetadata dropped = Schema.instance.getTableMetadata("ks", droppedTable);
        Assert.assertNotNull(dropped);
        SchemaTestUtil.announceTableDrop("ks", droppedTable);
        Assert.assertNull(Schema.instance.getTableMetadata(dropped.id));

        Throwable compactionFailure = null;
        try
        {
            runCompactionBounded(journal);
        }
        catch (Throwable t)
        {
            compactionFailure = t;
        }

        CommandChanges after = journal.load(storeId, t1);
        int t1Count = after == null ? 0 : after.count();
        Assert.assertEquals("live transaction T1's journal record must survive compaction of segments " +
                            "in which an earlier record (T0) references dropped table " + droppedTable,
                            1, t1Count);
        if (compactionFailure != null)
            throw new AssertionError("journal compaction must succeed after table " + droppedTable +
                                     " referenced by T0 was dropped", compactionFailure);
    }

    /** T0 touches only the dropped table (TableMetadatas.One on write, WithUnknown(count=1) on read). */
    @Test
    public void singleTableTxnOnDroppedTable() throws Throwable
    {
        Txn txn0 = AccordTestUtils.createTxn("BEGIN TRANSACTION\n" +
                                             "  LET row1 = (SELECT * FROM ks.gone1 WHERE k=1 AND c=0);\n" +
                                             "  SELECT row1.v;\n" +
                                             "  IF row1 IS NULL THEN\n" +
                                             "    INSERT INTO ks.gone1 (k, c, v) VALUES (1, 0, 1);\n" +
                                             "  END IF\n" +
                                             "COMMIT TRANSACTION");
        dropThenCompact("single", 10, "gone1", txn0);
    }

    /** T0 touches the live table and a table that is then dropped (TableMetadatas.Multi on write). */
    @Test
    public void multiTableTxnWithDroppedTable() throws Throwable
    {
        Txn txn0 = AccordTestUtils.createTxn("BEGIN TRANSACTION\n" +
                                             "  LET row1 = (SELECT * FROM ks.live WHERE k=1 AND c=0);\n" +
                                             "  SELECT row1.v;\n" +
                                             "  IF row1 IS NULL THEN\n" +
                                             "    INSERT INTO ks.live (k, c, v) VALUES (1, 0, 1);\n" +
                                             "    INSERT INTO ks.gone2 (k, c, v) VALUES (3, 0, 1);\n" +
                                             "  END IF\n" +
                                             "COMMIT TRANSACTION");
        dropThenCompact("multi", 20, "gone2", txn0);
    }
}
