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

package org.apache.cassandra.service.accord.journal;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import accord.api.Journal.CommandUpdate;
import accord.api.ProgressLog.NoOpProgressLog;
import accord.api.RemoteListeners.NoOpRemoteListeners;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink.NoOpNotifySink;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Participants;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.RandomSource;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.AccordConfig.JournalConfig.ReplayMode;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

/**
 * Replay must not report success if it was aborted.
 *
 * Journal failure policy STOP_JOURNAL. One command in the journal cannot be replayed (its record
 * fails to deserialise). Replay's callback calls Journal.handleError, which for STOP_JOURNAL logs and
 * returns false; Replay sets abort and leaves its loop, and must then return false.
 *
 * startupReplayFailureKillsJvm: during daemon startup (setup not completed) handleError first runs
 * JVMStabilityInspector.inspectJournalThrowable, which kills the JVM for every policy except
 * ALLOW_UNSAFE_STARTUP, so serving after a partial replay is pre-empted by the JVM exit.
 *
 * replayAfterSetupMustNotReportSuccessAfterFailure: once setup is complete (e.g. the operator REPLAY op in
 * AccordDebugKeyspace), no kill happens, so Replay.replay must itself report the failure.
 */
public class ReplayStopJournalAbortTest
{
    private static final int STORE_ID = 1;
    private static final int GOOD_COMMANDS = 64;
    private static final AtomicInteger counter = new AtomicInteger();

    private JVMStabilityInspector.Killer originalKiller;
    private AccordJournal journal;
    private final ListAppender<ILoggingEvent> journalLog = new ListAppender<>();
    private Logger journalLogger;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        StorageService.instance.initServer();
    }

    @After
    public void tearDown()
    {
        if (journalLogger != null)
            journalLogger.detachAppender(journalLog);
        if (originalKiller != null)
            JVMStabilityInspector.replaceKiller(originalKiller);
        StorageService.instance.registerDaemon(null);
        if (journal != null)
            journal.stop();
    }

    @Test(timeout = 60_000)
    public void startupReplayFailureKillsJvm() throws Throwable
    {
        KillerForTests killer = new KillerForTests();
        originalKiller = JVMStabilityInspector.replaceKiller(killer);
        Assert.assertFalse(StorageService.instance.isDaemonSetupCompleted());

        CommandStore store = prepareStoreWithOnePoisonedCommand(true);
        boolean result = Replay.replay(journal, ReplayMode.ALL, new CommandStore[]{ store }, null);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!killer.wasKilled() && System.nanoTime() < deadline)
            Thread.sleep(10);

        awaitReplayFailureHandled();
        Assert.assertTrue("during startup a STOP_JOURNAL replay failure should request a JVM exit (Replay returned " + result + ')',
                          killer.wasKilled());
    }

    @Test(timeout = 60_000)
    public void replayAfterSetupMustNotReportSuccessAfterFailure() throws Throwable
    {
        assertReplayAfterSetupReportsFailure(true);
    }

    /**
     * The poisoned command is replayed last, so it fails after Replay has submitted every command; Replay must
     * wait for it before reporting its result.
     */
    @Test(timeout = 60_000)
    public void replayAfterSetupMustNotReportSuccessAfterLateFailure() throws Throwable
    {
        assertReplayAfterSetupReportsFailure(false);
    }

    private void assertReplayAfterSetupReportsFailure(boolean poisonFirst) throws Throwable
    {
        // no JVM kill is expected once setup is completed and the policy is STOP_JOURNAL
        KillerForTests killer = new KillerForTests(false);
        originalKiller = JVMStabilityInspector.replaceKiller(killer);
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup();
        StorageService.instance.registerDaemon(daemon);
        Assert.assertTrue(StorageService.instance.isDaemonSetupCompleted());

        CommandStore store = prepareStoreWithOnePoisonedCommand(poisonFirst);
        boolean result;
        Throwable thrown = null;
        try
        {
            result = Replay.replay(journal, ReplayMode.ALL, new CommandStore[]{ store }, null);
        }
        catch (Throwable t)
        {
            result = false;
            thrown = t;
        }
        awaitReplayFailureHandled();
        Assert.assertFalse("STOP_JOURNAL: one of " + (GOOD_COMMANDS + 1) + " commands failed to replay and replay was aborted, " +
                           "but Replay.replay reported success (returned true); thrown=" + thrown,
                           result);
    }

    /** Waits (at most 10 s) until Journal.handleError has logged the replay failure of the poisoned command. */
    private void awaitReplayFailureHandled() throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline)
        {
            synchronized (journalLog.list)
            {
                for (ILoggingEvent e : journalLog.list)
                    if (e.getFormattedMessage().contains("Could not replay command"))
                        return;
            }
            Thread.sleep(10);
        }
        Assert.fail("precondition: the poisoned command's replay failure was never handled by Journal.handleError");
    }

    /**
     * Writes GOOD_COMMANDS valid command records and one undeserialisable record (the smallest TxnId, so replayed
     * first, or the largest, so replayed last) for STORE_ID into an AccordJournal with failure policy STOP_JOURNAL.
     * The caller replays the store with the real AccordCommandStoreReplayer.
     */
    private CommandStore prepareStoreWithOnePoisonedCommand(boolean poisonFirst) throws Throwable
    {
        File dir = new File(Files.createTempDirectory("replay-stop-journal-" + counter.incrementAndGet()));
        dir.deleteRecursiveOnExit();
        journal = new AccordJournal(new TestParams()
        {
            @Override public Params.FailurePolicy failurePolicy() { return Params.FailurePolicy.STOP_JOURNAL; }
        }, dir, Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL));
        journal.start(null);
        journalLogger = (Logger) LoggerFactory.getLogger(org.apache.cassandra.journal.Journal.class);
        journalLog.start();
        journalLogger.addAppender(journalLog);

        RandomSource rs = RandomSource.wrap(new Random(42));
        TreeSet<TxnId> ids = new TreeSet<>();
        while (ids.size() < GOOD_COMMANDS + 1)
            ids.add(AccordGens.txnIds(ignore -> Txn.Kind.Write, ignore -> Routable.Domain.Key).next(rs)); // the participants below are keys
        TxnId poisoned = poisonFirst ? ids.first() : ids.last();
        List<TxnId> good = new ArrayList<>(ids);
        good.remove(poisoned);

        // an undeserialisable COMMAND_DIFF record: flags == 0 trips Invariants.require(readFlags != 0) in CommandChanges.deserializeNext
        journal.segments.asyncWrite(new JournalKey(poisoned, JournalKey.Type.COMMAND_DIFF, STORE_ID), (out, userVersion) -> out.writeInt(0));
        TableMetadata tbl = Schema.instance.getTableMetadata("ks", "tbl");
        Participants<?> participants = RoutingKeys.of(AccordTestUtils.key(tbl, 1).toUnseekable());
        for (TxnId txnId : good)
        {
            Command command = Command.NotDefined.notDefined(txnId, SaveStatus.NotDefined, Status.Durability.NotDurable,
                                                            StoreParticipants.create(null, participants, null, null, participants, participants), Ballot.ZERO);
            journal.saveCommandById(STORE_ID, new CommandUpdate(null, command), () -> {});
        }
        journal.closeCurrentSegmentForTestingIfNonEmpty();

        // precondition: the poisoned record really cannot be loaded
        try
        {
            journal.load(STORE_ID, poisoned);
            Assert.fail("expected the poisoned record to fail to load");
        }
        catch (Throwable expected) { }

        // reuse the node service, ranges and executor of a test command store, but give the store under replay our journal
        AccordCommandStore base = AccordTestUtils.createAccordCommandStore(System::currentTimeMillis, "ks", "tbl");
        return new AccordCommandStore(STORE_ID, base.node(), base.agent(), null,
                                                          cs -> new NoOpProgressLog(),
                                                          cs -> new DefaultLocalListeners(null, new NoOpRemoteListeners(), new NoOpNotifySink()),
                                                          base.unsafeGetRangesForEpoch(), journal, base.executor());
    }
}
