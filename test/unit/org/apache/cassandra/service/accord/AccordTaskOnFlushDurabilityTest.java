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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Journal.CommandUpdate;
import accord.api.Journal.FieldUpdates;
import accord.api.RoutingKey;
import accord.local.CheckedCommands;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.SafeCommandStore;
import accord.primitives.FullRoute;
import accord.primitives.PartialTxn;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.journal.AccordJournal;
import org.apache.cassandra.service.accord.journal.CommandChangeWriter;

import static accord.local.FindKeys.DECLARED;
import static accord.local.LoadKeys.SYNC;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;

/**
 * A task must not reply before every journal record it wrote is durable: {@code SafeTask.runMayThrow} defers
 * {@code finish(result)} until the store's latest journal record is durable, so the reply is not externalised early.
 *
 * {@link AccordCommandStore#appendCommands} used to hand that callback to the last {@link CommandUpdate} only. The
 * journal invokes a callback inline when its update writes no record (an unchanged command), so a task that loaded
 * two commands and changed only one could reply while the changed command's record was still awaiting fsync, whenever
 * the unchanged command happened to be visited last. A command that is merely loaded yields such a no-op update
 * ({@code null -> Uninitialised}). The same problem applied to field updates: a task that wrote a RedundantBefore /
 * SafeToRead record and produced only no-op command diffs passed {@code null} to {@code persistFieldUpdatesInternal},
 * so the field record was never awaited either.
 *
 * {@link DeferringJournal} keeps the journal's durability contract ({@code Journal.onDurable} fires only after fsync,
 * and inline when no record is written) but lets the test control when fsync happens, so the ordering under test is
 * deterministic.
 */
public class AccordTaskOnFlushDurabilityTest
{
    private static final AtomicLong clock = new AtomicLong(0);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        StorageService.instance.initServer();
    }

    /**
     * Real {@link AccordJournal} (so compaction and replay work) that defers durability callbacks until
     * {@link #fsync()}; callbacks for updates that write no record, or for records already flushed, run inline, as the
     * real journal does. Written and fsynced record counts are tracked independently of which record carries the
     * callback, so the test can tell whether a reply was sent while a record the task wrote was still unflushed.
     * The task under test runs on another thread, so state is guarded by this object's monitor.
     */
    static class DeferringJournal extends AccordJournal
    {
        private final List<Runnable> pendingCallbacks = new ArrayList<>();
        int commandRecordsWritten, fieldRecordsWritten;
        int commandRecordsFlushed, fieldRecordsFlushed;

        DeferringJournal(Params params) { super(params); }

        @Override
        public void saveCommand(CommandStore commandStore, CommandUpdate update, Runnable onFlush)
        {
            if (CommandChangeWriter.make(update.before, update.after) == null)
            {
                super.saveCommand(commandStore, update, onFlush);
                return;
            }
            synchronized (this)
            {
                ++commandRecordsWritten;
                if (onFlush != null) pendingCallbacks.add(onFlush);
            }
            super.saveCommand(commandStore, update, null);
        }

        @Override
        public void saveStoreState(CommandStore commandStore, FieldUpdates fieldUpdates, Runnable onFlush)
        {
            boolean writes = fieldUpdates.newRedundantBefore != null
                             || fieldUpdates.newBootstrapBeganAt != null
                             || fieldUpdates.newSafeToRead != null
                             || fieldUpdates.newRangesForEpoch != null
                             || fieldUpdates.newPermanentlyUnsafeToRead != null;
            if (!writes)
            {
                super.saveStoreState(commandStore, fieldUpdates, onFlush);
                return;
            }
            synchronized (this)
            {
                ++fieldRecordsWritten;
                if (onFlush != null) pendingCallbacks.add(onFlush);
            }
            super.saveStoreState(commandStore, fieldUpdates, null);
        }

        void fsync()
        {
            List<Runnable> callbacks;
            synchronized (this)
            {
                commandRecordsFlushed = commandRecordsWritten;
                fieldRecordsFlushed = fieldRecordsWritten;
                callbacks = new ArrayList<>(pendingCallbacks);
                pendingCallbacks.clear();
            }
            callbacks.forEach(Runnable::run);
        }

        synchronized boolean hasUnflushedWrites()
        {
            return commandRecordsFlushed < commandRecordsWritten || fieldRecordsFlushed < fieldRecordsWritten;
        }

        synchronized int commandRecordsWritten()
        {
            return commandRecordsWritten;
        }

        @Override
        public boolean isDurable(CommandStore commandStore)
        {
            return !hasUnflushedWrites();
        }

        @Override
        public void onDurable(CommandStore commandStore, Runnable onDurable)
        {
            synchronized (this)
            {
                if (hasUnflushedWrites())
                {
                    pendingCallbacks.add(onDurable);
                    return;
                }
            }
            onDurable.run();
        }
    }

    private static DeferringJournal newJournal()
    {
        if (new File(DatabaseDescriptor.getAccordJournalDirectory()).exists())
            ServerTestUtils.cleanupDirectory(DatabaseDescriptor.getAccordJournalDirectory());
        AccordConfig.JournalConfig spec = new AccordConfig.JournalConfig();
        spec.flushPeriod = new DurationSpec.IntSecondsBound(1);
        spec.flushMode = Params.FlushMode.BATCH;
        DeferringJournal journal = new DeferringJournal(spec);
        journal.start(null);
        return journal;
    }

    private static AccordCommandStore newCommandStore(DeferringJournal journal)
    {
        AccordCommandStore store = AccordTestUtils.createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        try
        {
            Field f = AccordCommandStore.class.getDeclaredField("journal");
            f.setAccessible(true);
            f.set(store, journal);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError(e);
        }
        return store;
    }

    /** Waits for the task's reply, propagating any failure it reported. */
    private static void awaitReply(SettableFuture<Void> replied) throws Exception
    {
        try
        {
            replied.get(30, TimeUnit.SECONDS);
        }
        catch (ExecutionException e)
        {
            Assert.fail("task failed: " + e.getCause());
        }
        catch (TimeoutException e)
        {
            Assert.fail("task did not reply after its journal records were flushed");
        }
    }

    /** Waits until the task under test has written a record or replied. */
    private static void awaitProgress(DeferringJournal journal, SettableFuture<Void> replied) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!journal.hasUnflushedWrites() && !replied.isDone() && System.nanoTime() < deadline)
            Thread.sleep(1);
    }

    /**
     * Two commands loaded, only one changed. Whichever order the commands are visited in, the reply must not be sent
     * while an unflushed record for the changed command is outstanding.
     */
    @Test
    public void replyAwaitsChangedCommandRecordWhenAnotherLoadedCommandIsUnchanged() throws Throwable
    {
        List<String> early = new ArrayList<>();
        for (int i = 0; i < 8; i++)
        {
            for (boolean modifyFirst : new boolean[]{ true, false })
            {
                DeferringJournal journal = newJournal();
                AccordCommandStore commandStore = newCommandStore(journal);

                TxnId a = txnId(1, clock.incrementAndGet(), 1);
                TxnId b = txnId(1, clock.incrementAndGet(), 1);
                TxnId modified = modifyFirst ? a : b;
                PartialTxn partialTxn = createPartialTxn(0);
                RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
                FullRoute<?> route = RoutingKeys.of(routingKey).toRoute(routingKey);
                ExecutionContext ctx = ExecutionContext.contextFor(a, b, route, SYNC, DECLARED, "Test");

                SettableFuture<Void> replied = SettableFuture.create();
                List<Throwable> failures = new ArrayList<>();
                commandStore.execute(ctx, (Consumer<SafeCommandStore>) safe -> CheckedCommands.preaccept(safe, modified, partialTxn, route), (ignore, t) -> {
                    if (t != null) failures.add(t);
                    replied.set(null);
                });

                awaitProgress(journal, replied);
                if (replied.isDone() && journal.hasUnflushedWrites())
                    early.add("modified=" + modified + " unchanged=" + (modifyFirst ? b : a));

                journal.fsync();
                awaitReply(replied);
                if (!failures.isEmpty())
                    throw new AssertionError(failures.get(0));
            }
        }
        Assert.assertTrue("task replied before the changed command's journal record was durable: " + early, early.isEmpty());
    }

    /**
     * A task that changes nothing may reply from state written by another task, so must not reply before that state
     * is durable.
     */
    @Test
    public void unchangedReplyAwaitsAnotherTasksRecord() throws Throwable
    {
        DeferringJournal journal = newJournal();
        AccordCommandStore commandStore = newCommandStore(journal);

        TxnId txnId = txnId(1, clock.incrementAndGet(), 1);
        PartialTxn partialTxn = createPartialTxn(0);
        RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = RoutingKeys.of(routingKey).toRoute(routingKey);
        ExecutionContext ctx = ExecutionContext.contextFor(txnId, null, route, SYNC, DECLARED, "Test");

        SettableFuture<Void> writerReplied = SettableFuture.create();
        commandStore.execute(ctx, (Consumer<SafeCommandStore>) safe -> CheckedCommands.preaccept(safe, txnId, partialTxn, route),
                             (ignore, t) -> writerReplied.set(null));
        awaitProgress(journal, writerReplied);
        Assert.assertTrue("expected the writer to await its record", journal.hasUnflushedWrites() && !writerReplied.isDone());

        SettableFuture<Void> readerRan = SettableFuture.create();
        SettableFuture<Void> readerReplied = SettableFuture.create();
        commandStore.execute(ctx, (Consumer<SafeCommandStore>) safe -> {
            Assert.assertNotNull(safe.ifInitialised(txnId));
            readerRan.set(null);
        }, (ignore, t) -> readerReplied.set(null));
        readerRan.get(30, TimeUnit.SECONDS);
        Thread.sleep(100);
        Assert.assertFalse("task replied with state that was not yet durable", readerReplied.isDone());

        journal.fsync();
        awaitReply(writerReplied);
        awaitReply(readerReplied);
    }

    /**
     * A field update plus a command loaded but never changed: the field record is the only record the task writes,
     * so the reply must wait for it rather than for the (inline) no-op command diff.
     */
    @Test
    public void replyAwaitsFieldUpdateRecordWhenOnlyCommandDiffIsNoop() throws Throwable
    {
        DeferringJournal journal = newJournal();
        AccordCommandStore commandStore = newCommandStore(journal);
        TxnId untouched = txnId(1, clock.incrementAndGet(), 1);

        SettableFuture<Void> replied = SettableFuture.create();
        List<Throwable> failures = new ArrayList<>();
        commandStore.execute(ExecutionContext.unsequenced(untouched, "Test"),
                             (SafeCommandStore safe) -> safe.setSafeToRead(commandStore.unsafeGetSafeToRead()),
                             (ignore, t) -> {
                                 if (t != null) failures.add(t);
                                 replied.set(null);
                             });

        awaitProgress(journal, replied);
        Assert.assertEquals("expected the field update to be the only record written", 0, journal.commandRecordsWritten());
        boolean repliedWithPendingWrites = replied.isDone() && journal.hasUnflushedWrites();

        journal.fsync();
        awaitReply(replied);
        if (!failures.isEmpty())
            throw new AssertionError(failures.get(0));
        Assert.assertFalse("task replied before its field-update record was durable", repliedWithPendingWrites);
    }
}
