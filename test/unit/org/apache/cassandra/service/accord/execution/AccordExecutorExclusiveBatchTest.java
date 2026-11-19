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

package org.apache.cassandra.service.accord.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.coordinate.Coordinations;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.TestAgent;
import accord.impl.basic.InMemoryJournal;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.ExecutionContext;
import accord.local.FindKeys;
import accord.local.LoadKeys;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.local.cfk.CommandsForKey;
import accord.local.durability.DurabilityService;
import accord.primitives.Ballot;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.async.Cancellable;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by LLM.
 *
 * <p>Exercises {@code ExclusiveExecutor.Batch} (see {@code AccordConfig.queue_exclusive_batch_limit}): a store's task on
 * a hot key may be followed, by the same thread and without the lock, by tasks queued behind it on its key, passing the
 * key's state between them. We submit a backlog of work to one store over a couple of hot keys, and every key task
 * increments a counter held in its key's {@link CommandsForKey} (as {@code maxUniqueHlc}). Some tasks have a txnId (so
 * that their command entries are also owned by their batch), some fail having incremented (so that their update must
 * be discarded), some submit a nested task (a consequence: on their own key, sometimes ATOMIC, a batch may execute it
 * inline; on the other key, it must stop the batch), some are cancelled
 * shortly after submission, and plain runnables are interleaved on the store's exclusive executor. We check that:
 * <ul>
 *   <li>every callback completes, and the executor quiesces, with no exception reported to the agent (in particular,
 *   by the guard that forbids modifying an entry owned by a batch);</li>
 *   <li>at most one task runs on the store at a time;</li>
 *   <li>the tasks that succeeded on each key observed exactly the counter values 0..n-1, each once, and the key ends
 *   with n: no update was lost, duplicated or leaked from a failed task;</li>
 *   <li>top-level tasks on a single key without a txnId run in submission order;</li>
 *   <li>no cache references are leaked, and no entry remains owned;</li>
 *   <li>batches form when enabled (and run most of the tasks), and never when disabled.</li>
 * </ul>
 */
public class AccordExecutorExclusiveBatchTest
{
    private static final int TIMEOUT_SECONDS = 60;
    private static final TableId TABLE_ID = TableId.fromUUID(new java.util.UUID(0, 1));
    private static final int KEYS = 3;
    /** keys that are rarely used, so whose entries are rarely owned by a batch while their operations are performed */
    private static final int COLD_KEYS = 32;
    private static final int TASKS = 4000;
    private static final int BATCH_LIMIT = 8;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void signalLoopTest() throws InterruptedException
    {
        assertBatched(run(Kind.SIGNAL, BATCH_LIMIT, 0));
    }

    @Test
    public void signalLoopWithContinueTest() throws InterruptedException
    {
        assertBatched(run(Kind.SIGNAL, BATCH_LIMIT, 8));
    }

    @Test
    public void syncSubmitTest() throws InterruptedException
    {
        assertBatched(run(Kind.SYNC, BATCH_LIMIT, 0));
    }

    @Test
    public void syncSubmitWithContinueTest() throws InterruptedException
    {
        assertBatched(run(Kind.SYNC, BATCH_LIMIT, 8));
    }

    @Test
    public void runWithLockTest() throws InterruptedException
    {
        assertBatched(run(Kind.SYNC_WITH_LOCK, BATCH_LIMIT, 0));
    }

    @Test
    public void disabledTest() throws InterruptedException
    {
        assertEquals(0, run(Kind.SIGNAL, 0, 0));
    }

    private static void assertBatched(long batched)
    {
        assertTrue("expected some tasks to follow a batch", batched > 0);
    }

    enum Kind { SIGNAL, SYNC, SYNC_WITH_LOCK }

    static class RecordingAgent extends TestAgent
    {
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        @Override
        public void onException(Throwable t)
        {
            if (!isDeliberate(t))
                failure.compareAndSet(null, t);
            super.onException(t);
        }

        private static boolean isDeliberate(Throwable t)
        {
            for (; t != null ; t = t.getCause())
            {
                if (t.getMessage() != null && t.getMessage().startsWith("deliberate"))
                    return true;
                if (t instanceof java.util.concurrent.CancellationException)
                    return true;
            }
            return false;
        }
    }

    private static AccordExecutor executor(Kind kind, TestAgent agent)
    {
        switch (kind)
        {
            default: throw new AssertionError();
            case SIGNAL: return new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
            case SYNC: return new AccordExecutorSyncSubmit(0, RUN_WITHOUT_LOCK, 4, i -> "Sync" + i, agent);
            case SYNC_WITH_LOCK: return new AccordExecutorSyncSubmit(0, RUN_WITH_LOCK, 1, i -> "SyncWithLock" + i, agent);
        }
    }

    /** @return the number of tasks run as followers of a batch */
    private long run(Kind kind, int batchLimit, int continueLimit) throws InterruptedException
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RecordingAgent agent = new RecordingAgent();
        AccordExecutor executor = executor(kind, agent);
        executor.unsafeSetExclusiveBatchLimit(batchLimit);
        executor.unsafeSetExclusiveContinueLimit(continueLimit);
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        AccordCommandStore store = commandStore(TABLE_ID, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        RoutingKey[] keys = new RoutingKey[KEYS + COLD_KEYS];
        List<ConcurrentLinkedQueue<Integer>> ranByKey = new ArrayList<>();
        List<ConcurrentLinkedQueue<Long>> observedByKey = new ArrayList<>();
        for (int i = 0 ; i < keys.length ; ++i)
        {
            keys[i] = new TokenKey(TABLE_ID, partitioner.getToken(Int32Type.instance.decompose(i)));
            ranByKey.add(new ConcurrentLinkedQueue<>());
            observedByKey.add(new ConcurrentLinkedQueue<>());
        }

        AtomicInteger running = new AtomicInteger();
        AtomicReference<String> violation = new AtomicReference<>();
        AtomicInteger unexpectedFailures = new AtomicInteger(), cancelled = new AtomicInteger();
        int nestedCount = 0, plainCount = 0;
        for (int i = 0 ; i < TASKS ; ++i)
        {
            if (isNested(i)) ++nestedCount;
            if (isNestedOther(i)) ++nestedCount;
            if (isPlain(i)) ++plainCount;
        }
        CountDownLatch done = new CountDownLatch(TASKS + nestedCount + plainCount);

        try
        {
            ExclusiveExecutor exclusive = store.exclusiveExecutor();
            List<Cancellable> cancel = new ArrayList<>();
            for (int i = 0 ; i < TASKS ; ++i)
            {
                int index = i;
                int keyIndex = isCold(i) ? KEYS + (i / 5) % COLD_KEYS : i % KEYS;
                RoutingKey key = keys[keyIndex];
                // some tasks declare two keys, so that batches may span several
                int[] taskKeys = isMultiKey(i) && !isCold(i) ? new int[] { keyIndex, (keyIndex + 1) % KEYS } : new int[] { keyIndex };
                RoutingKey[] taskRoutingKeys = new RoutingKey[taskKeys.length];
                for (int k = 0 ; k < taskKeys.length ; ++k)
                    taskRoutingKeys[k] = keys[taskKeys[k]];
                TxnId txnId = hasTxnId(i) ? TxnId.fromValues(1, 1 + i, 0, new Id(1)) : null;
                boolean fails = fails(i);
                ExecutionContext context = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(taskRoutingKeys.clone()) /* sorts in place */, LoadKeys.SYNC, FindKeys.CONFLICTS, "task" + i);
                // the value we observed on each of our keys, which we report only if we succeed
                long[] observed = new long[taskKeys.length];
                Cancellable cancellable = store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {
                    enter(running, violation, "task" + index);
                    try
                    {
                        for (int k = 0 ; k < taskKeys.length ; ++k)
                        {
                            // a task with a txnId or several keys takes its claims only once all of them are loaded,
                            // so may be reordered with respect to other tasks on its key
                            if (txnId == null && taskKeys.length == 1)
                                ranByKey.get(taskKeys[k]).add(index);
                            observed[k] = increment((SaferCommandStore) safeStore, taskRoutingKeys[k]);
                        }
                        // a nested task on our key (and txnId) may be executed inline by a batch; one on another key
                        // may not (as the batch does not own the key), so must stop it
                        for (int nestedKeyIndex : nestedKeys(index, keyIndex))
                        {
                            RoutingKey nestedKey = keys[nestedKeyIndex];
                            ExecutionContext nested = ExecutionContext.contextFor(nestedKeyIndex == keyIndex ? txnId : null, null, RoutingKeys.of(nestedKey),
                                                                                  LoadKeys.SYNC, FindKeys.CONFLICTS, "nested" + index + "_" + nestedKeyIndex);
                            // some nested tasks on our key are ATOMIC, so take fifo positions (ahead of any priority claim)
                            if (nestedKeyIndex == keyIndex && isAtomicNested(index))
                                nested = atomic(nested);
                            long[] nestedObserved = new long[] { -1 };
                            store.execute(nested, (Consumer<? super SafeCommandStore>) nestedStore -> {
                                enter(running, violation, "nested" + index);
                                try { nestedObserved[0] = increment((SaferCommandStore) nestedStore, nestedKey); }
                                finally { exit(running); }
                            }, (success, fail) -> {
                                // a nested task is cancelled if its parent fails
                                if (fail == null) observedByKey.get(nestedKeyIndex).add(nestedObserved[0]);
                                else if (!fails) unexpectedFailures.incrementAndGet();
                                done.countDown();
                            });
                        }
                        if (fails)
                            throw new RuntimeException("deliberate failure of task" + index);
                    }
                    finally
                    {
                        exit(running);
                    }
                }, (success, fail) -> {
                    if (fail == null)
                    {
                        for (int k = 0 ; k < taskKeys.length ; ++k)
                            observedByKey.get(taskKeys[k]).add(observed[k]);
                    }
                    else if (fail instanceof java.util.concurrent.CancellationException)
                    {
                        cancelled.incrementAndGet();
                        // we were cancelled before running, so never submitted our nested tasks
                        for (int ignore : nestedKeys(index, keyIndex))
                            done.countDown();
                    }
                    else if (!fails) unexpectedFailures.incrementAndGet();
                    done.countDown();
                });
                if (isCancelled(i))
                    cancel.add(cancellable);
                if (cancel.size() >= 4)
                {
                    cancel.forEach(Cancellable::cancel);
                    cancel.clear();
                }

                if (isPlain(i))
                {
                    exclusive.execute(() -> {
                        enter(running, violation, "plain" + index);
                        exit(running);
                        done.countDown();
                    });
                }
            }
            cancel.forEach(Cancellable::cancel);

            assertTrue("timed out with " + done.getCount() + " outstanding", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            executor.waitForQuiescence();
            assertNull(violation.get(), violation.get());
            if (agent.failure.get() != null)
                throw new AssertionError("unexpected exception reported to the agent", agent.failure.get());
            assertEquals(0, unexpectedFailures.get());

            for (int k = 0 ; k < keys.length ; ++k)
            {
                List<Integer> ran = new ArrayList<>(ranByKey.get(k));
                for (int i = 1 ; i < ran.size() ; ++i)
                    assertTrue("key " + k + " ran task" + ran.get(i) + " after task" + ran.get(i - 1), ran.get(i - 1) < ran.get(i));

                List<Long> observed = new ArrayList<>(observedByKey.get(k));
                observed.sort(Long::compare);
                for (int i = 0 ; i < observed.size() ; ++i)
                    assertEquals("key " + k + " observed counter values " + summarise(observed), i, (long) observed.get(i));
                assertEquals("key " + k + " final counter", observed.size(), counter(store, keys[k]));
            }

            String referenced = stillReferencedOrOwned(store);
            assertNull(referenced, referenced);
            long batches = exclusive.unsafeBatchCount(), batched = exclusive.unsafeBatchedCount(), inlined = exclusive.unsafeInlinedCount();
            if (batchLimit > 0) assertTrue("expected some consequences to be executed inline", inlined > 0);
            else assertEquals(0, inlined);
            System.out.println("### " + kind + " batchLimit=" + batchLimit + " continueLimit=" + continueLimit
 + " batches=" + batches + " batched=" + batched + " inlined=" + inlined + " continued=" + exclusive.unsafeContinuedCount()
                               + " parked=" + exclusive.unsafeParkedCount() + " cancelled=" + cancelled.get() + " of " + (TASKS + nestedCount + plainCount));
            return batched;
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** increment the counter held by our key's CommandsForKey, returning the value we found */
    private static long increment(SaferCommandStore safeStore, RoutingKey key)
    {
        SaferCommandsForKey safeCfk = (SaferCommandsForKey) safeStore.task.refs.get(key);
        long value = maxUniqueHlc(safeCfk.current());
        // fresh arrays, so that the update is recognised as a change (see CommandsForKey.hasChanges)
        safeCfk.set(CommandsForKey.SerializerSupport.create(key, new CommandsForKey.TxnInfo[0], value + 1, new CommandsForKey.Unmanaged[0],
                                                            TxnId.NONE, CommandsForKey.NO_BOUNDS_INFO, false));
        return value;
    }

    private static final java.lang.reflect.Field MAX_UNIQUE_HLC;
    static
    {
        try
        {
            MAX_UNIQUE_HLC = CommandsForKey.class.getDeclaredField("maxUniqueHlc");
            MAX_UNIQUE_HLC.setAccessible(true);
        }
        catch (NoSuchFieldException e)
        {
            throw new AssertionError(e);
        }
    }

    private static long maxUniqueHlc(CommandsForKey cfk)
    {
        try { return MAX_UNIQUE_HLC.getLong(cfk); }
        catch (IllegalAccessException e) { throw new AssertionError(e); }
    }

    private static long counter(AccordCommandStore store, RoutingKey key)
    {
        try (AccordCommandStore.ExclusiveCaches caches = store.lockCaches())
        {
            AccordCacheEntry<RoutingKey, CommandsForKey, ?> entry = caches.commandsForKeys().getUnsafe(key);
            assertFalse(entry == null || !entry.isLoaded());
            return maxUniqueHlc(entry.getExclusive());
        }
    }

    private static String summarise(List<Long> sorted)
    {
        StringBuilder out = new StringBuilder();
        for (int i = 0 ; i < sorted.size() ; ++i)
        {
            if (sorted.get(i) != i)
                return out.append("... first divergence at ").append(i).append(": ").append(sorted.subList(Math.max(0, i - 2), Math.min(sorted.size(), i + 3))).toString();
        }
        return "0.." + (sorted.size() - 1);
    }

    private static boolean hasTxnId(int i) { return i % 3 != 0; }
    private static boolean fails(int i) { return i % 23 == 7; }
    private static boolean isNested(int i) { return i % 17 == 3; }
    private static boolean isMultiKey(int i) { return i % 7 == 2; }
    private static boolean isCold(int i) { return i % 5 == 4; }
    private static boolean isNestedOther(int i) { return i % 19 == 5; }
    private static boolean isAtomicNested(int i) { return (i / 17) % 2 == 0; }

    private static ExecutionContext atomic(ExecutionContext wrap)
    {
        return new ExecutionContext.Wrapped()
        {
            @Override public ExecutionContext wrapped() { return wrap; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.ATOMIC; }
        };
    }

    private static int[] nestedKeys(int i, int keyIndex)
    {
        int other = (keyIndex + 1) % KEYS;
        if (isNested(i)) return isNestedOther(i) ? new int[] { keyIndex, other } : new int[] { keyIndex };
        return isNestedOther(i) ? new int[] { other } : new int[0];
    }
    private static boolean isPlain(int i) { return i % 11 == 5; }
    private static boolean isCancelled(int i) { return i % 29 == 11; }

    private static void enter(AtomicInteger running, AtomicReference<String> violation, String id)
    {
        int count = running.incrementAndGet();
        if (count != 1)
            violation.compareAndSet(null, id + " ran concurrently with " + (count - 1) + " other task(s) on the same store");
    }

    private static void exit(AtomicInteger running)
    {
        running.decrementAndGet();
    }

    private static String stillReferencedOrOwned(AccordCommandStore store)
    {
        try (AccordCommandStore.ExclusiveCaches caches = store.lockCaches())
        {
            for (AccordCacheEntry<?, ?, ?> entry : caches.commands())
            {
                if (entry.references() != 0)
                    return "leaked " + entry.references() + " references to " + entry;
                if (entry.isOwned())
                    return entry + " remains owned by a batch";
            }
            for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
            {
                if (entry.references() != 0)
                    return "leaked " + entry.references() + " references to " + entry;
                if (entry.isOwned())
                    return entry + " remains owned by a batch";
            }
        }
        return null;
    }

    /**
     * A command store with an in-memory journal and no persistence, so that we need no schema, cluster metadata or
     * commit log. We deliberately do not use {@code AccordAgent}, as reporting an exception there initialises
     * {@code AccordSystemMetrics}, which requires a started {@code AccordService}.
     */
    private static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor, TestAgent agent)
    {
        AtomicLong clock = new AtomicLong();
        LongSupplier now = clock::incrementAndGet;
        Id nodeId = new Id(1);
        NodeCommandStoreService node = new NodeCommandStoreService()
        {
            private final ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, this::now);
            private long stamp = 0;

            @Override public AsyncExecutor someExecutor() { return null; }
            @Override public ExclusiveAsyncExecutor someExclusiveExecutor() { return null; }
            @Override public accord.api.Timeouts timeouts() { return null; }
            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }
            @Override public DurabilityService durability() { return null; }
            @Override public Id id() { return nodeId; }
            @Override public long epoch() { return 1; }
            @Override public long now() { return now.getAsLong(); }
            @Override public long uniqueNow(long atLeast) { return now.getAsLong(); }
            @Override public long elapsed(TimeUnit units) { return elapsed.applyAsLong(units); }
            @Override public TopologyManager topology() { throw new UnsupportedOperationException(); }
            @Override public Coordinations coordinations() { return new Coordinations(); }
            @Override public Scheduler scheduler() { return null; }
            @Override public long currentStamp() { return stamp; }
            @Override public void updateStamp() { ++stamp; }
            @Override public boolean isReplaying() { return false; }
            @Override public void reportLocalExecution(TxnId txnId, Route<?> route, Ballot ballot, Timestamp applyAt, Writes writes, Result result) {}
        };

        return new AccordCommandStore(0, node, agent, null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
