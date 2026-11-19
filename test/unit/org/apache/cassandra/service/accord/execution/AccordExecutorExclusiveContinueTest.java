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
import java.util.function.Supplier;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by LLM.
 *
 * <p>Exercises {@code ExclusiveExecutor.tryContinue} (see {@code AccordConfig.queue_exclusive_continue_limit}): a thread
 * that has just run a command store's task may complete it and prepare and run the next without handing off. We submit
 * a backlog of work to one store - sync key tasks over a few hot keys, some of which fail and some of which submit a
 * nested task on their key, interleaved with plain runnables on the store's exclusive executor - and check that:
 * <ul>
 *   <li>every task and callback completes, and the executor quiesces;</li>
 *   <li>at most one task runs on the store at a time;</li>
 *   <li>top-level tasks on a key without a txnId run in submission order;</li>
 *   <li>no cache references are leaked;</li>
 *   <li>continuation happens when enabled, and does not when disabled or when tasks run holding the lock.</li>
 * </ul>
 */
public class AccordExecutorExclusiveContinueTest
{
    private static final int TIMEOUT_SECONDS = 60;
    private static final TableId TABLE_ID = TableId.fromUUID(new java.util.UUID(0, 1));
    private static final int KEYS = 4;
    private static final int TASKS = 4000;
    private static final int CONTINUE_LIMIT = 8;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void signalLoopTest() throws InterruptedException
    {
        long continued = run(() -> new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent()), CONTINUE_LIMIT);
        assertTrue("expected some tasks to be run by continuation", continued > 0);
    }

    @Test
    public void syncSubmitTest() throws InterruptedException
    {
        long continued = run(() -> new AccordExecutorSyncSubmit(0, RUN_WITHOUT_LOCK, 4, i -> "Sync" + i, new TestAgent()), CONTINUE_LIMIT);
        assertTrue("expected some tasks to be run by continuation", continued > 0);
    }

    @Test
    public void disabledTest() throws InterruptedException
    {
        long continued = run(() -> new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent()), 0);
        assertEquals(0, continued);
    }

    @Test
    public void syncSubmitDisabledTest() throws InterruptedException
    {
        long continued = run(() -> new AccordExecutorSyncSubmit(0, RUN_WITHOUT_LOCK, 4, i -> "Sync" + i, new TestAgent()), 0);
        assertEquals(0, continued);
    }

    @Test
    public void runWithLockTest() throws InterruptedException
    {
        // tasks run holding the lock, so are completed by the loop as normal and never continued
        long continued = run(() -> new AccordExecutorSyncSubmit(0, RUN_WITH_LOCK, 1, i -> "SyncWithLock" + i, new TestAgent()), CONTINUE_LIMIT);
        assertEquals(0, continued);
    }

    private long run(Supplier<AccordExecutor> executorFactory, int continueLimit) throws InterruptedException
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        AccordExecutor executor = executorFactory.get();
        executor.unsafeSetExclusiveContinueLimit(continueLimit);
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        AccordCommandStore store = commandStore(TABLE_ID, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        RoutingKey[] keys = new RoutingKey[KEYS];
        List<ConcurrentLinkedQueue<Integer>> ranByKey = new ArrayList<>();
        for (int i = 0 ; i < KEYS ; ++i)
        {
            keys[i] = new TokenKey(TABLE_ID, partitioner.getToken(Int32Type.instance.decompose(i)));
            ranByKey.add(new ConcurrentLinkedQueue<>());
        }

        AtomicInteger running = new AtomicInteger();
        AtomicReference<String> violation = new AtomicReference<>();
        AtomicInteger expectedFailures = new AtomicInteger(), unexpectedFailures = new AtomicInteger();
        // every top-level SafeTask, nested SafeTask and plain runnable counts down once
        int nestedCount = 0, plainCount = 0;
        for (int i = 0 ; i < TASKS ; ++i)
        {
            if (isNested(i)) ++nestedCount;
            if (isPlain(i)) ++plainCount;
        }
        CountDownLatch done = new CountDownLatch(TASKS + nestedCount + plainCount);

        try
        {
            ExclusiveExecutor exclusive = store.exclusiveExecutor();
            for (int i = 0 ; i < TASKS ; ++i)
            {
                int index = i;
                int keyIndex = i % KEYS;
                RoutingKey key = keys[keyIndex];
                // tasks with a txnId take their key claims only once their command has loaded, which is asynchronous,
                // so may be reordered with respect to other tasks on their key; we check order only for those without
                TxnId txnId = hasTxnId(i) ? TxnId.fromValues(1, 1 + i, 0, new Id(1)) : null;
                boolean fails = fails(i);
                ExecutionContext context = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key), LoadKeys.SYNC, FindKeys.CONFLICTS, "task" + i);
                store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {
                    enter(running, violation, "task" + index);
                    try
                    {
                        if (txnId == null)
                            ranByKey.get(keyIndex).add(index);
                        if (isNested(index))
                        {
                            ExecutionContext nested = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key), LoadKeys.SYNC, FindKeys.CONFLICTS, "nested" + index);
                            store.execute(nested, (Consumer<? super SafeCommandStore>) ignore -> {
                                enter(running, violation, "nested" + index);
                                exit(running);
                            }, (success, fail) -> {
                                // a nested task is cancelled if its parent fails
                                if (fail != null && !fails) unexpectedFailures.incrementAndGet();
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
                    if (fail != null)
                    {
                        if (fails) expectedFailures.incrementAndGet();
                        else unexpectedFailures.incrementAndGet();
                    }
                    done.countDown();
                });

                if (isPlain(i))
                {
                    exclusive.execute(() -> {
                        enter(running, violation, "plain" + index);
                        exit(running);
                        done.countDown();
                    });
                }
            }

            assertTrue("timed out with " + done.getCount() + " outstanding", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            executor.waitForQuiescence();
            assertNull(violation.get(), violation.get());
            assertEquals(0, unexpectedFailures.get());
            int failCount = 0;
            for (int i = 0 ; i < TASKS ; ++i)
                if (fails(i)) ++failCount;
            assertEquals(failCount, expectedFailures.get());

            for (int k = 0 ; k < KEYS ; ++k)
            {
                List<Integer> ran = new ArrayList<>(ranByKey.get(k));
                int expect = 0;
                for (int i = k ; i < TASKS ; i += KEYS)
                    if (!hasTxnId(i)) ++expect;
                assertEquals(expect, ran.size());
                for (int i = 1 ; i < ran.size() ; ++i)
                    assertTrue("key " + k + " ran task" + ran.get(i) + " after task" + ran.get(i - 1), ran.get(i - 1) < ran.get(i));
            }

            String referenced = stillReferenced(store);
            assertNull(referenced, referenced);
            long continued = exclusive.unsafeContinuedCount();
            System.out.println("### " + executor.getClass().getSimpleName() + " limit=" + continueLimit + " continued=" + continued + " of " + (TASKS + nestedCount + plainCount));
            return continued;
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static boolean hasTxnId(int i) { return i % 3 != 0; }
    private static boolean fails(int i) { return i % 23 == 7; }
    private static boolean isNested(int i) { return i % 17 == 3; }
    private static boolean isPlain(int i) { return i % 11 == 5; }

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

    private static String stillReferenced(AccordCommandStore store)
    {
        try (AccordCommandStore.ExclusiveCaches caches = store.lockCaches())
        {
            for (AccordCacheEntry<?, ?, ?> entry : caches.commands())
            {
                if (entry.references() != 0)
                    return "leaked " + entry.references() + " references to " + entry;
            }
            for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
            {
                if (entry.references() != 0)
                    return "leaked " + entry.references() + " references to " + entry;
            }
        }
        return null;
    }

    /**
     * A command store with an in-memory journal and no persistence, so that we need no schema, cluster metadata or
     * commit log. We deliberately do not use {@code AccordAgent}, as reporting an exception there initialises
     * {@code AccordSystemMetrics}, which requires a started {@code AccordService}.
     */
    private static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor)
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

        return new AccordCommandStore(0, node, new TestAgent(), null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
