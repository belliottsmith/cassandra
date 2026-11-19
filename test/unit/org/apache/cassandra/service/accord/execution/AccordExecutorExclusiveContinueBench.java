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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;


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

/**
 * This benchmark has been authored entirely by LLM. It is not a test (it asserts nothing, so has no JUnit test methods,
 * and is not picked up by the test suites); run its {@link #main} explicitly to compare {@code queue_exclusive_continue_limit}
 * settings.
 *
 * <p>Closed loop: each command store has {@link #CONCURRENCY} tasks outstanding; each task's callback hands a replacement
 * to one of {@link #CLIENTS} client threads, which submit it externally (as a messaging thread would; submitting from
 * the callback itself would make the replacement a consequence of the task, inheriting its position and references).
 * Each task spins for {@link #WORK_NANOS} while running, declares one key (from a set of one, for a hot key, or many)
 * and no txnId, so that it costs a cache claim but no load or journal write.
 */
public class AccordExecutorExclusiveContinueBench
{
    private static final TableId TABLE_ID = TableId.fromUUID(new java.util.UUID(0, 1));
    private static final int THREADS = Integer.getInteger("bench.threads", 4);
    private static final int CONCURRENCY = Integer.getInteger("bench.concurrency", 32);
    private static final int CLIENTS = Integer.getInteger("bench.clients", 2);
    /** submit each replacement from the callback itself, so that it is a consequence of the task (inheriting its position and references) */
    private static final boolean CONSEQUENCES = Boolean.getBoolean("bench.consequences");
    /** the number of (consecutive) keys each task declares */
    private static final int KEYS_PER_TASK = Integer.getInteger("bench.keys_per_task", 1);
    private static final long WORK_NANOS = Long.getLong("bench.work_nanos", 2000);
    private static final long WARMUP_MILLIS = Long.getLong("bench.warmup_millis", 2000);
    private static final long MEASURE_MILLIS = Long.getLong("bench.measure_millis", 4000);
    private static final int ROUNDS = Integer.getInteger("bench.rounds", 2);

    enum Kind { SYNC, SIGNAL }

    public static void main(String[] args) throws InterruptedException
    {
        setUp();
        new AccordExecutorExclusiveContinueBench().bench();
        System.exit(0);
    }

    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public void bench() throws InterruptedException
    {
        List<String> results = new ArrayList<>();
        for (int round = 0 ; round < ROUNDS ; ++round)
        {
            for (Kind kind : Kind.values())
            {
                for (int stores : new int[] { 1, 4 })
                {
                    for (int keys : KEYS_PER_TASK > 1 ? new int[] { KEYS_PER_TASK, 8 } : new int[] { 1, 64 })
                    {
                        // { continue limit, batch limit }
                        for (int[] limits : new int[][] { { 0, 0 }, { 8, 0 }, { 0, 8 }, { 8, 8 } })
                            results.add(run(kind, stores, keys, limits[0], limits[1]));
                    }
                }
            }
        }
 System.out.println("### consequences=" + CONSEQUENCES + " keysPerTask=" + KEYS_PER_TASK + " threads=" + THREADS + " clients=" + CLIENTS + " concurrency=" + CONCURRENCY + " work=" + WORK_NANOS + "ns");
        results.forEach(r -> System.out.println("### " + r));
    }

    private static AccordExecutor executor(Kind kind)
    {
        IntFunction<String> name = i -> "Bench" + i;
        switch (kind)
        {
            default: throw new AssertionError();
            case SYNC: return new AccordExecutorSyncSubmit(0, RUN_WITHOUT_LOCK, THREADS, name, new TestAgent());
            case SIGNAL: return new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, THREADS, -1, -1, TimeUnit.MICROSECONDS, name, new TestAgent());
        }
    }

    private String run(Kind kind, int storeCount, int keyCount, int limit, int batchLimit) throws InterruptedException
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        AccordExecutor executor = executor(kind);
        executor.unsafeSetExclusiveContinueLimit(limit);
        executor.unsafeSetExclusiveBatchLimit(batchLimit);
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(64 << 20);
            executor.setWorkingSetSize(32 << 20);
        });

        AccordCommandStore[] stores = new AccordCommandStore[storeCount];
        for (int i = 0 ; i < storeCount ; ++i)
            stores[i] = commandStore(i, TABLE_ID, partitioner, executor);

        RoutingKey[] keys = new RoutingKey[keyCount];
        for (int i = 0 ; i < keyCount ; ++i)
            keys[i] = new TokenKey(TABLE_ID, partitioner.getToken(Int32Type.instance.decompose(i)));

        AtomicLong completed = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch drained = new CountDownLatch(storeCount * CONCURRENCY);
        LinkedBlockingQueue<Runnable> pending = new LinkedBlockingQueue<>();
        Thread[] clients = new Thread[CLIENTS];
        for (int i = 0 ; i < CLIENTS ; ++i)
        {
            clients[i] = new Thread(() -> {
                try
                {
                    while (true)
                        pending.take().run();
                }
                catch (InterruptedException ignore) {}
            }, "BenchClient" + i);
            clients[i].setDaemon(true);
            clients[i].start();
        }

        try
        {
            for (AccordCommandStore store : stores)
            {
                for (int i = 0 ; i < CONCURRENCY ; ++i)
                {
                    long seq = i;
                    pending.add(() -> submit(store, keys, seq, completed, stop, drained, pending));
                }
            }

            Thread.sleep(WARMUP_MILLIS);
            long startCompleted = completed.get();
            long startContinued = continued(stores);
            long startBatched = batched(stores);
            long start = System.nanoTime();
            Thread.sleep(MEASURE_MILLIS);
            long elapsed = System.nanoTime() - start;
            long done = completed.get() - startCompleted;
            long continued = continued(stores) - startContinued;
            long batched = batched(stores) - startBatched;
            long inlined = 0;
            for (AccordCommandStore store : stores)
                inlined += store.exclusiveExecutor().unsafeInlinedCount();
            stop.set(true);
            drained.await(30, TimeUnit.SECONDS);

            double perSecond = done * 1e9 / elapsed;
            return String.format("%-6s stores=%d keys=%-2d continue=%d batch=%d  %,10.0f tasks/s  continued=%4.1f%%  batched=%4.1f%%  inlined(total)=%d",
                                 kind, storeCount, keyCount, limit, batchLimit, perSecond,
                                 done == 0 ? 0 : 100.0 * continued / done, done == 0 ? 0 : 100.0 * batched / done, inlined);
        }
        finally
        {
            for (Thread client : clients)
                client.interrupt();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static long batched(AccordCommandStore[] stores)
    {
        long sum = 0;
        for (AccordCommandStore store : stores)
            sum += store.exclusiveExecutor().unsafeBatchedCount();
        return sum;
    }

    private static long continued(AccordCommandStore[] stores)
    {
        long sum = 0;
        for (AccordCommandStore store : stores)
            sum += store.exclusiveExecutor().unsafeContinuedCount();
        return sum;
    }

    private static void submit(AccordCommandStore store, RoutingKey[] keys, long seq, AtomicLong completed, AtomicBoolean stop, CountDownLatch drained, LinkedBlockingQueue<Runnable> pending)
    {
        RoutingKey[] taskKeys = new RoutingKey[Math.min(KEYS_PER_TASK, keys.length)];
        for (int i = 0 ; i < taskKeys.length ; ++i)
            taskKeys[i] = keys[(int) ((seq + i) % keys.length)];
        ExecutionContext context = ExecutionContext.contextFor(null, null, RoutingKeys.of(taskKeys), LoadKeys.SYNC, FindKeys.CONFLICTS, "bench");
        store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {
            long until = System.nanoTime() + WORK_NANOS;
            while (System.nanoTime() < until) {}
        }, (success, fail) -> {
            completed.incrementAndGet();
            if (stop.get()) drained.countDown();
            else if (CONSEQUENCES) submit(store, keys, seq + CONCURRENCY, completed, stop, drained, pending);
            else pending.add(() -> submit(store, keys, seq + CONCURRENCY, completed, stop, drained, pending));
        });
    }

    private static AccordCommandStore commandStore(int id, TableId tableId, IPartitioner partitioner, AccordExecutor executor)
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

        return new AccordCommandStore(id, node, new TestAgent(), null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
