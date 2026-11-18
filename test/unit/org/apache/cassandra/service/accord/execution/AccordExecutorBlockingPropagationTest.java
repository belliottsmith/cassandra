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
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.ExecutionContext;
import accord.local.FindKeys;
import accord.local.LoadKeys;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordFailedKeyTestHarness;
import org.apache.cassandra.service.accord.AccordFailedKeyTestHarness.RecordingAgent;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A batched (INCR) task should prefer, in each batch, the keys on which another task is queued behind it: the queue
 * tells the head when a follower arrives ({@code STILL_RUNNABLE_NEWLY_BLOCKING}) or when it becomes head with followers
 * ({@code NEWLY_BLOCKING_RUNNABLE}), and {@code NonSyncState.prepareExclusive} takes its {@code blocking} keys first.
 *
 * <p>These tests use the shape of a DepsCalculator continuation: an ASYNC parent runs with some of its keys and submits
 * an ATOMIC INCR continuation for the rest. The continuation inherits the parent's references and fifo position at
 * preSetup, but takes its queue positions only when it is registered, at the end of the parent's round, while the
 * parent still holds its own positions on the keys it did not process. A SYNC key task waits on one inherited key,
 * joining before the continuation is registered (as an earlier consequence of the parent, or externally while the
 * parent runs), while the continuation runs, or while the key is still loading. A parent that holds fifo claims (INCR
 * with a txnId) shares its fifo position with the continuation, which therefore queues behind it instead.
 *
 * <p>We check that the continuation's {@code blocking} set contains only keys with a task queued behind it, and that its
 * next batch is the key the waiter needs. Batches are of one key so the choice is observable. {@code AccordExecutor}
 * reads the batch sizes once, statically, so this class must run in its own JVM.
 */
public class AccordExecutorBlockingPropagationTest
{
    private static final int TIMEOUT_SECONDS = 30;
    private static final int KEYS = 8;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.getAccord().queue_nonsync_min_batch_size = 1;
        DatabaseDescriptor.getAccord().queue_nonsync_max_batch_size = 1;
    }

    @Before
    public void requireBatchSizeTookEffect()
    {
        Invariants.require(AccordExecutor.NONSYNC_MAX_BATCH_SIZE == 1 && AccordExecutor.NONSYNC_MIN_BATCH_SIZE == 1,
                           "expected batches of one, found %d..%d: the config was read before setUp set it",
                           AccordExecutor.NONSYNC_MIN_BATCH_SIZE, AccordExecutor.NONSYNC_MAX_BATCH_SIZE);
    }

    enum WaiterJoins
    {
        /** submitted by the parent before it submits the continuation, so registered just before it */
        AS_EARLIER_CONSEQUENCE,
        /** submitted from another thread, and queued, while the parent runs */
        EXTERNALLY_WHILE_PARENT_RUNS,
        /** submitted from another thread, and queued, while the continuation runs its first batch */
        EXTERNALLY_WHILE_CHILD_RUNS,
        /** submitted from another thread while the parent runs, with the key still loading */
        EXTERNALLY_WHILE_KEY_LOADS,
        /**
         * submitted from another thread while the parent runs, but the parent is INCR with a txnId so holds fifo claims,
         * which the continuation shares and so queues behind
         */
        EXTERNALLY_WHILE_FIFO_PARENT_RUNS,
    }

    @Test
    public void waiterRegisteredBeforeChildAsConsequence() throws Throwable
    {
        test(WaiterJoins.AS_EARLIER_CONSEQUENCE, 1);
    }

    @Test
    public void waiterQueuedExternallyWhileParentRuns() throws Throwable
    {
        test(WaiterJoins.EXTERNALLY_WHILE_PARENT_RUNS, 2);
    }

    @Test
    public void waiterQueuedExternallyWhileChildRuns() throws Throwable
    {
        test(WaiterJoins.EXTERNALLY_WHILE_CHILD_RUNS, 3);
    }

    /**
     * The load drains the bag and notifies the continuation (a fifo claim, so first) before the waiter re-queues, so
     * the continuation learns it is blocking only when the waiter re-queues.
     */
    @Test
    public void waiterQueuedWhileInheritedKeyLoads() throws Throwable
    {
        test(WaiterJoins.EXTERNALLY_WHILE_KEY_LOADS, 4);
    }

    @Test
    public void waiterQueuedExternallyWhileFifoParentRuns() throws Throwable
    {
        test(WaiterJoins.EXTERNALLY_WHILE_FIFO_PARENT_RUNS, 5);
    }

    private void test(WaiterJoins joins, int tableIdLsb) throws Throwable
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, tableIdLsb));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = new RoutingKey[KEYS];
        for (int i = 0 ; i < KEYS ; ++i)
            keys[i] = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(i)));
        RoutingKey slow = joins == WaiterJoins.EXTERNALLY_WHILE_KEY_LOADS ? keys[KEYS - 1] : null;
        Condition released = Condition.newOneTimeCondition();

        RecordingAgent agent = new RecordingAgent();
        // enough threads that one may block in the slow load while others run the parent, and later the continuation
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 4, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(type -> {
            AccordFailedKeyTestHarness.setInMemoryFunctions(type);
            if (slow != null)
                setSlowLoadFunction(type, slow, released);
        });
        AccordCommandStore store = AccordFailedKeyTestHarness.commandStore(tableId, partitioner, executor, agent);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));
        Run run = new Run(executor, store);
        Condition parentDone = Condition.newOneTimeCondition();
        try
        {
            for (RoutingKey key : keys)
            {
                if (!key.equals(slow))
                    AccordFailedKeyTestHarness.loadIntoCache(executor, store, key);
            }

            boolean fifoParent = joins == WaiterJoins.EXTERNALLY_WHILE_FIFO_PARENT_RUNS;
            ExecutionContext parentContext = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(keys), fifoParent ? LoadKeys.INCR : LoadKeys.ASYNC,
                                                                         FindKeys.CONFLICTS, "parent");
            if (fifoParent)
                parentContext = AccordExecutionTestUtils.idempotent(parentContext);
            AtomicBoolean parentRan = new AtomicBoolean();
            store.execute(parentContext,
                          (Consumer<? super SafeCommandStore>) safeStore -> {
                              SafeTask<?> parent = ((SaferCommandStore) safeStore).task;
                              Invariants.require(parent.isCacheQueuedFifo() == fifoParent);
                              if (!parentRan.compareAndSet(false, true))
                                  return; // an INCR parent runs for each of its keys; it submits on its first round only
                              RoutingKeys active = parent.nonSync().active;
                              Invariants.require(active.size() == 1 && (slow == null || !active.contains(slow)),
                                                 "expected the parent to run with one loaded key, found %s", active);
                              List<RoutingKey> remaining = remaining(keys, active);
                              switch (joins)
                              {
                                  case AS_EARLIER_CONSEQUENCE:
                                      run.blocked.set(remaining.get(remaining.size() - 1));
                                      run.submitWaiter();
                                      run.submitChild(txnId, keys, batch -> {});
                                      break;

                                  case EXTERNALLY_WHILE_PARENT_RUNS:
                                  case EXTERNALLY_WHILE_FIFO_PARENT_RUNS:
                                      run.blocked.set(remaining.get(remaining.size() - 1));
                                      run.submitWaiterExternally(Task.State.WAITING_ON_TXN);
                                      run.submitChild(txnId, keys, batch -> {});
                                      break;

                                  case EXTERNALLY_WHILE_CHILD_RUNS:
                                      run.submitChild(txnId, keys, batch -> {
                                          if (run.batches.size() != 1)
                                              return;
                                          List<RoutingKey> left = remaining(remaining.toArray(new RoutingKey[0]), batch);
                                          run.blocked.set(left.get(left.size() - 1));
                                          run.submitWaiterExternally(Task.State.WAITING_ON_TXN);
                                      });
                                      break;

                                  case EXTERNALLY_WHILE_KEY_LOADS:
                                      run.blocked.set(slow);
                                      // the waiter cannot queue on the key until it loads
                                      run.submitWaiterExternally(Task.State.LOADING_REQUIRED);
                                      run.submitChild(txnId, keys, batch -> {
                                          if (run.batches.size() != 1)
                                              return;
                                          released.signal();
                                          run.awaitUnderLock(() -> run.waiter.get().compareTo(Task.State.WAITING_ON_TXN) >= 0,
                                                             "the waiter never re-queued after the load");
                                      });
                                      break;
                              }
                          },
                          (success, fail) -> { run.fail(fail); parentDone.signal(); });

            assertTrue("the parent never completed", parentDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the continuation never completed", run.childDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the waiter never completed", run.waiterDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            released.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertNull("unexpected failure", run.failure.get());
        assertNull("unexpected internal error: " + agent.exceptions, agent.exceptions.stream().findFirst().orElse(null));
        assertEquals("the continuation must process every key its parent did not: " + run.batches, KEYS - 1, run.batches.size());
        // a key is blocking only if some task is queued behind us on it; anything else is noise that hides the real ones
        assertEquals("the continuation considered keys blocking that nobody waits on; at each batch:\n" + String.join("\n", run.snapshots),
                     "", String.join("", run.phantoms));
        int firstBatchAfterWaiter = joins == WaiterJoins.EXTERNALLY_WHILE_CHILD_RUNS || joins == WaiterJoins.EXTERNALLY_WHILE_KEY_LOADS ? 1 : 0;
        assertEquals("the first batch after the waiter queued must be the key it waits on; batches were " + run.batches
                     + "; at each batch:\n" + String.join("\n", run.snapshots),
                     RoutingKeys.of(run.blocked.get()), run.batches.get(firstBatchAfterWaiter));
    }

    /** the state shared by a parent, its continuation and the waiter */
    private static class Run
    {
        final AccordExecutor executor;
        final AccordCommandStore store;
        final AtomicReference<RoutingKey> blocked = new AtomicReference<>();
        final AtomicReference<SafeTask<?>> waiter = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<RoutingKeys> batches = new CopyOnWriteArrayList<>();
        /** per batch: the continuation's batch sets and every key's queue, taken under the executor lock */
        final List<String> snapshots = new CopyOnWriteArrayList<>();
        /** per batch: the keys in the continuation's blocking set with no task queued behind it */
        final List<String> phantoms = new CopyOnWriteArrayList<>();
        final Condition childDone = Condition.newOneTimeCondition();
        final Condition waiterDone = Condition.newOneTimeCondition();

        Run(AccordExecutor executor, AccordCommandStore store)
        {
            this.executor = executor;
            this.store = store;
        }

        void fail(Throwable fail)
        {
            if (fail != null)
                failure.compareAndSet(null, fail);
        }

        /** a key task: SYNC, declaring no txnId, so it needs nothing loaded beyond the key it shares with the continuation */
        void submitWaiter()
        {
            ExecutionContext context = ExecutionContext.contextFor(null, null, RoutingKeys.of(blocked.get()), LoadKeys.SYNC, FindKeys.CONFLICTS, "waiter");
            waiter.set((SafeTask<?>) store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {},
                                                   (success, fail) -> { fail(fail); waiterDone.signal(); }));
        }

        /** submit the waiter from another thread, so it is not a consequence of the running task, and await {@code state} */
        void submitWaiterExternally(Task.State state)
        {
            join(start(this::submitWaiter));
            awaitUnderLock(() -> waiter.get().compareTo(state) >= 0, "the waiter never reached " + state);
        }

        /** the shape of a DepsCalculator: INCR, ATOMIC wrt its submitter, abandoning partial success, on its txnId */
        void submitChild(TxnId txnId, RoutingKey[] keys, Consumer<RoutingKeys> onBatch)
        {
            ExecutionContext wrapped = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(keys), LoadKeys.INCR, FindKeys.CONFLICTS, "deps");
            ExecutionContext context = new ExecutionContext.Wrapped()
            {
                @Override public ExecutionContext wrapped() { return wrapped; }
                @Override public ExecutionSequence executionSequence() { return ExecutionSequence.ATOMIC; }
                @Override public boolean abandonPartialSuccess() { return true; }
            };
            store.executeContinuation(context,
                                      (Consumer<? super SafeCommandStore>) safeStore -> {
                                          SafeTask<?> child = ((SaferCommandStore) safeStore).task;
                                          RoutingKeys batch = child.nonSync().active;
                                          batches.add(batch);
                                          onBatch.accept(batch);
                                          snapshot(child, keys);
                                      },
                                      (success, fail) -> { fail(fail); childDone.signal(); });
        }

        private void snapshot(SafeTask<?> child, RoutingKey[] keys)
        {
            join(start(() -> executor.executeDirectlyWithLock(() -> {
                SafeTask.NonSyncState<?> nonSync = child.nonSync();
                TreeSet<RoutingKey> phantom = new TreeSet<>();
                StringBuilder out = new StringBuilder();
                out.append("batch ").append(batches.size()).append(' ').append(nonSync.active)
                   .append(": blocking=").append(nonSync.blocking).append(", notBlocking=").append(nonSync.notBlocking);
                for (RoutingKey key : keys)
                {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key);
                    List<SafeTask<?>> queued = entry == null ? List.of() : entry.unsafeQueuedTasks();
                    if (nonSync.blocking != null && nonSync.blocking.contains(key) && queued.size() < 2)
                        phantom.add(key);
                    List<String> brief = new ArrayList<>();
                    for (SafeTask<?> task : queued)
                        brief.add(task.executionContext().reason() + task.toBriefString());
                    out.append("\n    ").append(key).append(": ").append(brief);
                }
                if (!phantom.isEmpty())
                    phantoms.add("batch " + batches.size() + ": " + phantom + "; ");
                snapshots.add(out.toString());
            })));
        }

        /** poll from another thread under the executor lock, which is where task state is published */
        void awaitUnderLock(BooleanSupplier until, String failure)
        {
            AtomicBoolean result = new AtomicBoolean();
            join(start(() -> {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
                while (!result.get() && System.nanoTime() < deadline)
                {
                    executor.executeDirectlyWithLock(() -> result.set(until.getAsBoolean()));
                    if (!result.get())
                        Thread.yield();
                }
            }));
            if (!result.get())
                throw new AssertionError(failure);
        }
    }

    /** as {@link AccordFailedKeyTestHarness#setInMemoryFunctions}, but loading {@code slow} waits for {@code released} */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setSlowLoadFunction(AccordCache.Type type, RoutingKey slow, Condition released)
    {
        type.unsafeSetLoadFunction((BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, k) -> {
            if (slow.equals(k))
                released.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return k instanceof RoutingKey ? new CommandsForKey((RoutingKey) k) : null;
        });
    }

    private static List<RoutingKey> remaining(RoutingKey[] keys, RoutingKeys exclude)
    {
        List<RoutingKey> remaining = new ArrayList<>();
        for (RoutingKey key : keys)
        {
            if (!exclude.contains(key))
                remaining.add(key);
        }
        remaining.sort(RoutingKey::compareTo);
        return remaining;
    }

    private static Thread start(Runnable run)
    {
        Thread thread = new Thread(run);
        thread.start();
        return thread;
    }

    private static void join(Thread thread)
    {
        try { thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)); }
        catch (InterruptedException e) { throw new AssertionError(e); }
        if (thread.isAlive())
            throw new AssertionError(thread + " did not finish");
    }
}
