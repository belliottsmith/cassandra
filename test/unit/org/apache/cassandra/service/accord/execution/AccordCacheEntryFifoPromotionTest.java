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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;
import accord.local.Node.Id;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status;
import org.apache.cassandra.service.accord.execution.SafeTask.NonSyncState;
import org.apache.cassandra.service.accord.execution.Task.State;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Fifo claims on cache entries always run ahead of priority claims, so a task that becomes fifo (on its first run, at
 * setup, or by promotion) overtakes every claim queued ahead of it by priority. To prevent a stream of such tasks on a hot
 * key starving the claims they overtake, on each entry a task becomes fifo on, the lowest waiting task it overtook is
 * promoted to fifo too, behind it, and transitively for each task so promoted.
 *
 * Driven through real {@link SafeTask}s and the real {@code prepareExclusiveMayThrow} / {@code waitOnTxnsExclusive}, in
 * the manner of {@link AccordCacheEntrySafeTaskCycleTest}.
 */
public class AccordCacheEntryFifoPromotionTest
{
    private static final TableId TABLE_ID = TableId.fromUUID(new java.util.UUID(0, 1));
    private static int nextId = 0;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void clearInlineMocks()
    {
        org.mockito.Mockito.framework().clearInlineMocks();
    }

    /**
     * W (sync) leads k by priority; H (INCR) upgrades and overtakes it on k; a later H2 upgrades too. Without promotion
     * W ends up behind both, and behind every subsequent upgrade.
     */
    @Test
    public void overtakenWaiterIsPromotedBehindUpgrader()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry(), k3 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tW = world.commandEntry(), tH = world.commandEntry(), tH2 = world.commandEntry();

        SafeTask<?> w = world.syncTask("W", 10, tW, k);
        world.placeAll(w);
        assertTrue(w.is(State.WAITING_TO_RUN));

        SafeTask<?> h = world.incrTask("H", 20, tH, k, k2);
        world.placeAll(h);
        world.firstRun(h);

        assertTrue("W was overtaken on k, so must be promoted", w.isCacheQueuedFifo());
        assertTrue(w.fifoAt > h.fifoAt);
        // H led k once upgraded, so took it in its first batch (with RELEASE_QUEUE), and W is next
        assertTrue(k.isLockedBy(h));
        assertEquals(Arrays.asList(w), k.unsafeQueuedTasks());
        assertEquals(1, k.unsafeFifoSize());
        assertEquals(0, w.waitingForKeyCount());
        assertEquals(0, w.waitingForTxnCount());

        // a later upgrade can no longer overtake W
        SafeTask<?> h2 = world.incrTask("H2", 30, tH2, k, k3);
        world.placeAll(h2);
        world.firstRun(h2);
        assertEquals(Arrays.asList(w, h2), k.unsafeQueuedTasks());
        assertEquals(2, k.unsafeFifoSize());
        assertFalse("H2 overtook nobody", hasPriorityClaims(k));
    }

    /**
     * Promoting W moves it to fifo on j, where it overtakes X, which must therefore be promoted (behind W) too.
     */
    @Test
    public void promotionCascades()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry(), j = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tW = world.commandEntry(), tX = world.commandEntry(), tH = world.commandEntry();

        SafeTask<?> x = world.syncTask("X", 5, tX, j);
        world.placeAll(x);
        SafeTask<?> w = world.syncTask("W", 10, tW, k, j);
        world.placeAll(w);
        SafeTask<?> h = world.incrTask("H", 20, tH, k, k2);
        world.placeAll(h);
        world.firstRun(h);

        assertTrue(w.isCacheQueuedFifo());
        assertTrue(x.isCacheQueuedFifo());
        assertTrue(h.fifoAt < w.fifoAt && w.fifoAt < x.fifoAt);
        assertTrue(k.isLockedBy(h));
        assertEquals(Arrays.asList(w), k.unsafeQueuedTasks());
        assertEquals(Arrays.asList(w, x), j.unsafeQueuedTasks());
        // W now leads k (behind H's lock) and j, X is behind it on j
        assertEquals(0, w.waitingForKeyCount());
        assertEquals(1, x.waitingForKeyCount());
        assertTrue(x.is(State.WAITING_ON_KEY));
    }

    /**
     * Candidates that share entries: on k, A and B are both overtaken by H; on j, C precedes A precedes B. H promotes only
     * the lowest it overtook on k (A); A's promotion overtakes C on j, so C is promoted behind A. B is left in place: it
     * has not yet been overtaken by more than one fifo claim more than the waiters ahead of it.
     */
    @Test
    public void onePromotionPerKeyCascadesOverSharedEntries()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry(), j = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tA = world.commandEntry(), tB = world.commandEntry(), tC = world.commandEntry(), tH = world.commandEntry();

        SafeTask<?> c = world.syncTask("C", 3, tC, j);
        world.placeAll(c);
        SafeTask<?> a = world.syncTask("A", 5, tA, k, j);
        world.placeAll(a);
        SafeTask<?> b = world.syncTask("B", 8, tB, k, j);
        world.placeAll(b);
        SafeTask<?> h = world.incrTask("H", 20, tH, k, k2);
        world.placeAll(h);
        world.firstRun(h);

        assertTrue(a.isCacheQueuedFifo() && c.isCacheQueuedFifo());
        assertFalse(b.isCacheQueuedFifo());
        assertTrue(k.isLockedBy(h));
        assertEquals(Arrays.asList(a, b), k.unsafeQueuedTasks());
        assertEquals(1, k.unsafeFifoSize());
        assertEquals(Arrays.asList(a, c, b), j.unsafeQueuedTasks());
        assertEquals(2, j.unsafeFifoSize());
        assertEquals(0, a.waitingForKeyCount());
        assertEquals(1, c.waitingForKeyCount());
        assertEquals(2, b.waitingForKeyCount());
    }

    /**
     * Each fifo claim that overtakes a waiter promotes the lowest promotable waiter ahead of it, so a waiter is overtaken
     * at most once more than the number of promotable waiters ahead of it.
     */
    @Test
    public void eachOvertakingPromotesTheLowestWaiter()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry(), k3 = world.keyEntry(), k4 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tA = world.commandEntry(), tB = world.commandEntry(), tC = world.commandEntry();
        AccordCacheEntry<?, ?, ?> tH = world.commandEntry(), tH2 = world.commandEntry(), tH3 = world.commandEntry();

        SafeTask<?> a = world.syncTask("A", 5, tA, k);
        world.placeAll(a);
        SafeTask<?> b = world.syncTask("B", 6, tB, k);
        world.placeAll(b);
        SafeTask<?> c = world.syncTask("C", 7, tC, k);
        world.placeAll(c);
        SafeTask<?> h = world.incrTask("H", 20, tH, k, k2);
        SafeTask<?> h2 = world.incrTask("H2", 21, tH2, k, k3);
        SafeTask<?> h3 = world.incrTask("H3", 22, tH3, k, k4);
        world.placeAll(h);
        world.placeAll(h2);
        world.placeAll(h3);

        world.firstRun(h);
        assertTrue(a.isCacheQueuedFifo());
        assertFalse(b.isCacheQueuedFifo() || c.isCacheQueuedFifo());
        world.firstRun(h2);
        assertTrue(b.isCacheQueuedFifo());
        assertFalse(c.isCacheQueuedFifo());
        world.firstRun(h3);
        assertTrue(c.isCacheQueuedFifo());
        // H took k's lock; H2 and H3 each overtook the waiters (but not each other's promotions), so each is followed by
        // the waiter it promoted
        assertEquals(Arrays.asList(a, h2, b, h3, c), k.unsafeQueuedTasks());
        assertFalse(hasPriorityClaims(k));
    }

    /**
     * A fifo claim only affects queue placement: an ASYNC waiter, and an UNSEQUENCED sync waiter, may be promoted, and
     * keep their declared sequence, so their (non-ATOMIC) failure semantics.
     */
    @Test
    public void promotionDoesNotImplyAtomic()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry(), j = world.keyEntry(), j2 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tU = world.commandEntry(), tS = world.commandEntry(), tH = world.commandEntry(), tH2 = world.commandEntry();

        // UNSEQUENCED, so bagged; moved into the sorted region when H arrives behind it by priority
        SafeTask<?> u = world.task("U", 10, Mode.SYNC, ExecutionContext.ExecutionSequence.UNSEQUENCED, tU, k);
        world.placeAll(u);
        SafeTask<?> h = world.incrTask("H", 20, tH, k, k2);
        world.placeAll(h);
        world.firstRun(h);

        assertTrue(u.isCacheQueuedFifo());
        assertTrue(u.isUnsequenced());
        assertFalse(u.isAtomic());
        assertEquals(Arrays.asList(u), k.unsafeQueuedTasks());

        SafeTask<?> s = world.task("S", 30, Mode.ASYNC, ExecutionContext.ExecutionSequence.BY_PRIORITY, tS, j);
        world.placeAll(s);
        SafeTask<?> h2 = world.incrTask("H2", 40, tH2, j, j2);
        world.placeAll(h2);
        world.firstRun(h2);

        assertTrue(s.isCacheQueuedFifo());
        assertTrue(s.isSequencedByPriority());
        assertFalse(s.isAtomic());
        assertEquals(Arrays.asList(s), j.unsafeQueuedTasks());

        // as is an INCR task holding its txnId, upgraded to fifo on its first run
        assertTrue(h.isCacheQueuedFifo());
        assertFalse(h.isAtomic());
    }

    /**
     * A fifo claim taken at setup (an ATOMIC task) overtakes a waiter, which is promoted once the ATOMIC task has placed
     * all of its claims.
     */
    @Test
    public void setupFifoClaimPromotesOvertaken()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tW = world.commandEntry(), tC = world.commandEntry();

        SafeTask<?> w = world.syncTask("W", 10, tW, k);
        world.placeAll(w);
        assertTrue(w.is(State.WAITING_TO_RUN));

        SafeTask<?> c = world.atomicSetup("C", 20, tC, k);
        assertTrue("C's fifo claim overtook W", w.is(State.WAITING_ON_KEY));
        assertFalse(w.isCacheQueuedFifo());
        world.waitOnTxns(c);

        assertTrue(w.isCacheQueuedFifo());
        assertTrue(w.fifoAt > c.fifoAt);
        assertEquals(Arrays.asList(c, w), k.unsafeQueuedTasks());
        assertEquals(2, k.unsafeFifoSize());
        assertTrue(c.is(State.WAITING_TO_RUN));
        assertEquals(1, w.waitingForKeyCount());
    }

    /** a claim queued behind the upgrader by priority is not overtaken, so is left alone */
    @Test
    public void laterClaimsAreNotPromoted()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tW = world.commandEntry(), tY = world.commandEntry(), tH = world.commandEntry();

        SafeTask<?> w = world.syncTask("W", 10, tW, k);
        world.placeAll(w);
        SafeTask<?> h = world.incrTask("H", 20, tH, k, k2);
        world.placeAll(h);
        SafeTask<?> y = world.syncTask("Y", 30, tY, k);
        world.placeAll(y);
        world.firstRun(h);

        assertTrue(w.isCacheQueuedFifo());
        assertFalse(y.isCacheQueuedFifo());
        assertTrue(k.isLockedBy(h));
        assertEquals(Arrays.asList(w, y), k.unsafeQueuedTasks());
        assertEquals(1, k.unsafeFifoSize());
    }

    /**
     * An ATOMIC consequence inherits its parent's fifoAt, so takes a fifo claim ahead of the waiters its parent promoted
     * (this is intended: it is part of its parent's work, which they were promoted behind). It must still promote the
     * priority waiters it overtakes.
     */
    @Test
    public void inheritedFifoAtOvertakesPromotedWaiters()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tW = world.commandEntry(), tZ = world.commandEntry(), tH = world.commandEntry(), tC = world.commandEntry();

        SafeTask<?> w = world.syncTask("W", 10, tW, k);
        world.placeAll(w);
        SafeTask<?> z = world.syncTask("Z", 15, tZ, k);
        world.placeAll(z);
        SafeTask<?> h = world.incrTask("H", 20, tH, k, k2);
        world.placeAll(h);
        world.firstRun(h);

        // one per key: H promoted W, but not Z
        assertTrue(w.isCacheQueuedFifo());
        assertFalse(z.isCacheQueuedFifo());
        assertTrue(k.isLockedBy(h));
        assertEquals(Arrays.asList(w, z), k.unsafeQueuedTasks());

        // H's ATOMIC consequence inherits H's position and fifoAt, so sorts ahead of W, which was promoted behind H
        SafeTask<?> c = world.atomicSetup("C", h.position, h.fifoAt, tC, k);
        assertEquals(Arrays.asList(c, w, z), k.unsafeQueuedTasks());
        world.waitOnTxns(c);

        // and having overtaken Z (by priority), promotes it
        assertTrue(z.isCacheQueuedFifo());
        assertTrue(z.fifoAt > w.fifoAt);
        assertEquals(Arrays.asList(c, w, z), k.unsafeQueuedTasks());
        assertEquals(3, k.unsafeFifoSize());
    }

    /** a waiting continuation is promoted like any other waiting task */
    @Test
    public void continuationIsPromoted()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry();
        AccordCacheEntry<?, ?, ?> tW = world.commandEntry(), tH = world.commandEntry();

        SafeTask<?> w = world.syncTask("W", 10, tW, k);
        w.setContinuation();
        world.placeAll(w);
        SafeTask<?> h = world.incrTask("H", 20, tH, k, k2);
        world.placeAll(h);
        world.firstRun(h);

        assertTrue(w.isCacheQueuedFifo());
        assertEquals(Arrays.asList(w), k.unsafeQueuedTasks());
    }

    private static boolean hasPriorityClaims(AccordCacheEntry<?, ?, ?> entry)
    {
        return entry.unsafeQueuedTasks().size() > entry.unsafeFifoSize();
    }

    enum Mode { SYNC, ASYNC, INCR }

    static class World
    {
        final AccordCommandStore store;
        final AtomicLong uniqueCreatedAt = new AtomicLong();
        final Map<SafeTask<?>, List<AccordCacheEntry<?, ?, ?>>> declared = new java.util.IdentityHashMap<>();

        World()
        {
            AccordExecutor executor = mock(AccordExecutor.class);
            set(executor, "runnable", new TaskQueueRunnable<Task>());
            set(executor, "loading", new TaskQueueStandalone<SafeTask<?>>(Task.ExecutorQueue.LOADING));
            set(executor, "waiting", new TaskQueueStandalone<SafeTask<?>>(Task.ExecutorQueue.WAITING));
            set(executor, "uniqueCreatedAt", uniqueCreatedAt);

            store = mock(AccordCommandStore.class);
            when(store.executor()).thenReturn(executor);
            when(store.exclusiveExecutor()).thenReturn(new ExclusiveExecutor(executor));
            when(store.id()).thenReturn(0);
            accord.local.NodeCommandStoreService node = mock(accord.local.NodeCommandStoreService.class);
            when(node.id()).thenReturn(new Id(1));
            when(store.node()).thenReturn(node);
        }

        AccordCacheEntry<?, ?, ?> commandEntry()
        {
            AccordCacheEntry<?, ?, ?> entry = new AccordCacheEntry<>(TxnId.fromValues(1, ++nextId, 0, new Id(1)), null);
            entry.unsafeSetStatus(Status.LOADED);
            return entry;
        }

        AccordCacheEntry<?, ?, ?> keyEntry()
        {
            RoutingKey routingKey = new TokenKey(TABLE_ID, DatabaseDescriptor.getPartitioner().getToken(Int32Type.instance.decompose(++nextId)));
            AccordCacheEntry<?, ?, ?> entry = new SaferCommandsForKey.CommandsForKeyCacheEntry(routingKey, null);
            entry.unsafeSetStatus(Status.LOADED);
            return entry;
        }

        /** a sync BY_PRIORITY task declaring txnId {@code refs[0]} and the remaining keys */
        SafeTask<?> syncTask(String name, long position, AccordCacheEntry<?, ?, ?>... refs)
        {
            return task(name, position, Mode.SYNC, ExecutionContext.ExecutionSequence.BY_PRIORITY, refs);
        }

        /** an INCR BY_PRIORITY task declaring txnId {@code refs[0]}, so holding it across runs */
        SafeTask<?> incrTask(String name, long position, AccordCacheEntry<?, ?, ?>... refs)
        {
            return task(name, position, Mode.INCR, ExecutionContext.ExecutionSequence.BY_PRIORITY, refs);
        }

        SafeTask<?> task(String name, long position, Mode mode, ExecutionContext.ExecutionSequence sequence, AccordCacheEntry<?, ?, ?>... refs)
        {
            SafeTask<?> task = create(name, position, mode, sequence, refs);
            task.unsafeSetStateExclusive(State.WAITING_ON_KEY);
            store.executor().waiting.enqueue(task);
            return task;
        }

        private SafeTask<?> create(String name, long position, Mode mode, ExecutionContext.ExecutionSequence sequence, AccordCacheEntry<?, ?, ?>... refs)
        {
            ExecutionContext context = mock(ExecutionContext.class);
            when(context.primaryTxnId()).thenReturn((TxnId) refs[0].key());
            when(context.executionKind()).thenReturn(ExecutionKind.OTHER);
            when(context.describe()).thenReturn(name);

            Function<? super SafeCommandStore, Object> function = ignore -> null;
            SafeTask<Object> task = new SafeTask<>(store, context, function, uniqueCreatedAt);
            task.position = position;
            if (mode != Mode.SYNC)
            {
                set(task, "optional", new NonSyncState<>(task, context));
                task.setNonSyncExclusive();
                if (mode == Mode.INCR)
                    task.setIncrementalExclusive();
            }
            set(task, "keys", refs.length - 1);
            task.setCacheQueuedExclusive();
            task.setSequencedExclusive(sequence);
            declared.put(task, Arrays.asList(refs));
            return task;
        }

        /**
         * An ATOMIC sync task that takes its fifo claims at setup (as completeSetupOfLoaded does), but has not yet reached
         * waitOnTxnsExclusive.
         */
        SafeTask<?> atomicSetup(String name, long position, AccordCacheEntry<?, ?, ?>... refs)
        {
            return atomicSetup(name, position, uniqueCreatedAt.incrementAndGet(), refs);
        }

        /** as above, with the given fifoAt, e.g. inherited from a parent as preSetup does */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        SafeTask<?> atomicSetup(String name, long position, long fifoAt, AccordCacheEntry<?, ?, ?>... refs)
        {
            SafeTask<?> task = create(name, position, Mode.SYNC, ExecutionContext.ExecutionSequence.ATOMIC, refs);
            task.fifoAt = fifoAt;
            task.setCacheQueuedFifoExclusive();
            task.unsafeSetStateExclusive(State.LOADING_REQUIRED);
            for (AccordCacheEntry<?, ?, ?> entry : refs)
            {
                ((Map) task.refs).put(entry.key(), entry.isCommandsForKey() ? new SaferCommandsForKey((AccordCacheEntry) entry)
                                                                           : new SaferCommand((AccordCacheEntry) entry));
                entry.addFifo(task);
            }
            return task;
        }

        /** the real waitOnTxnsExclusive, as invoked once a task's required entries are loaded */
        void waitOnTxns(SafeTask<?> task)
        {
            try
            {
                java.lang.reflect.Method m = SafeTask.class.getDeclaredMethod("waitOnTxnsExclusive");
                m.setAccessible(true);
                m.invoke(task);
            }
            catch (java.lang.reflect.InvocationTargetException e)
            {
                throw new RuntimeException(e.getCause());
            }
            catch (ReflectiveOperationException e)
            {
                throw new RuntimeException(e);
            }
        }

        /** claim every declared entry, txnId first, as waitOnTxnsExclusive / queueOnKeysExclusive would */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        void placeAll(SafeTask<?> task)
        {
            for (AccordCacheEntry<?, ?, ?> entry : declared.get(task))
            {
                ((Map) task.refs).put(entry.key(), entry.isCommandsForKey() ? new SaferCommandsForKey((AccordCacheEntry) entry)
                                                                           : new SaferCommand((AccordCacheEntry) entry));
                RunnableStatus status = task.ensureCacheQueued(entry);
                if (!task.isSync() && entry.isCommandsForKey())
                {
                    switch (status)
                    {
                        case NEWLY_RUNNABLE: task.nonSync().onNewHead(entry); break;
                        case NEWLY_BLOCKING_RUNNABLE: task.nonSync().onNewBlockingHead(entry); break;
                        default: break;
                    }
                }
                else if (status == RunnableStatus.NOT_RUNNABLE)
                {
                    if (task.is(State.WAITING_TO_RUN))
                    {
                        task.unqueue(store.exclusiveExecutor());
                        task.unsafeSetStateExclusive(State.WAITING_ON_KEY);
                        store.executor().waiting.enqueue(task);
                    }
                    set(task, "waitingFor", task.waitingFor + (entry.isCommandsForKey() ? 1 : SafeTask.WAITING_FOR_TXN_INCR));
                }
            }

            if (task.isSync() && task.waitingFor == 0)
            {
                task.unqueue(store.executor().waiting);
                task.waitToRunExclusive();
            }
        }

        /** the real first prepare of an INCR task: upgrade to fifo, take the txnId with HOLD_QUEUE */
        void firstRun(SafeTask<?> task)
        {
            assertEquals("must lead its txnId to prepare", 0, task.waitingFor);
            if (!task.is(State.WAITING_TO_RUN))
            {
                task.unqueue(store.executor().waiting);
                task.waitToRunExclusive();
            }
            task.prepareExclusiveMayThrow();
            assertTrue(task.isCacheQueuedFifo());
        }
    }

    static void set(Object target, String name, Object value)
    {
        try
        {
            for (Class<?> k = target.getClass() ; k != null ; k = k.getSuperclass())
            {
                try
                {
                    Field f = k.getDeclaredField(name);
                    f.setAccessible(true);
                    f.set(target, value);
                    return;
                }
                catch (NoSuchFieldException ignore) { }
            }
            throw new NoSuchFieldException(name);
        }
        catch (Throwable t)
        {
            throw new RuntimeException("could not set " + name, t);
        }
    }
}
