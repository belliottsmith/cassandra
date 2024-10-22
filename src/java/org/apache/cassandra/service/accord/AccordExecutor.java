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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.IntrusivePriorityHeap;
import accord.utils.Invariants;
import accord.utils.QuadConsumer;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;
import accord.utils.TriConsumer;
import accord.utils.TriFunction;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.ConcurrentLinkedStack;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.EVICTED;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordExecutor implements CacheSize, AccordCachingState.OnLoaded, AccordCachingState.OnSaved, Executor
{
    private static final int MAX_QUEUED_LOADS_PER_EXECUTOR = 64;
    private static final int MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR = 8;

    private final Interruptible loop;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hasWork = lock.newCondition();

    final Agent agent;
    final AccordStateCache stateCache;
    private final Function<Runnable, Future<?>> loadExecutor;
    private final Executor rangeLoadExecutor;

    private final ConcurrentLinkedStack<Object> submitted = new ConcurrentLinkedStack<>();

    private final TaskPriorityHeap<AccordTask<?>> scanningRanges = new TaskPriorityHeap<>(); // never queried, just parked here while scanning
    private final TaskPriorityHeap<AccordTask<?>> loading = new TaskPriorityHeap<>(); // never queried, just parked here while loading

    private @Nullable TaskPriorityHeap<AccordTask<?>> waitingToLoadRangeTxns; // this is kept null whenever the queue is empty (i.e. normally)
    private final TaskPriorityHeap<AccordTask<?>> waitingToLoadRangeTxnsCollection = new TaskPriorityHeap<>();

    private final TaskPriorityHeap<AccordTask<?>> waitingToLoad = new TaskPriorityHeap<>();
    private final TaskPriorityHeap<Task> waitingToRun = new TaskPriorityHeap<>();
    private final AccordCachingState.OnLoaded onRangeLoaded = this::onRangeLoaded;

    private int nextPosition;
    private int activeLoads, activeRangeLoads;
    private int tasks;
    private int running;

    AccordExecutor(String name, AccordStateCacheMetrics metrics, Agent agent)
    {
        this(name, metrics, Stage.READ.executor(), Stage.MUTATION.executor(), Stage.ACCORD_RANGE_LOADER.executor(), agent);
    }

    AccordExecutor(String name, AccordStateCacheMetrics metrics, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor, Executor rangeLoadExecutor, Agent agent)
    {
        this(name, metrics, loadExecutor::submit, saveExecutor::submit, rangeLoadExecutor, agent);
    }

    AccordExecutor(String name, AccordStateCacheMetrics metrics, Function<Runnable, Future<?>> loadExecutor, Function<Runnable, Future<?>> saveExecutor, Executor rangeLoadExecutor, Agent agent)
    {
        this.stateCache = new AccordStateCache(saveExecutor, this, 8 << 20, metrics);
        this.loadExecutor = loadExecutor;
        this.rangeLoadExecutor = rangeLoadExecutor;
        this.loop = executorFactory().infiniteLoop(name, this::run, SAFE, NON_DAEMON, UNSYNCHRONIZED);
        this.agent = agent;
    }

    public boolean hasTasks()
    {
        return !submitted.isEmpty() || tasks > 0 || running > 0;
    }

    private void enqueueWorkExclusive()
    {
        hasWork.signal();
    }

    private void enqueueLoadsUnsafe()
    {
        outer: while (true)
        {
            TaskPriorityHeap<AccordTask<?>> queue = waitingToLoadRangeTxns == null || activeRangeLoads >= MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR ? waitingToLoad : waitingToLoadRangeTxns;
            AccordTask<?> next = queue.peek();
            if (next == null)
            {
                Invariants.checkState(waitingToLoad.isEmpty());
                return;
            }

            while (true)
            {
                boolean isForRangeTxn = next.hasRanges();
                AccordCachingState<?, ?> load = next.peekWaitingToLoad();
                if (isForRangeTxn)
                {
                    if (next.rangeLoader().hasStarted())
                    {
                        // if we haven't submitted our range scan yet, this goes first
                        for (AccordTask<?> op : load.loadingOrWaiting().waiters())
                        {
                            if (!op.hasRanges())
                            {
                                isForRangeTxn = false;
                                break;
                            }
                        }
                    }

                    if (isForRangeTxn && activeRangeLoads >= MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR)
                    {
                        if (queue == waitingToLoad)
                        {
                            removeFromQueue(queue, next);
                            if (waitingToLoadRangeTxns == null)
                                waitingToLoadRangeTxns = waitingToLoadRangeTxnsCollection;
                            waitingToLoadRangeTxns.append(next);
                        }
                        continue outer;
                    }

                    if (!next.rangeLoader().hasStarted())
                    {
                        removeFromQueue(queue, next);
                        scanningRanges.append(next);
                        ++activeRangeLoads;
                        ++activeLoads;
                        next.rangeLoader().start(rangeLoadExecutor);
                        continue outer;
                    }
                }

                Invariants.checkState(load != null);
                AccordCachingState.OnLoaded onLoaded = this;
                ++activeLoads;
                if (isForRangeTxn)
                {
                    ++activeRangeLoads;
                    onLoaded = onRangeLoaded;
                }

                for (AccordTask<?> op : stateCache.load(loadExecutor, load, onLoaded))
                {
                    if (op == next) continue;
                    if (!op.onLoading(load))
                        updateQueue(waitingToLoadQueue(op), op);
                }
                Object prev = next.pollWaitingToLoad();
                Invariants.checkState(prev == load);
                if (next.peekWaitingToLoad() == null)
                    break;

                if (activeLoads >= MAX_QUEUED_LOADS_PER_EXECUTOR)
                    return;
            }

            updateQueue(queue, next);
        }
    }

    private TaskPriorityHeap<?> waitingToLoadQueue(AccordTask<?> op)
    {
        if (op.rangeLoader() == null)
            return waitingToLoad;

        if (waitingToLoad.contains(op))
            return waitingToLoad;
        if (scanningRanges.contains(op))
            return scanningRanges;
        Invariants.checkState(waitingToLoadRangeTxns != null && waitingToLoadRangeTxns.contains(op));
        return waitingToLoadRangeTxns;
    }

    private void consumeUnsafe(Object object)
    {
        try
        {
            if (object instanceof AccordTask<?>)
                loadUnsafe((AccordTask<?>) object);
            else
                ((SubmitAsync) object).acceptUnsafe(this);
        }
        catch (Throwable t)
        {
            agent.onUncaughtException(t);
        }
    }

    private void updateQueue(TaskPriorityHeap<?> removeFrom, AccordTask<?> op)
    {
        removeFromQueue(removeFrom, op);
        updateQueue(op);
    }

    private void removeFromQueue(TaskPriorityHeap removeFrom, AccordTask<?> op)
    {
        removeFrom.remove(op);
        if (removeFrom == waitingToLoadRangeTxns && waitingToLoadRangeTxns.isEmpty())
            waitingToLoadRangeTxns = null;
    }

    private void updateQueue(AccordTask<?> op)
    {
        switch (op.state())
        {
            default: throw new AssertionError("Unexpected state: " + op.state());
            case WAITING_TO_LOAD:
                waitingToLoad.append(op);
                enqueueLoadsUnsafe();
                break;
            case LOADING:
                loading.append(op);
                break;
            case WAITING_TO_RUN:
                op.runQueuedAt = nanoTime();
                boolean signal = waitingToRun.isEmpty();
                waitingToRun.append(op);
                if (signal) enqueueWorkExclusive();
                break;
        }
    }

    private TaskPriorityHeap<?> queue(AccordTask<?> op)
    {
        switch (op.state())
        {
            default: throw new AssertionError("Unexpected state: " + op.state());
            case WAITING_TO_LOAD:
                return waitingToLoadQueue(op);
            case LOADING:
                return loading;
            case WAITING_TO_RUN:
                return waitingToRun;
        }
    }

    public <R> void submit(AccordTask<R> operation)
    {
        onEvent(AccordExecutor::loadUnsafe, Function.identity(), operation);
    }

    public <R> void cancel(AccordTask<R> operation)
    {
        onEvent(AccordExecutor::cancelUnsafe, OnCancel::new, operation);
    }

    public void onScannedRanges(AccordTask<?> op, Throwable fail)
    {
        onEvent(AccordExecutor::onScannedRangesUnsafe, OnScannedRanges::new, op, fail);
    }

    public <K, V> void onSaved(AccordCachingState<K, V> saved, Object identity, Throwable fail)
    {
        onEvent(AccordExecutor::onSavedUnsafe, OnSaved::new, saved, identity, fail);
    }

    @Override
    public <K, V> void onLoaded(AccordCachingState<K, V> loaded, V value, Throwable fail)
    {
        onEvent(AccordExecutor::onLoadedUnsafe, OnLoaded::new, loaded, value, fail, false);
    }

    public <K, V> void onRangeLoaded(AccordCachingState<K, V> loaded, V value, Throwable fail)
    {
        onEvent(AccordExecutor::onLoadedUnsafe, OnLoaded::new, loaded, value, fail, true);
    }

    private <P1> void onEvent(BiConsumer<AccordExecutor, P1> sync, Function<P1, ?> async, P1 p1)
    {
        onEvent((e, c, p1a, p2a, p3) -> c.accept(e, p1a), (f, p1a, p2a, p3) -> f.apply(p1a), sync, async, p1, null, null);
    }

    private <P1, P2> void onEvent(TriConsumer<AccordExecutor, P1, P2> sync, BiFunction<P1, P2, ?> async, P1 p1, P2 p2)
    {
        onEvent((e, c, p1a, p2a, p3) -> c.accept(e, p1a, p2a), (f, p1a, p2a, p3) -> f.apply(p1a, p2a), sync, async, p1, p2, null);
    }

    private <P1, P2, P3> void onEvent(QuadConsumer<AccordExecutor, P1, P2, P3> sync, TriFunction<P1, P2, P3, ?> async, P1 p1, P2 p2, P3 p3)
    {
        onEvent((e, c, p1a, p2a, p3a) -> c.accept(e, p1a, p2a, p3a), TriFunction::apply, sync, async, p1, p2, p3);
    }

    private <P1, P2, P3, P4> void onEvent(QuintConsumer<AccordExecutor, P1, P2, P3, P4> sync, QuadFunction<P1, P2, P3, P4, Object> async, P1 p1, P2 p2, P3 p3, P4 p4)
    {
        onEvent(sync, async, p1, p1, p2, p3, p4);
    }

    private <P1s, P1a, P2, P3, P4> void onEvent(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        boolean applySync = true;
        if (!lock.tryLock())
        {
            submitted.push(async.apply(p1a, p2, p3, p4));
            applySync = false;
            if (!lock.tryLock())
                return;
        }

        try
        {
            try
            {
                drainSubmittedUnsafe();
            }
            catch (Throwable t)
            {
                if (applySync)
                {
                    try { sync.accept(this, p1s, p2, p3, p4); }
                    catch (Throwable t2) { t.addSuppressed(t2); }
                }
                throw t;
            }
            if (applySync)
                sync.accept(this, p1s, p2, p3, p4);
        }
        finally
        {
            lock.unlock();
            postUnlock();
        }
    }

    private void drainSubmittedUnsafe()
    {
        submitted.drain(AccordExecutor::consumeUnsafe, this, true);
    }

    private void postUnlock()
    {
        if (submitted.isEmpty() || !lock.tryLock())
            return;

        try
        {
            hasWork.signal();
        }
        finally
        {
            lock.unlock();
        }
    }

    private void submitUnsafe(AsyncPromise<Void> result, Runnable run)
    {
        ++tasks;
        PlainRunnable op = new PlainRunnable(result, run);
        op.queuePosition = ++nextPosition;
        boolean signal = waitingToRun.isEmpty();
        waitingToRun.append(op);
        if (signal) hasWork.signal();
    }

    private void loadUnsafe(AccordTask<?> op)
    {
        ++tasks;
        op.queuePosition = ++nextPosition;
        op.setup();
        updateQueue(op);
    }

    private void cancelUnsafe(AccordTask<?> op)
    {
        // TODO (required): loading cache entries are now eligible for eviction; make sure we handle correctly
        switch (op.state())
        {
            default:
            case INITIALIZED:
                throw new AssertionError("Unexpected state for: " + op);

            case LOADING:
                loading.remove(op);
                break;

            case WAITING_TO_LOAD:
                removeFromQueue(waitingToLoadQueue(op), op);
                break;

            case WAITING_TO_RUN:
                waitingToRun.remove(op);
                break;

            case RUNNING:
            case COMPLETING:
            case WAITING_TO_FINISH:
            case FINISHED:
            case FAILED:
                return; // cannot safely cancel
        }
        op.realCancel();
    }

    private void onScannedRangesUnsafe(AccordTask<?> op, Throwable fail)
    {
        --activeLoads;
        --activeRangeLoads;
        if (fail != null)
        {
            --tasks;
            try { op.fail(fail); }
            catch (Throwable t) { agent.onUncaughtException(t); }
            enqueueLoadsUnsafe();
        }
        else
        {
            op.rangeLoader().scanned();
            updateQueue(scanningRanges, op);
        }
    }

    private <K, V> void onSavedUnsafe(AccordCachingState<K, V> state, Object identity, Throwable fail)
    {
        cache().saved(state, identity, fail);
    }

    private <K, V> void onLoadedUnsafe(AccordCachingState<K, V> loaded, V value, Throwable fail, boolean isForRange)
    {
        boolean enqueueLoads = activeLoads-- == MAX_QUEUED_LOADS_PER_EXECUTOR;
        if (isForRange) enqueueLoads |= activeRangeLoads-- == MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR;
        if (enqueueLoads)
            enqueueLoadsUnsafe();

        if (loaded.status() == EVICTED)
            return;

        try (BufferList<AccordTask<?>> ops = loaded.loading().copyWaiters())
        {
            if (fail != null)
            {
                for (AccordTask<?> op : ops)
                {
                    TaskPriorityHeap queue = queue(op);
                    try
                    {
                        op.fail(fail);
                    }
                    catch (Throwable t)
                    {
                        agent.onUncaughtException(t);
                    }

                    queue.remove(op);
                    --tasks;
                }
                cache().failedToLoad(loaded);
            }
            else
            {
                boolean enqueueWork = false;
                for (AccordTask<?> op : ops)
                {
                    op.onLoad(loaded);
                    if (!op.isLoading())
                    {
                        enqueueWork = true;
                        loading.remove(op);
                        waitingToRun.append(op);
                    }
                }
                cache().loaded(loaded, value);
                if (enqueueWork)
                    enqueueWorkExclusive();
            }
        }
    }

    protected void run(Interruptible.State state) throws InterruptedException
    {
        lock.lock();
        try
        {
            running = 1;
            while (true)
            {
                drainSubmittedUnsafe();
                Task op = waitingToRun.poll();

                if (op != null)
                {
                    --tasks;
                    try { op.run(); }
                    catch (Throwable t) { op.fail(t); }
                    finally { op.cleanup(); }
                }
                else
                {
                    running = 0;
                    hasWork.await();
                    if (waitingToRun.isEmpty() && submitted.isEmpty())
                        return;
                }
            }
        }
        catch (Throwable t)
        {
            running = 0;
            throw t;
        }
        finally
        {
            lock.unlock();
            postUnlock();
        }
    }

    protected void runWithLock(Interruptible.State state) throws InterruptedException
    {
        lock.lock();
        try
        {
            running = 1;
            while (true)
            {
                drainSubmittedUnsafe();
                Task op = waitingToRun.poll();

                if (op != null)
                {
                    --tasks;
                    try { op.run(); }
                    catch (Throwable t) { op.fail(t); }
                    finally { op.cleanup(); }
                }
                else
                {
                    running = 0;
                    hasWork.await();
                }
            }
        }
        catch (Throwable t)
        {
            running = 0;
            throw t;
        }
        finally
        {
            lock.unlock();
            postUnlock();
        }
    }

    protected void runWithoutLock(Interruptible.State state) throws InterruptedException
    {
        Task op = null;
        while (true)
        {
            lock.lock();
            try
            {
                if (op != null)
                    op.cleanup();

                while (true)
                {
                    drainSubmittedUnsafe();
                    op = waitingToRun.poll();
                    if (op != null)
                        break;
                    hasWork.await();
                }
                --tasks;
                ++running;
            }
            finally
            {
                lock.unlock();
            }

            try { op.run(); }
            catch (Throwable t)
            {
                try { op.fail(t); }
                catch (Throwable t2)
                {
                    t2.addSuppressed(t);
                    agent.onUncaughtException(t2);
                }
            }
        }
    }

    public boolean isInThread()
    {
        return lock.isHeldByCurrentThread();
    }

    public void shutdown()
    {
        loop.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return loop.awaitTermination(timeout, unit);
    }

    public Future<?> submit(Runnable run)
    {
        AsyncPromise<Void> result = new AsyncPromise<>();
        onEvent(AccordExecutor::submitUnsafe, SubmitPlainRunnable::new, result, run);
        return result;
    }

    public void execute(Runnable command)
    {
        submit(command);
    }

    @VisibleForTesting
    public AccordStateCache cache()
    {
        return stateCache;
    }

    @Override
    public void setCapacity(long bytes)
    {
        Invariants.checkState(isInThread());
        stateCache.setCapacity(bytes);
    }

    @Override
    public long capacity()
    {
        return stateCache.capacity();
    }

    @Override
    public int size()
    {
        return stateCache.size();
    }

    @Override
    public long weightedSize()
    {
        return stateCache.weightedSize();
    }

    public static abstract class Task extends IntrusivePriorityHeap.Node
    {
        int queuePosition;

        abstract protected void run();
        abstract protected void fail(Throwable fail);
        abstract protected void cleanup();
    }

    public class PlainRunnable extends Task
    {
        final AsyncPromise<Void> result;
        final Runnable run;

        public PlainRunnable(AsyncPromise<Void> result, Runnable run)
        {
            this.result = result;
            this.run = run;
        }

        protected void run()
        {
            run.run();
            result.trySuccess(null);
        }

        protected void fail(Throwable t)
        {
            result.tryFailure(t);
            agent.onUncaughtException(t);
        }

        @Override
        protected void cleanup()
        {
        }
    }

    static final class TaskPriorityHeap<T extends Task> extends IntrusivePriorityHeap<T>
    {
        @Override
        public int compare(T o1, T o2)
        {
            return Integer.compare(o1.queuePosition, o2.queuePosition);
        }
        public void append(T op)
        {
            super.append(op);
        }

        public T poll()
        {
            ensureHeapified();
            return pollNode();
        }

        public T peek()
        {
            ensureHeapified();
            return peekNode();
        }

        public void remove(T remove)
        {
            super.remove(remove);
        }

        public boolean contains(T contains)
        {
            return super.contains(contains);
        }
    }

    private abstract static class SubmitAsync
    {
        abstract void acceptUnsafe(AccordExecutor executor);
    }

    private static class SubmitPlainRunnable extends SubmitAsync
    {
        final AsyncPromise<Void> result;
        final Runnable run;

        private SubmitPlainRunnable(AsyncPromise<Void> result, Runnable run)
        {
            this.result = result;
            this.run = run;
        }

        @Override
        void acceptUnsafe(AccordExecutor executor)
        {
            executor.submitUnsafe(result, run);
        }
    }

    private static class OnLoaded<K, V> extends SubmitAsync
    {
        static final int FAIL = 1;
        static final int RANGE = 2;
        final AccordCachingState<K, V> loaded;
        final Object result;
        final int flags;

        OnLoaded(AccordCachingState<K, V> loaded, V success, Throwable fail, boolean isForRange)
        {
            this.loaded = loaded;
            int flags = isForRange ? RANGE : 0;
            if (fail == null)
            {
                result = success;
            }
            else
            {
                result = fail;
                flags |= FAIL;
            }
            this.flags = flags;
        }

        V success()
        {
            return (flags & FAIL) == 0 ? (V) result : null;
        }

        Throwable fail()
        {
            return (flags & FAIL) == 0 ? null : (Throwable) result;
        }

        boolean isForRange()
        {
            return (flags & RANGE) != 0;
        }

        @Override
        void acceptUnsafe(AccordExecutor executor)
        {
            executor.onLoadedUnsafe(loaded, success(), fail(), isForRange());
        }
    }

    private static class OnScannedRanges extends SubmitAsync
    {
        final AccordTask<?> scanned;
        final Throwable fail;

        private OnScannedRanges(AccordTask<?> scanned, Throwable fail)
        {
            this.scanned = scanned;
            this.fail = fail;
        }

        @Override
        void acceptUnsafe(AccordExecutor executor)
        {
            executor.onScannedRangesUnsafe(scanned, fail);
        }
    }

    private static class OnSaved<K, V> extends SubmitAsync
    {
        final AccordCachingState<K, V> state;
        final Object identity;
        final Throwable fail;

        private OnSaved(AccordCachingState<K, V> state, Object identity, Throwable fail)
        {
            this.state = state;
            this.identity = identity;
            this.fail = fail;
        }

        @Override
        void acceptUnsafe(AccordExecutor executor)
        {
            executor.onSavedUnsafe(state, identity, fail);
        }
    }

    private static class OnCancel<R> extends SubmitAsync
    {
        final AccordTask<R> cancel;

        private OnCancel(AccordTask<R> cancel)
        {
            this.cancel = cancel;
        }

        @Override
        void acceptUnsafe(AccordExecutor executor)
        {
            executor.cancelUnsafe(cancel);
        }
    }
}
