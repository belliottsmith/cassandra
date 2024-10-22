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
import org.apache.cassandra.concurrent.Shutdownable;
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
import static org.apache.cassandra.service.accord.AccordTask.State.FAILED;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AccordExecutor implements CacheSize, AccordCachingState.OnLoaded, AccordCachingState.OnSaved, Executor, Shutdownable
{
    static abstract class LockLoopAccordExecutor extends AccordExecutor
    {
        public enum Mode { RUN_WITH_LOCK, RUN_WITHOUT_LOCK }

        final Condition hasWork = lock.newCondition();

        public LockLoopAccordExecutor(AccordStateCacheMetrics metrics, Function<Runnable, Future<?>> loadExecutor, Function<Runnable, Future<?>> saveExecutor, Executor rangeLoadExecutor, Agent agent)
        {
            super(metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
        }

        abstract Shutdownable shutdownable();

        @Override
        void notifyWorkExclusive()
        {
            hasWork.signal();
        }

        @Override
        void postUnlock()
        {
            if (!submitted.isEmpty() || !lock.tryLock())
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

        @Override public boolean isTerminated() { return shutdownable().isTerminated(); }
        @Override public void shutdown() { shutdownable().shutdown(); }
        @Override public Object shutdownNow() { return shutdownable().shutdownNow(); }
        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return shutdownable().awaitTermination(timeout, unit);
        }

        Interruptible.Task task(Mode mode)
        {
            return mode == Mode.RUN_WITH_LOCK ? this::runWithLock : this::runWithoutLock;
        }

        protected void runWithLock(Interruptible.State state) throws InterruptedException
        {
            lock.lockInterruptibly();
            try
            {
                running = 1;
                while (true)
                {
                    drainSubmittedExclusive();
                    Task op = waitingToRun.poll();

                    if (op != null)
                    {
                        --tasks;
                        try { op.run(); }
                        catch (Throwable t) { op.fail(t); }
                        finally { op.cleanupExclusive(); }
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
                        op.cleanupExclusive();

                    while (true)
                    {
                        drainSubmittedExclusive();
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
    }

    static class InfiniteLoopAccordExecutor extends LockLoopAccordExecutor
    {
        private final Interruptible loop;

        public InfiniteLoopAccordExecutor(Mode mode, String name, AccordStateCacheMetrics metrics, Agent agent)
        {
            this(mode, name, metrics, Stage.READ.executor(), Stage.MUTATION.executor(), Stage.ACCORD_RANGE_LOADER.executor(), agent);
        }

        public InfiniteLoopAccordExecutor(Mode mode, String name, AccordStateCacheMetrics metrics, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor, Executor rangeLoadExecutor, Agent agent)
        {
            this(mode, name, metrics, loadExecutor::submit, saveExecutor::submit, rangeLoadExecutor, agent);
        }

        public InfiniteLoopAccordExecutor(Mode mode, String name, AccordStateCacheMetrics metrics, Function<Runnable, Future<?>> loadExecutor, Function<Runnable, Future<?>> saveExecutor, Executor rangeLoadExecutor, Agent agent)
        {
            super(metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
            this.loop = executorFactory().infiniteLoop(name, task(mode), SAFE, NON_DAEMON, UNSYNCHRONIZED);
        }

        @Override
        Shutdownable shutdownable()
        {
            return loop;
        }
    }

    static abstract class AbstractPooledAccordExecutor extends AccordExecutor
    {
        final ExecutorPlus executor;

        public AbstractPooledAccordExecutor(ExecutorPlus executor, AccordStateCacheMetrics metrics, Function<Runnable, Future<?>> loadExecutor, Function<Runnable, Future<?>> saveExecutor, Executor rangeLoadExecutor, Agent agent)
        {
            super(metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
            this.executor = executor;
        }

        @Override public boolean isTerminated() { return executor.isTerminated(); }
        @Override public void shutdown() { executor.shutdown(); }
        @Override public Object shutdownNow() { return executor.shutdownNow(); }
        @Override
        public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
        {
            return executor.awaitTermination(timeout, units);
        }
    }

    // TODO (expected): move to test package
    static class TestAccordExecutor extends AbstractPooledAccordExecutor
    {
        public TestAccordExecutor(String name, AccordStateCacheMetrics metrics, Agent agent)
        {
            this(name, metrics, Stage.READ.executor(), Stage.MUTATION.executor(), Stage.ACCORD_RANGE_LOADER.executor(), agent);
        }

        public TestAccordExecutor(String name, AccordStateCacheMetrics metrics, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor, Executor rangeLoadExecutor, Agent agent)
        {
            this(name, metrics, loadExecutor::submit, saveExecutor::submit, rangeLoadExecutor, agent);
        }

        public TestAccordExecutor(String name, AccordStateCacheMetrics metrics, Function<Runnable, Future<?>> loadExecutor, Function<Runnable, Future<?>> saveExecutor, Executor rangeLoadExecutor, Agent agent)
        {
            super(executorFactory().sequential(name), metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
        }

        protected void run()
        {
            lock.lock();
            try
            {
                running = 1;
                while (true)
                {
                    drainSubmittedExclusive();
                    Task op = waitingToRun.poll();
                    if (op == null)
                        return;

                    --tasks;
                    try { op.run(); }
                    catch (Throwable t) { op.fail(t); }
                    finally { op.cleanupExclusive(); }
                }
            }
            catch (Throwable t)
            {
                throw t;
            }
            finally
            {
                running = 0;
                lock.unlock();
                postUnlock();
            }
        }

        @Override
        void postUnlock()
        {
            if (!submitted.isEmpty() || !waitingToRun.isEmpty())
                executor.execute(this::run);
        }

        @Override
        void notifyWorkExclusive()
        {
            executor.execute(this::run);
        }
    }

    // WARNING: this is a shared object, so close is NOT idempotent
    public static final class ExclusiveStateCache implements AutoCloseable
    {
        final ReentrantLock lock;
        final AccordStateCache cache;

        public ExclusiveStateCache(ReentrantLock lock, AccordStateCache cache)
        {
            this.lock = lock;
            this.cache = cache;
        }

        public AccordStateCache get()
        {
            return cache;
        }

        @Override
        public void close()
        {
            lock.unlock();
        }
    }

    private static final int MAX_QUEUED_LOADS_PER_EXECUTOR = 64;
    private static final int MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR = 8;

    final ReentrantLock lock = new ReentrantLock();

    final Agent agent;
    private final AccordStateCache cache;
    private final Function<Runnable, Future<?>> loadExecutor;
    private final Executor rangeLoadExecutor;

    final ConcurrentLinkedStack<Object> submitted = new ConcurrentLinkedStack<>();

    private final TaskPriorityHeap<AccordTask<?>> scanningRanges = new TaskPriorityHeap<>(); // never queried, just parked here while scanning
    private final TaskPriorityHeap<AccordTask<?>> loading = new TaskPriorityHeap<>(); // never queried, just parked here while loading

    private @Nullable TaskPriorityHeap<AccordTask<?>> waitingToLoadRangeTxns; // this is kept null whenever the queue is empty (i.e. normally)
    private final TaskPriorityHeap<AccordTask<?>> waitingToLoadRangeTxnsCollection = new TaskPriorityHeap<>();

    private final TaskPriorityHeap<AccordTask<?>> waitingToLoad = new TaskPriorityHeap<>();
    final TaskPriorityHeap<Task> waitingToRun = new TaskPriorityHeap<>();
    private final AccordCachingState.OnLoaded onRangeLoaded = this::onRangeLoaded;
    private final ExclusiveStateCache locked;

    private int nextPosition;
    private int activeLoads, activeRangeLoads;
    int tasks;
    int running;

    AccordExecutor(AccordStateCacheMetrics metrics, Function<Runnable, Future<?>> loadExecutor, Function<Runnable, Future<?>> saveExecutor, Executor rangeLoadExecutor, Agent agent)
    {
        this.cache = new AccordStateCache(saveExecutor, this, 8 << 20, metrics);
        this.loadExecutor = loadExecutor;
        this.rangeLoadExecutor = rangeLoadExecutor;
        this.agent = agent;
        this.locked = new ExclusiveStateCache(lock, cache);;
    }

    public ExclusiveStateCache lockCache()
    {
        //noinspection LockAcquiredButNotSafelyReleased
        lock.lock();
        return locked;
    }

    public AccordStateCache cacheUnsafe()
    {
        return cache;
    }

    public boolean hasTasks()
    {
        return !submitted.isEmpty() || tasks > 0 || running > 0;
    }

    abstract void notifyWorkExclusive();
    abstract void postUnlock();

    private void enqueueLoadsExclusive()
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

                for (AccordTask<?> op : cache.load(loadExecutor, load, onLoaded))
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

    private void consumeExclusive(Object object)
    {
        try
        {
            if (object instanceof AccordTask<?>)
                loadExclusive((AccordTask<?>) object);
            else
                ((SubmitAsync) object).acceptExclusive(this);
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
                enqueueLoadsExclusive();
                break;
            case LOADING:
                loading.append(op);
                break;
            case WAITING_TO_RUN:
                op.runQueuedAt = nanoTime();
                boolean signal = waitingToRun.isEmpty();
                waitingToRun.append(op);
                if (signal) notifyWorkExclusive();
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
        onEvent(AccordExecutor::loadExclusive, Function.identity(), operation);
    }

    public <R> void cancel(AccordTask<R> operation)
    {
        onEvent(AccordExecutor::cancelExclusive, OnCancel::new, operation);
    }

    public void onScannedRanges(AccordTask<?> op, Throwable fail)
    {
        onEvent(AccordExecutor::onScannedRangesExclusive, OnScannedRanges::new, op, fail);
    }

    public <K, V> void onSaved(AccordCachingState<K, V> saved, Object identity, Throwable fail)
    {
        onEvent(AccordExecutor::onSavedExclusive, OnSaved::new, saved, identity, fail);
    }

    @Override
    public <K, V> void onLoaded(AccordCachingState<K, V> loaded, V value, Throwable fail)
    {
        onEvent(AccordExecutor::onLoadedExclusive, OnLoaded::new, loaded, value, fail, false);
    }

    public <K, V> void onRangeLoaded(AccordCachingState<K, V> loaded, V value, Throwable fail)
    {
        onEvent(AccordExecutor::onLoadedExclusive, OnLoaded::new, loaded, value, fail, true);
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
                drainSubmittedExclusive();
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

    void drainSubmittedExclusive()
    {
        submitted.drain(AccordExecutor::consumeExclusive, this, true);
    }

    private void submitExclusive(AsyncPromise<Void> result, Runnable run)
    {
        ++tasks;
        PlainRunnable op = new PlainRunnable(result, run);
        op.queuePosition = ++nextPosition;
        boolean signal = waitingToRun.isEmpty();
        waitingToRun.append(op);
        if (signal) notifyWorkExclusive();
    }

    private void loadExclusive(AccordTask<?> op)
    {
        ++tasks;
        op.queuePosition = ++nextPosition;
        op.setupExclusive();
        updateQueue(op);
    }

    private void cancelExclusive(AccordTask<?> op)
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

                // TODO (required): set RUNNING before we release lock
            case RUNNING:
            case COMPLETING:
            case WAITING_TO_FINISH:
            case FINISHED:
            case FAILED:
                return; // cannot safely cancel
        }
        op.cancelExclusive();
    }

    private void onScannedRangesExclusive(AccordTask<?> op, Throwable fail)
    {
        --activeLoads;
        --activeRangeLoads;
        if (fail != null)
        {
            --tasks;
            try
            {
                op.fail(fail);
            }
            catch (Throwable t)
            {
                agent.onUncaughtException(t);
            }
        }
        else if (op.state() != FAILED)
        {
            op.rangeLoader().scanned();
            updateQueue(scanningRanges, op);
        }
        enqueueLoadsExclusive();
    }

    private <K, V> void onSavedExclusive(AccordCachingState<K, V> state, Object identity, Throwable fail)
    {
        cache.saved(state, identity, fail);
    }

    private <K, V> void onLoadedExclusive(AccordCachingState<K, V> loaded, V value, Throwable fail, boolean isForRange)
    {
        boolean enqueueLoads = activeLoads-- == MAX_QUEUED_LOADS_PER_EXECUTOR;
        if (isForRange) enqueueLoads |= activeRangeLoads-- == MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR;
        if (enqueueLoads)
            enqueueLoadsExclusive();

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

                    removeFromQueue(queue, op);
                    --tasks;
                }
                cache.failedToLoad(loaded);
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
                cache.loaded(loaded, value);
                if (enqueueWork)
                    notifyWorkExclusive();
            }
        }
    }

    public boolean isInThread()
    {
        return lock.isHeldByCurrentThread();
    }

    public Future<?> submit(Runnable run)
    {
        AsyncPromise<Void> result = new AsyncPromise<>();
        onEvent(AccordExecutor::submitExclusive, SubmitPlainRunnable::new, result, run);
        return result;
    }

    public void execute(Runnable command)
    {
        submit(command);
    }

    @Override
    public void setCapacity(long bytes)
    {
        Invariants.checkState(isInThread());
        cache.setCapacity(bytes);
    }

    @Override
    public long capacity()
    {
        return cache.capacity();
    }

    @Override
    public int size()
    {
        return cache.size();
    }

    @Override
    public long weightedSize()
    {
        return cache.weightedSize();
    }

    public static abstract class Task extends IntrusivePriorityHeap.Node
    {
        int queuePosition;

        /**
         * Prepare to run while holding the state cache lock
         */
        abstract protected void preRunExclusive();

        /**
         * Run the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract protected void run();
        /**
         * Fail the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract protected void fail(Throwable fail);

        /**
         * Cleanup the command while holding the state cache lock
         */
        abstract protected void cleanupExclusive();
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

        @Override
        protected void preRunExclusive() {}

        @Override
        protected void run()
        {
            run.run();
            result.trySuccess(null);
        }

        @Override
        protected void fail(Throwable t)
        {
            result.tryFailure(t);
            agent.onUncaughtException(t);
        }

        @Override
        protected void cleanupExclusive() {}
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
        abstract void acceptExclusive(AccordExecutor executor);
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
        void acceptExclusive(AccordExecutor executor)
        {
            executor.submitExclusive(result, run);
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
        void acceptExclusive(AccordExecutor executor)
        {
            executor.onLoadedExclusive(loaded, success(), fail(), isForRange());
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
        void acceptExclusive(AccordExecutor executor)
        {
            executor.onScannedRangesExclusive(scanned, fail);
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
        void acceptExclusive(AccordExecutor executor)
        {
            executor.onSavedExclusive(state, identity, fail);
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
        void acceptExclusive(AccordExecutor executor)
        {
            executor.cancelExclusive(cancel);
        }
    }
}
