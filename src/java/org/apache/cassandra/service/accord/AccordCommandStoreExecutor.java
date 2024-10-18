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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.concurrent.ConcurrentLinkedStack;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.utils.Invariants.illegalState;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.EVICTED;

public class AccordCommandStoreExecutor implements CacheSize, AccordCachingState.OnLoaded, AccordCachingState.OnSaved, Executor
{
    private final ExecutorPlus delegate;

    private static final int MAX_QUEUED_LOADS_PER_EXECUTOR = 64;
    private static final int MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR = 8;

    private final ReentrantLock lock = new ReentrantLock();

    final Agent agent;
    final AccordStateCache stateCache;
    private final Function<Runnable, Future<?>> loadExecutor;
    private final Executor rangeLoadExecutor;

    private final ConcurrentLinkedStack<Object> submitted = new ConcurrentLinkedStack<>();

    private final OperationPriorityHeap scanningRanges = new OperationPriorityHeap<>(); // never queried, just parked here while scanning
    private final OperationPriorityHeap loading = new OperationPriorityHeap<>(); // never queried, just parked here while loading

    private @Nullable OperationPriorityHeap waitingToLoadRangeTxns; // this is kept null whenever the queue is empty (i.e. normally)
    private final OperationPriorityHeap waitingToLoadRangeTxnsCollection = new OperationPriorityHeap<>();

    private final OperationPriorityHeap waitingToLoad = new OperationPriorityHeap<>();
    private final OperationPriorityHeap waitingToRun = new OperationPriorityHeap<>();
    private final Runnable work = this::run;
    private final AccordCachingState.OnLoaded onRangeLoaded = this::onRangeLoaded;

    private int nextPosition;
    private int activeLoads, activeRangeLoads;
    private int tasks;
    private boolean running;

    private volatile int isWorkQueued;
    private static final AtomicIntegerFieldUpdater<AccordCommandStoreExecutor> isWorkQueuedUpdater = AtomicIntegerFieldUpdater.newUpdater(AccordCommandStoreExecutor.class, "isWorkQueued");

    AccordCommandStoreExecutor(AccordStateCacheMetrics metrics, SequentialExecutorPlus delegate, Agent agent)
    {
        this(metrics, Stage.READ.executor(), Stage.MUTATION.executor(), Stage.ACCORD_RANGE_LOADER.executor(), delegate, agent);
    }

    AccordCommandStoreExecutor(AccordStateCacheMetrics metrics, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor, Executor rangeLoadExecutor, SequentialExecutorPlus delegate, Agent agent)
    {
        this(metrics, loadExecutor::submit, saveExecutor::submit, rangeLoadExecutor, delegate, agent);
    }

    AccordCommandStoreExecutor(AccordStateCacheMetrics metrics, Function<Runnable, Future<?>> loadExecutor, Function<Runnable, Future<?>> saveExecutor, Executor rangeLoadExecutor, SequentialExecutorPlus delegate, Agent agent)
    {
        this.stateCache = new AccordStateCache(saveExecutor, this, 8 << 20, metrics);
        this.loadExecutor = loadExecutor;
        this.rangeLoadExecutor = rangeLoadExecutor;
        this.delegate = delegate;
        this.agent = agent;
    }

    public boolean hasTasks()
    {
        return !submitted.isEmpty() || tasks > 0 || delegate.getPendingTaskCount() > 0 || delegate.getActiveTaskCount() > 0;
    }

    private void enqueueWork()
    {
        if (isWorkQueuedUpdater.compareAndSet(this, 0, 1))
            delegate.execute(work);
    }

    private void enqueueWorkExclusive()
    {
        if (!running && isWorkQueuedUpdater.compareAndSet(this, 0, 1))
            delegate.execute(work);
    }

    private void enqueueLoadsUnsafe()
    {
        outer: while (true)
        {
            OperationPriorityHeap queue = waitingToLoadRangeTxns == null || activeRangeLoads >= MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR ? waitingToLoad : waitingToLoadRangeTxns;
            AsyncOperation<?> next = queue.peek();
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
                        for (AsyncOperation<?> op : load.loadingOrWaiting().waiters())
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

                for (AsyncOperation<?> op : stateCache.load(loadExecutor, load, onLoaded))
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

    private OperationPriorityHeap waitingToLoadQueue(AsyncOperation<?> op)
    {
        if (waitingToLoad.contains(op)) return waitingToLoad;
        else if (waitingToLoadRangeTxns != null && waitingToLoadRangeTxns.contains(op)) return waitingToLoadRangeTxns;
        else throw illegalState("%s not in waitingToLoad or waitingToLoadRangeTxns", op);
    }

    private void consumeUnsafe(Object object)
    {
        try
        {
            if (object instanceof AsyncOperation<?>)
                loadUnsafe((AsyncOperation<?>) object);
            else
                ((Deferred) object).acceptUnsafe(this);
        }
        catch (Throwable t)
        {
            agent.onUncaughtException(t);
        }
    }

    private void updateQueue(OperationPriorityHeap removeFrom, AsyncOperation<?> op)
    {
        removeFromQueue(removeFrom, op);
        updateQueue(op);
    }

    private void removeFromQueue(OperationPriorityHeap removeFrom, AsyncOperation<?> op)
    {
        removeFrom.remove(op);
        if (removeFrom == waitingToLoadRangeTxns && waitingToLoadRangeTxns.isEmpty())
            waitingToLoadRangeTxns = null;
    }

    private void updateQueue(AsyncOperation<?> op)
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
                waitingToRun.append(op);
                enqueueWorkExclusive();
                break;
        }
    }

    private OperationPriorityHeap<?> queue(AsyncOperation<?> op)
    {
        switch (op.state())
        {
            default: throw new AssertionError("Unexpected state: " + op.state());
            case WAITING_TO_LOAD:
                if (op.rangeLoader() == null)
                    return waitingToLoad;

                if (waitingToLoad.contains(op))
                    return waitingToLoad;
                if (scanningRanges.contains(op))
                    return scanningRanges;
                Invariants.checkState(waitingToLoadRangeTxns != null && waitingToLoadRangeTxns.contains(op));
                return waitingToLoadRangeTxns;
            case LOADING:
                return loading;
            case WAITING_TO_RUN:
                return waitingToRun;
        }
    }

    public <R> void submit(AsyncOperation<R> operation)
    {
        onEvent(AccordCommandStoreExecutor::loadUnsafe, Function.identity(), operation);
    }

    public <R> void cancel(AsyncOperation<R> operation)
    {
        onEvent(AccordCommandStoreExecutor::cancelUnsafe, OnCancel::new, operation);
    }

    public void onScannedRanges(AsyncOperation<?> op, Throwable fail)
    {
        onEvent(AccordCommandStoreExecutor::onScannedRangesUnsafe, OnScannedRanges::new, op, fail);
    }

    public <K, V> void onSaved(AccordCachingState<K, V> saved, Object identity, Throwable fail)
    {
        onEvent(AccordCommandStoreExecutor::onSavedUnsafe, OnSaved::new, saved, identity, fail);
    }

    @Override
    public <K, V> void onLoaded(AccordCachingState<K, V> loaded, V value, Throwable fail)
    {
        onEvent(AccordCommandStoreExecutor::onLoadedUnsafe, OnLoaded::new, loaded, value, fail, false);
    }

    public <K, V> void onRangeLoaded(AccordCachingState<K, V> loaded, V value, Throwable fail)
    {
        onEvent(AccordCommandStoreExecutor::onLoadedUnsafe, OnLoaded::new, loaded, value, fail, true);
    }

    private <P1> void onEvent(BiConsumer<AccordCommandStoreExecutor, P1> sync, Function<P1, ?> async, P1 p1)
    {
        onEvent((e, c, p1a, p2a, p3) -> c.accept(e, p1a), (f, p1a, p2a, p3) -> f.apply(p1a), sync, async, p1, null, null);
    }

    private <P1, P2> void onEvent(TriConsumer<AccordCommandStoreExecutor, P1, P2> sync, BiFunction<P1, P2, ?> async, P1 p1, P2 p2)
    {
        onEvent((e, c, p1a, p2a, p3) -> c.accept(e, p1a, p2a), (f, p1a, p2a, p3) -> f.apply(p1a, p2a), sync, async, p1, p2, null);
    }

    private <P1, P2, P3> void onEvent(QuadConsumer<AccordCommandStoreExecutor, P1, P2, P3> sync, TriFunction<P1, P2, P3, ?> async, P1 p1, P2 p2, P3 p3)
    {
        onEvent((e, c, p1a, p2a, p3a) -> c.accept(e, p1a, p2a, p3a), TriFunction::apply, sync, async, p1, p2, p3);
    }

    private <P1, P2, P3, P4> void onEvent(QuintConsumer<AccordCommandStoreExecutor, P1, P2, P3, P4> sync, QuadFunction<P1, P2, P3, P4, Object> async, P1 p1, P2 p2, P3 p3, P4 p4)
    {
        onEvent(sync, async, p1, p1, p2, p3, p4);
    }

    private <P1s, P1a, P2, P3, P4> void onEvent(QuintConsumer<AccordCommandStoreExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
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
        submitted.drain(AccordCommandStoreExecutor::consumeUnsafe, this, true);
    }

    private void postUnlock()
    {
        if (!submitted.isEmpty())
            enqueueWork();
    }

    private <R> void loadUnsafe(AsyncOperation<R> op)
    {
        ++tasks;
        op.unsafeSetQueuePosition(++nextPosition);
        op.setup();
        updateQueue(op);
    }

    private void cancelUnsafe(AsyncOperation<?> op)
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

    private void onScannedRangesUnsafe(AsyncOperation<?> op, Throwable fail)
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

    private <K, V> void onLoadedUnsafe(AccordCachingState<K, V> loaded, V success, Throwable fail)
    {
        onLoadedUnsafe(loaded, success, fail, false);
    }

    private <K, V> void onRangeLoadedUnsafe(AccordCachingState<K, V> loaded, V success, Throwable fail)
    {
        onLoadedUnsafe(loaded, success, fail, true);
    }

    private <K, V> void onLoadedUnsafe(AccordCachingState<K, V> loaded, V value, Throwable fail, boolean isForRange)
    {
        boolean enqueueLoads = activeLoads-- == MAX_QUEUED_LOADS_PER_EXECUTOR;
        if (isForRange) enqueueLoads |= activeRangeLoads-- == MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR;
        if (enqueueLoads)
            enqueueLoadsUnsafe();

        if (loaded.status() == EVICTED)
            return;

        try (BufferList<AsyncOperation<?>> ops = loaded.loading().copyWaiters())
        {
            if (fail != null)
            {
                for (AsyncOperation<?> op : ops)
                {
                    OperationPriorityHeap queue = queue(op);
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
                for (AsyncOperation<?> op : ops)
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

    protected void run()
    {
        AsyncOperation<?> op = null;
        lock.lock();
        try
        {
            running = true;
            while (true)
            {
                drainSubmittedUnsafe();
                op = waitingToRun.poll();

                if (op == null)
                {
                    isWorkQueued = 0;
                    running = false;
                    return;
                }

                --tasks;
                op.run();
                op = null;
            }

        }
        catch (Throwable t)
        {
            if (op != null)
            {
                try { op.fail(t); }
                catch (Throwable t2) { t.addSuppressed(t2); }
            }

            running = false;
            delegate.execute(work);
            throw t;
        }
        finally
        {
            lock.unlock();
            postUnlock();
        }
    }

    public boolean isInThread()
    {
        return lock.isHeldByCurrentThread();
    }

    public void shutdown()
    {
        delegate.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return delegate.awaitTermination(timeout, unit);
    }

    public Future<?> submit(Runnable task)
    {
        return delegate.submit(() -> {
            lock.lock();
            try
            {
                task.run();
                drainSubmittedUnsafe();
            }
            finally
            {
                lock.unlock();
                postUnlock();
            }
        });
    }

    public ExecutorPlus delegate()
    {
        return delegate;
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

    static final class OperationPriorityHeap<R> extends IntrusivePriorityHeap<AsyncOperation<R>>
    {
        @Override
        public int compare(AsyncOperation<R> o1, AsyncOperation<R> o2)
        {
            return o1.compareTo(o2);
        }
        public void append(AsyncOperation<R> op)
        {
            super.append(op);
        }

        public AsyncOperation<R> poll()
        {
            ensureHeapified();
            return pollNode();
        }

        public AsyncOperation<R> peek()
        {
            ensureHeapified();
            return peekNode();
        }

        public void remove(AsyncOperation<R> remove)
        {
            super.remove(remove);
        }

        public boolean contains(AsyncOperation<R> contains)
        {
            return super.contains(contains);
        }
    }

    private abstract static class Deferred
    {
        abstract void acceptUnsafe(AccordCommandStoreExecutor executor);
    }

    private static class OnLoaded<K, V> extends Deferred
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
        void acceptUnsafe(AccordCommandStoreExecutor executor)
        {
            executor.onLoadedUnsafe(loaded, success(), fail(), isForRange());
        }
    }

    private static class OnScannedRanges extends Deferred
    {
        final AsyncOperation<?> scanned;
        final Throwable fail;

        private OnScannedRanges(AsyncOperation<?> scanned, Throwable fail)
        {
            this.scanned = scanned;
            this.fail = fail;
        }

        @Override
        void acceptUnsafe(AccordCommandStoreExecutor executor)
        {
            executor.onScannedRangesUnsafe(scanned, fail);
        }
    }

    private static class OnSaved<K, V> extends Deferred
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
        void acceptUnsafe(AccordCommandStoreExecutor executor)
        {
            executor.onSavedUnsafe(state, identity, fail);
        }
    }

    private static class OnCancel<R> extends Deferred
    {
        final AsyncOperation<R> cancel;

        private OnCancel(AsyncOperation<R> cancel)
        {
            this.cancel = cancel;
        }

        @Override
        void acceptUnsafe(AccordCommandStoreExecutor executor)
        {
            executor.cancelUnsafe(cancel);
        }
    }
}
