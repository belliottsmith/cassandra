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
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.RoutingKey;
import accord.impl.AbstractAsyncExecutor;
import accord.local.Command;
import accord.local.cfk.CommandsForKey;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.DebuggableTask.DebuggableTaskRunner;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.AccordConfig.QueueBalancingModel;
import org.apache.cassandra.config.AccordConfig.QueuePriorityModel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.metrics.AccordExecutorMetrics;
import org.apache.cassandra.metrics.AccordReplicaMetrics;
import org.apache.cassandra.metrics.AccordSystemMetrics;
import org.apache.cassandra.metrics.LogLinearDecayingHistograms;
import org.apache.cassandra.metrics.LogLinearDecayingHistograms.LogLinearDecayingHistogram;
import org.apache.cassandra.metrics.ShardedDecayingHistograms;
import org.apache.cassandra.metrics.ShardedDecayingHistograms.DecayingHistogramsShard;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExecutor;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.LoadExecutor;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.SaveExecutor;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.UniqueSave;
import org.apache.cassandra.service.accord.execution.ExclusiveExecutor.ExclusiveExecutorTask;
import org.apache.cassandra.service.accord.execution.IOTaskWrapper.WrappableIOTask;
import org.apache.cassandra.service.accord.execution.deadline.PreAcceptDeadlines;
import org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup;
import org.apache.cassandra.service.accord.execution.Task.ExecutorQueue;
import org.apache.cassandra.service.accord.execution.Task.GlobalGroup;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.utils.Functions.returningVoid;
import static accord.utils.async.AsyncCallbacks.flatCallback;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.AccordCache.CommandAdapter.COMMAND_ADAPTER;
import static org.apache.cassandra.service.accord.execution.AccordCache.CommandsForKeyAdapter.CFK_ADAPTER;
import static org.apache.cassandra.service.accord.execution.AccordCache.registerJfrListener;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.EVICTED;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.LOAD;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.RANGE_LOAD;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.RANGE_SCAN;
import static org.apache.cassandra.service.accord.execution.Task.State.EXECUTED;
import static org.apache.cassandra.service.accord.execution.Task.State.FAILED;
import static org.apache.cassandra.service.accord.execution.Task.State.UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.TaskQueueMulti.COUNTER_MASKS;
import static org.apache.cassandra.service.accord.execution.TaskQueueMulti.overflowBit;
import static org.apache.cassandra.service.accord.execution.TaskQueueMulti.selectByOverflowBits;
import static org.apache.cassandra.service.accord.execution.TaskQueueMulti.setOverflowWhenLessEqual;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * NOTE: We assume that NO BLOCKING TASKS are submitted to this executor AND WAITED ON by another task executing on this executor.
 *  (as we do not immediately schedule additional threads for submitted tasks, but schedule new threads only if necessary when the submitting execution completes)
 */
public abstract class AccordExecutor implements CacheSize, LoadExecutor<SafeTask<?>, Boolean>, SaveExecutor, Shutdownable, AbstractAsyncExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AccordExecutor.class);

    static final QueuePriorityModel PRIORITY_MODEL;
    static final QueueBalancingModel BALANCING_MODEL;
    static final long AGE_LIMIT;
    static final boolean BALANCE_BY_POSITION;
    static final long GLOBAL_QUEUE_LIMITS, EXCLUSIVE_QUEUE_LIMITS;
    static final int NONSYNC_MIN_BATCH_SIZE, NONSYNC_MAX_BATCH_SIZE, NONSYNC_BLOCKED_LIMIT;
    static final int FIFO_PROMOTE_PER_KEY;
    static final boolean NONSYNC_ENABLED, CACHE_QUEUES_ENABLED;
    private static final long LOADING_GROUPS = overflowBit(LOAD) | overflowBit(RANGE_LOAD) | overflowBit(RANGE_SCAN);

    static
    {
        AccordConfig config = DatabaseDescriptor.getAccord();
        AGE_LIMIT = config.queue_priority_age_limit.to(TimeUnit.MICROSECONDS);
        PRIORITY_MODEL = config.queue_priority_model;
        BALANCING_MODEL = config.queue_balancing_model;
        NONSYNC_MIN_BATCH_SIZE = config.queue_nonsync_min_batch_size;
        NONSYNC_MAX_BATCH_SIZE = config.queue_nonsync_max_batch_size;
        CACHE_QUEUES_ENABLED = config.queue_key_ordering_enabled;
        NONSYNC_ENABLED = CACHE_QUEUES_ENABLED && config.queue_nonsync_enabled;
        if (!CACHE_QUEUES_ENABLED && config.queue_nonsync_enabled)
            logger.info("config.queue_key_ordering_enabled is false; accord.queue_nonsync_enabled forced to false as well");
        NONSYNC_BLOCKED_LIMIT = config.queue_nonsync_blocked_limit == null ? 8 : config.queue_nonsync_blocked_limit;
        FIFO_PROMOTE_PER_KEY = config.queue_fifo_promote_per_key == null ? 1 : Math.max(0, config.queue_fifo_promote_per_key);
        switch (BALANCING_MODEL)
        {
            default: throw new UnhandledEnum(BALANCING_MODEL);
            case PRIORITY_ONLY:
            case BLENDED_PRIORITY_PHASE_FAIR:
                BALANCE_BY_POSITION = true;
                break;
            case PHASE_ONLY:
            case PHASE_FAIR:
                BALANCE_BY_POSITION = false;
        }

        {
            // TODO (required): pick default max loads/saves/range loads based on number of threads
            long global = COUNTER_MASKS, exclusive = COUNTER_MASKS;
            global ^= (0x7fL ^ 1) << (RANGE_SCAN.ordinal() * 8);
            if (config.queue_active_limits != null)
            {
                long[] limits = parseEnumParams(config.queue_active_limits, "queue_active_limits");
                global = selectByOverflowBits(setOverflowWhenLessEqual(limits[0], 0), global, limits[0]);
                exclusive = selectByOverflowBits(setOverflowWhenLessEqual(limits[1], 0), exclusive, limits[1]);
            }
            GLOBAL_QUEUE_LIMITS = global;
            EXCLUSIVE_QUEUE_LIMITS = exclusive;
        }

    }

    public static final ShardedDecayingHistograms HISTOGRAMS = new ShardedDecayingHistograms();

    public interface AccordExecutorFactory
    {
        AccordExecutor get(int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent);
    }

    public enum Mode { RUN_WITH_LOCK, RUN_WITHOUT_LOCK }

    // WARNING: this is a shared object, so close is NOT idempotent
    public static final class ExclusiveGlobalCaches extends GlobalCaches implements AutoCloseable
    {
        final AccordExecutor executor;

        public ExclusiveGlobalCaches(AccordExecutor executor, AccordCache global, AccordCache.Type<TxnId, Command, SaferCommand> commands, AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey> commandsForKey)
        {
            super(global, commands, commandsForKey);
            this.executor = executor;
        }

        @Override
        public void close()
        {
            try
            {
                // eviction may submit work, so beforeUnlock must run after to ensure notification is sent
                try { global.tryShrinkOrEvict(executor.lock); }
                finally { executor.beforeUnlockExternal(); }
            }
            finally
            {
                // must unlock however we exit, or we leak the lock (and, for a loop thread, wedge the executor)
                executor.unlock(TaskRunner.get());
            }
        }
    }

    public static class GlobalCaches
    {
        public final AccordCache global;
        public final AccordCache.Type<TxnId, Command, SaferCommand> commands;
        public final AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey> commandsForKey;

        public GlobalCaches(AccordCache global, AccordCache.Type<TxnId, Command, SaferCommand> commands, AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey> commandsForKey)
        {
            this.global = global;
            this.commands = commands;
            this.commandsForKey = commandsForKey;
        }
    }

    final LogLinearDecayingHistograms histograms;
    final LogLinearDecayingHistogram elapsedPreparingToRun;
    final LogLinearDecayingHistogram elapsedWaitingToRun;
    final LogLinearDecayingHistogram elapsedRunning;
    final LogLinearDecayingHistogram elapsed;
    final LogLinearDecayingHistogram keys;
    public final AccordReplicaMetrics.Shard replicaMetrics;

    private final Lock lock;
    final Agent agent;
    public final int executorId;
    private final AccordCache cache;
    private final ExclusiveGlobalCaches caches;
    final AtomicLong uniqueCreatedAt = new AtomicLong();
    final DebugExecutor debug = DebugExecutor.maybeDebug();

    private long maxWorkingSetSizeInBytes;
    private long maxWorkingCapacityInBytes;

    final TaskQueueStandalone<SafeTask<?>> loading = new TaskQueueStandalone<>(ExecutorQueue.LOADING);
    final TaskQueueStandalone<SafeTask<?>> waiting = new TaskQueueStandalone<>(ExecutorQueue.WAITING);
    final TaskQueueRunnable<Task> runnable = new TaskQueueRunnable<>();

    private final Tranches tranches = new Tranches(this);

    /** PreAccepts postponed until their deadline (see {@link SafeTask#tryDeferExclusive}); ordered by txnId */
    final TaskQueueStandalone<SafeTask<?>> deferred = new TaskQueueStandalone<>(ExecutorQueue.DEFERRED);
    // TODO (expected): replace with proper metrics (count, and time deferred)
    private volatile long deferredCount;

    long nextPosition = 1;
    int tasks;

    private boolean hasPausedLoading;
    // racy-read statistics of the loading pauses, for JfrDiagnostics (written under the executor lock)
    private volatile long loadingPauses, loadingPausedNanos;
    private long loadingPausedAt;

    private List<Condition> waitingForQuiescence;

    // the command stores' executors, for diagnostics
    private final List<ExclusiveExecutor> storeExecutors = new CopyOnWriteArrayList<>();

    AccordExecutor(Lock lock, int executorId, Agent agent)
    {
        this.lock = lock;
        this.executorId = executorId;
        this.cache = new AccordCache(this, 0);
        this.agent = agent;

        final AccordCache.Type<TxnId, Command, SaferCommand> commands;
        final AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey> commandsForKey;
        commands = cache.newType(TxnId.class, COMMAND_ADAPTER, AccordCacheMetrics.CommandsCacheMetrics.newShard(lock));
        registerJfrListener(executorId, commands, "Command");

        commandsForKey = cache.newType(RoutingKey.class, CFK_ADAPTER, AccordCacheMetrics.CommandsForKeyCacheMetrics.newShard(lock));
        registerJfrListener(executorId, commandsForKey, "CommandsForKey");

        this.caches = new ExclusiveGlobalCaches(this, cache, commands, commandsForKey);

        DecayingHistogramsShard histogramsShard = HISTOGRAMS.newShard(lock);
        this.histograms = histogramsShard.unsafeGetInternal();
        this.elapsedPreparingToRun = AccordExecutorMetrics.INSTANCE.elapsedPreparingToRun.forShard(histogramsShard);
        this.elapsedWaitingToRun = AccordExecutorMetrics.INSTANCE.elapsedWaitingToRun.forShard(histogramsShard);
        this.elapsedRunning = AccordExecutorMetrics.INSTANCE.elapsedRunning.forShard(histogramsShard);
        this.elapsed = AccordExecutorMetrics.INSTANCE.elapsed.forShard(histogramsShard);
        this.keys = AccordExecutorMetrics.INSTANCE.keys.forShard(histogramsShard);
        this.replicaMetrics = new AccordReplicaMetrics.Shard(histogramsShard);
    }

    abstract boolean isInLoop();
    abstract void beforeUnlockExternal();
    abstract <P1> void submit(Consumer<P1> sync, Function<P1, Task> async, P1 p1);
    public abstract boolean hasTasks();
    public abstract boolean isOwningThread();

    public final Lock unsafeLock()
    {
        return lock;
    }

    public final void lock(TaskRunner self)
    {
        long startAt = DEBUG_EXECUTION ? Clock.Global.nanoTime() : 0;
        if (!self.tryEnterAccordLockedExecutor(this))
            throw new UnsupportedOperationException("To ensure system performance, it is not permitted to utilise multiple AccordExecutor simultaneously with the same thread");
        try
        {
            //noinspection LockAcquiredButNotSafelyReleased
            lock.lock();
        }
        catch (Throwable t)
        {
            self.exitAccordLockedExecutor();
            throw t;
        }
        if (DEBUG_EXECUTION) debug.onEnterLock(startAt);
    }

    public final void unlock(TaskRunner self)
    {
        self.exitAccordLockedExecutor();
        if (DEBUG_EXECUTION) debug.onExitLock();
        lock.unlock();
    }

    public final boolean tryLock(TaskRunner self)
    {
        AccordExecutor active = self.accordActiveExecutor();
        if (active != null && active != this)
            return false;

        AccordExecutor locked = self.accordLockedExecutor();
        if (locked != null && locked != this)
            return false;

       return onTryLock(self, lock.tryLock());
    }

    final boolean onTryLock(TaskRunner self, boolean result)
    {
        if (result && !self.tryEnterAccordLockedExecutor(this))
        {
            // shouldn't be possible, we should have checked this already
            lock.unlock();
            throw Invariants.illegalState("%s already holds %s, so cannot lock %s", Thread.currentThread(), self.accordLockedExecutor(), this);
        }
        return result;
    }

    public ExclusiveGlobalCaches lockCaches()
    {
        lock(TaskRunner.get(Thread.currentThread()));
        return caches;
    }

    public AccordCache cacheExclusive()
    {
        Invariants.require(isOwningThread());
        return cache;
    }

    public AccordCache cacheUnsafe()
    {
        return cache;
    }

    final boolean hasAlreadyWaitingToRun()
    {
        return runnable.hasWaitingToRun();
    }

    void updateWaitingToRunExclusive()
    {
        // TODO (expected): this should not be invoked on every update of waiting to run
        maybeUnpauseLoading();
    }

    // drain only new work; specifically leave anything that would call completeTask queued. this is to ensure
    // tasks have been registered against their tranche before we potentially complete it
    void drainUnqueuedNewWorkExclusive()
    {
    }

    final Task pollAlreadyWaitingToRunExclusive()
    {
        Task next = runnable.poll();
        if (DEBUG_EXECUTION && next != null) DebugTask.get(next).onPolled();
        return next;
    }

    public void waitForQuiescence()
    {
        TaskRunner self = TaskRunner.get();
        Condition condition;
        lock(self);
        try
        {
            drainUnqueuedNewWorkExclusive();
            if (tasks == 0)
                return;

            if (waitingForQuiescence == null)
                waitingForQuiescence = new ArrayList<>();
            condition = Condition.newOneTimeCondition();
            waitingForQuiescence.add(condition);
        }
        finally
        {
            unlock(self);
        }
        condition.awaitThrowUncheckedOnInterrupt();
    }

    protected void notifyQuiescentExclusive()
    {
        if (waitingForQuiescence != null)
        {
            waitingForQuiescence.forEach(Condition::signalAll);
            waitingForQuiescence = null;
        }
        tranches.finishAll();
    }

    public void afterSubmittedAndConsequences(Runnable run)
    {
        TaskRunner self = TaskRunner.get();

        lock(self);
        try
        {
            drainUnqueuedNewWorkExclusive();
            if (tasks == 0)
            {
                run.run();
                return;
            }

            tranches.registerWait(run);
        }
        finally
        {
            unlock(self);
        }
    }

    private boolean hasNonLoadingWaitingToRun()
    {
        return runnable.hasWaitingToRunExcluding(LOADING_GROUPS);
    }

    final void maybeUnpauseLoading()
    {
        if (hasPausedLoading && (cache.weightedSize() < maxWorkingCapacityInBytes || !hasNonLoadingWaitingToRun()))
        {
            hasPausedLoading = false;
            loadingPausedNanos += Clock.Global.nanoTime() - loadingPausedAt;
            runnable.restart(LOADING_GROUPS);
        }
    }

    final void maybePauseLoading()
    {
        if (hasPausedLoading)
            return;

        if (!hasPausedLoading && cache.weightedSize() >= maxWorkingCapacityInBytes && hasNonLoadingWaitingToRun())
        {
            AccordSystemMetrics.metrics.pausedExecutorLoading.inc();
            hasPausedLoading = true;
            loadingPausedAt = Clock.Global.nanoTime();
            ++loadingPauses;
            runnable.stop(LOADING_GROUPS);
        }
    }

    public ExclusiveExecutor newExclusiveExecutor(int commandStoreId)
    {
        ExclusiveExecutor result = new ExclusiveExecutor(this, commandStoreId);
        storeExecutors.add(result);
        return result;
    }

    /**
     * Whether this executor supports postponing tasks until a deadline (see {@link #setDeferredWakeupExclusive}).
     */
    boolean supportsDeferral()
    {
        return false;
    }

    /**
     * Arrange for {@link #releaseDueDeferredExclusive} to be invoked (with the lock held) at or soon after {@code atNanos},
     * replacing any prior arrangement; or cancel any arrangement if {@code atNanos == NO_WAKEUP}.
     * Invoked only while holding the lock.
     */
    void setDeferredWakeupExclusive(long atNanos)
    {
        throw new UnsupportedOperationException();
    }

    static final long NO_WAKEUP = Long.MIN_VALUE;

    final void deferExclusive(SafeTask<?> task)
    {
        Invariants.require(supportsDeferral());
        //noinspection NonAtomicOperationOnVolatileField
        ++deferredCount; // only modified with the lock held
        deferred.enqueue(task);
        if (deferred.peek() == task)
            updateDeferredWakeupExclusive(task, task.commandStore.node().now());
    }

    public long unsafeDeferredCount()
    {
        return deferredCount;
    }

    final boolean hasDeferred()
    {
        return !deferred.isEmpty();
    }

    /**
     * Release deferred tasks strictly in txnId order, for as long as the head is due; and arrange a wakeup for the next.
     * @return true if any task was released
     */
    final boolean releaseDueDeferredExclusive()
    {
        SafeTask<?> next = deferred.peek();
        if (next == null)
        {
            // we may have published a wakeup for a task that has since been cancelled
            if (supportsDeferral()) setDeferredWakeupExclusive(NO_WAKEUP);
            return false;
        }

        long nowHlc = next.commandStore.node().now();
        long now = nanoTime();
        boolean released = false;
        while (next != null && isDue(next, nowHlc, now))
        {
            next.unqueue(deferred);
            released = true;
            try { next.releaseDeferredExclusive(); }
            catch (Throwable t) { next.tryFailAndCompleteUnexecutedExclusive(t, FAILED); next.unhandledException(t); }
            next = deferred.peek();
        }

        // note: also cancels the wakeup if next == null
        updateDeferredWakeupExclusive(next, nowHlc);
        return released;
    }

    /**
     * Due once our HLC deadline has passed; but as HLC is not monotonic, we also release once we have waited the maximum
     * penalty since local receipt, so that a backwards clock adjustment cannot hold us for longer than this.
     */
    private static boolean isDue(SafeTask<?> task, long nowHlc, long now)
    {
        return task.deadlineHlc() <= nowHlc || now - task.createdAt >= PreAcceptDeadlines.MAX_PENALTY_NANOS;
    }

    private void updateDeferredWakeupExclusive(SafeTask<?> head, long nowHlc)
    {
        if (head == null)
        {
            setDeferredWakeupExclusive(NO_WAKEUP);
            return;
        }

        long now = nanoTime();
        long byHlc = now + TimeUnit.MICROSECONDS.toNanos(Math.max(0, head.deadlineHlc() - nowHlc));
        long byReceipt = head.createdAt + PreAcceptDeadlines.MAX_PENALTY_NANOS;
        setDeferredWakeupExclusive(byReceipt - byHlc < 0 ? byReceipt : byHlc);
    }

    public ExclusiveAsyncExecutor newExclusiveExecutor()
    {
        return new ExclusiveExecutor(this);
    }

    @Override
    public <K, V> IOTaskLoad<?, ?> load(SafeTask<?> parent, Boolean isForRange, AccordCacheEntry<K, V, ?> entry)
    {
        IOTaskLoad<?, ?> result = newLoad(entry, isForRange);
        result.inherit(parent).submitExclusiveNoExcept();
        return result;
    }

    @Override
    public Cancellable save(AccordCacheEntry<?, ?, ?> entry, UniqueSave identity, Runnable save)
    {
        IOTaskSave task = new IOTaskSave(this, entry, identity, save);
        task.submitExclusiveNoExcept();
        return task;
    }

    void submitTask(Task task)
    {
        submit(Task::submitExclusiveNoExcept, i -> i, task);
    }

    public Future<?> submit(Runnable run)
    {
        Task inherit = inherit();
        PlainRunnable task = new PlainRunnable(this, new AsyncPromise<>(), run, GlobalGroup.OTHER);
        if (inherit != null) inherit.addConsequence(task);
        else submitTask(task);
        return task.result;
    }

    public void execute(Runnable run)
    {
        Task inherit = inherit();
        PlainRunnable task = new PlainRunnable(this, null, run, GlobalGroup.OTHER);
        if (inherit != null) inherit.addConsequence(task);
        else submitTask(task);
    }

    public <T> AsyncChain<T> buildDebuggableContinuation(Callable<T> call, Object describe)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super T, Throwable> callback)
            {
                return submitContinuation(new PlainChainDebuggable<>(AccordExecutor.this, call, callback, GlobalGroup.OTHER, null, describe));
            }
        };
    }

    @Override
    public Cancellable executeContinuation(Runnable run, BiConsumer<? super Void, Throwable> callback)
    {
        return submitContinuation(new PlainChain<>(this, returningVoid(run), callback, null, GlobalGroup.OTHER, ExclusiveGroup.OTHER));
    }

    @Override
    public <V> Cancellable executeContinuation(Callable<V> call, BiConsumer<? super V, Throwable> callback)
    {
        return submitContinuation(new PlainChain<>(this, call, callback, null, GlobalGroup.OTHER, ExclusiveGroup.OTHER));
    }

    @Override
    public <V> Cancellable flatExecuteContinuation(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback)
    {
        return submitContinuation(new PlainChain<>(this, call, flatCallback(callback), null, GlobalGroup.OTHER, ExclusiveGroup.OTHER));
    }

    private Cancellable submitContinuation(PlainChain<?> task)
    {
        Task inherit = inherit();
        if (inherit == null) submitTask(task);
        else
        {
            task.setContinuation();
            inherit.addConsequence(task);
        }
        return task;
    }

    public void submitExclusive(Runnable runnable)
    {
        new PlainRunnable(this, null, runnable, GlobalGroup.OTHER).submitExclusiveNoExcept();
    }

    Cancellable submitExclusive(Task parent, GlobalGroup group, WrappableIOTask wrap)
    {
        IOTaskWrapper task = new IOTaskWrapper(this, wrap, group);
        task.inherit(parent).submitExclusiveNoExcept();
        return task;
    }

    Task inherit()
    {
        TaskRunner self = TaskRunner.get();

        if (self.accordActiveExecutor() != this)
            return null;

        Task task = self.accordActiveSelfTask();
        if (task instanceof ExclusiveExecutorTask)
            task = ((ExclusiveExecutorTask)task).queue.task;
        return task;
    }

    void registerExclusive(Task task)
    {
        Invariants.require(isOwningThread());
        Invariants.require(task.is(UNREGISTERED));
        if (task.hasInherited())
        {
            tranches.addInherited(task.tranche());
        }
        else
        {
            if (task.position == 0)
                task.position = nextPosition++;
            else if (nextPosition < task.position)
                nextPosition = task.position + 1;
            task.setTranche(tranches.addNew());
        }
        ++tasks;
    }

    final void completedTaskExclusive(Task task)
    {
        Invariants.require(task.compareTo(EXECUTED) >= 0);
        if (DEBUG_EXECUTION) DebugTask.get(task).onCompleted(debug);
        unregisterExclusive(task);
        runnable.cleanup(task);
        cache.tryShrinkOrEvict(lock);
        maybeUnpauseLoading();
    }

    // a blocked task that will never run should not prevent tranches from proceeding
    final void unregisterBlockedExclusive(Task task)
    {
        unregisterExclusive(task);
        maybeUnpauseLoading();
    }

    final void unregisterExclusive(Task task)
    {
        int tranche = task.takeTrancheExclusive();
        if (tranche < 0)
            return;

        --tasks;
        tranches.complete(tranche);
    }

    void onScannedRangesExclusive(SafeTask<?> task, Throwable fail)
    {
        // the task may have already been cancelled, in which case we don't need to fail it
        if (task.state().isDone())
            return;

        SafeTask<?>.RangeTxnScanner scanner = task.rangeScanner();
        if (scanner == null && fail == null) fail = new CancellationException();
        if (fail != null) task.tryFailAndCompleteUnexecutedExclusive(fail, FAILED);
        else scanner.scannedExclusive();
    }

    <K, V> void onSavedExclusive(AccordCacheEntry<K, V, ?> state, Object identity, Throwable fail)
    {
        cache.saved(state, identity, fail);
    }

    <K, V> void onLoadedExclusive(AccordCacheEntry<K, V, ?> loaded, V value, Throwable fail)
    {
        if (loaded.status() == EVICTED)
            return;

        try (ArrayBuffers.BufferList<SafeTask<?>> tasks = loaded.drainWaitingToLoad())
        {
            if (fail != null)
            {
                // every task that was waiting for this load must be cleaned up, so one failing to do so cannot be
                // allowed to abandon the others in a queue they will never leave
                for (SafeTask<?> task : tasks)
                {
                    if (task.isDone())
                        continue;
                    try { task.onFailedToLoadExclusive(loaded, fail); }
                    catch (Throwable t) { task.unhandledException(t); }
                }
                cache.failedToLoad(loaded, fail);
            }
            else
            {
                cache.loaded(loaded, value);
                // we sort to avoid unsafe reentry within queue updates, which can corrupt the state
                tasks.sort(AccordCacheEntryQueue::compareForNotify);
                for (SafeTask<?> task : tasks)
                {
                    if (task.isDone())
                        continue;
                    try { task.onLoadOneExclusive(loaded); }
                    catch (Throwable t) { task.unhandledException(t); }
                }
            }
        }

        maybePauseLoading();
    }

    public void executeDirectlyWithLock(Runnable command)
    {
        TaskRunner self = TaskRunner.get();
        lock(self);
        AccordExecutor prevActive = self.accordActiveExecutor();
        try
        {
            self.setAccordActiveExecutor(this);
            command.run();
        }
        finally
        {
            beforeUnlockExternal();
            self.setAccordActiveExecutor(prevActive);
            unlock(self);
        }
    }

    @Override
    public void setCapacity(long bytes)
    {
        Invariants.require(isOwningThread());
        cache.setCapacity(bytes);
        refreshCapacity();
    }

    public void setWorkingSetSize(long bytes)
    {
        Invariants.require(isOwningThread());
        maxWorkingSetSizeInBytes = bytes;
        refreshCapacity();
    }

    private void refreshCapacity()
    {
        maxWorkingCapacityInBytes = cache.capacity() + maxWorkingSetSizeInBytes;
        if (maxWorkingCapacityInBytes < maxWorkingSetSizeInBytes)
            maxWorkingCapacityInBytes = Long.MAX_VALUE;
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

    public int unsafePreparingToRunCount()
    {
        return loading.size();
    }

    /** tasks waiting on another task's transaction or key (WAITING_ON_TXN / WAITING_ON_KEY) (racy read) */
    public int unsafeWaitingOnTxnOrKeyCount()
    {
        return waiting.size();
    }

    /** tasks awaiting submission or completion under the executor lock, where the executor keeps such a list (racy read) */
    public int unsafePendingCount()
    {
        return 0;
    }

    /** completions deferred because a dispatch pass exceeded its completion budget (racy read) */
    public int unsafeDeferredCompletionCount()
    {
        return 0;
    }

    /** completions ever deferred because a dispatch pass exceeded its completion budget (racy read) */
    public long unsafeDeferredCompletionTotal()
    {
        return 0;
    }

    /** whether loads are paused because the cache is over capacity plus working set (racy read) */
    public boolean unsafeIsLoadingPaused()
    {
        return hasPausedLoading;
    }

    /** the number of times loading has been paused since startup (racy read) */
    public long unsafeLoadingPauses()
    {
        return loadingPauses;
    }

    /** the total time loading has been paused since startup, in nanoseconds, including any current pause (racy read) */
    public long unsafeLoadingPausedNanos()
    {
        long total = loadingPausedNanos;
        if (hasPausedLoading)
            total += Math.max(0, Clock.Global.nanoTime() - loadingPausedAt);
        return total;
    }

    /** the cache size at which loading is paused: capacity plus the working set (racy read) */
    public long unsafeMaxWorkingCapacity()
    {
        return maxWorkingCapacityInBytes;
    }

    public int unsafeWaitingToRunCount()
    {
        return runnable.waitingCount();
    }

    public int unsafeRunningCount()
    {
        return runnable.assigned.size();
    }

    public Stream<? extends DebuggableTaskRunner> active()
    {
        return Stream.of();
    }

    public int executorId()
    {
        return executorId;
    }

    public static <O> IntFunction<O> constant(O out)
    {
        return ignore -> out;
    }

    <K, V> IOTaskLoad<K, V> newLoad(AccordCacheEntry<K, V, ?> entry, boolean isForRange)
    {
        return new IOTaskLoad<>(this, entry, isForRange ? RANGE_LOAD : LOAD);
    }

    public List<TaskInfo> taskSnapshot()
    {
        List<TaskInfo> result = new ArrayList<>();
        TaskRunner self = TaskRunner.get();
        lock(self);
        try
        {
            addToSnapshot(result, loading, TaskInfo.Status.LOADING, TaskInfo.Status.LOADING);
            addToSnapshot(result, deferred, TaskInfo.Status.LOADING, TaskInfo.Status.LOADING);
            for (TaskQueue queue : runnable.queues)
            {
                if (queue != null)
                    addToSnapshot(result, queue, TaskInfo.Status.WAITING_TO_RUN, TaskInfo.Status.WAITING_TO_RUN);
            }
            addToSnapshot(result, runnable.assigned, TaskInfo.Status.RUNNING, TaskInfo.Status.WAITING_TO_RUN);
        }
        finally
        {
            unlock(self);
        }
        result.sort(TaskInfo::compareTo);
        return result;
    }

    /** per command store queue state, for diagnostics (see {@link #diagnosticsSnapshot}) */
    public static final class StoreDiagnostics
    {
        public final int commandStoreId;
        // tasks waiting on another task's txn or key / loading or scanning ranges / queued at the store, ready to run
        public int waiting, loading, queued, running;
        public long oldestWaitingNanos, oldestLoadingNanos, oldestQueuedNanos;
        // cumulative, from the store's ExclusiveExecutor
        public long runs, runNanos;

        StoreDiagnostics(int commandStoreId)
        {
            this.commandStoreId = commandStoreId;
        }
    }

    /** a task that has been waiting on another task's txn or key for a while, and what it is waiting on */
    public static final class WaitingDiagnostics
    {
        public int commandStoreId;
        public String task, txnId, state;
        public long ageNanos, position;
        public boolean fifo;
        // its cache entries, how many of them it does not yet hold (scanning at most MAX_REFS_SCANNED), and on how many
        // of those it is blocked: locked by another task, or not among the queued tasks that may run (of the first
        // MAX_QUEUES_INSPECTED unlocked ones)
        public int refs, notHeld, blockedOn;
        // the first entry it is blocked on: its key, holder, and the queue in front of it
        public String entry, holder, holderState, queueHead;
        public long holderAgeNanos = -1;
        public int queueSize, fifoSize, runnablePrefix, indexInQueue = -1;
        // if the queue head is a batched (non-sync / incremental) task: whether it has this entry in the keys it will
        // process next (blocking: others wait on them; notBlocking: ready, nobody waiting), the sizes of those sets, and
        // its progress (keys processed of its total); -1 if not applicable
        public int headInBlocking = -1, headInNotBlocking = -1, headBlocking = -1, headNotBlocking = -1, headProcessed = -1, headKeys = -1;
    }

    private static final int MAX_REFS_SCANNED = 1 << 16;
    private static final int MAX_QUEUES_INSPECTED = 256;
    private static final int MAX_DESCRIPTION = 160;

    /**
     * Under the executor lock: per command store counts and oldest ages of the tasks waiting on a txn or key, loading,
     * queued at their store and running; and the (at most) maxWaiting oldest tasks that have been waiting on a txn or
     * key for at least minWaitingNanos, with the cache entry each is blocked on. Bounded work, for periodic diagnostics.
     */
    public void diagnosticsSnapshot(long minWaitingNanos, int maxWaiting, Consumer<StoreDiagnostics> stores, Consumer<WaitingDiagnostics> waiters)
    {
        long now = Clock.Global.nanoTime();
        java.util.Map<Integer, StoreDiagnostics> byStore = new java.util.TreeMap<>();
        for (ExclusiveExecutor e : storeExecutors)
        {
            StoreDiagnostics d = byStore.computeIfAbsent(e.commandStoreId, StoreDiagnostics::new);
            d.runs = e.diagnosticRuns;
            d.runNanos = e.diagnosticRunNanos;
        }
        // the oldest waiting tasks: a max-heap by createdAt (so the youngest of the kept is at the head)
        java.util.PriorityQueue<SafeTask<?>> oldest = new java.util.PriorityQueue<>(Math.max(1, maxWaiting), (a, b) -> Long.compare(b.createdAt, a.createdAt));
        List<WaitingDiagnostics> details = new ArrayList<>();

        TaskRunner self = TaskRunner.get();
        lock(self);
        try
        {
            for (int i = 0 ; i < waiting.size() ; ++i)
            {
                SafeTask<?> t = waiting.getSingle(i);
                StoreDiagnostics d = byStore.computeIfAbsent(t.commandStore.id(), StoreDiagnostics::new);
                long age = now - t.createdAt;
                d.waiting++;
                d.oldestWaitingNanos = Math.max(d.oldestWaitingNanos, age);
                if (maxWaiting > 0 && age >= minWaitingNanos)
                {
                    if (oldest.size() < maxWaiting) oldest.add(t);
                    else if (t.createdAt < oldest.peek().createdAt) { oldest.poll(); oldest.add(t); }
                }
            }
            for (int i = 0 ; i < loading.size() ; ++i)
            {
                SafeTask<?> t = loading.getSingle(i);
                StoreDiagnostics d = byStore.computeIfAbsent(t.commandStore.id(), StoreDiagnostics::new);
                d.loading++;
                d.oldestLoadingNanos = Math.max(d.oldestLoadingNanos, now - t.createdAt);
            }
            for (TaskQueue<?> queue : runnable.queues)
            {
                if (queue != null)
                    addQueuedDiagnostics(byStore, queue, now, false);
            }
            addQueuedDiagnostics(byStore, runnable.assigned, now, true);

            for (SafeTask<?> t : oldest)
                details.add(waitingDiagnostics(t, now));
        }
        finally
        {
            unlock(self);
        }
        byStore.values().forEach(stores);
        details.sort((a, b) -> Long.compare(b.ageNanos, a.ageNanos));
        details.forEach(waiters);
    }

    private static void addQueuedDiagnostics(java.util.Map<Integer, StoreDiagnostics> byStore, TaskQueue<?> queue, long now, boolean isAssigned)
    {
        for (int i = 0 ; i < queue.size() ; ++i)
        {
            Task t = queue.getSingle(i);
            if (!(t instanceof ExclusiveExecutorTask))
                continue;
            ExclusiveExecutor q = ((ExclusiveExecutorTask) t).queue;
            StoreDiagnostics d = byStore.computeIfAbsent(q.commandStoreId, StoreDiagnostics::new);
            if (q.task != null)
            {
                if (isAssigned) d.running++;
                else
                {
                    d.queued++;
                    d.oldestQueuedNanos = Math.max(d.oldestQueuedNanos, now - q.task.createdAt);
                }
            }
            for (TaskQueue<?> q0 : q.queues)
            {
                if (q0 == null)
                    continue;
                for (int j = 0 ; j < q0.size() ; ++j)
                {
                    d.queued++;
                    d.oldestQueuedNanos = Math.max(d.oldestQueuedNanos, now - q0.getSingle(j).createdAt);
                }
            }
        }
    }

    private static WaitingDiagnostics waitingDiagnostics(SafeTask<?> t, long now)
    {
        WaitingDiagnostics w = new WaitingDiagnostics();
        w.commandStoreId = t.commandStore.id();
        w.task = TaskLabels.label(t, TaskLabels.Prefix.RUN);
        Object txnId = t.executionContext().primaryTxnId();
        w.txnId = txnId == null ? "" : txnId.toString();
        w.state = String.valueOf(t.currentState());
        w.ageNanos = now - t.createdAt;
        w.position = t.position;
        w.fifo = t.isCacheQueuedFifo();
        w.refs = t.refs.size();
        // the entry holding it up: the first that another task has locked, or on which it is not (yet) among the tasks
        // that may run; tasks only lock their entries when they run, so merely not holding an entry means nothing
        int scanned = 0, inspected = 0;
        AccordCacheEntry<?, ?, ?> first = null;
        for (accord.local.SafeState<?> safeState : t.refs.values())
        {
            if (++scanned > MAX_REFS_SCANNED)
                break;
            AccordCacheEntry<?, ?, ?> entry = SaferState.global(safeState);
            if (entry.isLockedBy(t))
                continue;
            w.notHeld++;
            boolean blocked;
            if (entry.isLocked()) blocked = true;
            else if (inspected++ < MAX_QUEUES_INSPECTED)
            {
                int index = entry.unsafeQueuedTasks().indexOf(t);
                blocked = index < 0 || index >= entry.unsafeRunnablePrefix();
            }
            else blocked = false;
            if (blocked && ++w.blockedOn == 1)
                first = entry;
        }
        if (first != null)
        {
            w.entry = truncate(String.valueOf(first.key()));
            SafeTask<?> holder = first.isLocked() ? first.lockedBy() : null;
            if (holder != null)
            {
                w.holder = truncate(TaskLabels.label(holder, TaskLabels.Prefix.RUN) + ' ' + holder.executionContext().primaryTxnId());
                w.holderState = String.valueOf(holder.currentState());
                w.holderAgeNanos = now - holder.createdAt;
            }
            List<SafeTask<?>> queued = first.unsafeQueuedTasks();
            w.queueSize = queued.size();
            w.fifoSize = first.unsafeFifoSize();
            w.runnablePrefix = first.unsafeRunnablePrefix();
            w.indexInQueue = queued.indexOf(t);
            if (!queued.isEmpty())
            {
                SafeTask<?> head = queued.get(0);
                w.queueHead = truncate(TaskLabels.label(head, TaskLabels.Prefix.RUN) + ' ' + head.executionContext().primaryTxnId()
                                       + ' ' + head.currentState() + " age=" + (now - head.createdAt) / 1_000_000 + "ms"
                                       + (head.isCacheQueuedFifo() ? " fifo" : ""));
                SafeTask.NonSyncState<?> nonSync = head.nonSync();
                if (nonSync != null)
                {
                    Object key = first.key();
                    w.headInBlocking = nonSync.blocking != null && nonSync.blocking.contains(key) ? 1 : 0;
                    w.headInNotBlocking = nonSync.notBlocking != null && nonSync.notBlocking.contains(key) ? 1 : 0;
                    w.headBlocking = nonSync.blocking == null ? 0 : nonSync.blocking.size();
                    w.headNotBlocking = nonSync.notBlocking == null ? 0 : nonSync.notBlocking.size();
                    w.headProcessed = nonSync.processed;
                    w.headKeys = head.keys;
                }
            }
        }
        return w;
    }

    private static String truncate(String s)
    {
        return s.length() <= MAX_DESCRIPTION ? s : s.substring(0, MAX_DESCRIPTION);
    }

    private static void addToSnapshot(List<TaskInfo> snapshot, TaskQueue<?> queue, TaskInfo.Status ifCurrent, TaskInfo.Status ifQueued)
    {
        for (int i = 0 ; i < queue.size() ; ++i)
        {
            Task t = queue.getSingle(i);
            if (t instanceof ExclusiveExecutorTask)
            {
                ExclusiveExecutor q = ((ExclusiveExecutorTask) t).queue;
                snapshot.add(new TaskInfo(ifCurrent, q.commandStoreId, q.task));
                for (TaskQueue<?> q0 : q.queues)
                {
                    if (q0 != null)
                    {
                        for (int j = 0 ; j < q0.size() ; ++j)
                            snapshot.add(new TaskInfo(ifQueued, q.commandStoreId, q0.getSingle(j)));
                    }
                }
            }
            else
            {
                int commmandStoreId = t instanceof SafeTask ? ((SafeTask<?>) t).commandStore.id() : -1;
                snapshot.add(new TaskInfo(ifCurrent, commmandStoreId, t));
            }
        }
    }

    private static long[] parseEnumParams(String input, String describe)
    {
        String[] specs = input.split(";");
        if (specs.length != 2)
            throw new IllegalArgumentException("Invalid specifiers in " + describe + "; expect [GlobalGroup];[ExclusiveGroup] but got: " + input);
        long[] result = new long[2];
        result[0] = parseEnumParams(GlobalGroup::valueOf, specs[0], describe + " for GlobalGroup");
        result[1] = parseEnumParams(ExclusiveGroup::valueOf, specs[1], describe + " for ExclusiveGroup");
        return result;
    }

    private static long parseEnumParams(Function<String, ? extends Enum<?>> get, String input, String describe)
    {
        long result = 0;
        for (String spec : input.split(","))
        {
            if (spec.trim().isEmpty()) continue;

            String[] split = spec.split(":");
            if (split.length != 2)
                throw new IllegalArgumentException("Invalid specifier " + spec + " in " + describe + ": " + input);

            try
            {
                Enum<?> queue = get.apply(split[0]);
                long value = Long.parseLong(split[1]);
                if (value <= 0 || value >= 128)
                    throw new IllegalArgumentException("Invalid limit " + value + " in queue_active_limits: " + input);

                result |= value << (queue.ordinal() * 8);
            }
            catch (Throwable t)
            {
                throw new IllegalArgumentException("Invalid queue identifier " + split[0] + " in " + describe + ": " + input);
            }
        }

        return result;
    }
}
