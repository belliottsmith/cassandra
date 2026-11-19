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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import accord.api.ExclusiveAsyncExecutor;
import accord.local.ExecutionContext;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.IntrusiveHeapNode;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.service.accord.debug.DebugExecution;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup;
import org.apache.cassandra.service.accord.execution.Task.GlobalGroup;
import org.apache.cassandra.service.accord.execution.Task.GroupKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.service.accord.execution.Task.RunState;

import static accord.utils.Functions.returningVoid;
import static accord.utils.async.AsyncCallbacks.flatCallback;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.EXCLUSIVE_QUEUE_LIMITS;
import static org.apache.cassandra.service.accord.execution.Task.ExecutorQueue.RUNNABLE;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.COMMAND_STORE;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;

public final class ExclusiveExecutor extends TaskQueueMulti<Task> implements ExclusiveAsyncExecutor
{
    private static final AtomicReferenceFieldUpdater<ExclusiveExecutor, Thread> ownerUpdater = AtomicReferenceFieldUpdater.newUpdater(ExclusiveExecutor.class, Thread.class, "owner");

    static final class ExclusiveExecutorTask extends Task
    {
        final ExclusiveExecutor queue;

        ExclusiveExecutorTask(GlobalGroup group, ExclusiveExecutor queue)
        {
            super(group);
            this.queue = queue;
        }

        @Override AccordExecutor executor() { return queue.executor; }
        @Override void submitExclusiveMayThrow() { throw new UnsupportedOperationException(); }
        @Override boolean isNewWork() { return false; }
        @Override public void cancel() { throw new UnsupportedOperationException(); }
        @Override boolean runMayThrow() { throw new UnsupportedOperationException(); }
        @Override void unqueueIfQueued() {}
        @Override void reportFailureMayThrow(Throwable t, boolean isExclusive) { throw new UnsupportedOperationException(); }
        @Override void tryCancelExclusive(CancellationException cancelled) { throw new UnsupportedOperationException(); }

        boolean prepareTask()
        {
            Task task = queue.task;
            try
            {
                boolean run = task.prepareExclusiveMayThrow();
                task.setStateExclusive(State.PREPARED);
                setStateExclusive(State.PREPARED);
                if (run)
                {
                    queue.maybeFormBatchExclusive(true);
                    return true;
                }
                completeExclusiveMayThrow();
                return false;
            }
            catch (Throwable t)
            {
                task.setStateExclusive(State.FAILED);
                task.reportFailureExclusiveNoExcept(t, true);
                completeExclusiveMayThrow();
                return false;
            }
        }

        @Override
        void completeExclusiveMayThrow()
        {
            try
            {
                unsafeSetStateExclusive(WAITING_TO_RUN);
                executor().runnable.cleanup(this);
                queue.completeTask();
            }
            catch (Throwable t)
            {
                unhandledException(t);
            }
        }

        @Override
        public String description()
        {
            Task task = queue.task;
            return task == null ? describeIdle() : task.description();
        }

        @Override
        public String briefDescription()
        {
            Task task = queue.task;
            return task == null ? describeIdle() : task.briefDescription();
        }

        private String describeIdle()
        {
            // see ExclusiveExecutor.tryContinue: we may have completed our task without finding another to run
            return "ExclusiveExecutor[" + queue.commandStoreId + "] (completed by continuation)";
        }

        protected boolean isInHeap()
        {
            return super.isInHeap();
        }
    }

    private final AccordExecutor executor;
    final int commandStoreId;
    final ExclusiveExecutorTask selfTask;
    /**
     * The task currently being prepared, run or completed. Normally non-null whenever {@link #selfTask} is queued or
     * assigned, except that a continuation (see {@link #tryContinue}) may complete the task it ran and find no other,
     * in which case this is null until {@link #selfTask} completes (and polls again).
     */
    Task task;
    volatile Thread owner, waiting;
    // diagnostics (racy reads): tasks run by this store and the time spent running them; written only by the owner
    volatile long diagnosticRuns, diagnosticRunNanos;
    /** the number of tasks run by continuation (see {@link #tryContinue}); modified only with the executor lock held */
    private volatile long continuedCount;
    /**
     * The micro-batch formed with {@link #task} as its head, if any; see {@link Batch}. Set and cleared with the lock held,
     * and otherwise read only by the owner while running.
     */
    private Batch batch;
    /** the number of batches completed, and of followers that ran in them; modified only with the executor lock held */
    private volatile long batchCount, batchedCount, inlinedCount;
    private boolean isStopped;
    private volatile boolean visibleIsStopped;
    private boolean isTerminated;

    final DebugExecution.DebugExclusiveExecutor debug;

    ExclusiveExecutor(AccordExecutor executor)
    {
        this(executor, -1);
    }

    ExclusiveExecutor(AccordExecutor executor, int commandStoreId)
    {
        super(RUNNABLE, commandStoreId < 0 ? GroupKind.NONE : GroupKind.EXCLUSIVE, EXCLUSIVE_QUEUE_LIMITS);
        this.executor = executor;
        this.commandStoreId = commandStoreId;
        this.selfTask = new ExclusiveExecutorTask(globalGroup(commandStoreId), this);
        this.debug = DebugExecution.DebugExclusiveExecutor.maybeDebug(executor.debug, commandStoreId);
    }

    void runTask(TaskRunner self)
    {
        Thread thread = Thread.currentThread();
        if (!ownerUpdater.compareAndSet(this, null, thread))
        {
            if (DEBUG_EXECUTION) debug.onWaiting();
            Invariants.require(waiting == null);
            waiting = thread;
            outer:
            do
            {
                while (true)
                {
                    Thread owner = this.owner;
                    if (owner == thread) break outer;
                    if (owner == null) continue outer;
                    LockSupport.park();
                }
            }
            while (!ownerUpdater.compareAndSet(this, null, thread));
            Invariants.require(waiting == thread);
            waiting = null;
        }

        try
        {
            int continueLimit = executor.exclusiveContinueLimit;
            while (true)
            {
                runOneTask(self);
                if (--continueLimit < 0 || !tryContinue(self))
                    break;
            }
        }
        finally
        {
            // NOTE: we can ONLY safely release owner here due to AccordCacheEntry locking, which remains in place until SafeTask.releaseResourcesExclusive
            //       this also relies on SaferCommandStore$ExclusiveCaches.acquireIfLoaded returning false when the entry is locked
            owner = null;
        }
    }

    private void runOneTask(TaskRunner self)
    {
        if (isStopped && reject(task))
        {
            task.rejectAtRuntime(new RejectedExecutionException(commandStoreId + " is terminated. Cannot execute " + task.description()));
        }
        else
        {
            // diagnostics count the task, but not the followers of any batch it leads
            long start = Clock.Global.nanoTime();
            task.runNoExcept(self);
            diagnosticRunNanos += Clock.Global.nanoTime() - start;
            diagnosticRuns++;
        }

        Batch batch = this.batch;
        if (batch == null && executor.exclusiveBatchLimit > 0 && hasConsequencesForThisStore(task))
            batch = tryFormBatchAfterRun(self);
        if (batch != null)
            runBatch(self, batch);
    }

    /**
     * Our task has run, did not lead a batch (as it had no followers when it was prepared), but has submitted
     * consequences to this store: try (without blocking) to take the lock and form a batch now, so that we may execute
     * them inline (see {@link Batch}), picking up any followers that have queued behind it meanwhile.
     */
    private Batch tryFormBatchAfterRun(TaskRunner self)
    {
        boolean wasLocked = executor.isOwningThread();
        if (!wasLocked && !executor.tryLock(self))
            return null;

        try
        {
            maybeFormBatchExclusive(false);
            return batch;
        }
        finally
        {
            if (!wasLocked)
            {
                try { executor.beforeUnlockExternal(); }
                finally { executor.unlock(self); }
            }
        }
    }

    /**
     * Whether {@code task} has submitted SafeTask consequences to this store that a batch it led might execute inline
     * (see {@link SafeTask#mayInlineConsequence}); since we inline all or none of them, all must pass this check
     */
    private boolean hasConsequencesForThisStore(Task task)
    {
        if (!(task instanceof SafeTask<?>) || !task.is(RunState.RUN_SUCCESS) && !task.is(RunState.RUN_PERSISTING))
            return false;

        SafeTask<?> parent = (SafeTask<?>) task;
        boolean any = false;
        for (Task consequence = task.next ; consequence != null ; consequence = consequence.next)
        {
            if (!(consequence instanceof SafeTask<?>) || ((SafeTask<?>) consequence).commandStore.exclusiveExecutor() != this)
                continue;

            if (!parent.mayInlineConsequence((SafeTask<?>) consequence))
                return false;
            any = true;
        }
        return any;
    }

    /**
     * EXPERIMENTAL: see {@link org.apache.cassandra.config.AccordConfig#queue_exclusive_continue_limit}.
     * <p>
     * Having just run {@link #task}, and while still owning this executor, try (without blocking) to take the executor
     * lock, complete the task, and poll and prepare the next, so that we may run it immediately without handing off
     * to another thread. If we complete our task but cannot find another to prepare, we leave {@link #task} null;
     * {@link #selfTask} remains assigned, and its completion by the executor (as normal) will poll again.
     * <p>
     * This bypasses the executor's global queue for the continued tasks, but we count a dispatch against
     * {@link #selfTask}'s group for each, so that the global accounting is maintained.
     *
     * @return true iff {@link #task} has been replaced by a newly prepared task, that we should now run
     */
    private boolean tryContinue(TaskRunner self)
    {
        // a racy read, but only used to avoid taking the lock when there is (almost certainly) nothing to do
        if (waitingCount == 0)
            return false;

        // if we hold the lock already (RUN_WITH_LOCK) our task will be completed as normal by our caller
        if (executor.isOwningThread() || !executor.tryLock(self))
            return false;

        try
        {
            if (waitingCount == 0)
                return false;

            completeCurrentTaskExclusive();
            while (true)
            {
                Task next = pollMulti();
                task = next;
                if (DEBUG_EXECUTION) debug.onSetTask(next);
                if (next == null)
                    return false;

                if (DEBUG_EXECUTION) DebugTask.get(next).onPolled();
                executor.runnable.incrementDispatches(selfTask);
                if (prepareContinuedTaskExclusive())
                {
                    //noinspection NonAtomicOperationOnVolatileField
                    ++continuedCount;
                    if (DEBUG_EXECUTION) DebugTask.get(next).onPreRun();
                    return true;
                }
            }
        }
        finally
        {
            // we may have made work runnable (by our completion) that must be signalled to the executor's threads;
            // this is the same protocol used by ExclusiveGlobalCaches to release the lock outside of a loop
            try { executor.beforeUnlockExternal(); }
            finally { executor.unlock(self); }
        }
    }

    /**
     * Equivalent to {@link ExclusiveExecutorTask#prepareTask}, except that {@link #selfTask} is already prepared and
     * running, and if the task should not run we complete only the task (leaving {@link #task} null).
     * @return true if the task is prepared and should be run
     */
    private boolean prepareContinuedTaskExclusive()
    {
        Task task = this.task;
        try
        {
            boolean run = task.prepareExclusiveMayThrow();
            task.setStateExclusive(Task.State.PREPARED);
            if (run)
            {
                maybeFormBatchExclusive(true);
                return true;
            }
        }
        catch (Throwable t)
        {
            task.setStateExclusive(Task.State.FAILED);
            task.reportFailureExclusiveNoExcept(t, true);
        }
        completeCurrentTaskExclusive();
        return false;
    }

    /**
     * Complete {@link #task} (as {@link #completeTask} would) but without polling another, leaving {@link #task} null;
     * if it leads a batch, complete the batch
     */
    private void completeCurrentTaskExclusive()
    {
        Batch batch = this.batch;
        if (batch == null)
        {
            completeOneTaskExclusive();
        }
        else
        {
            this.batch = null;
            completeBatchExclusive(batch);
        }
    }

    private void completeOneTaskExclusive()
    {
        Task complete = task;
        try
        {
            complete.unsetQueue(kind);
            complete.completeExclusiveNoExcept();
        }
        catch (Throwable t)
        {
            complete.releaseResourcesExclusiveNoExcept();
            complete.unhandledException(t);
        }
        finally
        {
            active = 0;
            task = null;
            if (DEBUG_EXECUTION) debug.onSetTask(null);
        }
    }

    private boolean reject(Task task)
    {
        // continuations/in-progress incremental task represent work that has already started
        if (task.isContinuation() || task.hasIncrementalStarted())
            return false;

        if (!(task instanceof SafeTask<?>))
            return true;

        ExecutionContext context = ((SafeTask<?>) task).executionContext();
        return !(isTerminated ? (context instanceof Unterminatable) : context.isUnstoppable());
    }

    void completeTask()
    {
        // see Batch: complete its head (our task) and its followers, before we poll the next
        if (batch != null)
            completeCurrentTaskExclusive();

        Task complete = task;
        try
        {
            // a continuation may already have completed our task without polling another; see tryContinue
            if (complete != null)
            {
                complete.unsetQueue(kind);
                complete.completeExclusiveNoExcept();
            }
        }
        catch (Throwable t)
        {
            complete.releaseResourcesExclusiveNoExcept();
            throw t;
        }
        finally
        {
            active = 0;
            task = super.pollMulti();
            if (DEBUG_EXECUTION) debug.onSetTask(task);
            if (task != null)
            {
                selfTask.position = task.position;
                selfTask.unsafeSetStateExclusive(WAITING_TO_RUN);
                executor.runnable.enqueue(selfTask, false);
            }
        }
    }

    void enqueue(Task newTask, boolean incrementArrivals)
    {
        // selfTask may be assigned without a task, if a continuation completed its task but found no other; see tryContinue
        if (task != null || executor.runnable.isAssigned(selfTask))
        {
            if (incrementArrivals)
                executor.runnable.incrementArrivals(selfTask);
            // TODO (expected): restore some invariant here
//                Invariants.require(selfTask.isInHeap() || selfTask.is(RUNNING));
            super.enqueueMulti(newTask, incrementArrivals);
        }
        else
        {
            Invariants.require(waitingCount == 0);
            if (incrementArrivals)
                incrementArrivals(newTask);
            incrementDispatches(newTask);
            task = newTask;
            task.setQueue(kind);
            selfTask.position = newTask.position;
            selfTask.unsafeSetStateExclusive(WAITING_TO_RUN);
            executor.runnable.enqueue(selfTask, incrementArrivals);
            if (DEBUG_EXECUTION) debug.onSetTask(newTask);
        }
    }

    @Override
    void unqueue(Task remove)
    {
        if (remove == task) removeCurrentTask(remove);
        else super.unqueueMulti(remove);
    }

    boolean tryUnqueueWaiting(Task remove)
    {
        if (remove == task) return tryRemoveCurrentTask(remove);
        else return super.tryUnqueueWaiting(remove);
    }

    private boolean tryRemoveCurrentTask(IntrusiveHeapNode remove)
    {
        if (executor.runnable.isAssigned(selfTask))
            return false;

        removeCurrentTask(remove);
        return true;
    }

    private void removeCurrentTask(IntrusiveHeapNode remove)
    {
        Invariants.require(executor.runnable.isWaiting(selfTask));
        Invariants.require(remove == task);
        // cannot overwrite task while it is being executed - this cannot happen for SafeTask
        // but can for other tasks that don't track their own state

        decrementDispatches(task);
        task.unsetQueue(kind);
        active = 0;
        task = pollMulti();
        if (DEBUG_EXECUTION) debug.onSetTask(task);
        if (task == null) executor.runnable.unqueue(selfTask);
        else
        {
            selfTask.position = task.position;
            executor.runnable.requeue(selfTask);
        }
        Invariants.require(task == null || executor.runnable.isWaiting(selfTask));
    }

    public long unsafeContinuedCount()
    {
        return continuedCount;
    }

    public long unsafeBatchCount()
    {
        return batchCount;
    }

    /** the number of tasks that ran in a batch besides its head: followers, and inlined consequences */
    public long unsafeBatchedCount()
    {
        return batchedCount;
    }

    /** the number of consequences executed inline by a batch */
    public long unsafeInlinedCount()
    {
        return inlinedCount;
    }

    /**
     * The task whose execution is in progress, for {@link AccordExecutor#inherit}; invoked only by the owner
     */
    Task currentTask()
    {
        Batch batch = this.batch;
        if (batch != null && batch.running != null)
            return batch.running;
        return task;
    }

    /**
     * EXPERIMENTAL micro-batching; see {@link org.apache.cassandra.config.AccordConfig#queue_exclusive_batch_limit}.
     * <p>
     * A batch is formed (with the lock held) when {@link #task}, its head, has been prepared and will run, if any
     * followers are found; or once it has run, if it has consequences we might execute inline (see
     * {@link #tryFormBatchAfterRun}). Its followers are taken from the tasks queued behind the head on any of the entries
     * it holds (its keys and txnIds): repeatedly, on each such entry, we take the next task queued there that may follow,
     * i.e. that is queued only behind members on every entry it uses (see {@link SafeTask#isBatchableFollowerExclusive}),
     * until we find no more. So each follower may run once those before it have. Every entry used by a member is then
     * owned by the batch (see {@link AccordCacheEntry#OWNED}), freezing its queue and state until the batch completes,
     * and the followers leave the executor's queues (but retain their entry positions) to run in turn, after the head, on
     * the owner's thread without the lock. Each follower takes the value of each entry it uses from {@link #values}:
     * whatever the last member to modify it would have published, else the value it had when the batch formed.
     * <p>
     * Meanwhile any lock holder that would operate on this store's tasks, and so might modify an owned entry, parks the
     * operation to be performed once the batch completes; see {@link #parkIfBatchingExclusive}. This is guarded by
     * {@link AccordCacheEntry#requireNotOwned}. Since cache entries belong to a single store, only operations on behalf of
     * this store's tasks need be considered.
     * <p>
     * A consequence submitted to this store by a member inherits the member's position, so would normally be queued
     * ahead of (or, if fifo, certainly ahead of) many of the following members. So once a member has run, if the batch
     * owns every entry used by each of its consequences for this store (see {@link SafeTask#isInlinableConsequence}),
     * we add them to {@link #pending}, ordered as they would be queued; otherwise we stop. We then run either the first
     * pending consequence, if it would be runnable once placed behind only those tasks that have run; or else the next
     * follower, if no pending consequence would be placed ahead of it; or else we stop. So a consequence waits for any
     * followers that would be ahead of it (e.g. of equal position, but created earlier), and siblings run in queue order.
     * Consequences that do not execute exclusively on this store (or that are plain runnables, whose order relative to
     * the following members is a matter of queue selection), and those pending when we stop, are submitted as normal
     * when their parent completes. We also stop if the executor is stopped, or once we have exhausted our budget: each
     * task besides the head costs the number of entries it uses (keys and txnIds).
     * <p>
     * With the lock held, the batch completes: the owned entries are released; the followers that did not run return to
     * waiting as before (see {@link SafeTask#revertBatchedExclusive}); the inlined consequences are registered (with their
     * parents' tranches); the head completes; each other task that ran completes in the order it ran, a follower first
     * taking its locks (releasing its positions, see {@link SafeTask#lockBatchedExclusive}), and an inlined consequence
     * taking its locks unqueued (see {@link SafeTask#lockInlinedExclusive}); and finally any parked operations are
     * performed, in the order they were parked.
     */
    static final class Batch
    {
        final SafeTask<?> head;
        final ArrayList<SafeTask<?>> followers = new ArrayList<>();
        final ArrayList<Task.State> revertTo = new ArrayList<>();
        final ArrayList<AccordCacheEntry<?, ?, ?>> owned = new ArrayList<>();
        final IdentityHashMap<AccordCacheEntry<?, ?, ?>, Object> values = new IdentityHashMap<>();
        /** the number of followers that have run (or failed to prepare) */
        int followersRan;
        /** every task besides the head that has run (or failed to prepare), in the order they ran: followers, and inlined consequences */
        final ArrayList<SafeTask<?>> ran = new ArrayList<>();
        /** the consequences we executed inline */
        final Set<SafeTask<?>> inlined = Collections.newSetFromMap(new IdentityHashMap<>());
        /** the head and {@link #ran} */
        final Set<SafeTask<?>> hasRun = Collections.newSetFromMap(new IdentityHashMap<>());
        /** our remaining budget: each task besides the head costs the number of entries it uses; see {@link SafeTask#batchCost} */
        int remaining;
        /** consequences of tasks that have run, that we may yet execute inline, ordered as they would be queued */
        final ArrayList<SafeTask<?>> pending = new ArrayList<>();
        /** the parent of each consequence in {@link #pending} */
        final IdentityHashMap<SafeTask<?>, SafeTask<?>> parents = new IdentityHashMap<>();
        /** the member currently running, if any; modified only by the owner */
        SafeTask<?> running;
        /** operations that lock holders could not perform while the batch owned our entries */
        ArrayList<Runnable> parked;

        Batch(SafeTask<?> head, int limit)
        {
            this.head = head;
            this.remaining = limit;
        }

        boolean isOwned(AccordCacheEntry<?, ?, ?> entry)
        {
            return values.containsKey(entry);
        }

        boolean isMember(SafeTask<?> task)
        {
            return task == head || followers.contains(task);
        }

        /** with the lock held, record the current value of every entry used by {@code member} (that is not already recorded) and own it */
        void ownExclusive(SafeTask<?> member)
        {
            member.refs.forEach((k, v) -> {
                AccordCacheEntry<?, ?, ?> entry = SaferState.global(v);
                if (values.containsKey(entry))
                    return;
                // inflates the value if necessary, so must be done before we own the entry
                values.put(entry, entry.getExclusive());
                entry.setOwnedExclusive();
                owned.add(entry);
            });
        }

        Object valueOf(AccordCacheEntry<?, ?, ?> entry)
        {
            Invariants.require(values.containsKey(entry), "%s is not owned by the batch", entry);
            return values.get(entry);
        }

        void publish(AccordCacheEntry<?, ?, ?> entry, Object value)
        {
            // an entry we do not own was acquired while running, and is locked by the member, so cannot be used by others
            if (values.containsKey(entry))
                values.put(entry, value);
        }
    }

    /**
     * If our task may lead a batch (see {@link Batch}) find its followers, and if there are any (or we do not require
     * them) form the batch. Invoked with the lock held once our task has been prepared (requiring followers, since we
     * would otherwise freeze our entries while it runs for no benefit), or once it has run (see {@link #tryFormBatchAfterRun}).
     */
    private void maybeFormBatchExclusive(boolean requireFollowers)
    {
        int limit = executor.exclusiveBatchLimit;
        if (limit <= 0 || isStopped || !(task instanceof SafeTask<?>))
            return;

        Invariants.require(batch == null);
        SafeTask<?> head = (SafeTask<?>) task;
        List<AccordCacheEntry<?, ?, ?>> entries = head.batchEntriesExclusive();
        if (entries == null)
            return;

        Batch batch = new Batch(head, limit);
        // each entry is a lane of candidates, in the order they may become runnable there; a candidate may follow only
        // once those ahead of it on every entry it uses are members, so we take from each lane in turn until we find none
        List<List<SafeTask<?>>> lanes = new ArrayList<>(entries.size());
        for (int i = 0 ; i < entries.size() ; ++i)
        {
            List<SafeTask<?>> lane = new ArrayList<>();
            entries.get(i).collectQueuedInRunOrder(lane, limit);
            if (!lane.isEmpty())
                lanes.add(lane);
        }

        int budget = limit;
        int[] cursors = new int[lanes.size()];
        boolean progress = true;
        while (progress)
        {
            progress = false;
            for (int i = 0 ; i < lanes.size() ; ++i)
            {
                List<SafeTask<?>> lane = lanes.get(i);
                while (cursors[i] < lane.size())
                {
                    SafeTask<?> candidate = lane.get(cursors[i]);
                    if (!batch.isMember(candidate))
                    {
                        int cost = candidate.batchCost();
                        if (cost > budget || !candidate.isBatchableFollowerExclusive(batch::isMember))
                            break;
                        batch.followers.add(candidate);
                        budget -= cost;
                        progress = true;
                    }
                    ++cursors[i];
                }
            }
        }

        if (requireFollowers && batch.followers.isEmpty())
            return;

        batch.ownExclusive(head);
        for (int i = 0 ; i < batch.followers.size() ; ++i)
        {
            SafeTask<?> follower = batch.followers.get(i);
            batch.ownExclusive(follower);
            batch.revertTo.add(follower.joinBatchExclusive());
            if (DEBUG_EXECUTION) DebugTask.get(follower).onPolled();
        }
        this.batch = batch;
    }

    /**
     * Run the followers of our batch (and any inlined consequences), having run its head; invoked by the owner, without
     * the lock (unless RUN_WITH_LOCK)
     */
    private void runBatch(TaskRunner self, Batch batch)
    {
        try
        {
            if (!onRanBatched(batch, batch.head))
                return;

            while (!isStopped)
            {
                SafeTask<?> next;
                SafeTask<?> consequence = batch.pending.isEmpty() ? null : batch.pending.get(0);
                SafeTask<?> follower = batch.followersRan < batch.followers.size() ? batch.followers.get(batch.followersRan) : null;
                if (consequence != null && consequence.batchCost() <= batch.remaining && consequence.isRunnableIfPlaced(batch.hasRun::contains))
                {
                    next = consequence;
                    batch.pending.remove(0);
                    batch.parents.remove(consequence).removeConsequence(consequence);
                    consequence.joinBatchInline();
                    batch.inlined.add(consequence);
                }
                else if (follower != null && follower.batchCost() <= batch.remaining && !isPrecededByPending(batch, follower))
                {
                    next = follower;
                    ++batch.followersRan;
                }
                else break;

                if (!runBatched(self, batch, next) || !onRanBatched(batch, next))
                    break;
            }
        }
        finally
        {
            batch.running = null;
        }
    }

    /** whether any pending consequence would be placed ahead of {@code follower} on an entry they share */
    private static boolean isPrecededByPending(Batch batch, SafeTask<?> follower)
    {
        for (int i = 0 ; i < batch.pending.size() ; ++i)
        {
            if (follower.isPrecededOnSharedEntry(batch.pending.get(i)))
                return true;
        }
        return false;
    }

    /**
     * Prepare (from the batch's values) and run {@code task}, a follower or inlined consequence
     * @return false if we failed to prepare it, and should stop
     */
    private boolean runBatched(TaskRunner self, Batch batch, SafeTask<?> task)
    {
        batch.remaining -= task.batchCost();
        batch.ran.add(task);
        batch.running = task;
        try
        {
            task.prepareBatchedMayThrow(batch::valueOf);
        }
        catch (Throwable t)
        {
            // as a failed prepare; we will release whatever we managed to prepare when the batch completes
            task.setRunState(RunState.RUN_FAILED);
            task.reportFailureExclusiveNoExcept(t, false);
            return false;
        }

        if (DEBUG_EXECUTION) DebugTask.get(task).onPreRun();
        task.runNoExcept(self);
        return true;
    }

    /**
     * {@code task} has run as a member of the batch: record the values it would publish, and add to {@link Batch#pending}
     * any consequences it has submitted to this store (see {@link Batch}).
     * @return false if the batch should stop
     */
    private boolean onRanBatched(Batch batch, SafeTask<?> task)
    {
        batch.hasRun.add(task);
        task.forEachBatchedUpdate(batch::publish);
        if (isStopped || task.is(RunState.REJECTED))
            return false;

        // a failed task's SafeTask consequences will be cancelled when it completes
        if (task.is(RunState.RUN_FAILED))
            return true;

        for (Task consequence = task.next ; consequence != null ; consequence = consequence.next)
        {
            if (!(consequence instanceof SafeTask<?>) || ((SafeTask<?>) consequence).commandStore.exclusiveExecutor() != this)
                continue;

            SafeTask<?> add = (SafeTask<?>) consequence;
            // if we cannot execute it inline, it may be queued ahead of any of our remaining members
            if (!add.isInlinableConsequence(batch::isOwned))
                return false;

            int i = 0;
            while (i < batch.pending.size() && AccordCacheEntryQueue.compareForNotify(batch.pending.get(i), add) <= 0)
                ++i;
            batch.pending.add(i, add);
            batch.parents.put(add, task);
        }
        return true;
    }

    /**
     * Complete our batch (see {@link Batch}), including its head ({@link #task}), leaving {@link #task} null;
     * invoked with the lock held. {@link #task} remains set until we are done, so that any task made runnable meanwhile
     * is queued behind it (as when completing a single task) rather than assigned directly.
     */
    private void completeBatchExclusive(Batch batch)
    {
        Invariants.require(batch.head == task);
        try
        {
            for (int i = 0 ; i < batch.owned.size() ; ++i)
                batch.owned.get(i).clearOwnedExclusive();

            // those that did not run return to waiting first, so that they are notified as normal by those that did
            for (int i = batch.followersRan ; i < batch.followers.size() ; ++i)
            {
                SafeTask<?> follower = batch.followers.get(i);
                try { follower.revertBatchedExclusive(batch.revertTo.get(i)); }
                catch (Throwable t) { follower.unhandledException(t); }
            }

            // an inlined consequence was never submitted, so must be registered before its parent completes
            for (SafeTask<?> inlined : batch.inlined)
            {
                executor.registerInlinedExclusive(inlined);
                // nor did it arrive, so we account for its arrival as well as its dispatch
                incrementArrivals(inlined);
                executor.runnable.incrementArrivals(selfTask);
            }

            SafeTask<?> head = batch.head;
            try
            {
                head.unsetQueue(kind);
                head.completeExclusiveNoExcept();
            }
            catch (Throwable t)
            {
                head.releaseResourcesExclusiveNoExcept();
                head.unhandledException(t);
            }

            for (int i = 0 ; i < batch.ran.size() ; ++i)
            {
                SafeTask<?> ran = batch.ran.get(i);
                // we bypassed the executor's queues, but must account for having dispatched the task
                incrementDispatches(ran);
                executor.runnable.incrementDispatches(selfTask);
                try
                {
                    if (batch.inlined.contains(ran)) ran.lockInlinedExclusive();
                    else ran.lockBatchedExclusive();
                }
                catch (Throwable t) { ran.unhandledException(t); }
                ran.completeExclusiveNoExcept();
            }

            //noinspection NonAtomicOperationOnVolatileField
            ++batchCount;
            //noinspection NonAtomicOperationOnVolatileField
            batchedCount += batch.ran.size();
            //noinspection NonAtomicOperationOnVolatileField
            inlinedCount += batch.inlined.size();

            if (batch.parked != null)
            {
                for (int i = 0 ; i < batch.parked.size() ; ++i)
                {
                    try { batch.parked.get(i).run(); }
                    catch (Throwable t) { selfTask.unhandledException(t); }
                }
            }
        }
        finally
        {
            active = 0;
            task = null;
            if (DEBUG_EXECUTION) debug.onSetTask(null);
        }
    }

    /**
     * If this store has a batch in progress (see {@link Batch}) park {@code operation} to be performed once it completes,
     * and return true; otherwise return false, and the caller should perform the operation now. Invoked with the lock
     * held, before performing any operation on behalf of this store's tasks that might modify a cache entry's queue.
     * Operations are performed in the order they were parked.
     */
    boolean parkIfBatchingExclusive(Runnable operation)
    {
        Batch batch = this.batch;
        if (batch == null)
            return false;

        Invariants.require(executor.isOwningThread());
        if (batch.parked == null)
            batch.parked = new ArrayList<>();
        batch.parked.add(operation);
        return true;
    }

    public boolean inExecutor()
    {
        return owner == Thread.currentThread();
    }

    public boolean stopped()
    {
        return visibleIsStopped;
    }

    public void stop()
    {
        Invariants.require(inExecutor());
        this.isStopped = true;
        this.visibleIsStopped = true;
    }

    public void fullStop()
    {
        Invariants.require(inExecutor());
        this.visibleIsStopped = this.isTerminated = this.isStopped = true;
    }

    @Override
    public AsyncChain<Void> chain(Runnable run)
    {
        return AsyncChains.chain(this, run);
    }

    @Override
    public AsyncChain<Void> continuationChain(Runnable run)
    {
        return AsyncChains.continuationChain(this, run);
    }

    @Override
    public <V> AsyncChain<V> chain(Callable<V> call)
    {
        return AsyncChains.chain(this, call);
    }

    @Override
    public <V> AsyncChain<V> flatChain(Callable<? extends AsyncChain<V>> call)
    {
        return AsyncChains.flatChain(this, call);
    }

    @Override
    public void execute(Runnable run)
    {
        Task inherit = executor.inherit();
        PlainRunnable task = new PlainRunnable(executor, null, run, this, globalGroup(), ExclusiveGroup.OTHER);
        if (inherit != null) inherit.addConsequence(task);
        else executor.submitTask(task);
    }

    private Cancellable execute(PlainChain task)
    {
        Task inherit = executor.inherit();
        if (inherit != null) inherit.addConsequence(task);
        else executor.submitTask(task);
        return task;
    }

    private Cancellable executeContinuation(PlainChain task)
    {
        Task inherit = executor.inherit();
        if (inherit == null) executor.submitTask(task);
        else
        {
            task.setContinuation();
            inherit.addConsequence(task);
        }
        return task;
    }

    private static GlobalGroup globalGroup(int commandStoreId)
    {
        return commandStoreId < 0 ? GlobalGroup.OTHER : COMMAND_STORE;
    }

    private GlobalGroup globalGroup()
    {
        return globalGroup(commandStoreId);
    }

    @Override
    public Cancellable execute(Runnable run, BiConsumer<? super Void, Throwable> callback)
    {
        return execute(new PlainChain<>(executor, returningVoid(run), callback, ExclusiveExecutor.this, globalGroup(), ExclusiveGroup.OTHER));
    }

    @Override
    public <V> Cancellable execute(Callable<V> call, BiConsumer<? super V, Throwable> callback)
    {
        return execute(new PlainChain<>(executor, call, callback, ExclusiveExecutor.this, globalGroup(), ExclusiveGroup.OTHER));
    }

    @Override
    public <V> Cancellable flatExecute(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback)
    {
        return execute(new PlainChain<>(executor, call, flatCallback(callback), ExclusiveExecutor.this, globalGroup(), ExclusiveGroup.OTHER));
    }

    @Override
    public Cancellable executeContinuation(Runnable run, BiConsumer<? super Void, Throwable> callback)
    {
        return executeContinuation(new PlainChain<>(executor, returningVoid(run), callback, ExclusiveExecutor.this, globalGroup(), ExclusiveGroup.OTHER));
    }

    @Override
    public <V> Cancellable executeContinuation(Callable<V> call, BiConsumer<? super V, Throwable> callback)
    {
        return executeContinuation(new PlainChain<>(executor, call, callback, ExclusiveExecutor.this, globalGroup(), ExclusiveGroup.OTHER));
    }

    @Override
    public <V> Cancellable flatExecuteContinuation(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback)
    {
        return executeContinuation(new PlainChain<>(executor, call, flatCallback(callback), ExclusiveExecutor.this, globalGroup(), ExclusiveGroup.OTHER));
    }

    @Override
    public boolean tryExecuteImmediately(Runnable run)
    {
        Thread thread = Thread.currentThread();
        Thread owner = this.owner;
        if (owner != null && owner != thread)
            return false;

        TaskRunner self = TaskRunner.get(thread);
        AccordExecutor active = self.accordActiveExecutor();
        if (active != null && active != executor)
            return false; // prevent cross-executor locking/execution

        if (owner == null && !ownerUpdater.compareAndSet(this, null, thread))
            return false;

        try
        {
            if (active == null)
                self.setAccordActiveExecutor(executor);

            run.run();
        }
        catch (Throwable t)
        {
            selfTask.unhandledException(t);
        }
        finally
        {
            if (owner == null)
            {
                Thread waiting = this.waiting;
                Invariants.require(waiting != thread);
                this.owner = waiting;
                if (waiting == null) // recheck, to ensure happens-before relation with a new waiter that expects any non-null owner to notify it
                    waiting = this.waiting;
                if (waiting != null)
                    LockSupport.unpark(waiting);
            }

            if (active == null)
                self.setAccordActiveExecutor(null);
        }
        return true;
    }
}
