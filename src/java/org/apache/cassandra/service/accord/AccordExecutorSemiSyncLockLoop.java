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

import accord.api.Agent;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.utils.concurrent.ConcurrentLinkedStack;
import org.apache.cassandra.utils.concurrent.LockWithAsyncSignal;

import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;

abstract class AccordExecutorSemiSyncLockLoop extends AccordExecutor
{
    final LockWithAsyncSignal lock;
    final ConcurrentLinkedStack<Object> submitted = new ConcurrentLinkedStack<>();

    public AccordExecutorSemiSyncLockLoop(AccordStateCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        this(new LockWithAsyncSignal(), metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
    }

    private AccordExecutorSemiSyncLockLoop(LockWithAsyncSignal lock, AccordStateCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        super(lock, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
        this.lock = lock;
    }

    public boolean hasTasks()
    {
        return !submitted.isEmpty() || tasks > 0 || running > 0;
    }

    void notifyWorkAsync()
    {
        lock.signal();
    }

    @Override
    void notifyWorkExclusive()
    {
        lock.signal();
    }

    boolean isInThread()
    {
        return lock.isOwner(Thread.currentThread());
    }

    Interruptible.Task task(Mode mode)
    {
        return mode == RUN_WITH_LOCK ? this::runWithLock : this::runWithoutLock;
    }

    <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        boolean applySync = true;
        if (!lock.tryLock())
        {
            submitted.push(async.apply(p1a, p2, p3, p4));
            applySync = false;
            if (!lock.tryLock())
            {
                notifyWorkAsync();
                return;
            }
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
        }
    }

    void drainSubmittedExclusive()
    {
        submitted.drain(AccordExecutor::consumeExclusive, this, true);
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
                Task task = waitingToRun.poll();

                if (task != null)
                {
                    --tasks;
                    try
                    {
                        task.preRunExclusive();
                        task.run();
                    }
                    catch (Throwable t)
                    {
                        task.fail(t);
                    }
                    finally
                    {
                        task.cleanupExclusive();
                    }
                }
                else
                {
                    running = 0;
                    if (state != NORMAL)
                        return;

                    lock.clearSignal();
                    if (waitingToRun.isEmpty() && submitted.isEmpty())
                        lock.await();
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
        }
    }

    protected void runWithoutLock(Interruptible.State state) throws InterruptedException
    {
        boolean isRunning = false;
        Task task = null;
        try
        {
            while (true)
            {
                lock.lock();
                try
                {
                    if (task != null)
                    {
                        task.cleanupExclusive();
                        task = null;
                    }
                    else
                    {
                        ++running;
                        isRunning = true;
                    }

                    while (true)
                    {
                        drainSubmittedExclusive();
                        task = waitingToRun.poll();
                        if (task != null)
                            break;

                        if (state != NORMAL)
                            return;

                        lock.clearSignal();
                        if (waitingToRun.isEmpty() && submitted.isEmpty())
                        {
                            isRunning = false;
                            --running;
                            lock.await();
                            ++running;
                            isRunning = true;
                        }
                    }
                    --tasks;
                    task.preRunExclusive();
                }
                finally
                {
                    lock.unlock();
                }

                try
                {
                    task.run();
                }
                catch (Throwable t)
                {
                    try
                    {
                        task.fail(t);
                    }
                    catch (Throwable t2)
                    {
                        t2.addSuppressed(t);
                        agent.onUncaughtException(t2);
                    }
                }
            }
        }
        finally
        {
            if (task != null || isRunning)
            {
                lock.lock();
                try
                {
                    if (isRunning) --running;
                    if (task != null) task.cleanupExclusive();
                }
                finally
                {
                    lock.unlock();
                }
            }
        }
    }
}
