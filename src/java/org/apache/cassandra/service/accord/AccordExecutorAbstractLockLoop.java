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

import java.util.concurrent.locks.Lock;

import accord.api.Agent;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;

import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;

abstract class AccordExecutorAbstractLockLoop extends AccordExecutor
{
    AccordExecutorAbstractLockLoop(Lock lock, AccordStateCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        super(lock, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
    }

    void preStart() {}
    void prePoll() {}
    abstract void await() throws InterruptedException;

    Interruptible.Task task(Mode mode)
    {
        return mode == RUN_WITH_LOCK ? this::runWithLock : this::runWithoutLock;
    }

    protected void runWithLock(Interruptible.State state) throws InterruptedException
    {
        lock.lockInterruptibly();
        try
        {
            preStart();
            while (true)
            {
                running = 1;
                prePoll();
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

                    await();
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
            preStart();
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
                        prePoll();
                        task = waitingToRun.poll();
                        if (task != null)
                            break;

                        if (state != NORMAL)
                            return;

                        isRunning = false;
                        --running;
                        await();
                        ++running;
                        isRunning = true;
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
