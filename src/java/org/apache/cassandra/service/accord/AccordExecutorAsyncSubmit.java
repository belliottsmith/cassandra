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

import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import accord.api.Agent;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.utils.concurrent.LockWithAsyncSignal;

class AccordExecutorAsyncSubmit extends AccordExecutorAbstractSemiSyncSubmit
{
    private final AccordExecutorInfiniteLoops loops;
    private final LockWithAsyncSignal lock;

    public AccordExecutorAsyncSubmit(Mode mode, int threads, IntFunction<String> name, AccordStateCacheMetrics metrics, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor, ExecutorPlus rangeLoadExecutor, Agent agent)
    {
        this(mode, threads, name, metrics, constantFactory(loadExecutor::submit), constantFactory(saveExecutor::submit), constantFactory(rangeLoadExecutor::submit), agent);
    }

    public AccordExecutorAsyncSubmit(Mode mode, int threads, IntFunction<String> name, AccordStateCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        this(new LockWithAsyncSignal(), mode, threads, name, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
    }

    private AccordExecutorAsyncSubmit(LockWithAsyncSignal lock, Mode mode, int threads, IntFunction<String> name, AccordStateCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        super(lock, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
        this.lock = lock;
        this.loops = new AccordExecutorInfiniteLoops(mode, threads, name, this::task);
    }

    @Override
    void awaitExclusive() throws InterruptedException
    {
        lock.clearSignal();
        if (waitingToRun.isEmpty() && submitted.isEmpty())
            lock.await();
    }

    @Override
    boolean isInLoop()
    {
        return loops.isInLoop();
    }

    @Override
    void notifyWorkAsync()
    {
        lock.signal();
    }

    @Override
    void notifyWorkExclusive()
    {
        lock.signal();
    }

    @Override
    boolean isInThread()
    {
        return lock.isOwner(Thread.currentThread());
    }

    @Override
    public void shutdown()
    {
        loops.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return loops.shutdownNow();
    }

    @Override
    public boolean isTerminated()
    {
        return loops.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return loops.awaitTermination(timeout, unit);
    }
}
