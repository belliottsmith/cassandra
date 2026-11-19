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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.impl.TestAgent;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by LLM.
 *
 * <p>AccordExecutorSignalLoop queues work that is ready to run on one of two queues: command store work, and all other
 * work. A thread holding a permit must find a task in one of them, even if another thread takes the one it would have
 * found in the second queue it polls after it has polled the first. We submit a large volume of tiny tasks of both kinds
 * from several threads at once, so that both queues are contended, and require that every task runs and that no
 * exception is reported (a thread that failed to find its task would report an exception and strand the task).
 */
public class AccordExecutorSignalLoopReadyQueuesTest
{
    private static final int STORES = 4;
    private static final int PRODUCERS = 4;
    private static final int TASKS_PER_PRODUCER = 50_000;
    private static final int ROUNDS = 20;

    static class RecordingAgent extends TestAgent
    {
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        @Override
        public void onException(Throwable t)
        {
            failure.compareAndSet(null, t);
            super.onException(t);
        }
    }

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void mixedReadyWorkTest() throws InterruptedException
    {
        for (int round = 0 ; round < ROUNDS ; ++round)
        {
            for (int threads : new int[] { 2, 3, 4, 8 })
                run(threads);
        }
    }

    private static void run(int threads) throws InterruptedException
    {
        RecordingAgent agent = new RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, threads, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        try
        {
            ExclusiveExecutor[] stores = new ExclusiveExecutor[STORES];
            for (int i = 0 ; i < STORES ; ++i)
                stores[i] = executor.newExclusiveExecutor(i);

            CountDownLatch done = new CountDownLatch(PRODUCERS * TASKS_PER_PRODUCER);
            Thread[] producers = new Thread[PRODUCERS];
            for (int p = 0 ; p < PRODUCERS ; ++p)
            {
                int producer = p;
                producers[p] = new Thread(() -> {
                    for (int i = 0 ; i < TASKS_PER_PRODUCER ; ++i)
                    {
                        // alternate command store work with other work, so that both ready queues are in use
                        if ((i & 1) == 0) stores[(producer + i) % STORES].execute(done::countDown);
                        else executor.execute(done::countDown);
                    }
                }, "Producer" + p);
                producers[p].start();
            }
            for (Thread producer : producers)
                producer.join();

            // a stranded task leaves us waiting, so stop as soon as a failure has been reported
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            boolean completed;
            while (!(completed = done.await(100, TimeUnit.MILLISECONDS)) && agent.failure.get() == null && System.nanoTime() < deadline) {}
            if (agent.failure.get() != null)
                throw new AssertionError("unexpected exception reported to the agent with " + threads + " threads", agent.failure.get());
            assertTrue("timed out with " + threads + " threads and " + done.getCount() + " tasks outstanding", completed);
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
