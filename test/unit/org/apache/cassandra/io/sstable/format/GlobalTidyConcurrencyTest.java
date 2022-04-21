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

package org.apache.cassandra.io.sstable.format;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.junit.Assert.*;

public class GlobalTidyConcurrencyTest
{
    private static final Logger logger = LoggerFactory.getLogger(GlobalTidyConcurrencyTest.class);

    @BeforeClass
    public static void setup()
    {
        System.setProperty("cassandra.debugrefcount", "true");
    }

    /**
     * This is a basic concurrency test for {@link SSTableReader.GlobalTidy}
     *
     * The scenario is to emulate basic get -> work -> release workflow for the same
     * sstable in multiple threads.
     *
     * The test detects:
     * - assertion failures in GlobalTidy code
     * - resource leaks
     * - GlobalTidy.lookup modification when the relevant Ref is alive
     */
    @Test
    public void tidyVsGetRaceTest() throws InterruptedException
    {
        int NUM_THREADS = 32;
        Descriptor desc = createDescriptor();
        AtomicBoolean exit = new AtomicBoolean(false);
        AtomicBoolean leakHappened = new AtomicBoolean(false);
        CopyOnWriteArrayList<Object> errors = new CopyOnWriteArrayList<>();

        Ref.setOnLeak(state -> { logger.error("Leak detected {}", state); leakHappened.set(true); exit.set(true); });

        class TestThread extends Thread
        {
            public TestThread(int idx)
            {
                super("test-thread-" + idx);
            }

            @Override
            public void run()
            {
                try
                {
                    while (!exit.get())
                    {
                        Ref<SSTableReader.GlobalTidy> ref = SSTableReader.GlobalTidy.get(desc);
                        LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(1000000));
                        Ref<SSTableReader.GlobalTidy> currentTidy = SSTableReader.GlobalTidy.lookup.get(desc);
                        if (!currentTidy.refers(ref.get()))
                        {
                            String error = String.format("GlobalTidy to which we keep reference is different than the GlobalTidy associated with this descriptor in SSTableReader.GlobalTidy.lookup; local=%s, lookup=%s, descriptor=%s", ref.get(), currentTidy, desc);
                            ref.release();
                            throw new AssertionError(error);
                        }
                        ref.release();
                    }
                }
                catch (Throwable e)
                {
                    logger.error("Stopping test due to error ", e);
                    errors.add(e);
                    exit.set(true);
                }
            }
        }

        Thread[] threads = new Thread[NUM_THREADS];

        IntStream.range(0, NUM_THREADS)
                 .forEach(idx -> threads[idx] = new TestThread(idx));

        for (Thread thread : threads)
        {
            thread.start();
        }

        int NUM_TICKS = 10;
        for (int tick = 0; tick < NUM_TICKS && !exit.get(); tick++)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            logger.info("Tick {}...", tick);
        }
        exit.set(true);

        for (Thread thread : threads)
        {
            thread.join();
        }

        if (!errors.isEmpty())
        {
            errors.forEach(error -> logger.error("Error: ", error));
            fail("Unexpected errors in the test");
        }

        assertFalse("check the logs, LEAK happened", leakHappened.get());
    }

    private Descriptor createDescriptor()
    {
        return new Descriptor(BigFormat.instance.getLatestVersion(),
                              new File("whatever"),
                              "keyspace",
                              "table",
                              new SequenceBasedSSTableId(1),
                              SSTableFormat.Type.BIG);
    }
}