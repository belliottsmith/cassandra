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

package org.apache.cassandra.journal;

import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Journal.onDurable(pointer, callback) must run the callback only once the record at
 * pointer is fsynced. AccordJournal.saveCommand/append use exactly this API to delay
 * Accord replies until their journal records are durable.
 *
 * In GROUP/BATCH flush mode Flusher.isDurable compares the record's START position with
 * fsyncFinishedForPosition (the END of the last fsync). A record that is appended right
 * after an fsync starts exactly at that position and is reported durable at once.
 */
public class JournalOnDurableTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    /** GROUP mode with a long period: no flush happens unless the test asks for one. */
    static class GroupParams extends TestParams
    {
        @Override
        public FlushMode flushMode()
        {
            return FlushMode.GROUP;
        }

        @Override
        public long flushPeriod(TimeUnit units)
        {
            return units.convert(60, TimeUnit.SECONDS);
        }
    }

    @Test
    public void onDurableCallbackRunsOnlyAfterRecordIsFsynced() throws Exception
    {
        File directory = new File(Files.createTempDirectory("JournalOnDurableTest"));
        directory.deleteRecursiveOnExit();

        Journal<TimeUUID, Long> journal =
            new Journal<>("TestJournal", directory, new GroupParams(), TimeUUIDKeySupport.INSTANCE,
                          JournalTest.LongSerializer.INSTANCE, SegmentCompactor.noop(), new OpOrder());
        journal.start(0L);
        try
        {
            // record A, made durable by an explicit fsync
            RecordPointer a = journal.asyncWrite(nextTimeUUID(), 1L);
            CountDownLatch aDurable = new CountDownLatch(1);
            journal.onDurable(a, aDurable::countDown);
            journal.fsync();
            assertTrue("record A never reported durable", aDurable.await(10, TimeUnit.SECONDS));

            ActiveSegment<TimeUUID, Long> segment = journal.currentActiveSegment();
            int fsyncedBeforeB = segment.fsyncedTo();

            // record B, appended after the fsync; nobody requests a flush and the group
            // period is 60s, so B cannot be on disk yet
            RecordPointer b = journal.asyncWrite(nextTimeUUID(), 2L);
            AtomicInteger fsyncedToAtCallback = new AtomicInteger(-1);
            AtomicReference<String> where = new AtomicReference<>();
            CountDownLatch bCallback = new CountDownLatch(1);
            journal.onDurable(b, () -> {
                fsyncedToAtCallback.set(segment.fsyncedTo());
                where.set(Thread.currentThread().getName());
                bCallback.countDown();
            });

            // allow a short window: a correct implementation does not run the callback here
            if (bCallback.await(500, TimeUnit.MILLISECONDS))
            {
                int fsynced = fsyncedToAtCallback.get();
                if (fsynced <= b.position)
                    fail("onDurable callback for record B at " + b + " ran on thread '" + where.get() +
                         "' while the segment was fsynced only to " + fsynced +
                         " (fsynced before B was written: " + fsyncedBeforeB +
                         "); none of B's bytes are on disk");
            }

            // and once B is fsynced the callback must run
            journal.fsync();
            assertTrue("record B never reported durable after fsync", bCallback.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            journal.stop();
        }
    }
}
