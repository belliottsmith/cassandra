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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.CommitlogShutdownException;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.db.commitlog.CommitLogArchiver.disabled;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/* Check that shutting down a commit log segment manager will throw an exception rather than
 * waiting for a commitlog segment that will never come.
 */
public class AbstractCommitLogSegmentManagerShutdownTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static class TestCLSM extends AbstractCommitLogSegmentManager
    {
        Condition allocatingSecondSegment;
        Condition resumeAllocating;
        int allocatedSegments = 0;

        TestCLSM(CommitLog commitLog, String storageDirectory)
        {
            this(commitLog, storageDirectory, null, null);
        }
        TestCLSM(CommitLog commitLog, String storageDirectory, Condition allocatingSecondSegment, Condition resumeAllocating)
        {
            super(commitLog, storageDirectory);
            this.allocatingSecondSegment = allocatingSecondSegment;
            this.resumeAllocating = resumeAllocating;
        }

        @Override
        public CommitLogSegment.Allocation allocate(Mutation mutation, int size)
        {
            // simulate failure to allocate, move to a new segment
            CommitLogSegment segment = allocatingFrom();
            advanceAllocatingFrom(segment);
            return null;
        }

        @Override
        CommitLogSegment createSegment()
        {
            if (allocatedSegments == 1)
            {
                if (allocatingSecondSegment != null)
                    allocatingSecondSegment.signalAll();
                if (resumeAllocating != null)
                    assertTrue(resumeAllocating.awaitUninterruptibly(1, TimeUnit.MINUTES));
            }
            allocatedSegments++;

            // Dummy segment for the test
            return new CommitLogSegment(commitLog, this)
            {
                @Override ByteBuffer createBuffer(CommitLog commitLog) { return null; }
                @Override void write(int lastSyncedOffset, int nextMarker) { }
                @Override void flush(int startMarker, int nextMarker) {}
                @Override public long onDiskSize() { return 0; }
                @Override void discard(boolean deleteFile) { /* discarded, honest */ }
                @Override void discardUnusedTail() { /* definitely all gone */ }
                @Override synchronized void close() { /* thoroughly closed */ }
            };
        }

        @Override
        void discard(CommitLogSegment segment, boolean delete)
        {
        }
    }

    @Test
    public void throwsWaitingForCommitlogAfterShutdown() throws IOException, InterruptedException
    {
        TableMetadata tmd = MockSchema.newTableMetadata("aclsmtest");
        DecoratedKey pk = tmd.partitioner.decorateKey(UTF8Type.instance.fromString("key"));
        CommitLog commitLog = new CommitLog(disabled());
        Path storageDir = Files.createTempDirectory("aclsmtest" + nanoTime());
        Condition allocatingSecondSegment = Condition.newOneTimeCondition(); // for blocking the commitlog allocator thread
        Condition allocateCallerRunning = Condition.newOneTimeCondition(); // make sure allocator caller thread id running
        Condition resumeAllocating = Condition.newOneTimeCondition(); // unblock the commitlog allocator

        AbstractCommitLogSegmentManager clsm = new TestCLSM(commitLog, storageDir.toString(),
                                                            allocatingSecondSegment, resumeAllocating);
        clsm.start();
        assertTrue(allocatingSecondSegment.awaitUninterruptibly(1, TimeUnit.MINUTES));

        // Call ACLSM.allocate on another thread so the main thread can call shutdown while it is blocked
        AtomicReference<Throwable> allocateException = new AtomicReference<>();
        Thread callAllocate = new Thread(() -> {
            allocateCallerRunning.signalAll();

            try
            {
                // Allocate something large to force a new segment which should block awaiting a new segment
                clsm.allocate(new Mutation(PartitionUpdate.emptyUpdate(tmd, pk)),
                              DatabaseDescriptor.getCommitLogSegmentSize()+1);
            }
            catch (Throwable t)
            {
                allocateException.set(t);
            }
        });
        callAllocate.start();

        // make sure the future is running and waiting
        assertTrue(allocateCallerRunning.awaitUninterruptibly(1, TimeUnit.MINUTES));
        resumeAllocating.signalAll();
        clsm.shutdown();

        // Make sure the allocate call throws an exception after the segment manager is shutdown
        clsm.awaitTermination(30, TimeUnit.SECONDS);
        callAllocate.join(60000, 0);
        assertFalse(callAllocate.isAlive());

        assertTrue(allocateException.get() instanceof CommitlogShutdownException);
    }

    @Test
    public void allocateWhileShutdownTest() throws IOException, InterruptedException
    {
        CommitLog commitLog = new CommitLog(disabled());
        Path storageDir = Files.createTempDirectory("aclsmtest" + nanoTime());
        AbstractCommitLogSegmentManager clsm = new TestCLSM(commitLog, storageDir.toString());

        clsm.start();
        clsm.shutdown();
        assertTrue(clsm.awaitTermination(10, TimeUnit.SECONDS));

        try
        {
            clsm.awaitAvailableSegment(clsm.allocatingFrom());
            fail("Expected CommitlogShutdownException");
        }
        catch (CommitlogShutdownException ex)
        {
            assertEquals("commitlog segment will never be available - segment allocator is terminated", ex.getMessage());
        }
    }
}
