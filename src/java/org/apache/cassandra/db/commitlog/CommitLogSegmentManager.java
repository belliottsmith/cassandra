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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import com.google.common.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;

/**
 * Performs eager-creation of commit log segments in a background thread. All the
 * public methods are thread safe.
 */
public class CommitLogSegmentManager
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManager.class);

    /**
     * Segment that is ready to be used. The management thread fills this and blocks until consumed.
     */
    private final AtomicReference<CommitLogSegment> availableSegment = new AtomicReference<>();
    private final WaitQueue segmentPrepared = new WaitQueue();

    /** Active segments, containing unflushed data. The tail of this queue is the one we allocate writes to */
    private final ConcurrentLinkedQueue<CommitLogSegment> activeSegments = new ConcurrentLinkedQueue<>();

    /** The segment we are currently allocating commit log records to */
    private volatile CommitLogSegment allocatingFrom = null;

    /**
     * Tracks commitlog size, in multiples of the segment size.  We need to do this so we can "promise" size
     * adjustments ahead of actually adding/freeing segments on disk, so that the "evict oldest segment" logic
     * can see the effect of recycling segments immediately (even though they're really happening asynchronously
     * on the manager thread, which will take a ms or two).
     */
    private final AtomicLong size = new AtomicLong();

    private Thread managerThread;
    private final CommitLog commitLog;
    private volatile boolean shutdown;

    CommitLogSegmentManager(final CommitLog commitLog)
    {
        this.commitLog = commitLog;
    }

    void start()
    {
        // The run loop for the manager thread
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (!shutdown)
                {
                    try
                    {
                        assert availableSegment.get() == null;
                        logger.debug("No segments in reserve; creating a fresh one");
                        availableSegment.lazySet(CommitLogSegment.createSegment(commitLog));
                        if (shutdown)
                        {
                            CommitLogSegment next = availableSegment.getAndSet(null);
                            if (next != null)
                                next.discard(true);
                            return;
                        }

                        segmentPrepared.signalAll();
                        Thread.yield();

                        if (availableSegment.get() == null)
                            // Writing threads need another segment now.
                            continue;

                        // Writing threads are not waiting for new segments, we can spend time on other tasks.
                        // flush old Cfs if we're full
                        long unused = unusedCapacity();
                        if (unused < 0)
                        {
                            List<CommitLogSegment> segmentsToRecycle = new ArrayList<>();
                            long spaceToReclaim = 0;
                            for (CommitLogSegment segment : activeSegments)
                            {
                                if (segment == allocatingFrom)
                                    break;
                                segmentsToRecycle.add(segment);
                                spaceToReclaim += segment.onDiskSize();
                                if (spaceToReclaim + unused >= 0)
                                    break;
                            }
                            flushDataFrom(segmentsToRecycle, false);
                        }

                        do
                            LockSupport.park();
                        while (availableSegment.get() != null);
                    }
                    catch (Throwable t)
                    {
                        JVMStabilityInspector.inspectThrowable(t);
                        if (!CommitLog.handleCommitError("Failed managing commit log segments", t))
                            return;
                        // sleep some arbitrary period to avoid spamming CL
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                        // If we offered a segment, wait for it to be taken before reentering the loop.
                        // There could be a new segment in next not offered, but only on failure to discard it while
                        // shutting down-- nothing more can or needs to be done in that case.
                        while (availableSegment.get() != null);
                            LockSupport.park();
                    }
                }
            }
        };

        shutdown = false;
        managerThread = new Thread(runnable, "COMMIT-LOG-ALLOCATOR");
        managerThread.start();
    }

    /**
     * Reserve space in the current segment for the provided mutation or, if there isn't space available,
     * create a new segment.
     *
     * @return the provided Allocation object
     */
    public Allocation allocate(Mutation mutation, int size)
    {
        CommitLogSegment segment = allocatingFrom();

        Allocation alloc;
        while ( null == (alloc = segment.allocate(mutation, size)) )
        {
            // failed to allocate, so move to a new segment with enough room
            advanceAllocatingFrom(segment);
            segment = allocatingFrom;
        }

        return alloc;
    }

    // simple wrapper to ensure non-null value for allocatingFrom; only necessary on first call
    CommitLogSegment allocatingFrom()
    {
        CommitLogSegment r = allocatingFrom;
        if (r == null)
        {
            advanceAllocatingFrom(null);
            r = allocatingFrom;
        }
        return r;
    }

    /**
     * Fetches a new segment from the queue, creating a new one if necessary, and activates it
     */
    private void advanceAllocatingFrom(CommitLogSegment old)
    {
        while (true) {
            synchronized (this)
            {
                // do this in a critical section so we can atomically remove from availableSegments and add to allocatingFrom/activeSegments
                // see https://issues.apache.org/jira/browse/CASSANDRA-6557?focusedCommentId=13874432&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13874432
                if (allocatingFrom != old)
                    return;

                // If a segment is ready, take it now, otherwise wait for the management thread to construct it.
                CommitLogSegment next = availableSegment.getAndSet(null);
                if (next != null)
                {
                    // Success! Change allocatingFrom and activeSegments (which must be kept in order) before leaving
                    // the critical section.
                    allocatingFrom = next;
                    activeSegments.add(next);
                    break;
                }
            }

            do
            {
                WaitQueue.Signal prepared = segmentPrepared.register(commitLog.metrics.waitingOnSegmentAllocation.time());
                if (availableSegment.get() == null)
                    prepared.awaitUninterruptibly();
                else
                    prepared.cancel();
            }
            while (availableSegment.get() == null);
        }

        LockSupport.unpark(managerThread);

        if (old != null)
        {
            // Now we can run the user defined command just after switching to the new commit log.
            // (Do this here instead of in the recycle call so we can get a head start on the archive.)
            commitLog.archiver.maybeArchive(old);

            // ensure we don't continue to use the old file; not strictly necessary, but cleaner to enforce it
            old.discardUnusedTail();
        }

        // request that the CL be synced out-of-band, as we've finished a segment
        commitLog.requestExtraSync();
    }

    /**
     * Switch to a new segment, regardless of how much is left in the current one.
     *
     * Flushes any dirty CFs for this segment and any older segments, and then recycles
     * the segments
     */
    void forceRecycleAll(Iterable<UUID> droppedCfs)
    {
        List<CommitLogSegment> segmentsToRecycle = new ArrayList<>(activeSegments);
        CommitLogSegment last = segmentsToRecycle.get(segmentsToRecycle.size() - 1);
        advanceAllocatingFrom(last);

        // wait for the commit log modifications
        last.waitForModifications();

        // make sure the writes have materialized inside of the memtables by waiting for all outstanding writes
        // to complete
        Keyspace.writeOrder.awaitNewBarrier();

        // flush and wait for all CFs that are dirty in segments up-to and including 'last'
        Future<?> future = flushDataFrom(segmentsToRecycle, true);
        try
        {
            future.get();

            for (CommitLogSegment segment : activeSegments)
                for (UUID cfId : droppedCfs)
                    segment.markClean(cfId, segment.getContext());

            // now recycle segments that are unused, as we may not have triggered a discardCompletedSegments()
            // if the previous active segment was the only one to recycle (since an active segment isn't
            // necessarily dirty, and we only call dCS after a flush).
            for (CommitLogSegment segment : activeSegments)
                if (segment.isUnused())
                    discardSegment(segment);

            CommitLogSegment first;
            if ((first = activeSegments.peek()) != null && first.id <= last.id)
                logger.error("Failed to force-recycle all segments; at least one segment is still in use with dirty CFs.");
        }
        catch (Throwable t)
        {
            // for now just log the error and return false, indicating that we failed
            logger.error("Failed waiting for a forced recycle of in-use commit log segments", t);
        }
    }

    /**
     * Indicates that a segment is no longer in use and that it should be discarded.
     *
     * @param segment segment that is no longer in use
     */
    void discardSegment(final CommitLogSegment segment)
    {
        boolean archiveSuccess = commitLog.archiver.maybeWaitForArchiving(segment.getName());
        activeSegments.remove(segment);
        // if archiving (command) was not successful then leave the file alone. don't delete or recycle.
        logger.debug("Segment {} is no longer active and will be deleted {}", segment, archiveSuccess ? "now" : "by the archive script");
        segment.discard(archiveSuccess);
    }

    /**
     * Discard a segment that was written in a previous commit log cycle and has now been replayed.
     *
     * @param file segment file that is no longer in use.
     */
    void discardSegment(final File file)
    {
        // (don't decrease managed size, since this was never a "live" segment)
        logger.debug("(Unopened) segment {} is no longer needed and will be deleted now", file);
        FileUtils.deleteWithConfirm(file);
    }

    /**
     * Adjust the tracked on-disk size. Called by individual segments to reflect writes, allocations and discards.
     * @param addedSize
     */
    void addSize(long addedSize)
    {
        size.addAndGet(addedSize);
    }

    /**
     * @return the space (in bytes) used by all segment files.
     */
    public long onDiskSize()
    {
        return size.get();
    }

    private long unusedCapacity()
    {
        long total = DatabaseDescriptor.getTotalCommitlogSpaceInMB() * 1024 * 1024;
        long currentSize = size.get();
        logger.debug("Total active commitlog segment space used is {} out of {}", currentSize, total);
        return total - currentSize;
    }

    /**
     * Force a flush on all CFs that are still dirty in @param segments.
     *
     * @return a Future that will finish when all the flushes are complete.
     */
    private Future<?> flushDataFrom(List<CommitLogSegment> segments, boolean force)
    {
        if (segments.isEmpty())
            return Futures.immediateFuture(null);
        final ReplayPosition maxReplayPosition = segments.get(segments.size() - 1).getContext();

        // a map of CfId -> forceFlush() to ensure we only queue one flush per cf
        final Map<UUID, ListenableFuture<?>> flushes = new LinkedHashMap<>();

        for (CommitLogSegment segment : segments)
        {
            for (UUID dirtyCFId : segment.getDirtyCFIDs())
            {
                Pair<String,String> pair = Schema.instance.getCF(dirtyCFId);
                if (pair == null)
                {
                    // even though we remove the schema entry before a final flush when dropping a CF,
                    // it's still possible for a writer to race and finish his append after the flush.
                    logger.debug("Marking clean CF {} that doesn't exist anymore", dirtyCFId);
                    segment.markClean(dirtyCFId, segment.getContext());
                }
                else if (!flushes.containsKey(dirtyCFId))
                {
                    String keyspace = pair.left;
                    final ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(dirtyCFId);
                    // can safely call forceFlush here as we will only ever block (briefly) for other attempts to flush,
                    // no deadlock possibility since switchLock removal
                    flushes.put(dirtyCFId, force ? cfs.forceFlush() : cfs.forceFlush(maxReplayPosition));
                }
            }
        }

        return Futures.allAsList(flushes.values());
    }

    /**
     * Stops CL, for testing purposes. DO NOT USE THIS OUTSIDE OF TESTS.
     * Only call this after the AbstractCommitLogService is shut down.
     */
    public void stopUnsafe(boolean deleteSegments)
    {
        logger.debug("CLSM closing and clearing existing commit log segments...");

        shutdown();
        try
        {
            awaitTermination();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        for (CommitLogSegment segment : activeSegments)
            closeAndDeleteSegmentUnsafe(segment, deleteSegments);
        activeSegments.clear();

        allocatingFrom = null;

        size.set(0L);

        logger.debug("CLSM done with closing and clearing existing commit log segments.");
    }

    private static void closeAndDeleteSegmentUnsafe(CommitLogSegment segment, boolean delete)
    {
        try
        {
            segment.discard(delete);
        }
        catch (AssertionError ignored)
        {
            // segment file does not exist
        }
    }

    /**
     * Initiates the shutdown process for the management thread.
     */
    public void shutdown()
    {
        shutdown = true;

        // Release the management thread and delete prepared segment.
        CommitLogSegment next = availableSegment.getAndSet(null);
        if (next != null)
            next.discard(true);
        LockSupport.unpark(managerThread);
    }

    /**
     * Returns when the management thread terminates.
     */
    public void awaitTermination() throws InterruptedException
    {
        managerThread.join();
        managerThread = null;

        for (CommitLogSegment segment : activeSegments)
            segment.close();

        CompressedSegment.shutdown();
    }

    /**
     * @return a read-only collection of the active commit log segments
     */
    Collection<CommitLogSegment> getActiveSegments()
    {
        return Collections.unmodifiableCollection(activeSegments);
    }
}

