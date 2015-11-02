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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;

/**
 * Performs eager-creation of commit log segments in a background thread. All the
 * public methods are thread safe.
 */
public class CommitLogSegmentManager extends CommitLogSegmentCollection
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManager.class);

    Thread managerThread;
    private volatile boolean shutdown;

    CommitLogSegmentManager(final CommitLog commitLog)
    {
        super(commitLog);
    }

    void start()
    {
        super.init();
        // The run loop for the manager thread
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (!shutdown)
                {
                    try
                    {
                        logger.debug("No segments in reserve; creating a fresh one");

                        add(CommitLogSegment.createSegment(commitLog));

                        if (shutdown)
                            return;

                        Thread.yield();

                        // Writing threads need another segment now.
                        if (!hasHeadRoom())
                            continue;

                        // Writing threads are not waiting for new segments, we can spend time on other tasks.
                        // flush old Cfs if we're full
                        maybeFlushToReclaim();

                        do
                            LockSupport.park();
                        while (hasHeadRoom());
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
                        while (hasHeadRoom())
                            LockSupport.park();
                    }
                }
            }
        };

        managerThread = new Thread(runnable, "COMMIT-LOG-ALLOCATOR");
        managerThread.start();

        // for simplicity, ensure the first segment is allocated before continuing
        ensureFirstSegment();
    }

    void restart()
    {
        shutdown = false;
        start();
    }

    void requestHeadRoom()
    {
        LockSupport.unpark(managerThread);
    }

    private void maybeFlushToReclaim()
    {
        long unused = unusedCapacity();
        if (unused < 0)
        {
            long flushingSize = 0;
            List<CommitLogSegment> segmentsToRecycle = new ArrayList<>();
            for (CommitLogSegment segment : list(false))
            {
                flushingSize += segment.onDiskSize();
                segmentsToRecycle.add(segment);
                if (flushingSize + unused >= 0)
                    break;
            }
            flushDataFrom(segmentsToRecycle, false);
        }
    }

    /**
     * Reserve space in the current segment for the provided mutation or, if there isn't space available,
     * create a new segment.
     *
     * @return the provided Allocation object
     */
    public Allocation allocate(Mutation mutation, int size)
    {
        CommitLogSegment cur = allocatingFrom();

        Allocation alloc;
        while ( null == (alloc = cur.allocate(mutation, size)) )
        {
            // failed to allocate, so move to a new segment with enough room
            advanceAllocatingFrom(cur);
            cur = allocatingFrom();
        }

        return alloc;
    }

    /**
     * Advances the allocatingFrom pointer to the next free segment, if it is currently the segment provided
     */
    @DontInline
    boolean advanceAllocatingFrom(Node old)
    {
        if (!super.advanceAllocatingFrom(old))
            return false;

        // Now we can run the user defined command just after switching to the new commit log.
        // (Do this here instead of in the recycle call so we can get a head start on the archive.)
        commitLog.archiver.maybeArchive(old.segment);

        // ensure we don't continue to use the old file; not strictly necessary, but cleaner to enforce it
        old.segment.discardUnusedTail();
        return true;
    }

    /**
     * Switch to a new segment, regardless of how much is left in the current one.
     *
     * Flushes any dirty CFs for this segment and any older segments, and then discards the segments
     */
    void forceRecycleAll(Iterable<UUID> droppedCfs)
    {
        List<CommitLogSegment> segmentsToRecycle = getActiveSegments();
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

            for (CommitLogSegment segment : getActiveSegments())
                for (UUID cfId : droppedCfs)
                    segment.markClean(cfId, segment.getContext());

            // now recycle segments that are unused, as we may not have triggered a discardCompletedSegments()
            // if the previous active segment was the only one to recycle (since an active segment isn't
            // necessarily dirty, and we only call dCS after a flush).
            for (CommitLogSegment segment : getActiveSegments())
                if (segment.isUnused())
                    discardSegment(segment);

            CommitLogSegment first;
            if ((first = Iterables.getFirst(getActiveSegments(), null)) != null && first.id <= last.id)
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
        if (!remove(segment))
            return; // already discarded
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
        // (don't decrease managed size, since this was never a "allocatingFrom" segment)
        logger.debug("(Unopened) segment {} is no longer needed and will be deleted now", file);
        FileUtils.deleteWithConfirm(file);
    }

    /**
     * Adjust the tracked on-disk size. Called by individual segments to reflect writes, allocations and discards.
     * @param addedSize
     */
    void addSize(long addedSize)
    {
        activeSize.addAndGet(addedSize);
    }

    /**
     * @return the space (in bytes) used by all segment files.
     */
    public long onDiskSize()
    {
        return activeSize.get();
    }

    long unusedCapacity()
    {
        long total = DatabaseDescriptor.getTotalCommitlogSpaceInMB() * 1024 * 1024;
        long currentSize = activeSize.get();
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

        for (CommitLogSegment segment : list(true))
        {
            remove(segment);
            segment.discard(deleteSegments);
        }

        super.unset();
        activeSize.set(0L);

        logger.debug("CLSM done with closing and clearing existing commit log segments.");
    }

    /**
     * Initiates the shutdown process for the management thread.
     */
    public void shutdown()
    {
        shutdown = true;
        // we must ensure the manager wakes up, by consuming the unused segment
        consumeHeadRoom();
        // then ask it to wakeup
        requestHeadRoom();
    }

    /**
     * Returns when the management thread terminates.
     */
    public void awaitTermination() throws InterruptedException
    {
        managerThread.join();
        managerThread = null;

        for (CommitLogSegment segment : list(true))
            segment.close();

        CompressedSegment.shutdown();
        assert !segmentPrepared.hasWaiters();
    }

    /**
     * @return the set of active segments, optionally including the one we are allocating from
     */
    synchronized List<CommitLogSegment> getActiveSegments(boolean includeAllocatingFrom)
    {
        return list(includeAllocatingFrom);
    }

    /**
     * @return a read-only collection of the active commit log segments
     */
    List<CommitLogSegment> getActiveSegments()
    {
        return getActiveSegments(true);
    }
}

