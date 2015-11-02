/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.commitlog;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.collect.ImmutableList;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.utils.concurrent.WaitQueue;

abstract class CommitLogSegmentCollection
{

    /**
     * Node in linked-list maintaining collection of active/allocating/ready segments, in that order
     */
    static class Node
    {
        final CommitLogSegment segment;
        volatile Node next;
        public Node(CommitLogSegment segment)
        {
            this.segment = segment;
        }
    }

    // head <= reclaiming <= allocatingFrom <= tail

    private Node head; // dummy head of the queue; [head.next..allocatingFrom) == active(false)
    private Node tail; // last segment in queue; either allocatingFrom, or our spare.
    private volatile Node allocatingFrom; // segment serving current allocations

    private static AtomicReferenceFieldUpdater<CommitLogSegmentCollection, Node> allocatingFromUpdater = AtomicReferenceFieldUpdater.newUpdater(CommitLogSegmentCollection.class, Node.class, "allocatingFrom");

    final WaitQueue segmentPrepared = new WaitQueue();

    /**
     * Tracks commitlog size, in multiples of the segment size.  We need to do this asynchronously as compressed
     * segments do not know their final size in advance, so it is incremented with each sync.
     */
    final AtomicLong activeSize = new AtomicLong();

    final CommitLog commitLog;

    CommitLogSegmentCollection(CommitLog commitLog)
    {
        this.commitLog = commitLog;
    }

    abstract void requestHeadRoom();

    void init()
    {
        head = new Node(new DummySegment(commitLog));
        tail = allocatingFrom = head;
    }

    void unset()
    {
        head = tail = allocatingFrom = null;
    }

    void consumeHeadRoom()
    {
        Node allocatingFrom;
        while ((allocatingFrom = this.allocatingFrom) != tail)
            advanceAllocatingFrom(allocatingFrom);
    }

    // is there a spare segment ready?
    boolean hasHeadRoom()
    {
        return allocatingFrom != tail;
    }

    // wait for the first segment to be allocated
    void ensureFirstSegment()
    {
        advanceAllocatingFrom(head);
    }

    CommitLogSegment allocatingFrom()
    {
        return allocatingFrom.segment;
    }

    boolean advanceAllocatingFrom(CommitLogSegment segment)
    {
        Node cur = allocatingFrom;
        return cur.segment == segment && advanceAllocatingFrom(cur);
    }

    @DontInline
    boolean advanceAllocatingFrom(Node old)
    {
        Node cur = allocatingFrom;
        if (cur != old)
            return false;

        Node next = ensureNext(cur);

        // atomically swap us to the next; if we fail, somebody else beat us to it
        if (!allocatingFromUpdater.compareAndSet(this, cur, next))
            return false;

        // wake-up the manager thread if there are no spare segments
        if (next.next == null)
            requestHeadRoom();

        if (old == head)
            return false;

        // request that the CL be synced out-of-band, as we've finished a segment
        commitLog.requestExtraSync();
        return true;
    }

    synchronized void add(CommitLogSegment segment)
    {
        Node newNode = new Node(segment);
        tail = tail.next = newNode;
        segmentPrepared.signalAll();
    }

    synchronized boolean remove(CommitLogSegment remove)
    {
        Node prev = head, cur = prev.next;
        Node live = this.allocatingFrom;

        // if we're in the set of reclaiming segments
        while (cur != live)
        {
            if (cur.segment == remove)
            {
                prev.next = cur.next;
                return true;
            }

            prev = cur;
            cur = cur.next;
        }
        return false;
    }

    /**
     * returns the node following cur; if it is not set, wakes the manager thread and waits
     */
    Node ensureNext(Node cur)
    {
        Node next = cur.next;
        while (next == null)
        {
            // wait for allocation
            WaitQueue.Signal signal = segmentPrepared.register();
            requestHeadRoom();
            next = cur.next;
            if (next == null)
            {
                signal.awaitUninterruptibly();
                next = cur.next;
            }
            else
            {
                signal.cancel();
            }
        }
        return next;
    }

    /**
     * @return the set of active segments, optionally including the one we are allocating from
     */
    synchronized List<CommitLogSegment> list(boolean includeAllocatingFrom)
    {
        ImmutableList.Builder<CommitLogSegment> builder = ImmutableList.builder();
        Node cur = head.next;
        Node live = this.allocatingFrom;
        while (cur != live)
        {
            builder.add(cur.segment);
            cur = cur.next;
        }
        if (includeAllocatingFrom)
            builder.add(live.segment);
        return builder.build();
    }

}
