package org.apache.cassandra.concurrent.test;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

// TODO : allow safe wrapping of laps, so no total limit on safe number of events to process
// TODO : tidying up of values returned from the buffer
public final class LinkedPhasedRingBuffer<E> implements RingBuffer<E>
{

    private static final long UPDATE_MIN_POS_MASK = Math.max(255, Runtime.getRuntime().availableProcessors() * 32);

    private final PaddedLong readPosCache = new PaddedLong();
    private final PaddedLong readPosMin = new PaddedLong();
    private final PaddedLong writePosExact = new PaddedLong();
    private final PaddedLong writeLimit = new PaddedLong();

    private volatile Link<E> readHead, writeHead;
    private volatile int capacity;
    private final int sizeShift;
    private final int sizeMask;
    private final ReentrantLock readLock = new ReentrantLock(), writeLock = new ReentrantLock();
    private final int maxCapacity;
    private final WaitQueue notFull = new LinkedWaitQueue();

    private final static class Link<E>
    {
        private final RefArray<E> ring;
        private final IntArray readLap, writeLap;
        private volatile long offset, finishedReadingOffset = -1;
        private volatile int lap;
        private volatile Link<E> next;

        public Link(int size)
        {
            ring = new RefArray<>(size, false);
            readLap = new IntArray(ring.length(), false);
            writeLap = new IntArray(ring.length(), false);
        }

        public synchronized void clear(long offset)
        {
            if (this.offset != offset)
                return;
            // shouldn't be possible to be null unless already cleared, so don't waste the time
            if (ring.get(0) == null)
                return;
            for (int i = 1 ; i < ring.length() ; i++)
                ring.setOrdered(i, null);
            // should be able to piggyback off sync, but to be safe...
            ring.setVolatile(0, null);
        }

    }

    public LinkedPhasedRingBuffer(int linkBufferSize)
    {
        this(linkBufferSize, 0);
    }

    // maxCapacity is recommended to be several multiples of linkBufferSize, for performance purposes
    // if they are the same size, writes will be blocked aftre filling the queue until it is completely emptied,
    // i.e. it will be filled in rounds of linkBufferSize, and until that round is emptied no more can be added
    public LinkedPhasedRingBuffer(int linkBufferSize, int maxCapacity)
    {
        linkBufferSize = firstPow2(linkBufferSize);

        readHead = writeHead = new Link<>(linkBufferSize);
        readHead.next = readHead;
        sizeShift = Integer.numberOfTrailingZeros(readHead.ring.length());
        sizeMask = (1 << sizeShift) - 1;
        capacity = 1 << sizeShift;

        if (maxCapacity > 0)
        {
            maxCapacity = firstPow2(maxCapacity);
            if (maxCapacity < linkBufferSize)
                throw new IllegalArgumentException("Maximum capacity must be greater than or equal to twice the linkBufferSize");
            writeLimit.setVolatile(1 << sizeShift);
        }
        else
        {
            writeLimit.setVolatile(Long.MAX_VALUE);
        }

        this.maxCapacity = maxCapacity;
    }

    private static int firstPow2(int equalOrGreaterThan)
    {
        int ss = 1;
        while (1 << ss < equalOrGreaterThan)
            ss += 1;
        return 1 << ss;
    }

    private static final class ReadSnap<E>
    {
        Link<E> link;
        int lap;
        long position;
        long offset;
    }

    private int index(long pos, int sizeMask)
    {
        return (int) (pos & sizeMask);
    }

    private static long expectedOffset(long pos, int sizeMask)
    {
        return pos & ~sizeMask;
    }

    private boolean isNextOffset(long offset, long maybeNextOffset, int sizeMask)
    {
        return expectedOffset(maybeNextOffset - 1, sizeMask) == offset;
    }

    private final boolean findReadNext(ReadSnap<E> readSnap, int sizeMask)
    {
        Link<E> prev = readSnap.link;
        if (prev == null)
            return findReadFirst(readSnap, sizeMask);
        long wantOffset = expectedOffset(readSnap.position, sizeMask) + sizeMask + 1;
        Link<E> next = prev.next;
        long offset = next.offset;
        int lap = next.lap;
        if (offset < wantOffset)
            return false;
        if (offset > wantOffset)
            return findReadFirst(readSnap, sizeMask);
        if (offset != next.offset)
            return findReadFirst(readSnap, sizeMask);
        if (offset < readPosMin.getVolatile())
            return findReadFirst(readSnap, sizeMask);
        readSnap.lap = lap;
        readSnap.position = offset;
        readSnap.offset = offset;
        readSnap.link = next;
        return true;
    }

    private final boolean findReadFirst(ReadSnap<E> readSnap, int sizeMask)
    {
        restart: while (true)
        {

            final Link<E> head = readHead;

            long offset = head.offset;
            int lap = head.lap;

            // confirm lap is consistent with offset
            if (head.offset != offset)
                continue;

            long readPos = this.readPosCache.get();
            long minPos = this.readPosMin.get();
            if (readPos < minPos)
                readPos = minPos;

            long expectedOffset = expectedOffset(readPos, sizeMask);
            if (expectedOffset > offset)
            {
                // attempt to move head forwards one...
                if (!updateHead(head, offset, sizeMask))
                    return false;
            }
            else if (expectedOffset == offset)
            {

                readSnap.link = head;
                readSnap.lap = lap;
                readSnap.position = readPos;
                readSnap.offset = offset;
                return true;

            }

        }
    }

    // result of false indicates the head parameter was not stale, i.e. we should be
    // able to update the head pointer safely, but that head.next is not the correct new head;
    // this indicates we have caught up with the writers and reached the end of the buffer
    private boolean updateHead(Link<E> head, long offset, int sizeMask)
    {
        boolean fail = false;
        boolean updatedHead = false;
        readLock.lock();
        Link<E> newHead = head.next;
        if (readHead == head && head.offset == offset)
        {
            updatedHead = true;
            if (isNextOffset(offset, newHead.offset, sizeMask))
            {
                readHead = newHead;
                head.finishedReadingOffset = offset;
            } else
            {
                fail = true;
            }
        }
        readLock.unlock();
//        head.clear(offset);
        if (updatedHead)
            notFull.signalAll();
        if (fail)
            return false;
        return true;
    }

    private static final int END_OF_BUFFER = -1;
    private static final int RETRY = -2;

    private final int findFirstUnread(ReadSnap<E> readSnap, int sizeMask)
    {
        final int readIndex = index(readSnap.position, sizeMask);
        return findFirstUnread(readSnap.link, readIndex, readSnap.lap, sizeMask);
    }
    private final int findFirstUnread(Link<E> readFrom, int readIndex, int wantLap, int sizeMask)
    {
        while (true)
        {
            if (readIndex > sizeMask)
                return RETRY;

            int haveReadLap = readFrom.readLap.getVolatile(readIndex);
            if (haveReadLap == wantLap)
            {
                int haveWriteLap = readFrom.writeLap.get(readIndex);
                if (haveWriteLap == wantLap + 1)
                    return readIndex;
                else if (haveWriteLap == wantLap)
                    return END_OF_BUFFER;
                else
                    return RETRY;
            }
            else if (haveReadLap < wantLap)
                return END_OF_BUFFER;

            readIndex++;
        }

    }

    public E poll()
    {
        return peekOrPoll(true);
    }

    public E peek()
    {
        return peekOrPoll(false);
    }

    private final E peekOrPoll(boolean remove)
    {
        final int sizeMask = this.sizeMask;
        final ReadSnap<E> readSnap = new ReadSnap<>();

        restart: while (true)
        {

            if (!findReadFirst(readSnap, sizeMask))
                return null;

            final Link<E> readFrom = readSnap.link;
            final int wantLap = readSnap.lap;
            int readIndex = index(readSnap.position, sizeMask);

            while (true)
            {
                readIndex = findFirstUnread(readFrom, readIndex, wantLap, sizeMask);
                if (readIndex >= 0)
                {
                    final E r = readFrom.ring.get(readIndex);
                    if (remove && readFrom.readLap.cas(readIndex, wantLap, wantLap + 1))
                    {
                        final long nextReadPos = readSnap.offset + readIndex + 1;
                        if (((readIndex + 1) & UPDATE_MIN_POS_MASK) == 0)
                            this.readPosMin.ensureAtLeast(nextReadPos);
                        else
                            this.readPosCache.setVolatile(nextReadPos);
                        return r;

                    } else if (!remove && readFrom.readLap.getVolatile(readIndex) == wantLap)
                    {
                        return r;
                    }
                }
                switch (readIndex)
                {
                    case END_OF_BUFFER:
                        return null;
                    case RETRY:
                        continue restart;
                }
                readIndex++;
            }
        }
    }

    public WaitSignal notFull()
    {
        return notFull.register();
    }

    private final Link<E> findWriteHead(long expectedOffset, boolean secured)
    {

        while (true)
        {

            Link<E> writeTo = writeHead;
            if (writeTo.offset < expectedOffset)
            {
                writeLock.lock();
                try {

                    writeTo = writeHead;
                    if (writeTo.offset >= expectedOffset)
                        continue;

                    while (true)
                    {
                        final Link<E> next = writeTo.next;
                        if (next.finishedReadingOffset < next.offset)
                        {
                            // if the next link hasn't been fully consumed (i.e. last item is still in unread state)
                            // then create a new link and insert it straight after our current link
                            if (capacity == maxCapacity)
                                return null;
                            writeTo = insertNewWriteHead(writeTo, expectedOffset);
                            capacity += 1 << sizeShift;
                        }
                        else
                        {
                            if (next.lap == Integer.MAX_VALUE)
                            {
                                // this link has reached the end of its lifespan and must be trashed in order to avoid
                                // breaking the queue (which assumes positive values for any lap or pos)

                                if (writeTo.next == writeTo)
                                {
                                    writeTo = insertNewWriteHead(writeTo, expectedOffset);
                                    // unlink old, spent, head - leave it pointing to us, so forward progress can be
                                    // made by any old readers, but flag unlinked so cursors can avoid using it as a point
                                    // of reference
                                    writeTo.next = writeTo;
                                } else {
                                    capacity -= 1 << sizeShift;
                                    writeTo.next = writeTo.next.next;
                                    writeTo = writeTo.next;
                                    continue;
                                }

                            } else {

                                next.lap++;
                                if (maxCapacity > 0)
                                    writeLimit.setOrdered(expectedOffset + (1 << sizeShift));
//                                next.clear(next.offset);
                                next.offset = expectedOffset;
                                writeHead = writeTo = next;

                            }
                        }
                        break;
                    }

                } finally {
                    writeLock.unlock();
                }
            }
            else if (secured)
                while (writeTo.offset != expectedOffset)
                    writeTo = writeTo.next;

            return writeTo;
        }

    }

    public long offer(E val)
    {

        if (val == null)
            throw new IllegalArgumentException();

        final int sizeMask = this.sizeMask;

        while (true)
        {

            long maxWritePos = this.writeLimit.getVolatile();
            long writePos = writePosExact.get();
            while (writePos < maxWritePos)
                if (writePosExact.cas(writePos, writePos + 1))
                    break;
                else
                    writePos = writePosExact.getVolatile();

            final long expectedOffset = expectedOffset(writePos, sizeMask);

            if (writePos > maxWritePos)
                continue;

            Link<E> writeTo = findWriteHead(expectedOffset, writePos < maxWritePos);
            if (writeTo == null)
                return -1;

            if (writePos == maxWritePos)
                continue;

            final int writeIndex = index(writePos, sizeMask);
            writeTo.ring.setOrdered(writeIndex, val);
            writeTo.writeLap.setVolatile(writeIndex, writeTo.lap + 1);
            return writePos;
        }

    }

    // to ONLY be called by offer() (and while lock held)
    private Link<E> insertNewWriteHead(Link<E> currentHead, long offset)
    {
        Link<E> newWriteTo = new Link<>(1 << sizeShift);
        if (maxCapacity > 0)
            writeLimit.setOrdered(offset + (1 << sizeShift));
        newWriteTo.offset = offset;
        newWriteTo.next = currentHead.next;
        currentHead.next = newWriteTo;
        writeHead = newWriteTo;
        return newWriteTo;
    }

    public boolean stillContains(long pos)
    {
        return get(pos) != null;
    }

    public E get(long pos)
    {
        final int sizeMask = this.sizeMask;
        final long expectedOffset = expectedOffset(pos, sizeMask);
        final ReadSnap<E> readSnap = new ReadSnap<>();

        if (!findReadFirst(readSnap, sizeMask))
            return null;

        Link<E> readFrom = readSnap.link;
        if (readSnap.position > pos)
            return null;

        long offset = readFrom.offset;

        while (offset < expectedOffset)
            readFrom = readFrom.next;  // safe, as we know pos was once in the list, so we must find it or hit something bigger

        if (offset > expectedOffset)
            return null;

        int lap = readSnap.lap;
        int index = index(pos, sizeMask);
        E r = readFrom.ring.getVolatile(index);
        if (readFrom.readLap.get(index) == lap)
            return r;
        return null;
    }

    // return true iff was empty for entire duration of call (hopefully)
    public boolean isEmpty()
    {
        while (true)
        {
            final int sizeMask = this.sizeMask;
            final ReadSnap<E> readSnap = new ReadSnap<>();
            if (!findReadFirst(readSnap, sizeMask))
                return true;
            long pos = writePosExact.getVolatile() - 1;
            if (pos < 0)
                return true;
            long expectedOffset = expectedOffset(pos, sizeMask);
            Link<E> readFrom = readSnap.link;
            long offset = readFrom.offset;
            if (offset < expectedOffset)
                return false;
            if (offset > expectedOffset)
                continue;
            int index = index(pos, sizeMask);
            int readLap = readSnap.lap;
            if (readFrom.readLap.get(index) > readLap)
                if (writePosExact.getVolatile() == pos + 1)
                    return true;
            return false;
        }
    }

    public boolean remove(Object o)
    {
        return find(o, true) >= 0;
    }

    public boolean contains(Object o)
    {
        return find(o, false) >= 0;
    }

    private final long find(Object o, boolean remove)
    {
        final int sizeMask = this.sizeMask;
        final ReadSnap<E> readSnap = new ReadSnap<>();

        next: while (true)
        {

            if (!findReadNext(readSnap, sizeMask))
                return -1;

            final Link<E> readFrom = readSnap.link;
            final int wantLap = readSnap.lap;
            int readIndex = index(readSnap.position, sizeMask);

            while (true)
            {
                readIndex = findFirstUnread(readFrom, readIndex, wantLap, sizeMask);
                if (readIndex >= 0)
                {
                    final E r = readFrom.ring.get(readIndex);
                    if (r == o)
                        if (    (remove && readFrom.readLap.cas(readIndex, wantLap, wantLap + 1))
                            ||  (!remove && readFrom.readLap.getVolatile(readIndex) == wantLap))
                            return readSnap.offset + readIndex;
                }
                switch (readIndex)
                {
                    case END_OF_BUFFER:
                        return -1;
                    case RETRY:
                        continue next;
                }
                readIndex++;
            }
        }
    }

    public Iterator<E> iterator()
    {
        return new Iterator<E>()
        {

            final ReadSnap<E> readSnap = new ReadSnap<>();
            E ret = proceed();

            E proceed()
            {
                final int sizeMask = LinkedPhasedRingBuffer.this.sizeMask;
                final ReadSnap<E> readSnap = new ReadSnap<>();

                next: while (true)
                {

                    if (!findReadNext(readSnap, sizeMask))
                        return null;

                    final Link<E> readFrom = readSnap.link;
                    final int wantLap = readSnap.lap;
                    int readIndex = index(readSnap.position, sizeMask);

                    while (true)
                    {
                        readIndex = findFirstUnread(readFrom, readIndex, wantLap, sizeMask);
                        if (readIndex >= 0)
                        {
                            final E r = readFrom.ring.get(readIndex);
                            if (readFrom.readLap.getVolatile(readIndex) == wantLap)
                            {
                                readSnap.position = readIndex + readSnap.offset;
                                return r;
                            }
                        }
                        switch (readIndex)
                        {
                            case END_OF_BUFFER:
                                return null;
                            case RETRY:
                                continue next;
                        }
                        readIndex++;
                    }
                }
            }

            @Override
            public boolean hasNext()
            {
                return ret != null;
            }

            @Override
            public E next()
            {
                E r = ret;
                ret = proceed();
                return ret;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public int size()
    {
        final int sizeMask = this.sizeMask;
        int size = 0;
        ReadSnap<E> readSnap = new ReadSnap<>();
        for (int i = 0 ; i < 10 ; i++)
        {
            // attempt to get stable size
            long writePos = writePosExact.getVolatile();
            long readPos;
            while (true)
            {
                if (!findReadFirst(readSnap, sizeMask))
                    return 0;
                int readIndex = findFirstUnread(readSnap, sizeMask);
                switch (readIndex)
                {
                    case END_OF_BUFFER:
                        return 0;
                    case RETRY:
                        continue;
                }
                readPos = readIndex + readSnap.offset;
                break;
            }
            size = (int) (writePos - readPos);
            if (writePos == writePosExact.getVolatile())
                return size;
        }

        // failed, so just give best guess
        return size;
    }

    // queue MUST not be in use when this is called
    public void reset()
    {
        Link<E> head = writeHead, cur = head;
        do
        {
            cur.readLap.fill(0);
            cur.writeLap.fill(0);
            cur.lap = -1;
            cur.offset = -1;
            cur.finishedReadingOffset = -1;
            cur = cur.next;
        } while (cur != head);
        readPosCache.setOrdered(0);
        readPosMin.setOrdered(0);
        writePosExact.setOrdered(0);
        capacity = 0;
        readHead = writeHead;
        writeHead.lap = 0;
        writeHead.offset = 0;
    }

    public int capacity()
    {
        return capacity;
    }

}
