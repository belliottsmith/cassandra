package org.apache.cassandra.concurrent.test;

import java.util.concurrent.locks.ReentrantLock;

// TODO : allow safe wrapping of laps, so no total limit on safe number of events to process
// TODO : tidying up of values from the buffer - see if possible without negatively impacting performance; if not, use separate thread
public final class LinkedPhasedRingBuffer<E> implements RingBuffer<E>
{

    private static final long UPDATE_MIN_POS_MASK = Math.max(255, Runtime.getRuntime().availableProcessors() * 32);

    private final PaddedLong readPosCache = new PaddedLong();
    private final PaddedLong readPosMin = new PaddedLong();
    private final PaddedLong writePosExact = new PaddedLong();

    private volatile Link<E> readHead, writeHead;
    private volatile int capacity;
    private final int sizeShift;
    private final int sizeMask;
    private final ReentrantLock readLock = new ReentrantLock(), writeLock = new ReentrantLock();

    private final static class Link<E>
    {
        private final StridedRefArray<E> ring;
        private final StridedIntArray readLap, writeLap;
        private volatile long offset, finishedReadingOffset = -1;
        private volatile int lap;
        private volatile Link<E> next;

        public Link(int size)
        {
            ring = new StridedRefArray<>(size);
            readLap = new StridedIntArray(ring.length());
            writeLap = new StridedIntArray(ring.length());
        }

    }

    public LinkedPhasedRingBuffer(int bufferSize)
    {
        readHead = writeHead = new Link<>(bufferSize);
        readHead.next = readHead;
        sizeShift = Integer.numberOfTrailingZeros(readHead.ring.length());
        sizeMask = (1 << sizeShift) - 1;
        capacity = 1 << sizeShift;
    }

    private static final class ReadSnap<E>
    {
        Link<E> link;
        int lap;
        long position;
    }

    public int size()
    {
        return isEmpty() ? 0 : 1;
    }

    private int index(long pos)
    {
        return index(pos, sizeMask);
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

    private final boolean findReadFrom(ReadSnap<E> readSnap, int sizeMask)
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
                if (!updateHead(head, head.next, offset, sizeMask))
                    return false;

            } else if (expectedOffset < offset)
            {
                continue;
            } else
            {

                readSnap.link = head;
                readSnap.lap = lap;
                readSnap.position = readPos;
                return true;

            }

        }
    }

    // false only if newHead is not the next head; if head was incorrect, does nothing and returns success
    private boolean updateHead(Link<E> head, Link<E> newHead, long offset, int sizeMask)
    {
        boolean fail = false;
        readLock.lock();
        if (readHead == head && head.offset == offset)
            if (isNextOffset(offset, newHead.offset, sizeMask))
            {
                head.finishedReadingOffset = offset;
                readHead = newHead;
            } else
            {
                fail = true;
            }
        readLock.unlock();
        if (fail)
            return false;
        return true;
    }

    public E poll()
    {
        final int sizeMask = this.sizeMask;
        final ReadSnap<E> readSnap = new ReadSnap<>();

        restart: while (true)
        {

            if (!findReadFrom(readSnap, sizeMask))
                return null;
            long readPos = readSnap.position;
            int wantLap = readSnap.lap;
            Link<E> readFrom = readSnap.link;

            while (true)
            {

                int readIndex = index(readPos, sizeMask);
                int haveReadLap = readFrom.readLap.getVolatile(readIndex);
                if (haveReadLap == wantLap)
                {
                    int haveWriteLap = readFrom.writeLap.get(readIndex);
                    if (haveWriteLap == wantLap + 1)
                    {
                        E r = readFrom.ring.get(readIndex);
                        if (readFrom.readLap.cas(readIndex, wantLap, wantLap + 1))
                        {
                            if (((readPos + 1) & UPDATE_MIN_POS_MASK) == 0)
                                this.readPosMin.ensureAtLeast(readPos + 1);
                            else
                                this.readPosCache.setVolatile(readPos + 1);
                            return r;
                        }
                    } else if (haveWriteLap == wantLap)
                        return null;
                    else
                        continue restart;
                }
                else if (haveReadLap < wantLap)
                    return null;

                readPos++;
                if ((readPos & sizeMask) == 0)
                {
                    readPosMin.ensureAtLeast(readPos);
                    continue restart;
                }
            }
        }
    }

    public long offer(E val)
    {

        final long writePos = this.writePosExact.incrementAndReturnOrig();
        final int writeIndex = index(writePos);
        final long expectedOffset = expectedOffset(writePos, sizeMask);

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
                            writeTo = linkNewHead(writeTo, expectedOffset);
                        }
                        else
                        {
                            if (next.lap == Integer.MAX_VALUE)
                            {
                                // this link has reached the end of its lifespan and must be trashed in order to avoid
                                // breaking the queue (which assumes positive values for any lap or pos)

                                if (writeTo.next == writeTo)
                                {
                                    writeTo = linkNewHead(writeTo, expectedOffset);
                                    // unlink old, spent, head - leave it pointing to us, so forward progress can be
                                    // made by any old readers, but flag unlinked so cursors can avoid using it as a point
                                    // of reference
                                    capacity -= 1 << sizeShift;
                                    writeTo.next = writeTo;
                                } else {
                                    writeTo.next = writeTo.next.next;
                                    writeTo = writeTo.next;
                                    continue;
                                }

                            } else {

                                next.lap++;
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
            else
            {
                while (writeTo.offset != expectedOffset)
                    writeTo = writeTo.next;
            }

            writeTo.ring.setOrdered(writeIndex, val);
            writeTo.writeLap.setVolatile(writeIndex, writeTo.lap + 1);
            return writePos;
        }

    }

    // to ONLY be called by offer() (and while lock held)
    private Link<E> linkNewHead(Link<E> currentHead, long offset)
    {
        capacity += 1 << sizeShift;
        // debug
//        if ((capacity & ((1 << 24) - 1)) == 0)
//            System.out.println("Allocated total " + capacity);
        Link<E> newWriteTo = new Link<>(1 << sizeShift);
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

        if (!findReadFrom(readSnap, sizeMask))
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
            if (!findReadFrom(readSnap, sizeMask))
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
        readHead = writeHead;
        writeHead.lap = 0;
        writeHead.offset = 0;
    }

    public int capacity()
    {
        return capacity;
    }

}
