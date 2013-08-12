//package org.apache.cassandra.concurrent.test;
//
//// TODO : allow safe wrapping of laps, so no total limit on safe number of events to process
//// TODO : tidying up of values from the buffer - see if possible without negatively impacting performance; if not, use separate thread
//public final class StridedChainedRingBufferBk<E> implements Buffer<E>
//{
//
//    // { poll start <= poll end <= add start <= add end == poll start + (1 << sizeShift) }
////    private final PaddedLong readPos = new PaddedLong();
//    private final PaddedLong writePos = new PaddedLong();
//    private volatile Link<E> readHead, writeHead;
//    private volatile int capacity;
//    private final int sizeShift;
//    private final int sizeMask;
//
//    private final static class Link<E>
//    {
//        private final StridedRefArray<E> ring;
//        private final StridedIntArray readLap, writeLap;
//        private final PaddedLong readPos = new PaddedLong();
//        private volatile long offset;
//        private volatile int lap;
//        private volatile Link<E> next, prev;
//        private volatile boolean unlinked = false;
//
//        public Link(int size)
//        {
//            ring = new StridedRefArray<>(size);
//            readLap = new StridedIntArray(ring.length());
//            writeLap = new StridedIntArray(ring.length());
//        }
//
//    }
//
//    public StridedChainedRingBufferBk(int bufferSize)
//    {
//        readHead = writeHead = new Link<>(bufferSize);
//        readHead.next = readHead;
//        sizeShift = Integer.numberOfTrailingZeros(readHead.ring.length());
//        sizeMask = (1 << sizeShift) - 1;
//        capacity = 1 << sizeShift;
//    }
//
//    public boolean isEmpty()
//    {
//        final int sizeMask = this.sizeMask;
//        final LinkState<E> findReadFrom = new LinkState<>();
//        if (!findReadFrom(sizeMask, findReadFrom))
//            return true;
//        long pos = writePos.getVolatile() - 1;
//        if (pos < 0)
//            return true;
//        long expectedOffset = expectedOffset(pos, sizeMask);
//        Link<E> readFrom = findReadFrom.link;
//        long offset = readFrom.offset;
//        if (offset < expectedOffset)
//            return false;
//        if (offset > expectedOffset)
//            return true;
//        int index = index(pos, sizeMask);
//        int readLap = findReadFrom.lap;
//        if (readFrom.writeLap.get(index) <= readLap)
//            return true;
//        return false;
//    }
//
//    private static final class LinkState<E>
//    {
//        Link<E> link;
//        int lap;
//        long position;
//    }
//
//    public int size()
//    {
//        return isEmpty() ? 0 : 1;
//    }
//
//    private int index(long pos)
//    {
//        return index(pos, sizeMask);
//    }
//
//    private int index(long pos, int sizeMask)
//    {
//        return (int) (pos & sizeMask);
//    }
//
//    private static long expectedOffset(long pos, int sizeMask)
//    {
//        return pos & ~sizeMask;
//    }
//
//    private final boolean findReadFrom(int sizeMask, LinkState<E> findReadFrom)
//    {
//        restart: while (true)
//        {
//
//            long readPos = this.readPos.getVolatile();
//            long expectedOffset = expectedOffset(readPos, sizeMask);
//            Link<E> readFrom = readHead;
//            boolean diffHead = false;
//
//            while (true)
//            {
//
//                long curOffset = readFrom.offset;
//                while (curOffset < expectedOffset)
//                {
//                    readFrom = readFrom.next;
//                    long nextOffset = readFrom.offset;
//                    if (nextOffset <= curOffset)
//                        return false;
//                    curOffset = nextOffset;
//                    diffHead = true;
//                }
//
//                if (curOffset > expectedOffset)
//                {
//                    if (readPos != this.readPos.getVolatile())
//                        continue restart;
//
//                    // we've no idea where we are or where the readPos should be,
//                    // as the list has clearly wrapped around (should be monotonically
//                    // increasing, so if it's suddenly jumped, we've gone very wrong)
//                    // so we need to scan the whole list looking for a sensible start
//                    // which we'll define as the first position with reads to perform
//                    // after the min offset link
//
//                    // find min link
//                    found: while (true)
//                    {
//                        Link<E> min = readHead;
//                        long prevOffset = min.prev.offset;
//                        while (true)
//                        {
//                            curOffset = min.offset;
//                            if (curOffset< prevOffset && !min.unlinked)
//                                break;
//                            prevOffset = curOffset;
//                            min = min.next;
//                        }
//                        while (true)
//                        {
//
//                        }
//                    }
//                }
//
//                if (diffHead)
//                    readHead = readFrom;
//
//                int wantLap = readFrom.lap;
//                if (readFrom.offset > expectedOffset)
//                    continue restart;
//
//                findReadFrom.link = readFrom;
//                findReadFrom.lap = wantLap;
//                findReadFrom.position = readPos;
//                return true;
//            }
//        }
//    }
//
//    public E poll()
//    {
//        final int sizeMask = this.sizeMask;
//        final LinkState<E> findReadFrom = new LinkState<>();
//
//        restart: while (true)
//        {
//
//            if (!findReadFrom(sizeMask, findReadFrom))
//                return null;
//            long readPos = findReadFrom.position;
//            int wantLap = findReadFrom.lap;
//            Link<E> readFrom = findReadFrom.link;
//
//            while (true)
//            {
//
//                int readIndex = index(readPos, sizeMask);
//                int haveReadLap = readFrom.readLap.getVolatile(readIndex);
//                if (haveReadLap == wantLap)
//                {
//                    int haveWriteLap = readFrom.writeLap.get(readIndex);
//                    if (haveWriteLap == wantLap + 1)
//                    {
//                        E r = readFrom.ring.get(readIndex);
//                        if (readFrom.readLap.cas(readIndex, wantLap, wantLap + 1))
//                        {
//                            this.readPos.setVolatile(readPos + 1);
//                            return r;
//                        }
//                    } else if (haveWriteLap == wantLap)
//                        return null;
//                    else
//                        continue restart;
//                }
//                else if (haveReadLap < wantLap)
//                    return null;
//
//                readPos++;
//                if ((readPos & sizeMask) == 0)
//                {
//                    // no need for cas, just want to ensure lower bound is in next block
//                    if (this.readPos.getVolatile() < readPos)
//                        this.readPos.setVolatile(readPos);
//                    continue restart;
//                }
//            }
//        }
//    }
//
//    public long offer(E val)
//    {
//
//        long writePos = this.writePos.incrementAndReturnOrig();
//        int writeIndex = index(writePos);
//        long expectedOffset = expectedOffset(writePos, sizeMask);
//
//        while (true)
//        {
//
//            Link<E> writeTo = writeHead;
//            if (writeTo.offset < expectedOffset)
//            {
//                synchronized (this) {
//
//                    writeTo = writeHead;
//                    if (writeTo.offset >= expectedOffset)
//                        continue;
//
//                    while (true)
//                    {
//                        Link<E> next = writeTo.next;
//                        if (next.readLap.get(sizeMask) == next.lap)
//                            writeTo = linkNewHead(writeTo, expectedOffset);
//                        else
//                        {
//                            if (next.lap == Integer.MAX_VALUE)
//                            {
//                                if (writeTo.next == writeTo)
//                                {
//                                    writeTo = linkNewHead(writeTo, expectedOffset);
//                                    // unlink old, spent, head - leave it pointing to us, so forward progress can be
//                                    // made by any old readers, but flag unlinked so cursors can avoid using it as a point
//                                    // of reference
//                                    writeTo.next.unlinked = true;
//                                    capacity -= 1 << sizeShift;
//                                    writeTo.next = writeTo;
//                                } else {
//                                    writeTo.next = writeTo.next.next;
//                                    writeTo = writeTo.next;
//                                    continue;
//                                }
//                            } else {
//                                next.lap++;
//                                next.offset = expectedOffset;
//                                writeHead = writeTo = next;
//                            }
//                        }
//                        break;
//                    }
//                }
//            }
//            else
//            {
//                while (writeTo.offset != expectedOffset)
//                    writeTo = writeTo.next;
//            }
//
//            writeTo.ring.setOrdered(writeIndex, val);
//            writeTo.writeLap.setVolatile(writeIndex, writeTo.lap + 1);
//            return writePos;
//        }
//
//    }
//
//    // to ONLY be called by offer() (and while lock held)
//    private Link<E> linkNewHead(Link<E> currentHead, long offset)
//    {
//        capacity += 1 << sizeShift;
//        Link<E> newWriteTo = new Link<>(1 << sizeShift);
//        newWriteTo.offset = offset;
//        newWriteTo.next = currentHead.next;
//        newWriteTo.prev = currentHead;
//        newWriteTo.next.prev = newWriteTo;
//        currentHead.next = newWriteTo;
//        writeHead = newWriteTo;
//        return newWriteTo;
//    }
//
//    public boolean stillContains(long pos)
//    {
//        return get(pos) != null;
//    }
//
//    public E get(long pos)
//    {
//        final int sizeMask = this.sizeMask;
//        final long expectedOffset = expectedOffset(pos, sizeMask);
//        final LinkState<E> findReadFrom = new LinkState<>();
//
//        if (!findReadFrom(sizeMask, findReadFrom))
//            return null;
//
//        Link<E> readFrom = findReadFrom.link;
//        if (findReadFrom.position > pos)
//            return null;
//
//        long offset = readFrom.offset;
//
//        while (offset < expectedOffset)
//            readFrom = readFrom.next;  // safe, as we know pos was once in the list, so we must find it or hit something bigger
//
//        if (offset > expectedOffset)
//            return null;
//
//        int lap = findReadFrom.lap;
//        int index = index(pos, sizeMask);
//        E r = readFrom.ring.getVolatile(index);
//        if (readFrom.readLap.get(index) == lap)
//            return r;
//        return null;
//    }
//
//    public void reset()
//    {
//        Link<E> head = readHead, cur = head;
//        do
//        {
//            cur.readLap.fill(0);
//            cur.writeLap.fill(0);
//            cur = cur.next;
//        } while (cur != head);
//        readPos.setOrdered(0);
//        writePos.setVolatile(0);
//    }
//
//    public int capacity()
//    {
//        return capacity;
//    }
//
//}
