package org.apache.cassandra.concurrent.test;

import org.apache.cassandra.concurrent.test.PaddedLong;

public final class StridedRingBuffer<E>
{

    // { poll start <= poll end <= add start <= add end == poll start + (1 << sizeShift) }
    private final StridedRefArray<E> ring;
    private final StridedIntArray readLap, writeLap;
    private final PaddedLong readPos = new PaddedLong();
    private final PaddedLong writePos = new PaddedLong();
    private final int sizeShift;
    private final int sizeMask;

    public StridedRingBuffer(int size)
    {
        ring = new StridedRefArray<>(size);
        readLap = new StridedIntArray(ring.length());
        writeLap = new StridedIntArray(ring.length());
        sizeShift = Integer.numberOfTrailingZeros(ring.length());
        sizeMask = (1 << sizeShift) - 1;
    }

    public boolean isEmpty()
    {
        long pos = writePos.getVolatile() - 1;
        if (pos < 0)
            return true;
        while (true)
        {
            int readLap = lap(pos);
            int index = index(pos);
            if (writeLap.get(index) <= readLap)
                return true;
            if (this.readLap.get(index) <= readLap)
                return false;
            pos++;
        }
    }

    public int size()
    {
        return isEmpty() ? 1 : 0;
    }

    private int lap(long pos)
    {
        return (int) (pos >>> sizeShift);
    }

    private int index(long pos)
    {
        return (int) (pos & sizeMask);
    }

    public E poll()
    {
        final int sizeMask = this.sizeMask;

        restart: while (true)
        {
            long readPos = this.readPos.getVolatile();
            int readIndex = index(readPos);
            int wantReadLap = lap(readPos);

            while (true)
            {

                int haveReadLap = readLap.getVolatile(readIndex);
                if (haveReadLap == wantReadLap)
                {
                    int haveWriteLap = writeLap.get(readIndex);
                    if (haveWriteLap == wantReadLap + 1)
                    {
                        E r = ring.get(readIndex);
                        if (readLap.cas(readIndex, wantReadLap, wantReadLap + 1))
                        {
                            this.readPos.setVolatile(readPos + 1);
                            return r;
                        }
                    } else if (haveWriteLap == wantReadLap)
                        return null;
                    else
                        continue restart;
                }
                else if (haveReadLap < wantReadLap)
                    return null;

                readPos++;
                readIndex++;
                if (readIndex > sizeMask)
                {
                    readIndex = 0;
                    wantReadLap = lap(readPos);
                }
            }
        }

    }

    public long add(E val)
    {
        return offer(val);
    }

    public long offer(E val)
    {
        long writePos = this.writePos.incrementAndReturnOrig();
        int writeIndex = index(writePos);
        int lap = lap(writePos);

        // spin until available
        while (lap(writePos) > readLap.getVolatile(writeIndex));

        ring.setOrdered(writeIndex, val);
        writeLap.setVolatile(writeIndex, lap + 1);

        return writePos;
    }

    public boolean stillContains(long pos)
    {
        return readLap.getVolatile(index(pos)) == lap(pos);
    }

    public E getVolatile(long pos)
    {
        return ring.getVolatile(index(pos));
    }

    public E get(long pos)
    {
        return ring.get(index(pos));
    }

}
