package org.apache.cassandra.concurrent.test;

import org.apache.commons.lang.*;

import java.util.Iterator;

// TODO : allow safe wrapping of laps, so no total limit on safe number of events to process
public final class PhasedRingBuffer<E> implements RingBuffer<E>
{

    // { poll start <= poll end <= add start <= add end == poll start + (1 << sizeShift) }
    private final RefArray<E> ring;
    private final IntArray readLap, writeLap;
    private final PaddedLong readPos = new PaddedLong();
    private final PaddedLong writePos = new PaddedLong();
    private final int sizeShift;
    private final int sizeMask;

    public PhasedRingBuffer(int size)
    {
        int ss = 1;
        while (1 << ss < size)
            ss++;
        size = 1 << ss;
        ring = new RefArray<>(size, true);
        readLap = new IntArray(size, true);
        writeLap = new IntArray(size, true);
        sizeShift = ss;
        sizeMask = (1 << ss) - 1;
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
        return isEmpty() ? 0 : 1;
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

    // dangerous method - should not be called if autoLap is false
    public long add(E val)
    {
        long r;
        while (-1 == (r = offer(val)));
        return r;
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

    public void reset()
    {
        readLap.fill(0);
        writeLap.fill(0);
        readPos.setOrdered(0);
        writePos.setOrdered(0);
    }

    @Override
    public boolean remove(Object o)
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean contains(Object o)
    {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<E> iterator()
    {
        throw new NotImplementedException();
    }

    @Override
    public E peek()
    {
        throw new NotImplementedException();
    }

    @Override
    public WaitSignal notFull()
    {
        throw new NotImplementedException();
    }

}
