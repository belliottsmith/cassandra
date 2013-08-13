package org.apache.cassandra.concurrent.test;

public class LinkedPhasedBlockingQueue<E> extends RingBufferBlockingQueue<E>
{

    private static final int DEFAULT_BUFFER_SIZE = 1 << 10;

    public LinkedPhasedBlockingQueue()
    {
        this(DEFAULT_BUFFER_SIZE);
    }

    public LinkedPhasedBlockingQueue(int linkBufferSize)
    {
        super(new LinkedPhasedRingBuffer<E>(linkBufferSize));
    }

    public LinkedPhasedBlockingQueue(int linkBufferSize, int maxCapacity)
    {
        super(new LinkedPhasedRingBuffer<E>(linkBufferSize, maxCapacity));
    }

}
