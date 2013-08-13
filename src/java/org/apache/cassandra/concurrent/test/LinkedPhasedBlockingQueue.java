package org.apache.cassandra.concurrent.test;

public class LinkedPhasedBlockingQueue<E> extends RingBufferBlockingQueue<E>
{

    private static final int DEFAULT_BUFFER_SIZE = 1 << 16;

    public LinkedPhasedBlockingQueue()
    {
        this(DEFAULT_BUFFER_SIZE);
    }

    public LinkedPhasedBlockingQueue(int bufferSize)
    {
        super(new LinkedPhasedRingBuffer<E>(bufferSize));
    }
}
