package org.apache.cassandra.concurrent.test;

public class PhasedBlockingQueue<E> extends RingBufferBlockingQueue<E>
{

    public PhasedBlockingQueue(int bufferSize)
    {
        super(new PhasedRingBuffer<E>(bufferSize));
    }
}
