package org.apache.cassandra.concurrent.test;

import java.util.concurrent.BlockingQueue;

public class TestQueue
{

    public static <E> BlockingQueue<E> testing()
    {
        return new LinkedPhasedBlockingQueue<>();
    }

}
