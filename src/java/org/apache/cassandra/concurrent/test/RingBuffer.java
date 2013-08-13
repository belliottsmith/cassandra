package org.apache.cassandra.concurrent.test;

import java.util.Iterator;

public interface RingBuffer<E>
{

    long offer(E val);
    E get(long pos);
    boolean stillContains(long pos);
    boolean isEmpty();
    int size();
    E poll();
    void reset();
    boolean remove(Object o);
    boolean contains(Object o);
    Iterator<E> iterator();
    E peek();
    WaitSignal notFull();

}
