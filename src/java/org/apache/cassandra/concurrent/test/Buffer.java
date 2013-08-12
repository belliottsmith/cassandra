package org.apache.cassandra.concurrent.test;

public interface Buffer<E>
{

    long offer(E val);
    E get(long pos);
    boolean stillContains(long pos);
    boolean isEmpty();
    int size();
    E poll();
    void reset();

}
