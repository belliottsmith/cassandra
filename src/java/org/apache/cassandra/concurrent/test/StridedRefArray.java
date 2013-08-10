package org.apache.cassandra.concurrent.test;

import org.apache.cassandra.concurrent.test.AtomicRefArrayUpdater;

public final class StridedRefArray<E>
{

    private final Object[] vals;

    public int length()
    {
        return vals.length;
    }

    public StridedRefArray(int size)
    {
        if (size < 256)
            size = 256;
        int ss = 1;
        while (1 << ss < size)
            ss++;
        vals = (E[]) new Object[1 << ss];
    }

    private static int real(int virt)
    {
        return  (virt & ~255)
              | ((virt & 15) << 4)
              | ((virt & (15 << 4)) >> 4);
    }

    public void set(int index, E upd)
    {
        vals[real(index)] = upd;
    }

    public void setOrdered(int index, E upd)
    {
        arrayUpdater.setOrdered(vals, real(index), upd);
    }

    public void setVolatile(int index, E upd)
    {
        arrayUpdater.setVolatile(vals, real(index), upd);
    }

    public E get(int index)
    {
        return (E) vals[real(index)];
    }

    public E getVolatile(int index)
    {
        return (E) arrayUpdater.getVolatile(vals, real(index));
    }

    public boolean cas(int index, E exp, E upd)
    {
        return arrayUpdater.compareAndSet(vals, real(index), exp, upd);
    }

    private static final AtomicRefArrayUpdater<Object> arrayUpdater = new AtomicRefArrayUpdater<Object>(Object[].class);

}
