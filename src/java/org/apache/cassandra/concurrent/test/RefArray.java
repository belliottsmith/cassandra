package org.apache.cassandra.concurrent.test;

import org.apache.cassandra.concurrent.test.AtomicRefArrayUpdater;

public final class RefArray<E>
{

    private final Object[] vals;
    private final boolean strided;

    public int length()
    {
        return vals.length;
    }

    public RefArray(int size, boolean strided)
    {
        if (size < 256)
            size = 256;
        int ss = 1;
        while (1 << ss < size)
            ss++;
        vals = (E[]) new Object[1 << ss];
        this.strided = strided;
    }

    private static int real(int virt)
    {
        return  (virt & ~255)
              | ((virt & 15) << 4)
              | ((virt & (15 << 4)) >> 4);
    }

    public void set(int index, E upd)
    {
        vals[strided ? real(index) : index] = upd;
    }

    public void setOrdered(int index, E upd)
    {
        arrayUpdater.setOrdered(vals, strided ? real(index) : index, upd);
    }

    public void setVolatile(int index, E upd)
    {
        arrayUpdater.setVolatile(vals, strided ? real(index) : index, upd);
    }

    public E get(int index)
    {
        return (E) vals[strided ? real(index) : index];
    }

    public E getVolatile(int index)
    {
        return (E) arrayUpdater.getVolatile(vals, strided ? real(index) : index);
    }

    public boolean cas(int index, E exp, E upd)
    {
        return arrayUpdater.compareAndSet(vals, strided ? real(index) : index, exp, upd);
    }

    private static final AtomicRefArrayUpdater<Object> arrayUpdater = new AtomicRefArrayUpdater<Object>(Object[].class);

}
