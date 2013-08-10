package org.apache.cassandra.concurrent.test;

import org.apache.cassandra.concurrent.test.AtomicArrayUpdater;

public class PaddedInt
{

    private final int[] padding = new int[32];

    public int get()
    {
        return padding[15];
    }

    public int getVolatile()
    {
        return AtomicArrayUpdater.getIntVolatile(padding, 15);
    }

    public void setVolatile(int upd)
    {
        AtomicArrayUpdater.setIntVolatile(padding, 15, upd);
    }

    public void set(int upd)
    {
        padding[15] = upd;
    }

    public boolean cas(int exp, int upd)
    {
        return AtomicArrayUpdater.compareAndSetInt(padding, 15, exp, upd);
    }

}
