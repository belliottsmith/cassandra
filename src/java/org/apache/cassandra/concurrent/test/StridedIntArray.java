package org.apache.cassandra.concurrent.test;

import org.apache.cassandra.concurrent.test.AtomicArrayUpdater;

import java.util.Arrays;

public final class StridedIntArray
{

    private final int[] vals;

    public int length()
    {
        return vals.length;
    }

    public StridedIntArray(int size)
    {
        if (size < 256)
            size = 256;
        int ss = 1;
        while (1 << ss < size)
            ss++;
        vals = new int[1 << ss];
    }

    private static int real(int virt)
    {
        return  (virt & ~255)
              | ((virt & 15) << 4)
              | ((virt & (15 << 4)) >> 4);
    }

    public void set(int index, int upd)
    {
        vals[real(index)] = upd;
    }

    public void setVolatile(int index, int upd)
    {
        AtomicArrayUpdater.setIntVolatile(vals, real(index), upd);
    }

    public int get(int index)
    {
        return vals[real(index)];
    }

    public int getVolatile(int index)
    {
        return AtomicArrayUpdater.getIntVolatile(vals, real(index));
    }

    public boolean cas(int index, int exp, int upd)
    {
        return AtomicArrayUpdater.compareAndSetInt(vals, real(index), exp, upd);
    }

    public void setOrdered(int index, int upd)
    {
        AtomicArrayUpdater.setIntOrdered(vals, real(index), upd);
    }

    public void fill(int val)
    {
        Arrays.fill(vals, val);
    }

}
