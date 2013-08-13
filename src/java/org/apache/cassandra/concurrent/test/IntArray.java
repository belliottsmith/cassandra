package org.apache.cassandra.concurrent.test;

import org.apache.cassandra.concurrent.test.AtomicArrayUpdater;

import java.util.Arrays;

public final class IntArray
{

    private final int[] vals;
    private final boolean strided;

    public int length()
    {
        return vals.length;
    }

    public IntArray(int size, boolean strided)
    {
        if (size < 256)
            size = 256;
        int ss = 1;
        while (1 << ss < size)
            ss++;
        vals = new int[1 << ss];
        this.strided = strided;
    }

    private static int real(int virt)
    {
        return  (virt & ~255)
              | ((virt & 15) << 4)
              | ((virt & (15 << 4)) >> 4);
    }

    public void set(int index, int upd)
    {
        vals[strided ? real(index) : index] = upd;
    }

    public void setVolatile(int index, int upd)
    {
        AtomicArrayUpdater.setIntVolatile(vals, strided ? real(index) : index, upd);
    }

    public int get(int index)
    {
        return vals[strided ? real(index) : index];
    }

    public int getVolatile(int index)
    {
        return AtomicArrayUpdater.getIntVolatile(vals, strided ? real(index) : index);
    }

    public boolean cas(int index, int exp, int upd)
    {
        return AtomicArrayUpdater.compareAndSetInt(vals, strided ? real(index) : index, exp, upd);
    }

    public void setOrdered(int index, int upd)
    {
        AtomicArrayUpdater.setIntOrdered(vals, strided ? real(index) : index, upd);
    }

    public void fill(int val)
    {
        Arrays.fill(vals, val);
    }

}
