package org.apache.cassandra.concurrent.test;

import org.apache.cassandra.concurrent.test.AtomicArrayUpdater;

import java.util.Arrays;

public final class FalseShareAwareLongArray
{

    private static int STRIDE_SHIFT = 3;

    private final long[] vals;

    public FalseShareAwareLongArray(int size)
    {
        vals = new long[(size + 1) << STRIDE_SHIFT];
    }

    public final void setVolatile(int index, long upd)
    {
        AtomicArrayUpdater.setLongVolatile(vals, (index + 1) << STRIDE_SHIFT, upd);
    }

    public final boolean cas(int index, long exp, long upd)
    {
        return AtomicArrayUpdater.compareAndSetLong(vals, (index + 1) << STRIDE_SHIFT, exp, upd);
    }

    public final long getVolatile(int index)
    {
        return AtomicArrayUpdater.getLongVolatile(vals, (index + 1) << STRIDE_SHIFT);
    }

    public int length()
    {
        return virt(vals.length);
    }

    private int real(int virt)
    {
        return (virt + 1) << STRIDE_SHIFT;
    }

    private int virt(int real)
    {
        return (real >> STRIDE_SHIFT) - 1;
    }

    public long[] toArray()
    {
        final int len = length();
        final long[] r = new long[len];
        for (int i = 0 ; i < len ; i++)
            r[i] = vals[real(i)];
        return r;
    }

    public long get(int i)
    {
        return vals[real(i)];
    }

    public String toString()
    {
        return Arrays.toString(toArray());
    }

    // exp is the expected current value to save a few cycles (maybe)
    public void ensureAtLeast(int index, long exp, long upd)
    {
        if (cas(index, exp, upd))
            return;
        while (true)
        {
            long cur = getVolatile(index);
            if (cur >= upd)
                return;
            if (cas(index, cur, upd))
                return;
        }
    }
}
