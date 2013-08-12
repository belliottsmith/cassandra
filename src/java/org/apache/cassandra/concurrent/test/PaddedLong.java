package org.apache.cassandra.concurrent.test;

import org.apache.cassandra.concurrent.test.AtomicArrayUpdater;

public final class PaddedLong
{

    private final long[] padding = new long[15];

    public long get()
    {
        return padding[7];
    }

    public long getVolatile()
    {
        return AtomicArrayUpdater.getLongVolatile(padding, 7);
    }

    public void setVolatile(long upd)
    {
        AtomicArrayUpdater.setLongVolatile(padding, 7, upd);
    }

    public void setOrdered(long upd)
    {
        AtomicArrayUpdater.setLongOrdered(padding, 7, upd);
    }

    public void set(long upd)
    {
        padding[7] = upd;
    }

    public boolean cas(long exp, long upd)
    {
        return AtomicArrayUpdater.compareAndSetLong(padding, 7, exp, upd);
    }

    public long addAndReturnOrig(long add)
    {
        while (true)
        {
            long cur = get();
            if (cas(cur, cur + add))
                return cur;
        }
    }

    public long incrementAndReturnOrig()
    {
        while (true)
        {
            long cur = get();
            if (cas(cur, cur + 1))
                return cur;
        }
    }

    public void ensureAtLeast(long val)
    {
        while (true)
        {
            long cur = get();
            if (cur >= val || cas(cur, val))
                return;
        }
    }

}
