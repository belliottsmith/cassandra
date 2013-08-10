package org.apache.cassandra.concurrent.test;

import static org.apache.cassandra.concurrent.test.AtomicArrayUpdater.compareAndSetInt;
import static org.apache.cassandra.concurrent.test.AtomicArrayUpdater.getIntVolatile;

public final class StridedTwoBitArray
{

    private final int[] vals;

    public StridedTwoBitArray(int size)
    {
        if (size >> 4 < 256)
            size = 256;
        else
            size = size >> 4;
        int ss = 1;
        while (1 << ss < size)
            ss++;
        vals = new int[1 << ss];
    }

    private static int bucketIndex(int virt)
    {
        return ((virt & 15) << 4) | (virt & ~(15 << 4));
    }

    private static int bitIndex(int virt)
    {
        return ((virt >> 4) & 15) << 1;
    }

    // no checking of input, so don't use incorrectly! (i.e. no more than expected bits)
    public void set(int index, int bits)
    {
        int bitIndex = bitIndex(index);
        int bucketIndex = bucketIndex(index);
        bits = (bits & 3) << bitIndex;
        int zero = ~(3 << bitIndex);
        while (true)
        {
            int cur = vals[bucketIndex];
            int next = (cur & zero) | bits;
            if (cur == next || compareAndSetInt(vals, bucketIndex, cur, next))
                return;
        }
    }

    public int cas(int index, int exp, int upd)
    {
        int bitIndex = bitIndex(index);
        int bucketIndex = bucketIndex(index);
        int ones = ~(3 << bitIndex);
        int zeros = ~(3 << bitIndex);
        exp = (exp & 3) << bitIndex;
        upd = (upd & 3) << bitIndex;
        while (true)
        {
            int cur = vals[bucketIndex];
            int next = (cur & zeros) | upd;
            if ((cur & ones) == exp)
                return (cur & ones) >>> bitIndex;
            if (compareAndSetInt(vals, bucketIndex, cur, next))
                return exp >>> bitIndex;
        }
    }

    public int get(int index)
    {
        int bitIndex = bitIndex(index);
        int bits = 3 << (bitIndex);
        return (vals[bucketIndex(index)] & bits) >>> bitIndex;
    }

    public int getVolatile(int index)
    {
        int bitIndex = bitIndex(index);
        int bits = 3 << (bitIndex);
        return (getIntVolatile(vals, bucketIndex(index)) & bits) >>> bitIndex;
    }

}
