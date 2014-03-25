package org.apache.cassandra.utils.concurrent;


import sun.misc.Unsafe;

public final class UnsafeAtomicReferenceArrayUpdater<E>
{

    // it's quite likely these could all be static. however since the object itself should be static,
    // we hope the VM will inline the methods and values as constant anyway (though this may be optimistic)
    private final long offset;
    private final int shift;

    public UnsafeAtomicReferenceArrayUpdater(Class<E[]> arrayType)
    {
        offset = unsafe.arrayBaseOffset(arrayType);
        shift = shift(unsafe.arrayIndexScale(arrayType));

    }

    public boolean compareAndSet(E[] trg, int i, E exp, E upd) {
        return unsafe.compareAndSwapObject(trg, offset + (i << shift), exp, upd);
    }

    public void putVolatile(E[] trg, int i, E val) {
        unsafe.putObjectVolatile(trg, offset + (i << shift), val);
    }

    public void putOrdered(E[] trg, int i, E val) {
        unsafe.putOrderedObject(trg, offset + (i << shift), val);
    }

    public Object get(E[] trg, int i) {
        return unsafe.getObject(trg, offset + (i << shift));
    }

    public Object getVolatile(E[] trg, int i) {
        return unsafe.getObjectVolatile(trg, offset + (i << shift));
    }

    private static int shift(int scale)
    {
        if (Integer.bitCount(scale) != 1)
            throw new IllegalStateException();
        return Integer.bitCount(scale - 1);
    }

    private static final Unsafe unsafe = GetUnsafe.unsafe;
}