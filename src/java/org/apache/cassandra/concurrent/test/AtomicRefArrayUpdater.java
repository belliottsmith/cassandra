package org.apache.cassandra.concurrent.test;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class AtomicRefArrayUpdater<T>
{

    static final Unsafe unsafe;
    static {
        unsafe = (Unsafe) AccessController.doPrivileged(
                new PrivilegedAction<Object>()
                {
                    @Override
                    public Object run()
                    {
                        try
                        {
                            Field f = Unsafe.class.getDeclaredField("theUnsafe");
                            f.setAccessible(true);
                            return f.get(null);
                        } catch (NoSuchFieldException e)
                        {
                            throw new Error();
                        } catch (IllegalAccessException e)
                        {
                            throw new Error();
                        }
                    }
                });
    }

    private final long offset;
    private final int shift;

    public AtomicRefArrayUpdater(Class<T[]> cl)
    {
        offset = unsafe.arrayBaseOffset(cl);
        int scale = unsafe.arrayIndexScale(cl);
        if (Integer.bitCount(scale) != 1)
            throw new Error();
        shift = Integer.numberOfTrailingZeros(scale);
    }


    public boolean compareAndSet(T[] obj, int index, T exp, T upd)
    {
        return unsafe.compareAndSwapObject(obj, offset + (index << shift), exp, upd);
    }

    public void setVolatile(T[] obj, int index, T upd)
    {
        unsafe.putObjectVolatile(obj, offset + (index << shift), upd);
    }

    public T getVolatile(T[] obj, int index)
    {
        return (T) unsafe.getObjectVolatile(obj, offset + (index << shift));
    }

    public void setOrdered(T[] obj, int index, T upd)
    {
        unsafe.putOrderedObject(obj, offset + (index << shift), upd);
    }
}
