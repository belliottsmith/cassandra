package org.apache.cassandra.concurrent.test;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class AtomicArrayUpdater
{

    static final Unsafe unsafe;
    private static final long longoffset, intoffset;
    private static final int longshift, intshift;
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
        longoffset = unsafe.arrayBaseOffset(long[].class);
        longshift = shift(unsafe.arrayIndexScale(long[].class));
        intoffset = unsafe.arrayBaseOffset(int[].class);
        intshift = shift(unsafe.arrayIndexScale(int[].class));
    }

    private static int shift(int scale)
    {
        if (Integer.bitCount(scale) != 1)
            throw new Error();
        return Integer.numberOfTrailingZeros(scale);
    }

    public static boolean compareAndSetLong(long[] obj, int index, long exp, long upd)
    {
        return unsafe.compareAndSwapLong(obj, longoffset + (index << longshift), exp, upd);
    }

    public static void setLongVolatile(long[] obj, int index, long upd)
    {
        unsafe.putLongVolatile(obj, longoffset + (index << longshift), upd);
    }

    public static long getLongVolatile(long[] obj, int index)
    {
        return unsafe.getLongVolatile(obj, longoffset + (index << longshift));
    }

    public static boolean compareAndSetInt(int[] obj, int index, int exp, int upd)
    {
        return unsafe.compareAndSwapInt(obj, intoffset + (index << intshift), exp, upd);
    }

    public static void setIntVolatile(int[] obj, int index, int upd)
    {
        unsafe.putIntVolatile(obj, intoffset + (index << intshift), upd);
    }

    public static int getIntVolatile(int[] obj, int index)
    {
        return unsafe.getIntVolatile(obj, intoffset + (index << intshift));
    }

    public static void setIntOrdered(int[] obj, int index, int upd)
    {
        unsafe.putOrderedInt(obj, intoffset + (index << intshift), upd);
    }

    public static void setLongOrdered(long[] obj, int index, long upd)
    {
        unsafe.putOrderedLong(obj, longoffset + (index << longshift), upd);
    }
}
