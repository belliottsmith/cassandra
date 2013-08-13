package org.apache.cassandra.concurrent.test;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class AtomicRefUpdater<T, V>
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

    public AtomicRefUpdater(Class<T> tclass, Class<V> vclass, String fieldName)
    {
        Field field = null;
        Class fieldClass = null;
        try {
            field = tclass.getDeclaredField(fieldName);
//            sun.reflect.misc.ReflectUtil.checkPackageAccess(tclass);
            fieldClass = field.getType();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        if (vclass != fieldClass)
            throw new ClassCastException();

        offset = unsafe.objectFieldOffset(field);
    }

    public boolean compareAndSet(T obj, V exp, V upd)
    {
        return unsafe.compareAndSwapObject(obj, offset, exp, upd);
    }

    public void setVolatile(T obj, V upd)
    {
        unsafe.putObjectVolatile(obj, offset, upd);
    }

    public V getVolatile(T obj)
    {
        return (V) unsafe.getObjectVolatile(obj, offset);
    }

}
