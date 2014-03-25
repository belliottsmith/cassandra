package org.apache.cassandra.utils.concurrent;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

class GetUnsafe
{
    static final sun.misc.Unsafe unsafe;
    static {
        unsafe = (sun.misc.Unsafe) AccessController.doPrivileged(
               new PrivilegedAction<Object>()
               {
                   @Override
                   public Object run()
                   {
                       try
                       {
                           Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                           f.setAccessible(true);
                           return f.get(null);
                       }
                       catch (NoSuchFieldException e)
                       {
                           // It doesn't matter what we throw;
                           // it's swallowed in getBestComparer().
                           throw new Error();
                       }
                       catch (IllegalAccessException e)
                       {
                           throw new Error();
                       }
                   }
               });
    }

}
