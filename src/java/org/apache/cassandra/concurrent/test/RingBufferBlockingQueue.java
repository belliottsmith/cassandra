package org.apache.cassandra.concurrent.test;

import org.apache.commons.lang.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class RingBufferBlockingQueue<E> implements BlockingQueue<E>
{

    private final RingBuffer<E> buffer;
    private final WaitQueue isNotEmpty;

    public RingBufferBlockingQueue(RingBuffer<E> buffer)
    {
        this.buffer = buffer;
        this.isNotEmpty = new LinkedWaitQueue();
    }

    @Override
    public final boolean offer(E e)
    {
        long pos = buffer.offer(e);
        if (pos != -1)
        {
            isNotEmpty.signalOne();
            return true;
        }
        return false;
    }

    private static final int SPIN = 1000;
    private static final int SPINCYCLE = 10;

    @Override
    public final E poll(long timeout, TimeUnit unit) throws InterruptedException
    {
        long until = -1;
        int spin = 0;
        while (true)
        {
            E r = buffer.poll();
            if (r != null)
                return r;

            if (until < 0)
                until = System.currentTimeMillis() + unit.toMillis(timeout);
            if (spin < SPIN)
                for (int i = 0 ; i < SPINCYCLE ; i++)
                    spin++;
            else
            {
                WaitSignal wait = this.isNotEmpty.register();
                if (null != (r = buffer.poll()))
                {
                    wait.cancel();
                    return r;
                } else
                {
                    if (!wait.waitUntil(until))
                        return null;
                    wait.cancel();
                }
            }
        }
    }

    @Override
    public final int size()
    {
        return buffer.size();
    }

    @Override
    public final boolean isEmpty()
    {
        return buffer.isEmpty();
    }

    @Override
    public final boolean remove(Object o)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public final int drainTo(Collection<? super E> c)
    {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public final int drainTo(Collection<? super E> trg, int maxElements)
    {
        int count = 0;
        while (count < maxElements)
        {
            E v = poll();
            if (v == null)
                break;

            trg.add(v);
            count++;
        }
        return count;
    }

    @Override
    public final E poll()
    {
        try
        {
            return poll(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e)
        {
            // shouldn't happen, but return null anyway in case weird stuff
            // happening with clock. considere logging error
            return null;
        }
    }

    @Override
    public final E take() throws InterruptedException
    {
        return poll(Long.MAX_VALUE >> 1, TimeUnit.MILLISECONDS);
    }

    @Override
    public final void clear()
    {
        buffer.reset();
    }

    @Override
    public void put(E e) throws InterruptedException
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new NotImplementedException();
    }

    @Override
    public int remainingCapacity()
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean contains(Object o)
    {
        throw new NotImplementedException();
    }

    @Override
    public E remove()
    {
        throw new NotImplementedException();
    }

    @Override
    public E element()
    {
        throw new NotImplementedException();
    }

    @Override
    public E peek()
    {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<E> iterator()
    {
        throw new NotImplementedException();
    }

    @Override
    public Object[] toArray()
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c)
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean add(E e)
    {
        throw new NotImplementedException();
    }

}
