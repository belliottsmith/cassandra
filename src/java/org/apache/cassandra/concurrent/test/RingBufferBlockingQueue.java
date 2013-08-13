package org.apache.cassandra.concurrent.test;

import org.apache.commons.lang.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class RingBufferBlockingQueue<E> implements BlockingQueue<E>
{

    private static final int SPIN = 1000;
    private static final int SPINCYCLE = 10;

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

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException
    {
        long until = -1;
        while (true)
        {
            long pos = buffer.offer(e);
            if (pos >= 0)
            {
                isNotEmpty.signalOne();
                return true;
            }

            if (until < 0)
                until = System.currentTimeMillis() + unit.toMillis(timeout);
            WaitSignal wait = buffer.notFull();
            if (buffer.offer(e) < 0)
            {
                isNotEmpty.signalOne();
                wait.cancel();
                return true;
            }
            else
            {
                if (!wait.waitUntil(until))
                    return false;
                wait.cancel();
            }
        }

    }

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
        return buffer.remove(o);
    }

    @Override
    public final E poll()
    {
        return buffer.poll();
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
    public E peek()
    {
        return buffer.peek();
    }

    // PROCEEDING IMPLEMENTATIONS ARE ESPECIALLY HACKY

    @Override
    public <T> T[] toArray(T[] a)
    {
        final List<E> acc = new ArrayList<E>();
        for (E e : this)
            acc.add(e);
        return acc.toArray(a);
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
    public void put(E e) throws InterruptedException
    {
        if (!offer(e, Long.MAX_VALUE >> 1, TimeUnit.MILLISECONDS))
            throw new NoSuchElementException();
    }

    @Override
    public int remainingCapacity()
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean contains(Object o)
    {
        return buffer.contains(o);
    }

    @Override
    public E remove()
    {
        return poll();
    }

    @Override
    public E element()
    {
        E r = peek();
        if (r == null)
            throw new NoSuchElementException();
        return r;
    }

    @Override
    public Iterator<E> iterator()
    {
        return buffer.iterator();
    }

    @Override
    public Object[] toArray()
    {
        return toArray(new Object[0]);
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        for (Object o : c)
            if (!contains(o))
                return false;
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c)
    {
        for (E e : c)
            add(e);
        return true;
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
        try
        {
            put(e);
        } catch (InterruptedException _)
        {
            throw new IllegalStateException();
        }
        return  true;
    }

}
