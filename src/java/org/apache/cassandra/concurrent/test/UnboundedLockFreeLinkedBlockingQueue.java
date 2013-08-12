package org.apache.cassandra.concurrent.test;

import org.apache.commons.lang.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public final class UnboundedLockFreeLinkedBlockingQueue<E> implements BlockingQueue<E>
{

    @SuppressWarnings("rawtypes")
    private static final AtomicRefUpdater<Node, Node> nextUpdater = new AtomicRefUpdater(Node.class, Node.class, "next");
    private static final AtomicRefUpdater<UnboundedLockFreeLinkedBlockingQueue, Node> headUpdater = new AtomicRefUpdater(UnboundedLockFreeLinkedBlockingQueue.class, Node.class, "head");

    private static final class Node<E>
    {
        volatile E payload;
        volatile Node<E> next;

        public Node(E payload)
        {
            this.payload = payload;
        }
    }

    // TODO: ensure no bugbears with stale tails wrapping around to self, or other assumptions can reach end of chain without incident

    // first item is a "dummy" but is constantly shifting to the most recently returned item (or currently returning item)
    private volatile Node<E> head = new Node<E>(null);
    // tail is only a time saver, not an accurate pointer to the tail; always follow it until .next = null to find actual tail
    private volatile Node<E> tail = head;
    private final WaitQueue isNotEmpty;

    public UnboundedLockFreeLinkedBlockingQueue()
    {
//        isNotEmpty = new UnboundedLinkedWaitQueue();
        isNotEmpty = new RingWaitQueue(32);
    }

    public UnboundedLockFreeLinkedBlockingQueue(WaitQueue queue)
    {
        isNotEmpty = queue;
    }

    @Override
    public boolean offer(E e)
    {

        final Node<E> node = new Node<E>(e);

        while (true)
        {
            Node<E> tl = tail();
            if (tl.next == null && nextUpdater.compareAndSet(tl, null, node))
            {
                tail = node;
                break;
            }
        }

        isNotEmpty.signalOne();
        return true;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException
    {
        long until = -1;
        while (true)
        {
            Node<E> hd = head, nxt = hd.next;

            if (nxt == null | nxt == hd)
            {
                if (nxt == hd)
                    continue;

                if (until < 0)
                    until = System.currentTimeMillis() + unit.toMillis(timeout);

                final WaitNotice isNotEmpty = this.isNotEmpty.register();
                if (hd.next == null)
                    isNotEmpty.waitUntil(until);
                isNotEmpty.cancel();

                if (hd.next == null)
                    return null;
                continue;
            }

            if (headUpdater.compareAndSet(this, hd, nxt))
            {
                E r = nxt.payload;
                nxt.payload = null;
                hd.next = hd;
                if (r != null)
                    return r;
            }
        }
    }

    private final Node<E> tail()
    {
        Node<E> tl = tail, nxt;
        while ((nxt = tl.next) != null)
        {
            if (nxt == tl)
            {
                tl = tail;
                nxt = tl.next;
                if (nxt == tl)
                    tl = refindTail();
            }
            else tl = tl.next;
        }
        return tl;
    }

    private final Node<E> refindTail()
    {
        Node<E> hd = head, nxt;
        while ((nxt = hd.next) != null)
        {
            if (nxt == hd)
                hd = head;
            else
                hd = nxt;
        }
        return hd;
    }

    @Override
    public E poll()
    {
        try
        {
            return poll(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e)
        {
            throw new IllegalStateException();
        }
    }

    // For performance reasons, we do not guarantee an accurate reporting of success
    // for removal - it is possible we believe we removed an item when we did not,
    // this can be rectified by using compareAndSet() in both remove() and poll()
    // however this isn't important for our use case
    @Override
    public boolean remove(Object o)
    {
        Node<E> hd = head;
        Node<E> tl = tail;
        while (tl.next != null)
            tl = tl.next;
        while (hd != tl)
        {
            if (hd.payload == o)
            {
                hd.payload = null;
                // possibly not true, but assume so
                return true;
            }
            hd = hd.next;
        }
        return false;
    }

    @Override
    public boolean isEmpty()
    {
        // if head.next == head, it's just been dequeued during the call, so just treat our call as if it happened fractionally earlier
        // in order to guarantee determinism
        return head.next == null;
    }

    @Override
    public int size()
    {
        Node<E> hd = head, nxt;
        int c = 0;
        while ((nxt = hd.next) != null)
        {
            if (nxt == hd)
            {
                hd = head;
                c = 0;
            } else
            {
                hd = nxt;
                c++;
            }
        }
        return c;
    }

    @Override
    public int drainTo(Collection<? super E> c)
    {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> trg, int maxElements)
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
    public <T> T[] toArray(T[] a)
    {
        ArrayList<E> accum = new ArrayList<>();
        Node<E> hd = head;
        while (hd != null)
        {
            Node<E> nxt = hd.next;
            if (nxt == hd)
            {
                accum.clear();
                hd = head;
            }
            hd = nxt;
            E payload = hd.payload;
            if (payload != null)
                accum.add(payload);
        }
        return accum.toArray(a);
    }

    @Override
    public E take() throws InterruptedException
    {
        return poll(Long.MAX_VALUE >> 1, TimeUnit.MILLISECONDS);
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
    public void clear()
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean add(E e)
    {
        throw new NotImplementedException();
    }
}
