package org.apache.cassandra.concurrent.test;

import java.util.concurrent.locks.LockSupport;

public class UnboundedLinkedWaitQueue implements WaitQueue
{

    private static final int SPIN = 100;

    private static final AtomicRefUpdater<Node, Node> nextUpdater = new AtomicRefUpdater(Node.class, Node.class, "next");
    private static final AtomicRefUpdater<Node, Thread> threadUpdater = new AtomicRefUpdater(Node.class, Thread.class, "thread");
    private static final AtomicRefUpdater<UnboundedLinkedWaitQueue, Node> headUpdater = new AtomicRefUpdater(UnboundedLinkedWaitQueue.class, Node.class, "head");

    private static final class Node implements WaitNotice
    {
        final UnboundedLinkedWaitQueue queue;
        volatile Thread thread;
        volatile Node next;

        public Node(UnboundedLinkedWaitQueue queue, Thread thread)
        {
            this.thread = thread;
            this.queue = queue;
        }

        public void waitForever() throws InterruptedException
        {
            int spin = 0;
            Thread thr;
            while ((thr = thread) != null)
            {
                if (thr.isInterrupted())
                {
                    cancel();
                    throw new InterruptedException();
                }
                if (spin > SPIN)
                    LockSupport.park();
                else
                    spin++;
            }
        }

        public boolean waitUntil(long until) throws InterruptedException
        {
            int spin = 0;
            Thread thr;
            while ((thr = thread) != null && System.currentTimeMillis() < until)
            {
                if (thr.isInterrupted())
                {
                    cancel();
                    throw new InterruptedException();
                }
                if (spin > SPIN)
                    LockSupport.parkUntil(until);
                else
                    spin++;
            }
            return thr == null;
        }

        // must test this ensures no signals disappear
        @Override
        public void cancel()
        {
            if (null != clear())
                queue.signalOne();
        }

        @Override
        public boolean valid()
        {
            return thread != null;
        }

        Thread clear()
        {
            while (true)
            {
                Thread thr = thread;
                if (thr == null)
                    return null;
                if (threadUpdater.compareAndSet(this, thr, null))
                    return thr;
            }
        }
    }

    private volatile Node head = new Node(this, null);
    // tail is only a time saver, not an accurate pointer to the tail; always follow it until .next = null to find actual tail
    private volatile Node tail = head;

    public UnboundedLinkedWaitQueue()
    {
    }

    @Override
    public WaitNotice register()
    {

        final Node node = new Node(this, Thread.currentThread());

        Node tl = tail;
        while (true)
        {
            if (nextUpdater.compareAndSet(tl, null, node))
            {
                tail = node;
                break;
            }
            while (tl.next != null)
            {
                Node nxt = tl.next;
                if (nxt == tl)
                {
                    tl = tail;
                    nxt = tl.next;
                    if (nxt == tl)
                        resetTail();
                }
                else
                    tl = tl.next;
            }
        }

        return node;
    }

    private void resetTail()
    {
        Node hd = head, nxt;
        while ((nxt = hd.next) != null)
        {
            if (nxt == hd)
                hd = head;
        }
        tail = hd;
    }

    @Override
    public void signalOne()
    {
        while (true)
        {
            Node hd = head, nxt = hd.next;
            if (nxt == null)
            {
                if (head == hd)
                    return;
                continue;
            }
            if (headUpdater.compareAndSet(this, hd, nxt))
            {
                hd.next = hd;
                Thread thr = nxt.clear();
                if (thr != null)
                {
                    LockSupport.unpark(thr);
                    return;
                }
            }
        }
    }

    @Override
    public void signalAll()
    {
        while (true)
        {
            Node hd = head, nxt = hd.next;
            if (nxt == null && head == hd)
                return;
            signalOne();
        }
    }

}