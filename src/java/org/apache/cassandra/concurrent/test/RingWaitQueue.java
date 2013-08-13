package org.apache.cassandra.concurrent.test;

import java.util.concurrent.locks.LockSupport;

public final class RingWaitQueue implements WaitQueue
{

    private static final int SPIN = 100;

    private final PhasedRingBuffer<Thread> queue;

    public RingWaitQueue(int threads)
    {
        queue = new PhasedRingBuffer<>(threads);
    }

    private final class Notice implements WaitSignal
    {

        final Thread thread;
        final long pos;

        private Notice(Thread thread, long pos)
        {
            this.thread = thread;
            this.pos = pos;
        }

        public void waitForever() throws InterruptedException
        {
            RingWaitQueue.this.waitForever(pos);
        }

        public boolean waitUntil(long until) throws InterruptedException
        {
            return RingWaitQueue.this.waitUntil(pos, until);
        }

        @Override
        public void cancel()
        {
            RingWaitQueue.this.cancel(pos);
        }

        @Override
        public boolean valid()
        {
            return queue.stillContains(pos);
        }

    }

    @Override
    public WaitSignal register()
    {
        final Thread thr = Thread.currentThread();
        return new Notice(thr, queue.add(thr));
    }

    @Override
    public void signalOne()
    {
        Thread t = queue.poll();
        if (t != null)
            LockSupport.unpark(t);
    }

    @Override
    public void signalAll()
    {
        while (true)
        {
            Thread t = queue.poll();
            if (t == null)
                return;
            LockSupport.unpark(t);
        }
    }

    public void waitForever(long pos) throws InterruptedException
    {
        int spin = 0;
        Thread thr = queue.get(pos);
        while ((queue.stillContains(pos)))
        {
            if (thr.isInterrupted())
            {
                cancel(pos);
                throw new InterruptedException();
            }
            if (spin > SPIN)
                LockSupport.park();
            else
                spin++;
        }
    }

    public boolean waitUntil(long pos, long until) throws InterruptedException
    {
        int spin = 0;
        Thread thr = queue.get(pos);
        while (System.currentTimeMillis() < until && queue.stillContains(pos))
        {
            if (thr.isInterrupted())
            {
                cancel(pos);
                throw new InterruptedException();
            }
            if (spin > SPIN)
                LockSupport.parkUntil(until);
            else
                spin++;
        }
        return !queue.stillContains(pos);
    }

    public void cancel(long pos)
    {
        if (queue.stillContains(pos))
            signalOne();
    }

}