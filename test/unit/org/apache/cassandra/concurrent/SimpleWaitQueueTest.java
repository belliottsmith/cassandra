package org.apache.cassandra.concurrent;

import org.apache.cassandra.concurrent.test.RingWaitQueue;
import org.apache.cassandra.concurrent.test.LinkedWaitQueue;
import org.apache.cassandra.concurrent.test.WaitSignal;
import org.apache.cassandra.concurrent.test.WaitQueue;
import org.junit.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class SimpleWaitQueueTest
{

    @Test
    public void testSerial() throws InterruptedException
    {
        testSerial(new LinkedWaitQueue());
        testSerial(new RingWaitQueue(32));
    }
    public void testSerial(final WaitQueue queue) throws InterruptedException
    {
        Thread[] ts = new Thread[4];
        for (int i = 0 ; i < ts.length ; i++)
            ts[i] = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                WaitSignal wait = queue.register();
                try
                {
                    wait.waitForever();
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        });
        for (int i = 0 ; i < ts.length ; i++)
            ts[i].start();
        Thread.sleep(100);
        queue.signalOne();
        queue.signalOne();
        queue.signalOne();
        queue.signalOne();
        for (int i = 0 ; i < ts.length ; i++)
        {
            ts[i].join(100);
            assertFalse(queue.getClass().getName(), ts[i].isAlive());
        }
    }


    @Test
    public void testCondition1() throws InterruptedException
    {
        testCondition1(new LinkedWaitQueue());
        testCondition1(new RingWaitQueue(32));
    }

    public void testCondition1(final WaitQueue queue) throws InterruptedException
    {
        final AtomicBoolean cond1 = new AtomicBoolean(false);
        final AtomicBoolean cond2 = new AtomicBoolean(false);
        final AtomicBoolean fail = new AtomicBoolean(false);
        Thread t1 = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Thread.sleep(200);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                WaitSignal wait = queue.register();
                if (!cond1.get())
                {
                    System.err.println("Condition should have already been met");
                    fail.set(true);
                }
            }
        });
        t1.start();
        Thread.sleep(50);
        cond1.set(true);
        Thread.sleep(300);
        queue.signalOne();
        t1.join(300);
        assertFalse(queue.getClass().getName(), t1.isAlive());
        assertFalse(fail.get());
    }

    @Test
    public void testCondition2() throws InterruptedException
    {
        testCondition2(new LinkedWaitQueue());
        testCondition2(new RingWaitQueue(32));
    }
    public void testCondition2(final WaitQueue queue) throws InterruptedException
    {
        final AtomicBoolean condition = new AtomicBoolean(false);
        final AtomicBoolean fail = new AtomicBoolean(false);
        Thread t = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                WaitSignal wait = queue.register();
                if (condition.get())
                {
                    System.err.println("");
                    fail.set(true);
                }

                try
                {
                    Thread.sleep(200);
                    wait.waitForever();
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                if (!condition.get())
                {
                    System.err.println("Woke up when condition not met");
                    fail.set(true);
                }
            }
        });
        t.start();
        Thread.sleep(50);
        condition.set(true);
        queue.signalOne();
        t.join(300);
        assertFalse(queue.getClass().getName(), t.isAlive());
        assertFalse(fail.get());
    }

}
