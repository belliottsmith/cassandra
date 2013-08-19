package org.apache.cassandra.concurrent.test.bench;

import org.apache.cassandra.concurrent.test.LinkedWaitQueue;
import org.apache.cassandra.concurrent.test.PaddedInt;
import org.apache.cassandra.concurrent.test.WaitSignal;

import java.util.concurrent.atomic.AtomicInteger;

public class OpDeltaLimiter
{

    private final AtomicInteger excessOps = new AtomicInteger();
    private final int maxExcessOps;
    private final LinkedWaitQueue freeOps = new LinkedWaitQueue();

    public OpDeltaLimiter(int maxExcessOps)
    {
        this.maxExcessOps = maxExcessOps;
    }

    public Producer producer()
    {
        return new Producer();
    }

    public Consumer consumer()
    {
        return new Consumer();
    }

    final class Producer implements OpLimiter
    {

        public void authorize(int ops) throws InterruptedException
        {
            while (true)
            {
                int excess = excessOps.get();
                int newExcess = excess + ops;
                if (newExcess < maxExcessOps)
                    if (excessOps.compareAndSet(excess, newExcess))
                        return;
                    else
                        continue;
                WaitSignal wait = freeOps.register();
                if (excessOps.get() + ops >= maxExcessOps)
                    wait.waitForever();
                wait.cancel();
            }
        }

        @Override
        public void complete()
        {
        }

    }

    final class Consumer implements OpLimiter
    {

        public void authorize(int ops) throws InterruptedException
        {
            excessOps.addAndGet(-ops);
            freeOps.signalAll();
        }

        @Override
        public void complete()
        {
        }

    }

}
