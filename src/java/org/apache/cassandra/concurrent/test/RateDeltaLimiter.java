package org.apache.cassandra.concurrent.test;

public class RateDeltaLimiter
{

    private final PaddedInt excessOps = new PaddedInt();
    private final int maxExcessOps;
    private final UnboundedLinkedWaitQueue freeOps = new UnboundedLinkedWaitQueue();

    public RateDeltaLimiter(int maxExcessOps)
    {
        this.maxExcessOps = maxExcessOps;
    }

    public void authorize(int ops) throws InterruptedException
    {
        while (true)
        {
            int excess = excessOps.getVolatile();
            int newExcess = excess + ops;
            if (newExcess < maxExcessOps)
                if (excessOps.cas(excess, newExcess))
                    return;
                else
                    continue;
            WaitNotice wait = freeOps.register();
            if (excessOps.getVolatile() + ops >= maxExcessOps)
                wait.waitForever();
        }
    }

    public void finish(int ops)
    {
        excessOps.addAndGet(-ops);
        freeOps.signalAll();
    }

}
