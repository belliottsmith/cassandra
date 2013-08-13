package org.apache.cassandra.concurrent.test.bench;

import org.apache.cassandra.concurrent.test.LinkedWaitQueue;
import org.apache.cassandra.concurrent.test.PaddedInt;
import org.apache.cassandra.concurrent.test.WaitSignal;

public class OpDeltaLimiter
{

    private final PaddedInt excessOps = new PaddedInt();
    private final int maxExcessOps;
    private final LinkedWaitQueue freeOps = new LinkedWaitQueue();

    public OpDeltaLimiter(int maxExcessOps)
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
            WaitSignal wait = freeOps.register();
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
