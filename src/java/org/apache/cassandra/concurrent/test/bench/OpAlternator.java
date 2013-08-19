package org.apache.cassandra.concurrent.test.bench;

import org.apache.cassandra.concurrent.test.LinkedWaitQueue;
import org.apache.cassandra.concurrent.test.WaitQueue;
import org.apache.cassandra.concurrent.test.WaitSignal;

import java.util.concurrent.atomic.AtomicBoolean;

public class OpAlternator
{

    final int opsPerRound;

    final AtomicBoolean turn = new AtomicBoolean(true);
    final AtomicBoolean alive = new AtomicBoolean(true);
    volatile int remainingOps;
    final WaitQueue turnChange = new LinkedWaitQueue();

    public OpAlternator(int opsPerRound)
    {
        this.opsPerRound = opsPerRound;
        remainingOps = opsPerRound;
    }

    public Handle first()
    {
        return new Handle(true);
    }

    public Handle second()
    {
        return new Handle(false);
    }

    final class Handle implements OpLimiter
    {

        final boolean first;

        public Handle(boolean first)
        {
            this.first = first;
        }

        @Override
        public void authorize(int ops) throws InterruptedException
        {
            while (true)
            {
                if (first != turn.get())
                {
                    final WaitSignal wait = turnChange.register();
                    if (first != turn.get() && alive.get())
                        wait.waitForever();
                    wait.cancel();
                }
                if (!alive.get())
                    return;
                if (remainingOps > 0)
                {
                    remainingOps -= ops;
                    return;
                }
                remainingOps = opsPerRound;
                turn.set(!first);
                turnChange.signalOne();
            }
        }

        @Override
        public void complete()
        {
            if (remainingOps <= 0)
            {
                remainingOps = opsPerRound;
                turn.set(!first);
                turnChange.signalOne();
            }
        }

    }

    public void close()
    {
        alive.set(false);
        turnChange.signalAll();
    }

}
