package org.apache.cassandra.concurrent.test;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkProcessor;
import org.apache.commons.lang.*;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.lmax.disruptor.RingBuffer.createMultiProducer;

public class DisruptorExecutorService extends AbstractExecutorService
{

    final RingBuffer<RContainer> ringBuffer;
    final CountDownLatch shutdown;
    final WorkProcessor<RContainer>[] workProcessors;
    volatile ExecutorService workExec;

    public int threads()
    {
        return workProcessors.length;
    }

    public DisruptorExecutorService(int threadCount, int bufferSize, boolean spin)
    {
        shutdown = new CountDownLatch(1);
        ringBuffer = RingBuffer.createMultiProducer(new EventFactory<RContainer>()
        {

            @Override
            public RContainer newInstance()
            {
                return new RContainer();
            }
        }, bufferSize, PhasedBackoffWaitStrategy.withLock(1, 10, TimeUnit.MILLISECONDS));
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        Sequence workSequence = new Sequence(-1);
        workProcessors = new WorkProcessor[threadCount];
        for (int i = 0 ; i < threadCount ; i++)
        {
            workProcessors[i] = new WorkProcessor<>(ringBuffer, sequenceBarrier,
                handler, new IgnoreExceptionHandler(), workSequence);
        }
    }

    @Override
    public void shutdown()
    {
        execute(new Runnable()
        {
            @Override
            public void run()
            {
                shutdown.countDown();
            }
        });
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean isShutdown()
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean isTerminated()
    {
        throw new NotImplementedException();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        if (!shutdown.await(timeout, unit))
            return false;
        for (WorkProcessor p : workProcessors)
            p.halt();
        workExec.shutdown();
        return workExec.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command)
    {
        long sequence = ringBuffer.next();
        RContainer event = ringBuffer.get(sequence);
        event.runnable = command;
        ringBuffer.publish(sequence);
    }

    private static final class RContainer
    {
        volatile Runnable runnable;
    }

    static final WorkHandler<RContainer> handler = new WorkHandler<RContainer>()
    {
        @Override
        public void onEvent(RContainer event) throws Exception
        {
            Runnable r = event.runnable;
            if (r != null)
                r.run();
        }
    };

    public void start()
    {
        workExec = Executors.newFixedThreadPool(workProcessors.length);
        for (WorkProcessor p : workProcessors)
            workExec.execute(p);
    }

}
