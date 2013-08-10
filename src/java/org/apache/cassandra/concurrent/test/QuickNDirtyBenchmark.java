package org.apache.cassandra.concurrent.test;

import com.google.common.util.concurrent.*;
import com.sun.management.OperatingSystemMXBean;
import org.apache.cassandra.concurrent.test.StridedRingBlockingQueue;
import org.apache.cassandra.concurrent.test.UnboundedLockFreeLinkedBlockingQueue;
import org.apache.commons.lang.*;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QuickNDirtyBenchmark
{

    static int TESTS = 10;
    static int CONSUMERS = 2;
    static int PRODUCERS = 2;

    // continuous tests
    static int TESTTIME = 10;
    static int GROUP = 25;
    static int REPORT_INTERVAL = 1;

    // workload tests
    static int WARMUPS = 10;
    static int WARMUPSIZE = 1 << 20;
    static int TESTSATONCE = 5;
    static int WORKLOADSIZE = 1 << 24;

    static int BUFFER_SIZE = 1 << 22;

    private static Runnable run = new Runnable()
    {
        @Override
        public void run()
        {
//            long time = System.currentTimeMillis();
//            if (time < 0)
//                System.out.println("ensure not optimised by hotspot");
        }
    };

    static ExecutorService build(BlockingQueue<Runnable> queue)
    {
//        return new ThreadPoolExecutor(CONSUMERS, CONSUMERS, 1L, TimeUnit.DAYS, queue);
        return new LightweightExecutorService(CONSUMERS, queue);
    }

    public static void main(String[] args) throws InterruptedException
    {
        benchContinuous();
    }
    public static void benchContinuous() throws InterruptedException
    {
        for (int i = 0 ; i < TESTS ; i++)
        {
//            benchContinuous("JDKABQ", build(new ArrayBlockingQueue<Runnable>(100000000)), TESTTIME);
//            benchContinuous("JDKLBQ", build(new LinkedBlockingQueue<Runnable>()), TESTTIME);
//            benchContinuous("LFQ", build(new UnboundedLockFreeLinkedBlockingQueue<Runnable>(new UnboundedLinkedWaitQueue())), TESTTIME);
//            benchContinuous("FJP", new ForkJoinPool(CONSUMERS), TESTTIME);
            benchContinuous("SRB", build(new StridedRingBlockingQueue<Runnable>(BUFFER_SIZE)), TESTTIME);
//            benchContinuous("DIS", new DisruptorExecutor(CONSUMERS, BUFFER_SIZE, false), TESTTIME);
//            benchContinuous("BAQ", build(new BlockingArrayQueue<Runnable>()), TESTTIME);
//            ForkJoinPool
        }
    }

    static void benchContinuous(String name, final ExecutorService exec, int seconds) throws InterruptedException
    {
        // gc to try to ensure level playing field
        System.gc();
        // setup producers / consumers
        final AtomicLong puts = new AtomicLong(0);
        final AtomicLong gets = new AtomicLong(0);
        final Runnable runadd = new Runnable()
        {
            @Override
            public void run()
            {
                gets.addAndGet(GROUP);
            }
        };
        final AtomicBoolean done = new AtomicBoolean(false);
        final Thread[] threads = new Thread[PRODUCERS];
        for (int i = 0 ; i < threads.length ; i++)
            threads[i] = new Thread(new Runnable(){
                @Override
                public void run()
                {
                    while (!done.get())
                    {
                        try{
                            for (int i = 1 ; i < GROUP ; i++)
                                    exec.execute(run);
                            exec.execute(runadd);
                            puts.addAndGet(GROUP);
                        } catch (RejectedExecutionException e)
                        {
                            System.err.println("Rejected Execution");
                            try
                            {
                                Thread.sleep(10);
                            } catch (InterruptedException e1) { }
                        }
                    }
                }
            });

        // init book keeping
        int laps = seconds / REPORT_INTERVAL;
        double lastputopc = 0, lastgetopc = 0, lastcycles = approxCyclesUsed(), firstcycles = lastcycles;
        long start = System.currentTimeMillis();
        long startCpu = cpuTime();

        // init
        for (int i = 0 ; i < threads.length ; i++)
            threads[i].start();

        // poll progress
        for (int i = 0 ; i < laps ; i++)
        {
            Thread.sleep(REPORT_INTERVAL * 1000);
            double putopc = puts.get(), getopc = gets.get(), cycles = approxCyclesUsed();
            double putopr = (putopc - lastputopc) / (REPORT_INTERVAL * 1000000);
            double getopr = (getopc - lastgetopc) / (REPORT_INTERVAL * 1000000);
            double cycler = (cycles - lastcycles) / ((putopr + getopr) * 1000000 / 2);
            System.out.println(String.format("%s: elapsed %d, averaging %.2f Mop/s in, %.2f Mop/s out, at %.2f cycles/op", name, REPORT_INTERVAL, putopr, getopr, cycler));
            lastputopc = putopc;
            lastgetopc = getopc;
            lastcycles = cycles;
        }

        // tidy up
        done.set(true);
        for (int i = 0 ; i < threads.length ; i++)
            while (threads[i].isAlive())
                threads[i].join(100);

        exec.shutdown();
        exec.awaitTermination(7, TimeUnit.DAYS);
        // gc to include any gc costs
        System.gc();

        // log total progress
        double elapsed = (System.currentTimeMillis() - start);
        double elapsedCpu = (cpuTime() - startCpu) / 1000000000d;
        double opr = puts.get() / (elapsed * 1000);
        double cycler = (approxCyclesUsed() - firstcycles) / puts.get();
        System.out.println(String.format("%s: total elapsed %.2fs (%.2fcpu), averaging %.2f Mop/s, at %.2f cycles/op", name, elapsed / 1000d, elapsedCpu, opr, cycler));

        if (gets.get() != puts.get())
            System.out.println(String.format("%s: put count mismatches get count: %d vs %d", name, puts.get(), gets.get()));
    }

    public static void benchRepeatTrials() throws InterruptedException
    {
        System.out.print("Warming up");
        for (int i = 0 ; i < WARMUPS ; i++)
        {
            benchRepeatTrials(build(new LinkedBlockingQueue<Runnable>()), WARMUPSIZE);
            benchRepeatTrials(build(new UnboundedLockFreeLinkedBlockingQueue<Runnable>()), WARMUPSIZE);
            System.out.print(".");
        }
        System.out.println("");
        System.out.println("Warm up complete, beginning tests...");
        for (int i = 0 ; i < TESTS ; i += TESTSATONCE)
        {
            for (int j = 0 ; j < TESTSATONCE; j++)
            {
                System.gc();
                long elapsed = benchRepeatTrials(build(new LinkedBlockingQueue<Runnable>()), WORKLOADSIZE);
                System.out.println("JDK: " + elapsed + "ms");
            }
            for (int j = 0 ; j < TESTSATONCE; j++)
            {
                System.gc();
                long elapsed = benchRepeatTrials(build(new UnboundedLockFreeLinkedBlockingQueue<Runnable>()), WORKLOADSIZE);
                System.out.println("LFQ: " + elapsed + "ms");
            }
        }
    }

    // should be easier to minimise GC activity
    static long benchRepeatTrials(final ExecutorService exec, int count) throws InterruptedException
    {
        final int prodCount = count / PRODUCERS;
        final Thread[] threads = new Thread[PRODUCERS];
        for (int i = 0 ; i < threads.length ; i++)
            threads[i] = new Thread(new Runnable(){
                @Override
                public void run()
                {
                    int count = prodCount;
                    for (int i = 0 ; i < count ; i++)
                        exec.execute(run);
                }
            });
        long start = System.currentTimeMillis();
        for (int i = 0 ; i < threads.length ; i++)
            threads[i].start();
        for (int i = 0 ; i < threads.length ; i++)
            threads[i].join();
        exec.shutdown();
        exec.awaitTermination(7, TimeUnit.DAYS);
        return System.currentTimeMillis() - start;
    }

    private static final double cyclesPerSecond = 2.2d;
    public static double approxCyclesUsed()
    {
        final double usage = ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
        return usage * cyclesPerSecond;
    }
    public static long cpuTime()
    {
        return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
    }

    private static final class LightweightExecutorService implements ExecutorService
    {

        private final CountDownLatch shutdown;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final Thread[] threads;
        private final BlockingQueue<Runnable> workQueue;
        LightweightExecutorService(int threadCount, BlockingQueue<Runnable> queue)
        {
            shutdown = new CountDownLatch(threadCount);
            threads = new Thread[threadCount];
            workQueue = queue;
            for (int i = 0 ; i != threadCount ; i++)
            {
                threads[i] = new Thread(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            while (true)
                            {
                                Runnable r = workQueue.take();
                                if (r != null)
                                    r.run();
                            }
                        } catch (InterruptedException e)
                        {
                            shutdown.countDown();
                        }
                    }
                });
                threads[i].start();
            }
        }


        @Override
        public void shutdown()
        {
            for (Thread t : threads)
                t.interrupt();
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
            return shutdown.await(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
            throw new NotImplementedException();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            throw new NotImplementedException();
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            throw new NotImplementedException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
        {
            throw new NotImplementedException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
        {
            throw new NotImplementedException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
        {
            throw new NotImplementedException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            throw new NotImplementedException();
        }

        @Override
        public void execute(Runnable command)
        {
            if (!workQueue.offer(command))
            {
                throw new RejectedExecutionException("Queue full");
            }
        }
    }

}
