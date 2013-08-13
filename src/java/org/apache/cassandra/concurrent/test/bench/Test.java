package org.apache.cassandra.concurrent.test.bench;

import org.apache.cassandra.concurrent.test.BlockingArrayQueue;
import org.apache.cassandra.concurrent.test.DisruptorExecutorService;
import org.apache.cassandra.concurrent.test.LinkedPhasedBlockingQueue;
import org.apache.cassandra.concurrent.test.LockFreeLinkedBlockingQueue;
import org.apache.cassandra.concurrent.test.PhasedBlockingQueue;

import java.lang.reflect.Constructor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

enum Test
{
    ABQ(ArrayBlockingQueue.class),
    LBQ(LinkedBlockingQueue.class),
    BAQ(BlockingArrayQueue.class),
    LFQ(LockFreeLinkedBlockingQueue.class),
    LPRB(LinkedPhasedBlockingQueue.class),
    PRB(PhasedBlockingQueue.class),
    DIS(true);

    final Class<? extends BlockingQueue> clazz;
    final boolean disruptor;
    final boolean reuse;

    BlockingQueue<Runnable> cache;
    DisruptorExecutorService disruptorCache;

    Test(Class<? extends BlockingQueue> clazz)
    {
        this(clazz, true);
    }

    Test(Class<? extends BlockingQueue> clazz, boolean reuse)
    {
        this.clazz = clazz;
        disruptor = false;
        this.reuse = reuse;
    }

    Test(boolean disruptor)
    {
        this.disruptor = true;
        clazz = null;
        reuse = true;
    }

    ExecutorService build(int threads)
    {
        BlockingQueue<Runnable> queue = getQueue();
        if (queue != null)
            return new ThreadPoolExecutor(threads, threads, 1L, TimeUnit.DAYS, queue);
        if (disruptor)
        {
            DisruptorExecutorService exec = null;
            if (disruptorCache != null && disruptorCache.threads() == threads)
                exec = disruptorCache;
            if (exec == null)
                exec = new DisruptorExecutorService(threads, Benchmark.BUFFER_SIZE, false);
            if (Benchmark.REUSE && reuse)
                disruptorCache = exec;
            exec.start();
            return exec;
        }
        throw new IllegalStateException();
    }

    BlockingQueue<Runnable> getQueue()
    {
        if (cache != null)
        {
            cache.clear();
            return cache;
        }
        if (clazz == null)
            return null;
        try
        {
            BlockingQueue<Runnable> r = null;
            // try no-arg constructor
            try {
                Constructor<? extends BlockingQueue> c = clazz.getConstructor();
                r = c.newInstance();
            } catch (NoSuchMethodException _) {}

            if (r == null)
            {
                // try single arg constructor
                try {
                    Constructor<? extends BlockingQueue> c = clazz.getConstructor(int.class);
                    r = c.newInstance(Benchmark.BUFFER_SIZE);
                } catch (NoSuchMethodException _) {}
            }

            if (r == null)
                throw new IllegalStateException("No suitable constructors found for " + clazz);

            if (Benchmark.REUSE & reuse)
                cache = r;

            return r;
        } catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    public static void clearCache()
    {
        for (Test test : values())
        {
            test.disruptorCache = null;
            test.cache = null;
        }
    }

}

