package org.apache.cassandra.concurrent.test;

import org.apache.commons.lang.*;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

final class MinimalExecutorService implements ExecutorService

    {

        private final CountDownLatch shutdown;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final Thread[] threads;
        private final BlockingQueue<Runnable> workQueue;
        MinimalExecutorService(int threadCount, BlockingQueue<Runnable> queue)
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

