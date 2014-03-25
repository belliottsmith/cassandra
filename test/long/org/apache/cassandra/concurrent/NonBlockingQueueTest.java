package org.apache.cassandra.concurrent;

import com.google.common.collect.*;

import org.apache.cassandra.utils.concurrent.NonBlockingQueue;
import org.apache.cassandra.utils.concurrent.SafeRemoveIterator;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;

import org.junit.*;
import org.slf4j.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.cassandra.utils.concurrent.NonBlockingQueue.Snap;

public class NonBlockingQueueTest
{

    private static final Logger logger = LoggerFactory.getLogger(NonBlockingQueueTest.class);

    final int THREAD_COUNT = 32;
    final int THREAD_MASK = THREAD_COUNT - 1;
    final ExecutorService exec = Executors.newFixedThreadPool(THREAD_COUNT, new NamedThreadFactory("Test"));

    @Test
    public void testSafeIteratorRemoval() throws ExecutionException, InterruptedException
    {
        testIteratorRemoval(true, 500, 1 << 14);
    }

    @Test
    public void testUnsafeIteratorRemoval() throws ExecutionException, InterruptedException
    {
        testIteratorRemoval(false, 500, 1 << 14);
    }

    // all we care about is that iterator removals don't delete anything other than they target,
    // since it makes no guarantees about removal succeeding. To try and test it thoroughly we will
    // spin deleting until all our intended deletes complete successfully.
    // We also partially test snapshots, iterators and views.
    public void testIteratorRemoval(final boolean safe, final int batchCount, final int batchSize) throws ExecutionException, InterruptedException
    {

        final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicInteger totalBatchCount = new AtomicInteger();
        final AtomicLong failedDeletes = new AtomicLong();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            final int offset = i;
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    for (int batch = 0 ; batch < batchCount ; batch++)
                    {
                        final int end = offset + (THREAD_COUNT * batchSize);
                        Snap<Integer> snap = queue.snap();
                        for (int i = offset ; i < end ; i+= THREAD_COUNT)
                            queue.append(i);

                        // check all my items are still there
                        snap = snap.extend();
                        int find = offset;
                        for (Integer v : snap)
                        {
                            if ((v & THREAD_MASK) == offset)
                            {
                                if (v != find)
                                {
                                    logger.error("Unexpected next value (1); expected {}, found {}", find, v);
                                    return Boolean.FALSE;
                                }
                                find += THREAD_COUNT;
                            }
                        }
                        if (find != end)
                        {
                            logger.error("Unexpected last value (1); expected {}, found {}", end, find);
                            return Boolean.FALSE;
                        }

                        // delete every other item, and loop until they're all gone, failing if we cannot delete more than
                        // 50% over 1000 tries
                        int tries = 0;
                        int notmissing = 0;
                        while (++tries <= 1000)
                        {
                            notmissing = 0;
                            find = offset;
                            SafeRemoveIterator<Integer> iter = snap.iterator();
                            while (iter.hasNext() && find != end)
                            {
                                Integer next = iter.next();
                                if ((next & THREAD_MASK) == offset)
                                {
                                    if (next == find)
                                        find += THREAD_COUNT << 1;
                                    else if (next > find)
                                    {
                                        logger.error("Unexpected next value (2) on try {}; expected {}, found {}", tries, find, next);
                                        return Boolean.FALSE;
                                    }
                                    else
                                    {
                                        if (safe)
                                            iter.safeRemove();
                                        else
                                            iter.remove();
                                        notmissing++;
                                    }
                                }
                            }
                            if (find != end)
                            {
                                logger.error("Unexpected last value (2) on try {}; expected {}, found {}", tries, end, find);
                                return Boolean.FALSE;
                            }
                            if (tries > 1 && notmissing < batchSize / 4)
                                break;
                        }
                        if (tries > 1000)
                        {
                            logger.error("Failed to delete 50% of items from the queue, despite 1000 tries (deleted {} of {})", end, find, batchSize - notmissing, batchSize);
                            return Boolean.FALSE;
                        }
                        // poll the entire queue, to permit GC
                        while (queue.advance() != null);

                        // racey stats
                        int da = deleteAttempts.addAndGet(tries - 1);
                        long fd = failedDeletes.addAndGet(notmissing);
                        int bc = totalBatchCount.incrementAndGet();
                        if (bc % 1000 == 0)
                            logger.warn("Batch {} of {} Complete. {}% transient deletes on average, after {} tries.", bc, batchCount * THREAD_COUNT, 100 * fd / ((batchSize / 2) * (double) bc), da / (double) bc);
                        if (safe && fd != 0)
                        {
                            logger.error("Failed to delete some items, despite running safe deletes");
                            return Boolean.FALSE;
                        }
                    }

                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }

    @Test
    public void testUnsafeIteratorRemoval2() throws ExecutionException, InterruptedException
    {
        testIteratorRemoval2(false, 500, 1 << 16);
    }
    @Test
    public void testSafeIteratorRemoval2() throws ExecutionException, InterruptedException
    {
        testIteratorRemoval2(true, 500, 1 << 16);
    }
    // similar to testIteratorRemoval, except every thread operates over the same range to test hyper competitive deletes
    public void testIteratorRemoval2(final boolean safe, final int batchCount, final int batchSize) throws ExecutionException, InterruptedException
    {
        final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicInteger totalBatchCount = new AtomicInteger();
        final AtomicLong failedDeletes = new AtomicLong();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    for (int batch = 0 ; batch < batchCount ; batch++)
                    {
                        Snap<Integer> snap = queue.snap();
                        for (int i = 0 ; i < batchSize ; i += 1)
                            queue.append(i);

                        // snap a range of the queue that should contain all the items we add
                        snap = snap.extend();
                        // delete every other item, and loop until they're all gone, failing if we cannot delete more than
                        // 50% over 1000 tries. Note that since we're operating over the same range as other operations here
                        // we delete every instance we see that isn't (mathematically) even
                        int tries = 0;
                        int notmissing = 0;
                        while (++tries <= 1000)
                        {
                            int find = 0;
                            notmissing = 0;
                            SafeRemoveIterator<Integer> iter = snap.iterator();
                            while (iter.hasNext())
                            {
                                Integer next = iter.next();
                                if ((next & 1) == 1)
                                {
                                    if (safe)
                                        iter.safeRemove();
                                    else
                                        iter.remove();
                                    notmissing++;
                                }
                                else if (next == find)
                                    find += 2;
                            }
                            if (find != batchSize)
                            {
                                logger.error("Unexpected last value; expected {}, found {}", batchSize, find);
                                return Boolean.FALSE;
                            }
                            if (tries > 1 && notmissing < batchSize / 4)
                                break;
                        }
                        if (tries > 1000)
                        {
                            logger.error("Failed to delete 50% of items from the queue, despite 1000 tries");
                            return Boolean.FALSE;
                        }
                        // poll the entire queue, to permit GC
                        while (queue.advance() != null);

                        int da = deleteAttempts.addAndGet(tries - 1);
                        long fd = failedDeletes.addAndGet(notmissing);
                        int bc = totalBatchCount.incrementAndGet();
                        if (bc % 1000 == 0)
                            logger.warn("Batch {} of {} Complete. {}% transient deletes on average, after {} tries.", bc, batchCount * THREAD_COUNT, 100 * fd / ((batchSize / 2) * (double) bc), da / (double) bc);

                        if (safe && fd != 0)
                        {
                            logger.error("Failed to delete some items, despite running safe deletes");
                            return Boolean.FALSE;
                        }
                    }

                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }

    @Test
    public void testSimpleAppendAndAdvance() throws ExecutionException, InterruptedException
    {
        final int batchSize = 1 << 20;
        final int batchCount = 20;
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
            final List<Future<int[]>> success = new ArrayList<>();
            for (int i = 0 ; i < THREAD_COUNT ; i++)
            {
                final int offset = i;
                success.add(exec.submit(new Callable<int[]>()
                {
                    @Override
                    public int[] call()
                    {
                        int[] items = new int[batchSize];
                        for (int i = 0 ; i < batchSize ; i++)
                        {
                            queue.append((i * THREAD_COUNT) + offset);
                            items[i] = queue.advance();
                        }
                        return items;
                    }
                }));
            }

            final boolean[] found = new boolean[batchSize * THREAD_COUNT];
            for (Future<int[]> result : success)
            {
                for (int i : result.get())
                {
                    Assert.assertFalse(found[i]);
                    found[i] = true;
                }
            }
            for (boolean b : found)
                Assert.assertTrue(b);
            Assert.assertTrue(queue.isEmpty());
            logger.warn("Batch {} of {}", batch + 1, batchCount);
        }
    }

    @Test
    public void testAdvanceAndIterate() throws ExecutionException, InterruptedException
    {
        final int batchSize = 1 << 24;
        final int batchCount = 20;
        final AtomicInteger errors = new AtomicInteger(0);
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            int rc = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
            final CountDownLatch latch = new CountDownLatch(rc * 2);
            for (int i = 0 ; i < rc ; i++)
            {
                final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
                queue.append(0);
                final AtomicInteger min = new AtomicInteger();
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (int i = 1 ; i < batchSize ; i++)
                        {
                            queue.append(i);
                            if (i >= 64)
                            {
                                queue.advance();
                                min.set(i - 63);
                            }
                        }
                        while (queue.advance() != null);
                        latch.countDown();
                    }
                });
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        while (!queue.isEmpty())
                        {
                            int bound = min.get();
                            for (Integer i : queue)
                            {
                                if (i < bound)
                                {
                                    errors.incrementAndGet();
                                    logger.error("Error: saw a previously advanced item: {} vs {}", i, bound);
                                }
                            }
                        }
                        latch.countDown();
                    }
                });
            }
            latch.await();
            Assert.assertTrue(errors.get() == 0);
            logger.warn("Batch {} of {}", batch + 1, batchCount);
        }
    }

    @Test
    public void testPreciseIteratorRemovals() throws ExecutionException, InterruptedException
    {
        final int batchSize = 1 << 22;
        final int batchCount = 20;
        final AtomicInteger errors = new AtomicInteger(0);
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            int rc = Math.max(1, Runtime.getRuntime().availableProcessors() / 3);
            final CountDownLatch latch = new CountDownLatch(rc * 3);
            final AtomicBoolean check = new AtomicBoolean(true);
            for (int i = 0 ; i < rc ; i++)
            {
                final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
                queue.append(0);
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (int i = 1 ; i < batchSize ; i++)
                            queue.append(i);
                        latch.countDown();
                    }
                });
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        while (check.get())
                            for (Integer i : queue);
                        latch.countDown();
                    }
                });
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            SafeRemoveIterator<Integer> iter = queue.iterator();
                            for (int i = 0; i < batchSize; i++)
                            {
                                while (!iter.hasNext()) {
                                    while (queue.isEmpty());
                                    iter = queue.iterator();
                                }
                                int next = iter.next().intValue();
                                if (next != i)
                                {
                                    errors.incrementAndGet();
                                    logger.error("expected {}, found {} ", i, next);
                                }
                                if (!iter.safeRemove())
                                {
                                    errors.incrementAndGet();
                                    logger.error("Failed to remove");
                                }
                            }
                            latch.countDown();
                            check.set(false);
                        }
                        catch (Exception e)
                        {
                            errors.incrementAndGet();
                            e.printStackTrace();
                        }
                    }
                });
            }
            latch.await();
            Assert.assertTrue(errors.get() == 0);
            logger.warn("Batch {} of {}", batch + 1, batchCount);
        }
    }

    @Test
    public void testConditionalAppendAndPoll() throws ExecutionException, InterruptedException
    {
        final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicLong totalOps = new AtomicLong();
        final int perThreadOps = 1 << 24;
        queue.append(-1);
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            final int offset = i;
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    final int operations = 1 << 24;
                    for (int i = 0 ; i < operations ; i++)
                    {
                        int v = (i * THREAD_COUNT) + offset;
                        while (true)
                        {
                            Snap<Integer> snap = queue.snap();
                            Integer tail = snap.tail();
                            if (queue.appendIfTail(tail, v))
                            {
                                Assert.assertTrue(Iterables.contains(snap.view(), v));
                                while (true)
                                {
                                    Integer peek = queue.peek();
                                    if (peek == tail || peek == v || peek == queue.tail())
                                        break;
                                    queue.advanceIfHead(peek);
                                }
                                break;
                            }
                            else
                            {
                                Assert.assertFalse(Iterables.contains(snap.view(), v));
                            }
                        }
                        long to = totalOps.incrementAndGet();
                        if ((to & ((1 << 20) - 1)) == 0)
                            logger.warn("Completed {}M ops of {}M", to >> 20, (perThreadOps * THREAD_COUNT) >> 20);
                    }
                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }

    @Test
    public void testNonCompetingDrainToWithTooManyThreads() throws InterruptedException
    {
        testNonCompetingDrainTo(true);
    }

    @Test
    public void testNonCompetingDrainToWithDedicatedThreads() throws InterruptedException
    {
        testNonCompetingDrainTo(false);
    }

    public void testNonCompetingDrainTo(boolean manyThreads) throws InterruptedException
    {
        final int batchSize = 1 << 24;
        final int batchCount = 20;
        final AtomicInteger errors = new AtomicInteger(0);
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            int rc = manyThreads ? Runtime.getRuntime().availableProcessors() * 2 : Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
            final CountDownLatch latch = new CountDownLatch(rc * 2);
            for (int i = 0 ; i < rc ; i++)
            {
                final AtomicBoolean done = new AtomicBoolean(false);
                final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        Random rand = ThreadLocalRandom.current();
                        for (int i = 0 ; i < batchSize ; i++)
                        {
                            // don't race too far ahead
                            if ((i & 1023) == 0)
                            {
                                int targetHeadroom = rand.nextInt(100000);
                                while (true)
                                {
                                    Integer head = queue.peek();
                                    if (head == null || i - head >= targetHeadroom || batchSize - head <= targetHeadroom)
                                        break;
                                }
                            }
                            queue.append(i);
                        }
                        done.set(true);
                        latch.countDown();
                    }
                });
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        Random random = ThreadLocalRandom.current();
                        List<Integer> sink = new ArrayList<>();
                        int last = -1;
                        while (!done.get() || !queue.isEmpty())
                        {
                            int fetch = (int) Math.sqrt(random.nextInt(Integer.MAX_VALUE));
                            int fetched = queue.drainTo(sink, fetch);
                            if (fetched > fetch)
                            {
                                errors.incrementAndGet();
                                logger.error("Fetched too many items");
                            }
                            if (fetched != sink.size())
                            {
                                errors.incrementAndGet();
                                logger.error("Fetched different number of items to returned");
                            }
                            if (sink.isEmpty())
                                continue;
                            for (int i : sink)
                            {
                                if (i != last + 1)
                                {
                                    errors.incrementAndGet();
                                    logger.error("Incorrect next item");
                                }
                                last = i;
                            }
                            sink.clear();
                        }
                        if (last != batchSize - 1)
                        {
                            errors.incrementAndGet();
                            logger.error("Incorrect last item (" + last + ")");
                        }
                        latch.countDown();
                    }
                });
            }
            latch.await();
            Assert.assertTrue(errors.get() == 0);
            logger.warn("Batch {} of {}", batch + 1, batchCount);
        }
    }

    @Test
    public void testCompetingDrainToWithTooManyThreads() throws InterruptedException, ExecutionException
    {
        testCompetingDrainTo(true);
    }

    @Test
    public void testCompetingDrainToWithDedicatedThreads() throws InterruptedException, ExecutionException
    {
        testCompetingDrainTo(false);
    }

    public void testCompetingDrainTo(boolean manyThreads) throws InterruptedException, ExecutionException
    {
        final int batchSize = 1 << 24;
        final int batchCount = manyThreads ? 5 : 20;
        final AtomicInteger errors = new AtomicInteger(0);
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            int rc = manyThreads ? Runtime.getRuntime().availableProcessors() * 2 : Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
            final NonBlockingQueue<Integer> queue = new NonBlockingQueue<>();
            final CountDownLatch latch = new CountDownLatch(rc + 1);
            final AtomicBoolean done = new AtomicBoolean(false);
            final List<Future<IBitSet>> seen = new ArrayList<>();
            exec.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    Random rand = ThreadLocalRandom.current();
                    for (int i = 0 ; i < batchSize ; i++)
                    {
                        // don't race too far ahead
                        if ((i & 1023) == 0)
                        {
                            int targetHeadroom = rand.nextInt(1000000);
                            while (true)
                            {
                                Integer head = queue.peek();
                                if (head == null || i - head >= targetHeadroom || batchSize - head <= targetHeadroom)
                                    break;
                            }
                        }
                        queue.append(i);
                    }
                    done.set(true);
                    latch.countDown();
                }
            });
            for (int i = 0 ; i < rc ; i++)
            {
                seen.add(exec.submit(new Callable<IBitSet>()
                {
                    @Override
                    public IBitSet call()
                    {
                        final IBitSet seen = new OpenBitSet(batchSize);
                        Random random = ThreadLocalRandom.current();
                        List<Integer> sink = new ArrayList<>();
                        while (!done.get() || !queue.isEmpty())
                        {
                            int fetch = (int) Math.sqrt(random.nextInt(Integer.MAX_VALUE));
                            int fetched = queue.drainTo(sink, fetch);
                            if (fetched > fetch)
                            {
                                errors.incrementAndGet();
                                logger.error("Fetched too many items");
                            }
                            if (fetched != sink.size())
                            {
                                errors.incrementAndGet();
                                logger.error("Fetched different number of items to returned");
                            }
                            if (sink.isEmpty())
                                continue;
                            for (int i : sink)
                            {
                                if (seen.get(i))
                                {
                                    errors.incrementAndGet();
                                    logger.error("Seen same number repeatedly on same thread");
                                }
                                seen.set(i);
                            }
                            sink.clear();
                        }
                        latch.countDown();
                        return seen;
                    }
                }));
            }
            latch.await();
            IBitSet merge = new OpenBitSet(batchSize);
            for (Future<IBitSet> f : seen)
            {
                IBitSet one = f.get();
                for (int i = 0 ; i < batchSize ; i++)
                {
                    if (one.get(i))
                    {
                        if (merge.get(i))
                        {
                            errors.incrementAndGet();
                            logger.error("Seen same number repeatedly on different threads");
                        }
                        merge.set(i);
                    }
                }
            }
            for (int i = 0 ; i < batchSize ; i++)
            {
                if (!merge.get(i))
                {
                    errors.incrementAndGet();
                    logger.error("Missing number");
                }
            }
            Assert.assertTrue(errors.get() == 0);
            logger.warn("Batch {} of {}", batch + 1, batchCount);
        }
    }

}
