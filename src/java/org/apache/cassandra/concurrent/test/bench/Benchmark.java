package org.apache.cassandra.concurrent.test.bench;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Benchmark
{

    static final double CLOCK_RATE = 2.2d;
    static final int REPEATS = 5;
    static final int WARMUPS = 1;
    // number of threads in/out (i.e. total thread count is double)
    static final int[] THREADS = new int[]{1, 2, 4, 8};
    static final Test[] TEST = new Test[]{Test.BAQ};
    static final int TEST_RUNTIME_SECONDS = 10;
    static final int OP_GROUPING = 100;
    static final int INSPECT_INTERVAL_MILLIS = 100;
    static final int BUFFER_SIZE = 1 << 14;
    static final int MAX_OP_DELTA = (int) (BUFFER_SIZE * 1.1d);
    static final boolean REUSE = true;
    static final boolean INTERLEAVE_TESTS = false;
    static final boolean PRINT_INSPECTIONS = false;
    static final boolean PRINT_PER_TEST_RESULTS = true;

    public static void main(String[] args) throws InterruptedException
    {
        bench();
    }

    private static void printSettings()
    {
        System.out.println();
        System.out.println();
        System.out.println("REPEATS:             " + Benchmark.REPEATS);
        System.out.println("INTERLEAVE REPEATS:  " + Benchmark.INTERLEAVE_TESTS);
        System.out.println("TEST TARGET TIME:    " + Benchmark.TEST_RUNTIME_SECONDS + "s");
        System.out.println("INSPECTION INTERVAL: " + Benchmark.INSPECT_INTERVAL_MILLIS + "ms");
        System.out.println("BUFFER SIZE:         " + Benchmark.BUFFER_SIZE);
        System.out.println("MAX BUFFER USE:      " + String.format("%.2fM items", Benchmark.MAX_OP_DELTA / 1000000d));
        System.out.println("REUSE BUFFER:        " + Benchmark.REUSE);
        System.out.println("OP GROUPING:         " + Benchmark.OP_GROUPING);
        System.out.println("CLOCK RATE:          " + Benchmark.CLOCK_RATE);
        System.out.println();
    }

    public static Measurements bench() throws InterruptedException
    {
        printSettings();
        if (WARMUPS > 0)
        {
            int[] remaining = new int[TEST.length];
            Arrays.fill(remaining, WARMUPS);
            System.out.println("Warming up...");
            Measurements discard = new Measurements();
            for (int i = 0; i < WARMUPS; i++)
                for (Test test : TEST)
                    runOne(test, 2, discard, i, remaining, WARMUPS, true);
        }
        System.out.println();
        System.out.println("Running tests...");
        final Measurements measurements = new Measurements();
        int[] remaining = new int[TEST.length];
        int totalTests = REPEATS * THREADS.length;
        Arrays.fill(remaining, totalTests);
        if (INTERLEAVE_TESTS)
            for (int i = 0; i < REPEATS; i++)
                for (int threadCount : THREADS)
                    for (Test test : TEST)
                        runOne(test, threadCount, measurements, i, remaining, totalTests, false);
        else
            for (Test test : TEST)
            {
                for (int threadCount : THREADS)
                {
                    Test.clearCache();
                    for (int i = 0; i < REPEATS; i++)
                        runOne(test, threadCount, measurements, i, remaining, totalTests, false);
                }
                // print progress
                System.out.println();
                System.out.println();
                measurements.print(true);
                System.out.println();
                System.out.println();
            }

        measurements.print(false);
        return measurements;
    }

    static void runOne(Test test, int threadCount, Measurements measurements, int round, int[] testsRemaining, int totalTests, boolean warmup) throws InterruptedException
    {
        int eta = calcTimeRemainingSeconds(measurements, testsRemaining, totalTests);
        final String formatstr = "ETA %ds. %s %s (Round %d) with %d threads";
        final String runType = warmup ? "Warming up" : "Benchmarking";
        System.out.println(String.format(formatstr, eta, runType, test, round + 1, threadCount * 2));
        new OneBench(test, threadCount, measurements).run();
        testsRemaining[Arrays.asList(TEST).indexOf(test)]--;
    }

    static int calcTimeRemainingSeconds(Measurements measurements, int[] testsRemaining, int totalTests)
    {
        long etaMillis = 0;
        for (int i = 0 ; i < TEST.length ; i++)
        {
            int remaining = testsRemaining[i];
            int complete = totalTests - remaining;
            if (complete == 0)
                etaMillis += TEST_RUNTIME_SECONDS * 1000 * remaining;
            else if (remaining != 0)
            {
                final Test test = TEST[i];
                long elapsedMillis = 0;
                for (int thread : THREADS)
                    elapsedMillis += measurements.get(test).get(thread).totalElapsedMillis(MeasurementIntervalType.TOTAL);
                etaMillis += elapsedMillis * (remaining / (double) complete);
            }
        }
        return (int) (etaMillis / 1000);
    }


    public static long cpuTime()
    {
        return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
    }

    static final class OneBench
    {

        final int threadCount;
        final Test test;
        final IntervalTypeMeasurements measurements;

        public OneBench(Test test, int threadCount, Measurements measurements)
        {
            this.threadCount = threadCount;
            this.test = test;
            this.measurements = measurements.get(test).get(threadCount);
        }

        void run() throws InterruptedException
        {

            // gc to try to ensure level playing field
            System.gc();

            final ExecutorService exec = test.build(threadCount);

            final OpDeltaLimiter limit = new OpDeltaLimiter(100000000);
            // setup producers / consumers
            final AtomicLong puts = new AtomicLong(0);
            final AtomicLong gets = new AtomicLong(0);
            final Runnable runadd = new Runnable()
            {
                @Override
                public void run()
                {
                    gets.addAndGet(OP_GROUPING);
                    limit.finish(OP_GROUPING);
                }
            };
            final AtomicBoolean done = new AtomicBoolean(false);
            final Thread[] threads = new Thread[this.threadCount];
            for (int i = 0; i < threads.length; i++)
                threads[i] = new Thread(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        while (!done.get())
                        {
                            try
                            {
                                limit.authorize(OP_GROUPING);
                                for (int i = 1; i < OP_GROUPING; i++)
                                    exec.execute(NOOP);
                                exec.execute(runadd);
                                puts.addAndGet(OP_GROUPING);
                            } catch (RejectedExecutionException e)
                            {
                                e.printStackTrace();
                            } catch (InterruptedException e)
                            {
                                e.printStackTrace();
                                throw new IllegalStateException();
                            }
                        }
                    }
                });

            // init book keeping
            int laps = (TEST_RUNTIME_SECONDS * 1000) / INSPECT_INTERVAL_MILLIS;
            long lastputopc = 0, lastgetopc = 0;
            long startCpu = cpuTime(), lastCpu = startCpu;
            long startTime = System.currentTimeMillis(), lastTime = startTime;

            // init
            for (int i = 0; i < threads.length; i++)
                threads[i].start();

            // poll progress
            for (int i = 0; i < laps; i++)
            {
                Thread.sleep(INSPECT_INTERVAL_MILLIS);
                long putopc = puts.get(), getopc = gets.get(), nowCpu = cpuTime(), nowTime = System.currentTimeMillis();
                Measurement m = new Measurement(
                        putopc - lastputopc,
                        getopc - lastgetopc,
                        nowCpu - lastCpu,
                        nowTime - lastTime);
                measurements.add(MeasurementIntervalType.INTERVAL, m);
                if (PRINT_INSPECTIONS)
                    m.print(test, MeasurementIntervalType.INTERVAL);
                lastputopc = putopc;
                lastgetopc = getopc;
                lastCpu = nowCpu;
                lastTime = nowTime;
            }

            // tidy up
            done.set(true);
            for (int i = 0; i < threads.length; i++)
                while (threads[i].isAlive())
                    threads[i].join(100);
            long elapsedMillisDonePuts = (System.currentTimeMillis() - startTime);
            long elapsedCpuDonePuts = cpuTime() - startCpu;
            Measurement donePuts = new Measurement(
                    puts.get(), gets.get(),
                    elapsedCpuDonePuts, elapsedMillisDonePuts);
            measurements.add(MeasurementIntervalType.DONE_PUTS, donePuts);

            exec.shutdown();
            exec.awaitTermination(7, TimeUnit.DAYS);
            // perform gc to include any gc costs
            System.gc();

            // log total progress
            long elapsedMillisTotal = (System.currentTimeMillis() - startTime);
            long elapsedCpuTotal = cpuTime() - startCpu;
            Measurement total = new Measurement(
                    puts.get(), gets.get(),
                    elapsedCpuTotal, elapsedMillisTotal);
            measurements.add(MeasurementIntervalType.TOTAL, total);
            if (PRINT_INSPECTIONS | PRINT_PER_TEST_RESULTS)
            {
                total.print(test, MeasurementIntervalType.TOTAL);
                if (total.opsIn != total.opsOut)
                    System.out.println(String.format("%s: opsIn mismatches opsOut: %d vs %d", test, puts.get(), gets.get()));
            }

        }

    }

    private static Runnable NOOP = new Runnable()
    {
        @Override
        public void run()
        {
        }
    };

    {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException(Thread t, Throwable e)
            {
                e.printStackTrace();
            }
        });
    }

}
