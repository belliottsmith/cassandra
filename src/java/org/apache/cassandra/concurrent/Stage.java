/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.concurrent;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ExecutorUtils;

import static java.util.stream.Collectors.toMap;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

public enum Stage
{
    READ              ("ReadStage",             "request",  DatabaseDescriptor::getConcurrentReaders,        DatabaseDescriptor::setConcurrentReaders,        Stage::multiThreadedLowSignalStage),
    MUTATION          ("MutationStage",         "request",  DatabaseDescriptor::getConcurrentWriters,        DatabaseDescriptor::setConcurrentWriters,        Stage::multiThreadedLowSignalStage),
    COUNTER_MUTATION  ("CounterMutationStage",  "request",  DatabaseDescriptor::getConcurrentCounterWriters, DatabaseDescriptor::setConcurrentCounterWriters, Stage::multiThreadedLowSignalStage),
    VIEW_MUTATION     ("ViewMutationStage",     "request",  DatabaseDescriptor::getConcurrentViewWriters,    DatabaseDescriptor::setConcurrentViewWriters,    Stage::multiThreadedLowSignalStage),
    GOSSIP            ("GossipStage",           "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    REQUEST_RESPONSE  ("RequestResponseStage",  "request",  () -> getAvailableProcessors(),                  null,                                            Stage::multiThreadedLowSignalStage),
    ANTI_ENTROPY      ("AntiEntropyStage",      "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    MIGRATION         ("MigrationStage",        "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    MISC              ("MiscStage",             "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    TRACING           ("TracingStage",          "internal", () -> 1,                                         null,                                            Stage::tracingExecutor),
    INTERNAL_RESPONSE ("InternalResponseStage", "internal", () -> getAvailableProcessors(),                  null,                                            Stage::multiThreadedStage),
    IMMEDIATE         ("ImmediateStage",        "internal", () -> 0,                                         null,                                            Stage::immediateExecutor);


    public static final long KEEP_ALIVE_SECONDS = 60; // seconds to keep "extra" threads alive for when idle
    public final String jmxName;
    Supplier<LocalAwareExecutorService> initialiser;
    private LocalAwareExecutorService executor = null;

    Stage(String jmxName, String jmxType, Supplier<Integer> numThreads, LocalAwareExecutorService.MaximumPoolSizeListener setNumThreads, ExecutorServiceInitialiser initialiser)
    {
        this.jmxName = jmxName;
        this.initialiser = () -> initialiser.init(jmxName, jmxType, numThreads.get(), setNumThreads);
    }

    private static String normalizeName(String stageName)
    {
        // Handle discrepancy between JMX names and actual pool names
        String upperStageName = stageName.toUpperCase();
        if (upperStageName.endsWith("STAGE"))
        {
            upperStageName = upperStageName.substring(0, stageName.length() - 5);
        }
        return upperStageName;
    }

    private static Map<String,Stage> nameMap = Arrays.stream(values())
                                                     .collect(toMap(s -> Stage.normalizeName(s.jmxName),
                                                                    s -> s));

    public static Stage fromPoolName(String stageName)
    {
        String upperStageName = normalizeName(stageName);

        Stage result = nameMap.get(upperStageName);
        if (result != null)
            return result;

        try
        {
            return valueOf(upperStageName);
        }
        catch (IllegalArgumentException e)
        {
            switch(upperStageName) // Handle discrepancy between configuration file and stage names
            {
                case "CONCURRENT_READS":
                    return READ;
                case "CONCURRENT_WRITERS":
                    return MUTATION;
                case "CONCURRENT_COUNTER_WRITES":
                    return COUNTER_MUTATION;
                case "CONCURRENT_MATERIALIZED_VIEW_WRITES":
                    return VIEW_MUTATION;
                default:
                    throw new IllegalStateException("Must be one of " + Arrays.stream(values())
                                                                              .map(Enum::toString)
                                                                              .collect(Collectors.joining(",")));
            }
        }
    }

    public LocalAwareExecutorService getExecutor()
    {
        if (executor == null)
        {
            synchronized (this)
            {
                if (executor == null)
                {
                    executor = initialiser.get();
                }
            }
        }
        return executor;
    }
    private static List<ExecutorService> executors()
    {
        return Stream.of(Stage.values())
                     .map(stage -> stage.getExecutor())
                     .collect(Collectors.toList());
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow()
    {
        ExecutorUtils.shutdownNow(executors());
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        List<ExecutorService> executors = executors();
        ExecutorUtils.shutdownNow(executors);
        ExecutorUtils.awaitTermination(timeout, units, executors);
    }

    static LocalAwareExecutorService tracingExecutor(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener setNumThreads)
    {
        RejectedExecutionHandler reh = (r, executor) -> MessagingService.instance().metrics.recordSelfDroppedMessage(Verb._TRACE);
        return new TracingExecutor(1,
                                   1,
                                   KEEP_ALIVE_SECONDS,
                                   TimeUnit.SECONDS,
                                   new ArrayBlockingQueue<>(1000),
                                   new NamedThreadFactory(jmxName),
                                   reh);
    }

    static LocalAwareExecutorService multiThreadedStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener setNumThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEP_ALIVE_SECONDS,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<>(),
                                                new NamedThreadFactory(jmxName),
                                                jmxType);
    }

    static LocalAwareExecutorService multiThreadedLowSignalStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener setNumThreads)
    {
        return SharedExecutorPool.SHARED.newExecutor(numThreads, setNumThreads, Integer.MAX_VALUE, jmxType, jmxName);
    }

    static LocalAwareExecutorService singleThreadedStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener setNumThreads)
    {
        return new JMXEnabledSingleThreadExecutor(jmxName, jmxType);
    }

    static LocalAwareExecutorService immediateExecutor(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener setNumThreads)
    {
        return ImmediateExecutor.INSTANCE;
    }

    @FunctionalInterface
    public interface ExecutorServiceInitialiser
    {
        public LocalAwareExecutorService init(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener setNumThreads);
    }

    /**
     * Returns core thread pool size
     */
    public int getCorePoolSize()
    {
        return getExecutor().getCorePoolSize();
    }

    /**
     * Allows user to resize core thread pool size
     */
    public void setCorePoolSize(int newCorePoolSize)
    {
        getExecutor().setCorePoolSize(newCorePoolSize);
    }

    /**
     * Returns maximum pool size of thread pool.
     */
    public int getMaximumPoolSize()
    {
        return getExecutor().getMaximumPoolSize();
    }

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setMaximumPoolSize(int newMaxWorkers)
    {
        getExecutor().setMaximumPoolSize(newMaxWorkers);
    }

    /**
     * The executor used for tracing.
     */
    private static class TracingExecutor extends ThreadPoolExecutor implements LocalAwareExecutorService
    {
        TracingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler)
        {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        }

        public void execute(Runnable command, ExecutorLocals locals)
        {
            assert locals == null;
            super.execute(command);
        }

        public void maybeExecuteImmediately(Runnable command)
        {
            execute(command);
        }

        @Override
        public int getActiveTaskCount()
        {
            return getActiveCount();
        }

        @Override
        public int getPendingTaskCount()
        {
            return getQueue().size();
        }
    }
}
