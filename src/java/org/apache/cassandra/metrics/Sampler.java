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
package org.apache.cassandra.metrics;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.utils.Clock;

import com.google.common.annotations.VisibleForTesting;

public abstract class Sampler<T>
{
    public enum SamplerType
    {
        READS, WRITES, LOCAL_READ_TIME, WRITE_SIZE, CAS_CONTENTIONS
    }

    @VisibleForTesting
    Clock clock = Clock.instance;

    @VisibleForTesting
    static final ThreadPoolExecutor samplerExecutor = new JMXEnabledThreadPoolExecutor(1, 1,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(1000),
            new NamedThreadFactory("Sampler"),
            "internal");

    private long endTimeMillis = -1;

    static
    {
        samplerExecutor.setRejectedExecutionHandler((runnable, executor) ->
        {
            MessagingService.instance().incrementDroppedMessages(Verb._SAMPLE);
        });
    }

    public void addSample(final T item, final int value)
    {
        if (isEnabled())
            samplerExecutor.submit(() -> insert(item, value));
    }

    protected abstract void insert(T item, long value);

    /**
     * @return true if the sampler is enabled.
     * A sampler is enabled between `beginSampling` and `finishSampling`.
     */
    public boolean isEnabled()
    {
        return endTimeMillis != -1;
    }

    // Disable the sampler
    public void disable()
    {
        endTimeMillis = -1;
    }

    /**
     * @return true if the sampler is active.
     * A sampler is active only if it is enabled and the current time is within the `durationMillis` when beginning sampling.
     */
    public boolean isActive()
    {
        return isEnabled() && clock.currentTimeMillis() <= endTimeMillis;
    }

    /**
     * Update the end time for the sampler. Implicitly, calling this method enables the sampler
     */
    public void updateEndTime(long endTimeMillis)
    {
        this.endTimeMillis = endTimeMillis;
    }

    /**
     * Begin sampling with the configured capacity and duration
     * @param capacity Number of sample items to keep in memory, the lower this is
     *                 the less accurate results are. For best results use value
     *                 close to cardinality, but understand the memory trade offs.
     * @param durationMillis Upperbound duration in milliseconds for sampling. The sampler
     *                       stops accepting new samples after exceeding the duration
     *                       even if {@link #finishSampling(int)}} is not called.
     */
    public abstract void beginSampling(int capacity, int durationMillis);

    /**
     * Stops samplings and return results
     * @param count Number of samples requested to retrive from the sampler
     * @return a list of samples, the size is the minimum of the total samples or {@param count}.
     */
    public abstract List<Sample<T>> finishSampling(int count);

    public abstract String toString(T value);

    /**
     * Represents the ranked items collected during a sample period
     */
    public static class Sample<S> implements Serializable
    {
        public final S value;
        public final long count;
        public final long error;

        public Sample(S value, long count, long error)
        {
            this.value = value;
            this.count = count;
            this.error = error;
        }

        @Override
        public String toString()
        {
            return "Sample [value=" + value + ", count=" + count + ", error=" + error + "]";
        }
    }
}
