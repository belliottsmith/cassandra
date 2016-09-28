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

package org.apache.cassandra.service.reads;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionSizeResponse;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.utils.concurrent.Condition;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

/**
 * Handles partition size responses from replicas
 *
 * Response data is stored in a vanilla hash map, with thread
 * safety achieved by synchronizing reads and writes to it.
 */
public class PartitionSizeCallback<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E,P>> implements RequestCallback<PartitionSizeResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger( PartitionSizeCallback.class );

    final Condition condition = newOneTimeCondition();
    private final long queryStartNanoTime;
    private final int blockFor;
    // this uses a plain reference, but is initialised before handoff to any other threads; the later updates
    // may not be visible to the threads immediately, but ReplicaPlan only contains final fields, so they will never see an uninitialised object
    final ReplicaPlan.Shared<E, P> replicaPlan;
    private static final AtomicIntegerFieldUpdater<PartitionSizeCallback> failuresUpdater
    = AtomicIntegerFieldUpdater.newUpdater(PartitionSizeCallback.class, "failures");
    private volatile int failures = 0;
    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;
    private final Map<InetAddressAndPort, Long> sizes;

    public PartitionSizeCallback(ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
    {
        this.queryStartNanoTime = queryStartNanoTime;
        this.replicaPlan = replicaPlan;
        this.blockFor = replicaPlan.get().readQuorum();
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        this.sizes = Maps.newHashMapWithExpectedSize(this.blockFor);

        if (logger.isTraceEnabled())
            logger.trace("Blockfor is {}; setting up requests to {}", blockFor, this.replicaPlan);

    }

    protected P replicaPlan()
    {
        return replicaPlan.get();
    }

    public Map<InetAddressAndPort, Long> get() throws ReadTimeoutException
    {
        boolean signaled = await(DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS), NANOSECONDS);

        synchronized (this)
        {
            boolean failed = failures > 0 && blockFor + failures > replicaPlan().contacts().size();
            if (signaled && !failed)
                return ImmutableMap.copyOf(sizes);

            throw failed ? new ReadFailureException(replicaPlan().consistencyLevel(), sizes.size(), blockFor, !sizes.isEmpty(), failureReasonByEndpoint)
                         : new ReadTimeoutException(replicaPlan().consistencyLevel(), sizes.size(), blockFor, !sizes.isEmpty());
        }
    }

    private boolean await(long timePastStart, TimeUnit unit)
    {
        long time = unit.toNanos(timePastStart) - (nanoTime() - queryStartNanoTime);
        try
        {
            return condition.await(time, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }

    public synchronized void handleResponse(InetAddressAndPort from, long size)
    {
        sizes.put(from, size);

        if (sizes.size() >= blockFor)
            condition.signalAll();
    }

    public void onResponse(Message<PartitionSizeResponse> message)
    {
        handleResponse(message.header.from, message.payload.partitionSize);
    }

    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        int n = waitingFor(from)
                ? failuresUpdater.incrementAndGet(this)
                : failures;

        failureReasonByEndpoint.put(from, failureReason);

        if (blockFor + n > replicaPlan().contacts().size())
            condition.signalAll();
    }

    /**
     * @return true if the message counts towards the blockFor threshold
     */
    private boolean waitingFor(InetAddressAndPort from)
    {
        return !replicaPlan().consistencyLevel().isDatacenterLocal() || DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from));
    }
}
