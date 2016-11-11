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

package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * Handles partition size responses from replicas
 *
 * Response data is stored in a vanilla hash map, with thread
 * safety achieved by synchronizing reads and writes to it.
 */
public class PartitionSizeCallback implements IAsyncCallback<PartitionSizeResponse>
{
    private final Map<InetAddress, Long> sizes;
    private final PartitionSizeCommand command;
    private final ConsistencyLevel consistencyLevel;
    private final int blockFor;

    private final long queryStartNanoTime;
    private final SimpleCondition condition;

    public PartitionSizeCallback(PartitionSizeCommand command, ConsistencyLevel consistencyLevel, int blockFor, long queryStartNanoTime)
    {
        sizes = new HashMap<>(blockFor);
        condition = new SimpleCondition();
        this.command = command;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistencyLevel = consistencyLevel;
        this.blockFor = blockFor;
    }

    public Map<InetAddress, Long> get() throws ReadTimeoutException
    {
        boolean signaled = await();

        synchronized (this)
        {
            if (!signaled)
                throw new ReadTimeoutException(consistencyLevel, sizes.size(), blockFor, !sizes.isEmpty());

            return ImmutableMap.copyOf(sizes);
        }
    }

    private boolean await()
    {
        long nanosLeft = TimeUnit.MILLISECONDS.toNanos(command.getTimeout()) - (System.nanoTime() - queryStartNanoTime);
        try
        {
            return condition.await(nanosLeft, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }

    public void response(MessageIn<PartitionSizeResponse> msg)
    {
        handleResponse(msg.from, msg.payload.partitionSize);
    }

    public synchronized void handleResponse(InetAddress from, long size)
    {
        sizes.put(from, size);

        if (sizes.size() >= blockFor)
            condition.signalAll();
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    @VisibleForTesting
    boolean isFinished()
    {
        return condition.isSignaled();
    }
}
