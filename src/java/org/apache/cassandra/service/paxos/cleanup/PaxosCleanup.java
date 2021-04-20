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

package org.apache.cassandra.service.paxos.cleanup;

import java.net.InetAddress;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.*;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.DatabaseDescriptor.getCasContentionTimeout;
import static org.apache.cassandra.config.DatabaseDescriptor.getWriteRpcTimeout;

public class PaxosCleanup extends AbstractFuture<Void> implements Runnable
{
    private final Collection<InetAddress> endpoints;
    private final UUID cfId;
    private final Collection<Range<Token>> ranges;
    private final boolean skippedReplicas;
    private final Executor executor;

    // references kept for debugging
    private PaxosStartPrepareCleanup startPrepare;
    private PaxosFinishPrepareCleanup finishPrepare;
    private PaxosCleanupSession session;
    private PaxosCleanupComplete complete;

    public PaxosCleanup(Collection<InetAddress> endpoints, UUID cfId, Collection<Range<Token>> ranges, boolean skippedReplicas, Executor executor)
    {
        this.endpoints = endpoints;
        this.cfId = cfId;
        this.ranges = ranges;
        this.skippedReplicas = skippedReplicas;
        this.executor = executor;
    }

    private <T> void addCallback(ListenableFuture<T> future, Consumer<T> onComplete)
    {
        Futures.addCallback(future, new FutureCallback<T>()
        {
            public void onSuccess(@Nullable T v)
            {
                onComplete.accept(v);
            }

            public void onFailure(Throwable throwable)
            {
                setException(throwable);
            }
        });
    }

    public static PaxosCleanup cleanup(Collection<InetAddress> endpoints, UUID cfId, Collection<Range<Token>> ranges, boolean skippedReplicas, Executor executor)
    {
        PaxosCleanup cleanup = new PaxosCleanup(endpoints, cfId, ranges, skippedReplicas, executor);
        executor.execute(cleanup);
        return cleanup;
    }

    public void run()
    {
        EndpointState localEpState = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
        startPrepare = PaxosStartPrepareCleanup.prepare(cfId, endpoints, localEpState, ranges);
        addCallback(startPrepare, this::finishPrepare);
    }

    private void finishPrepare(PaxosCleanupHistory result)
    {
        ScheduledExecutors.nonPeriodicTasks.schedule(() -> {
            finishPrepare = PaxosFinishPrepareCleanup.finish(endpoints, result);
            addCallback(finishPrepare, (v) -> startSession(result.highBound));
        }, Math.min(getCasContentionTimeout(), getWriteRpcTimeout()), TimeUnit.MILLISECONDS);
    }

    private void startSession(UUID lowBound)
    {
        session = new PaxosCleanupSession(endpoints, cfId, ranges);
        addCallback(session, (v) -> finish(lowBound));
        executor.execute(session);
    }

    private void finish(UUID lowBound)
    {
        complete = new PaxosCleanupComplete(endpoints, cfId, ranges, lowBound, skippedReplicas);
        addCallback(complete, this::set);
        executor.execute(complete);
    }
}
