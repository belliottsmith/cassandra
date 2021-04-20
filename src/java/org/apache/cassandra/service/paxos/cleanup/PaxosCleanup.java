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
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.*;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

public class PaxosCleanup extends AbstractFuture<Void> implements Runnable
{
    private final Collection<InetAddress> endpoints;
    private final UUID cfId;
    private final Collection<Range<Token>> ranges;
    private final Executor executor;

    // references kept for debugging
    private PaxosPrepareCleanup prepare;
    private PaxosCleanupSession session;
    private PaxosFinishCleanup finish;

    public PaxosCleanup(Collection<InetAddress> endpoints, UUID cfId, Collection<Range<Token>> ranges, Executor executor)
    {
        this.endpoints = endpoints;
        this.cfId = cfId;
        this.ranges = ranges;
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

    public static PaxosCleanup cleanup(Collection<InetAddress> endpoints, UUID cfId, Collection<Range<Token>> ranges, Executor executor)
    {
        PaxosCleanup cleanup = new PaxosCleanup(endpoints, cfId, ranges, executor);
        executor.execute(cleanup);
        return cleanup;
    }

    public void run()
    {
        EndpointState localEpState = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
        prepare = PaxosPrepareCleanup.prepare(endpoints, localEpState);
        addCallback(prepare, this::startSession);
    }

    private void startSession(UUID highBallot)
    {
        session = new PaxosCleanupSession(endpoints, cfId, ranges, highBallot);
        addCallback(session, this::finish);
        executor.execute(session);
    }

    private void finish(UUID highBallot)
    {
        finish = PaxosFinishCleanup.finish(endpoints, highBallot);
        addCallback(finish, this::set);
    }
}
