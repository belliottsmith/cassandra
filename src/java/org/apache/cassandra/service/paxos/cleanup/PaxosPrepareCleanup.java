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
import java.util.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.VoidSerializer;

/**
 * Determines the highest ballot we should attempt to repair
 */
public class PaxosPrepareCleanup extends AbstractFuture<UUID> implements IAsyncCallbackWithFailure<UUID>
{
    private final Set<InetAddress> waitingResponse;
    private UUID maxBallot = null;

    PaxosPrepareCleanup(Collection<InetAddress> endpoints)
    {
        this.waitingResponse = new HashSet<>(endpoints);
    }

    public static ListenableFuture<UUID> prepare(Collection<InetAddress> endpoints)
    {
        PaxosPrepareCleanup callback = new PaxosPrepareCleanup(endpoints);
        MessageOut<Void> message = new MessageOut<>(MessagingService.Verb.APPLE_PAXOS_CLEANUP_PREPARE, null, VoidSerializer.serializer);
        for (InetAddress endpoint : endpoints)
            MessagingService.instance().sendRR(message, endpoint, callback);
        return callback;
    }

    public void onFailure(InetAddress from)
    {
        setException(new RuntimeException("Received failure response from " + from));
    }

    public synchronized void response(MessageIn<UUID> msg)
    {
        if (isDone())
            return;

        if (!waitingResponse.remove(msg.from))
            throw new IllegalArgumentException("Received unexpected response from " + msg.from);

        if (Commit.isAfter(msg.payload, maxBallot))
            maxBallot = msg.payload;

        if (waitingResponse.isEmpty())
            set(maxBallot);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public static final IVerbHandler<Void> verbHandler = (message, id) -> {
        MessageOut<UUID> msg = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                Paxos.newBallot(null, ConsistencyLevel.SERIAL),
                                                UUIDSerializer.serializer);
        MessagingService.instance().sendReply(msg, id, message.from);
    };
}
