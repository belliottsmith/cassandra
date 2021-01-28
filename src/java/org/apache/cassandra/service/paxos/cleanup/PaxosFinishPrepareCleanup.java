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

import com.google.common.util.concurrent.AbstractFuture;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.VoidSerializer;

public class PaxosFinishPrepareCleanup extends AbstractFuture<Void> implements IAsyncCallbackWithFailure<Void>
{
    private final Set<InetAddress> waitingResponse;

    PaxosFinishPrepareCleanup(Collection<InetAddress> endpoints)
    {
        this.waitingResponse = new HashSet<>(endpoints);
    }

    public static PaxosFinishPrepareCleanup finish(Collection<InetAddress> endpoints, PaxosCleanupHistory result)
    {
        PaxosFinishPrepareCleanup callback = new PaxosFinishPrepareCleanup(endpoints);
        MessageOut<PaxosCleanupHistory> message = new MessageOut<>(MessagingService.Verb.APPLE_PAXOS_CLEANUP_FINISH_PREPARE, result, PaxosCleanupHistory.serializer);
        for (InetAddress endpoint : endpoints)
            MessagingService.instance().sendRRWithFailure(message, endpoint, callback);
        return callback;
    }

    public void onFailure(InetAddress from)
    {
        setException(new PaxosCleanupException("Timed out waiting on response from " + from));
    }

    public synchronized void response(MessageIn<Void> msg)
    {
        if (isDone())
            return;

        if (!waitingResponse.remove(msg.from))
            throw new IllegalArgumentException("Received unexpected response from " + msg.from);

        if (waitingResponse.isEmpty())
            set(null);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public static final IVerbHandler<PaxosCleanupHistory> verbHandler = (message, id) -> {
        PaxosState.ballotTracker().updateLowBound(message.payload.highBound);
        Schema.instance.getColumnFamilyStoreInstance(message.payload.cfId).syncPaxosRepairHistory(message.payload.history);
        MessageOut<Void> msg = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, null, VoidSerializer.serializer);
        MessagingService.instance().sendReply(msg, id, message.from);
    };
}
