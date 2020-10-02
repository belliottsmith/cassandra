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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_CLEANUP_PREPARE;
import static org.apache.cassandra.service.paxos.Paxos.newBallot;
import static org.apache.cassandra.service.paxos.PaxosState.ballotTracker;

/**
 * Determines the highest ballot we should attempt to repair
 */
public class PaxosPrepareCleanup extends AbstractFuture<UUID> implements IAsyncCallbackWithFailure<UUID>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosPrepareCleanup.class);
    private final Set<InetAddress> waitingResponse;
    private UUID maxBallot = null;

    PaxosPrepareCleanup(Collection<InetAddress> endpoints)
    {
        this.waitingResponse = new HashSet<>(endpoints);
    }

    /**
     * We run paxos repair as part of topology changes, so we include the local endpoint state in the paxos repair
     * prepare message to prevent racing with gossip dissemination and guarantee that every repair participant is aware
     * of the pending ring change during repair.
     */
    public static PaxosPrepareCleanup prepare(Collection<InetAddress> endpoints, EndpointState localEpState)
    {
        PaxosPrepareCleanup callback = new PaxosPrepareCleanup(endpoints);
        MessageOut<EndpointState> message = new MessageOut<>(APPLE_PAXOS_CLEANUP_PREPARE, localEpState, EndpointState.serializer);
        for (InetAddress endpoint : endpoints)
            MessagingService.instance().sendRRWithFailure(message, endpoint, callback);
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

    private static void maybeUpdateTopology(InetAddress endpoint, EndpointState remote)
    {
        EndpointState local = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (local == null || local.isSupersededBy(remote))
        {
            logger.trace("updating endpoint info for {} with {}", endpoint, remote);
            Map<InetAddress, EndpointState> states = Collections.singletonMap(endpoint, remote);

            Gossiper.runInGossipStageBlocking(() -> {
                Gossiper.instance.notifyFailureDetector(states);
                Gossiper.instance.applyStateLocally(states);
            });
            // TODO: We should also wait for schema pulls/pushes, however this would be quite an involved change to MigrationManager
            //       (which currently drops some migration tasks on the floor).
            //       Note it would be fine for us to fail to complete the migration task and simply treat this response as a failure/timeout.
        }
        // even if we have th latest gossip info, wait until pending range calculations are complete
        PendingRangeCalculatorService.instance.blockUntilFinished();
    }

    public static final IVerbHandler<EndpointState> verbHandler = (message, id) -> {
        maybeUpdateTopology(message.from, message.payload);
        UUID highBound = newBallot(ballotTracker().getHighBound(), ConsistencyLevel.SERIAL);
        MessageOut<UUID> msg = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, highBound, UUIDSerializer.serializer);
        MessagingService.instance().sendReply(msg, id, message.from);
    };
}
