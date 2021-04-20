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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.*;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

public class PaxosCleanupSession extends AbstractFuture<UUID> implements Runnable
{
    private static final Map<UUID, PaxosCleanupSession> sessions = new ConcurrentHashMap<>();

    private final UUID session = UUID.randomUUID();
    private final UUID cfId;
    private final Collection<Range<Token>> ranges;
    private final UUID before;
    private final Queue<InetAddress> pendingCleanups = new ConcurrentLinkedQueue<>();
    private InetAddress inProgress = null;

    public PaxosCleanupSession(Collection<InetAddress> endpoints, UUID cfId, Collection<Range<Token>> ranges, UUID before)
    {
        this.cfId = cfId;
        this.ranges = ranges;
        this.before = before;

        pendingCleanups.addAll(endpoints);
    }

    private static void setSession(PaxosCleanupSession session)
    {
        Preconditions.checkState(!sessions.containsKey(session.session));
        sessions.put(session.session, session);
    }

    private static void removeSession(PaxosCleanupSession session)
    {
        Preconditions.checkState(sessions.containsKey(session.session));
        sessions.remove(session.session);
    }

    public void run()
    {
        setSession(this);
        startNextOrFinish();
    }

    private void startCleanup(InetAddress endpoint)
    {
        PaxosCleanupRequest completer = new PaxosCleanupRequest(session, cfId, ranges, before);
        MessageOut<PaxosCleanupRequest> msg = new MessageOut<>(MessagingService.Verb.APPLE_PAXOS_CLEANUP_REQUEST, completer, PaxosCleanupRequest.serializer);
        MessagingService.instance().sendOneWay(msg, endpoint);
    }

    private synchronized void startNextOrFinish()
    {
        InetAddress endpoint = pendingCleanups.poll();

        if (endpoint == null)
            Preconditions.checkState(inProgress == null, "Unable to complete paxos cleanup session %s, still waiting on %s", session, inProgress);
        else
            Preconditions.checkState(inProgress == null, "Unable to start paxos cleanup on %s for %s, still waiting on response from %s", endpoint, session, inProgress);

        inProgress = endpoint;

        if (endpoint != null)
        {
            startCleanup(endpoint);
        }
        else
        {
            removeSession(this);
            set(before);
        }
    }

    private synchronized void finish(InetAddress from, PaxosCleanupResponse finished)
    {
        Preconditions.checkArgument(from.equals(inProgress), "Received unexpected cleanup complete response from %s for session %s. Expected %s", from, session, inProgress);
        inProgress = null;

        if (finished.wasSuccessful)
        {
            startNextOrFinish();
        }
        else
        {
            removeSession(this);
            setException(new RuntimeException(String.format("Paxos cleanup failed on %s with message: %s", from, finished.message)));
        }
    }

    public static void finishSession(InetAddress from, PaxosCleanupResponse response)
    {
        PaxosCleanupSession session = sessions.get(response.session);
        if (session != null)
            session.finish(from, response);
    }
}
