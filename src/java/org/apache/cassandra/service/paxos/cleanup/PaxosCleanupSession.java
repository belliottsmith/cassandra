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

import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.*;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class PaxosCleanupSession extends AbstractFuture<Void> implements Runnable,
                                                                         IEndpointStateChangeSubscriber,
                                                                         IFailureDetectionEventListener,
                                                                         IAsyncCallbackWithFailure<Void>
{
    private static final Map<UUID, PaxosCleanupSession> sessions = new ConcurrentHashMap<>();

    private static final long TIMEOUT_MILLIS;
    static
    {
        long timeoutSeconds = Integer.getInteger("cassandra.paxos_cleanup_session_timeout_seconds", (int) TimeUnit.HOURS.toSeconds(2));
        TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(timeoutSeconds);
    }

    private static class TimeoutTask implements Runnable
    {
        private final WeakReference<PaxosCleanupSession> ref;

        public TimeoutTask(PaxosCleanupSession session)
        {
            this.ref = new WeakReference<>(session);
        }

        @Override
        public void run()
        {
            PaxosCleanupSession session = ref.get();
            if (session == null || session.isDone())
                return;

            session.fail(String.format("Paxos cleanup session %s timed out", session.session));
        }

        private static void schedule(PaxosCleanupSession session, long delayMillis)
        {
            ScheduledExecutors.scheduledTasks.schedule(new TimeoutTask(session), delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private final UUID session = UUID.randomUUID();
    private final UUID cfId;
    private final Collection<Range<Token>> ranges;
    private final Queue<InetAddress> pendingCleanups = new ConcurrentLinkedQueue<>();
    private InetAddress inProgress = null;

    PaxosCleanupSession(Collection<InetAddress> endpoints, UUID cfId, Collection<Range<Token>> ranges)
    {
        this.cfId = cfId;
        this.ranges = ranges;

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

    @Override
    public void run()
    {
        setSession(this);
        TimeoutTask.schedule(this, TIMEOUT_MILLIS);
        startNextOrFinish();
    }

    private void startCleanup(InetAddress endpoint)
    {
        PaxosCleanupRequest completer = new PaxosCleanupRequest(session, cfId, ranges);
        MessageOut<PaxosCleanupRequest> msg = new MessageOut<>(MessagingService.Verb.APPLE_PAXOS_CLEANUP_REQUEST, completer, PaxosCleanupRequest.serializer);
        MessagingService.instance().sendRRWithFailure(msg, endpoint, this);
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
            set(null);
        }
    }

    private synchronized void fail(String message)
    {
        if (isDone())
            return;
        removeSession(this);
        setException(new PaxosCleanupException(message));
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
            fail(String.format("Paxos cleanup session %s failed on %s with message: %s", session, from, finished.message));
        }
    }

    public static void finishSession(InetAddress from, PaxosCleanupResponse response)
    {
        PaxosCleanupSession session = sessions.get(response.session);
        if (session != null)
            session.finish(from, response);
    }

    private synchronized void maybeKillSession(InetAddress unavailable, String reason)
    {
        // don't fail if we've already completed the cleanup for the unavailable endpoint,
        // if it's something that affects availability, the ongoing sessions will fail themselves
        if (!pendingCleanups.contains(unavailable))
            return;

        fail(String.format("Paxos cleanup session %s failed after %s %s", session, unavailable, reason));
    }

    @Override
    public void onJoin(InetAddress endpoint, EndpointState epState)
    {

    }

    @Override
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {

    }

    @Override
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {

    }

    @Override
    public void onAlive(InetAddress endpoint, EndpointState state)
    {

    }

    @Override
    public void onDead(InetAddress endpoint, EndpointState state)
    {
        maybeKillSession(endpoint, "marked dead");
    }

    @Override
    public void onRemove(InetAddress endpoint)
    {
        maybeKillSession(endpoint, "removed from ring");
    }

    @Override
    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        maybeKillSession(endpoint, "restarted");
    }

    @Override
    public void convict(InetAddress ep, double phi)
    {
        maybeKillSession(ep, "convicted by failure detector");
    }

    @Override
    public void onFailure(InetAddress from)
    {
        fail(from.toString() + " did not acknowledge the cleanup request for paxos cleanup session  " + session);
    }

    @Override
    public void response(MessageIn<Void> msg)
    {
        // noop, we're only interested in failures
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
