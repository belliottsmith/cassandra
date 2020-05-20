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

package org.apache.cassandra.service.paxos;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.concurrent.StageManager.getStage;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_COMMIT;
import static org.apache.cassandra.net.MessagingService.verbStages;
import static org.apache.cassandra.service.StorageProxy.canDoLocalRequest;
import static org.apache.cassandra.service.StorageProxy.shouldHint;
import static org.apache.cassandra.service.StorageProxy.submitHint;

// Does not support EACH_QUORUM, as no such thing as EACH_SERIAL
public class PaxosCommit implements IAsyncCallbackWithFailure<WriteResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosCommit.class);

    /**
     * Represents the current status of a commit action: it is a status rather than a result,
     * as the result may be unknown without sufficient responses (though in most cases it is final status).
     */
    static class Status
    {
        private final Paxos.MaybeFailure maybeFailure;

        Status(Paxos.MaybeFailure maybeFailure)
        {
            this.maybeFailure = maybeFailure;
        }

        boolean isSuccess() { return maybeFailure == null; }
        Paxos.MaybeFailure maybeFailure() { return maybeFailure; }

        public String toString() { return maybeFailure == null ? "Success" : maybeFailure.toString(); }
    }

    private static final Status success = new Status(null);

    private static final AtomicLongFieldUpdater<PaxosCommit> responsesUpdater = AtomicLongFieldUpdater.newUpdater(PaxosCommit.class, "responses");

    final Commit proposal;
    final boolean allowHints;
    final ConsistencyLevel consistency;

    private final int participants;
    private final int required;
    private final Consumer<Status> onDone;

    /**
     * packs two 32-bit integers;
     * bit 00-31: accepts
     * bit 32-63: failures/timeouts
     * 
     * {@link #accepts} 
     * {@link #failures}
     */
    private volatile long responses;

    public PaxosCommit(Commit proposal, boolean allowHints, ConsistencyLevel consistency, Paxos.Participants participants, Consumer<Status> onDone)
    {
        this.proposal = proposal;
        this.allowHints = allowHints;
        this.consistency = consistency;
        this.participants = participants.allNatural.size() + participants.allPending.size();
        this.onDone = onDone;
        this.required = participants.requiredFor(consistency, proposal.update.metadata());
        if (required == 0)
            onDone.accept(status());
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result
     */
    static Status sync(long deadline, Commit proposal, Paxos.Participants participants, ConsistencyLevel consistency, @Deprecated boolean allowHints)
    {
        SimpleCondition done = new SimpleCondition();
        PaxosCommit commit = new PaxosCommit(proposal, allowHints, consistency, participants, ignore -> done.signalAll());
        commit.start(participants, false);

        // We do not need to wait if a proposal is empty: so long as we have reached a QUORUM of acceptors, we have serialized
        // the operation with respect to others.  There is no other visible effect on the storage nodes, so there is no user
        // visible impact to failing to persist.  The incomplete round can be completed anytime.
        if (proposal.update.isEmpty())
            return success;

        try
        {
            done.awaitUntil(deadline);
        }
        catch (InterruptedException e)
        {
            return new Status(new Paxos.MaybeFailure(true, commit.participants, commit.required, 0, 0));
        }
        return commit.status();
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result
     */
    static <T extends Consumer<Status>> T async(Commit proposal, Paxos.Participants participants, ConsistencyLevel consistency, @Deprecated boolean allowHints, T onDone)
    {
        PaxosCommit commit = new PaxosCommit(proposal, allowHints, consistency, participants, onDone);
        commit.start(participants, true);
        return onDone;
    }

    /**
     * Send commit messages to peers (or self)
     */
    private void start(Paxos.Participants participants, boolean async)
    {
        boolean executeOnSelf = false;
        MessageOut<Commit> message = new MessageOut<>(PAXOS_COMMIT, proposal, Commit.serializer);
        for (int i = 0, mi = participants.allNatural.size(); i < mi ; ++i)
            executeOnSelf |= isSelfOrSend(message, participants.allNatural.get(i));

        if (!participants.allPending.isEmpty())
        {
            for (InetAddress destination : participants.allPending)
                executeOnSelf |= isSelfOrSend(message, destination);
        }

        if (executeOnSelf)
        {
            LocalAwareExecutorService executor = getStage(verbStages.get(PAXOS_COMMIT));
            if (async) executor.execute(this::executeOnSelf);
            else executor.maybeExecuteImmediately(this::executeOnSelf);
        }
    }

    /**
     * If isLocal return true; otherwise if the destination is alive send our message, and if not mark the callback with failure
     */
    private boolean isSelfOrSend(MessageOut<Commit> message, InetAddress destination)
    {
        if (canDoLocalRequest(destination))
            return true;

        if (FailureDetector.instance.isAlive(destination))
        {
            MessagingService.instance().sendRRWithFailure(message, destination, this);
        }
        else
        {
            onFailure(destination);
        }

        return false;
    }

    /**
     * Record a failure response, and maybe submit a hint to {@code from}
     */
    public void onFailure(InetAddress from)
    {
        response(false, from);

        if (allowHints && shouldHint(from))
            submitHint(proposal.makeMutation(), from, null);
    }

    /**
     * Record a success response
     */
    public void response(MessageIn<WriteResponse> msg)
    {
        response(true, msg.from);
    }

    /**
     * Execute locally and record response
     */
    public void executeOnSelf()
    {
        try
        {
            MessageOut<WriteResponse> response = RequestHandler.execute(proposal, FBUtilities.getBroadcastAddress());
            if (response != null)
                response(true, FBUtilities.getBroadcastAddress());
        }
        catch (Exception ex)
        {
            if (!(ex instanceof WriteTimeoutException))
                logger.error("Failed to apply paxos commit locally", ex);
            onFailure(FBUtilities.getBroadcastAddress());
        }
    }

    /**
     * Record a failure or success response if {@code from} contributes to our consistency.
     * If we have reached a final outcome of the commit, run {@code onDone}.
     */
    private void response(boolean success, InetAddress from)
    {
        if (consistency.isDatacenterLocal() && !ConsistencyLevel.isLocal(from))
            return;

        long responses = responsesUpdater.addAndGet(this, success ? 0x1L : 0x100000000L);
        // next two clauses mutually exclusive to ensure we only invoke onDone once, when either failed or succeeded
        if (accepts(responses) == required) // if we have received _precisely_ the required accepts, we have succeeded
            onDone.accept(status());
        else if (participants - failures(responses) == required - 1) // if we are _unable_ to receive the required accepts, we have failed
            onDone.accept(status());
    }

    /**
     * @return the Status as of now, which may be final or may indicate we have not received sufficient responses
     */
    Status status()
    {
        long responses = this.responses;
        if (isSuccessful(responses))
            return success;

        return new Status(new Paxos.MaybeFailure(participants, required, accepts(responses), failures(responses)));
    }

    private boolean isSuccessful(long responses)
    {
        return accepts(responses) >= required;
    }

    private static int accepts(long responses)
    {
        return (int) (responses & 0xffffffffL);
    }

    private static int failures(long responses)
    {
        return (int) (responses >>> 32);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public static class RequestHandler implements IVerbHandler<Commit>
    {
        @Override
        public void doVerb(MessageIn<Commit> message, int id)
        {
            MessageOut<WriteResponse> response = execute(message.payload, message.from);
            if (response == null)
                Paxos.sendFailureResponse("commit", message.from, message.payload.ballot, id);
            else
                MessagingService.instance().sendReply(response, id, message.from);
        }

        private static MessageOut<WriteResponse> execute(Commit commit, InetAddress from)
        {
            if (!Paxos.isInRangeAndShouldProcess(from, commit.update.partitionKey(), commit.update.metadata()))
                return null;

            PaxosState.commit(commit);
            Tracing.trace("Enqueuing acknowledge to {}", from);
            return WriteResponse.createMessage();
        }
    }

}
