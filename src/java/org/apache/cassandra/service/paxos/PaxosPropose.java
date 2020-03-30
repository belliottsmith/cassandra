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

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PROPOSE;
import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.service.paxos.PaxosPropose.Superseded.SideEffects.NO;
import static org.apache.cassandra.service.paxos.PaxosPropose.Superseded.SideEffects.MAYBE;
import static org.apache.cassandra.net.MessagingService.verbStages;

/**
 * In waitForNoSideEffect mode, we will not return failure to the caller until
 * we have received a complete set of refusal responses, or at least one accept,
 * indicating (respectively) that we have had no side effect, or that we cannot
 * know if we our proposal produced a side effect.
 */
public class PaxosPropose implements IAsyncCallbackWithFailure<PaxosPropose.Response>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosPropose.class);

    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();

    /**
     * Represents the current status of a propose action: it is a status rather than a result,
     * as the result may be unknown without sufficient responses (though in most cases it is final status).
     */
    static class Status
    {
        enum Outcome { SUCCESS, SUPERSEDED, MAYBE_FAILURE }
        final Outcome outcome;

        Status(Outcome outcome)
        {
            this.outcome = outcome;
        }
        Superseded superseded() { return (Superseded) this; }
        Paxos.MaybeFailure maybeFailure() { return ((MaybeFailure) this).info; }
        public String toString() { return "Success"; }
    }

    static class Superseded extends Status
    {
        enum SideEffects { NO, MAYBE }
        final UUID by;
        final SideEffects hadSideEffects;
        Superseded(UUID by, SideEffects hadSideEffects)
        {
            super(Outcome.SUPERSEDED);
            this.by = by;
            this.hadSideEffects = hadSideEffects;
        }

        public String toString() { return "Superseded(" + by + ',' + hadSideEffects + ')'; }
    }

    private static class MaybeFailure extends Status
    {
        final Paxos.MaybeFailure info;
        MaybeFailure(Paxos.MaybeFailure info)
        {
            super(Outcome.MAYBE_FAILURE);
            this.info = info;
        }

        public String toString() { return info.toString(); }
    }

    private static final Status success = new Status(Status.Outcome.SUCCESS);

    private static final AtomicLongFieldUpdater<PaxosPropose> responsesUpdater = AtomicLongFieldUpdater.newUpdater(PaxosPropose.class, "responses");
    private static final AtomicReferenceFieldUpdater<PaxosPropose, UUID> supersededByUpdater = AtomicReferenceFieldUpdater.newUpdater(PaxosPropose.class, UUID.class, "supersededBy");

    private static final long ACCEPT_INCREMENT = 1;
    private static final int  REFUSAL_SHIFT = 21;
    private static final long REFUSAL_INCREMENT = 1L << REFUSAL_SHIFT;
    private static final int  TIMEOUT_SHIFT = 42;
    private static final long TIMEOUT_INCREMENT = 1L << TIMEOUT_SHIFT;
    private static final long MASK = (1L << REFUSAL_SHIFT) - 1L;

    /** Wait until we know if we may have had side effects */
    private final boolean waitForNoSideEffect;
    /** Number of contacted nodes */
    private final int participants;
    /** Number of accepts required */
    private final int required;
    /** Invoke on reaching a terminal status */
    private final Runnable onDone;

    /**
     * bit 0-20:  accepts
     * bit 21-41: refusals/errors
     * bit 42-62: timeouts
     * bit 63:    ambiguous signal bit (i.e. those states that cannot be certain to signal uniquely flip this bit to take signal responsibility)
     *
     * {@link #accepts}
     * {@link #refusals}
     * {@link #timeouts}
     * {@link #failures} (timeouts+refusals/errors)
     */
    private volatile long responses;

    /** The newest superseding ballot from a refusal; only returned to the caller if we fail to reach a quorum */
    private volatile UUID supersededBy;

    private PaxosPropose(int participants, int required, boolean waitForNoSideEffect, Runnable onDone)
    {
        this.waitForNoSideEffect = waitForNoSideEffect;
        this.participants = participants;
        this.required = required;
        this.onDone = onDone;
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result.
     * @param waitForNoSideEffect if true, on failure we will wait until we can say with certainty there are no side effects
     *                            or until we know we will never be able to determine this with certainty
     */
    static Status sync(long deadline, Commit proposal, Paxos.Participants participants, boolean waitForNoSideEffect)
    {
        SimpleCondition done = new SimpleCondition();
        PaxosPropose propose = new PaxosPropose(participants.contact.size(), participants.required, waitForNoSideEffect, done::signalAll);
        propose.start(participants, proposal);

        try
        {
            done.awaitUntil(deadline);
        }
        catch (InterruptedException e)
        {
            return new MaybeFailure(new Paxos.MaybeFailure(true, participants.contact.size(), participants.required, 0, 0));
        }

        return propose.status();
    }

    private void start(Paxos.Participants participants, Commit proposal)
    {
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_PROPOSE, new Request(proposal), requestSerializer);
        boolean executeOnSelf = false;
        for (int i = 0, size = participants.contact.size() ; i < size ; ++i)
        {
            InetAddress destination = participants.contact.get(i);
            if (StorageProxy.canDoLocalRequest(destination)) executeOnSelf = true;
            else MessagingService.instance().sendRRWithFailure(message, destination, this);
        }

        if (executeOnSelf)
            StageManager.getStage(verbStages.get(APPLE_PAXOS_PROPOSE)).execute(() -> executeOnSelf(proposal));
    }

    /**
     * @return the result as of now; unless the result is definitive, it is only a snapshot of the present incomplete status
     */
    Status status()
    {
        long responses = this.responses;

        if (isSuccessful(responses))
            return success;

        if (cannotSucceed(responses) && supersededBy != null)
        {
            Superseded.SideEffects sideEffects = isFullyRefused(responses) ? NO : MAYBE;
            return new Superseded(supersededBy, sideEffects);
        }

        return new MaybeFailure(new Paxos.MaybeFailure(participants, required, accepts(responses), failures(responses)));
    }

    @Override
    public void response(MessageIn<Response> msg)
    {
        response(msg.payload, msg.from);
    }

    private void executeOnSelf(Commit proposal)
    {
        try
        {
            Response response = RequestHandler.execute(proposal, FBUtilities.getBroadcastAddress());
            if (response == null)
                return;

            response(response, FBUtilities.getBroadcastAddress());
        }
        catch (Exception ex)
        {
            if (!(ex instanceof WriteTimeoutException))
                logger.error("Failed to apply paxos proposal locally", ex);
            onFailure(FBUtilities.getBroadcastAddress());
        }
    }

    public void response(Response response, InetAddress from)
    {
        logger.trace("Propose response {} from {}", response, from);

        UUID supersededBy = response.supersededBy;
        if (supersededBy != null)
            supersededByUpdater.accumulateAndGet(this, supersededBy, (a, b) -> a == null ? b : b.timestamp() > a.timestamp() ? b : a);

        long increment = supersededBy == null
                ? ACCEPT_INCREMENT
                : REFUSAL_INCREMENT;

        update(increment);
    }

    @Override
    public void onFailure(InetAddress from)
    {
        logger.debug("Received paxos propose failure response from {}", from);
        update(REFUSAL_INCREMENT);
    }

    @Override
    public void onExpired(InetAddress from)
    {
        update(TIMEOUT_INCREMENT);
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }

    private void update(long increment)
    {
        long responses = responsesUpdater.addAndGet(this, increment);
        if (shouldSignal(responses))
            signalDone();
    }

    private boolean shouldSignal(long responses)
    {
        if (responses <= 0L) // already signalled via ambiguous signal bit
            return false;

        if (accepts(responses) == required) // first success state
            return true;

        if (participants - failures(responses) >= required) // not yet failed, not yet succeeded
            return false;

        if (!waitForNoSideEffect)
            return participants - failures(responses) == required - 1; // failed, and no need to wait to know if maybe had side effects

        if (refusals(responses) == participants) // we had no side effects
            return true;

        if (accepts(responses) >= 1) // we know we have had side effects; ambiguous state so flip signalling bit
            return responsesUpdater.getAndUpdate(this, x -> x | Long.MIN_VALUE) > 0L;

        // we haven't yet knowingly produced any side effects, but also do not know we haven't
        return false;
    }

    private void signalDone()
    {
        onDone.run();
    }

    private boolean isSuccessful(long responses)
    {
        return accepts(responses) >= required;
    }

    private boolean cannotSucceed(long responses)
    {
        return participants - failures(responses) < required;
    }

    // Note: this is only reliable if !failFast
    private boolean isFullyRefused(long responses)
    {
        return refusals(responses) == participants;
    }

    /** {@link #responses} */
    private static int accepts(long responses)
    {
        return (int) (responses & MASK);
    }

    /** {@link #responses} */
    private static int failures(long responses)
    {
        return timeouts(responses) + refusals(responses);
    }

    /** {@link #responses} */
    private static int refusals(long responses)
    {
        return (int) ((responses >>> REFUSAL_SHIFT) & MASK);
    }

    /** {@link #responses} */
    private static int timeouts(long responses)
    {
        return (int) ((responses >>> TIMEOUT_SHIFT) & MASK);
    }

    /**
     * A Proposal to submit to another node
     */
    static class Request
    {
        final Commit proposal;
        Request(Commit proposal)
        {
            this.proposal = proposal;
        }
    }

    /**
     * The response to a proposal, indicating success (if {@code supersededBy == null},
     * or failure, alongside the ballot that beat us
     */
    static class Response
    {
        final UUID supersededBy;
        Response(UUID supersededBy)
        {
            this.supersededBy = supersededBy;
        }
        public String toString() { return supersededBy == null ? "Accept" : "Reject(" + supersededBy + ')'; }
    }

    /**
     * The proposal request handler, i.e. receives a proposal from a peer and responds with either acccept/reject
     */
    public static class RequestHandler implements IVerbHandler<PaxosPropose.Request>
    {
        @Override
        public void doVerb(MessageIn<PaxosPropose.Request> message, int id)
        {
            Response response = execute(message.payload.proposal, message.from);
            if (response == null)
                Paxos.sendFailureResponse("propose", message.from, message.payload.proposal.ballot, id);
            else
                MessagingService.instance().sendReply(new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer), id, message.from);
        }

        public static Response execute(Commit proposal, InetAddress from)
        {
            if (!Paxos.isInRangeAndShouldProcess(from, proposal.update.partitionKey(), proposal.update.metadata()))
                return null;

            return new Response(PaxosState.acceptIfLatest(proposal));
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            Commit.serializer.serialize(request.proposal, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            Commit propose = Commit.serializer.deserialize(in, version);
            return new Request(propose);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return Commit.serializer.serializedSize(request.proposal, version);
        }
    }

    public static class ResponseSerializer implements IVersionedSerializer<Response>
    {
        public void serialize(Response response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.supersededBy != null);
            if (response.supersededBy != null)
                UUIDSerializer.serializer.serialize(response.supersededBy, out, version);
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isSuperseded = in.readBoolean();
            return isSuperseded ? new Response(UUIDSerializer.serializer.deserialize(in, version)) : new Response(null);
        }

        public long serializedSize(Response response, int version)
        {
            return response.supersededBy != null
                    ? TypeSizes.sizeof(true) + UUIDSerializer.serializer.serializedSize(response.supersededBy, version)
                    : TypeSizes.sizeof(false);
        }
    }
}
