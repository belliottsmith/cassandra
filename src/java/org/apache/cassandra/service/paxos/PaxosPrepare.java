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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.utils.NullableSerializer.*;

/**
 * Perform one paxos "prepare" attempt, with various optimisations.
 *
 * The prepare step entails asking for a quorum of nodes to promise to accept our later proposal. It can
 * yield one of five logical answers:
 *
 *   1) Success         - we have received a quorum of promises, and we know that a quorum of nodes
 *                        witnessed the prior round's commit (if any)
 *   2) Timeout         - we have not received enough responses at all before our deadline passed
 *   3) Failure         - we have received too many explicit failures to succeed
 *   4) Superseded      - we have been informed of a later ballot that has been promised
 *   5) FoundInProgress - we have been informed of an earlier promise that has been accepted
 *
 * Success hinges on two distinct criteria being met, as the quorum of promises may not guarantee a quorum of
 * witnesses of the prior round's commit.  We track this separately by recording those nodes that have witnessed
 * the prior round's commit.  On receiving a quorum of promises, we submit the prior round's commit to any promisers
 * that had not witnessed it, while continuing to wait for responses to our original request: as soon as we hear of
 * a quorum that have witnessed it, either by our refresh request or by responses to the original request, we yield Success.
 *
 * Success is also accompanied by a quorum of read responses, avoiding another round-trip to obtain this result.
 *
 * This operation may be started either with a solo Prepare command, or with a prefixed Commit command.
 * If we are completing an in-progress round we previously discovered, we save another round-trip by committing and
 * preparing simultaneously.
 */
public class PaxosPrepare implements IAsyncCallbackWithFailure<PaxosPrepare.Response>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);
    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();

    /**
     * Represents the current status of a prepare action: it is a status rather than a result,
     * as the result may be unknown without sufficient responses (though in most cases it is final status).
     */
    static class Status
    {
        enum Outcome { SUCCESS, SUPERSEDED, FOUND_IN_PROGRESS, INCOMPLETE_COMMIT, MAYBE_FAILURE}

        final Outcome outcome;
        Status(Outcome outcome)
        {
            this.outcome = outcome;
        }
        UUID supersededBy() { return ((Superseded) this).by; }
        Success success() { return (Success) this; }
        FoundInProgress foundInProgress() { return (FoundInProgress) this; }
        IncompleteCommit incompleteCommit() { return (IncompleteCommit) this; }
        Paxos.MaybeFailure maybeFailure() { return ((MaybeFailure) this).info; }
    }

    static class Success extends Status
    {
        final List<MessageIn<ReadResponse>> responses;

        Success(List<MessageIn<ReadResponse>> responses)
        {
            super(Outcome.SUCCESS);
            this.responses = responses;
        }

        public String toString() { return "Success"; }
    }

    /**
     * The ballot we sought promises for has been superseded by another proposer's
     */
    static class Superseded extends Status
    {
        final UUID by;

        Superseded(UUID by)
        {
            super(Outcome.SUPERSEDED);
            this.by = by;
        }

        public String toString() { return "Superseded(" + by + ')'; }
    }

    /**
     * We have been informed of a promise made by one of the replicas we contacted, that was not accepted by all replicas
     * (though may have been accepted by a majority; we don't know).
     * In this case we cannot readily know if we have prevented this proposal from being completed, so we attempt
     * to finish it ourselves (unfortunately leaving the proposer to timeout, given the current semantics)
     * TODO: we should inform the proposer of our completion of their request
     * TODO: we should consider waiting for more responses in case we encounter any successful commit, or a majority
     *       of acceptors?
     */
    static class FoundInProgress extends Status
    {
        final Commit partiallyAcceptedProposal;

        private FoundInProgress(Commit partiallyAcceptedProposal)
        {
            super(Outcome.FOUND_IN_PROGRESS);
            this.partiallyAcceptedProposal = partiallyAcceptedProposal;
        }

        public String toString() { return "FoundInProgress(" + partiallyAcceptedProposal.ballot + ')'; }
    }

    static class IncompleteCommit extends Status
    {
        final Commit incompleteCommit;
        final Iterable<InetAddress> missingFrom;

        private IncompleteCommit(Commit incompleteCommit, Iterable<InetAddress> missingFrom)
        {
            super(Outcome.INCOMPLETE_COMMIT);
            this.incompleteCommit = incompleteCommit;
            this.missingFrom = missingFrom;
        }
    }

    static class MaybeFailure extends Status
    {
        final Paxos.MaybeFailure info;
        private MaybeFailure(Paxos.MaybeFailure info)
        {
            super(Outcome.MAYBE_FAILURE);
            this.info = info;
        }

        public String toString() { return info.toString(); }
    }

    private UUID supersededBy; // cannot be promised, as a newer promise has been made
    private Commit latestAccepted; // the latest latestAcceptedButNotCommitted response we have received (which may still have been committed elsewhere)
    private Commit latestCommitted; // latest actually committed proposal

    private final int participants;
    private final int required;
    private int promises;
    private int failures;
    private final List<MessageIn<ReadResponse>> readResponses;
    private List<InetAddress> withLatest;
    private List<InetAddress> withoutLatest;

    private boolean isDone;

    private PaxosPrepare(DecoratedKey key, CFMetaData metadata, int participants, int required)
    {
        this.participants = participants;
        this.required = required;
        this.readResponses = new ArrayList<>(required);
        this.withLatest = new ArrayList<>(required);
        this.latestCommitted = this.latestAccepted = Commit.emptyCommit(key, metadata);
    }

    private boolean isSuperseded()
    {
        return supersededBy != null;
    }

    private boolean hasInProgressCommit()
    {
        // no need to commit a no-op; either it
        //   1) reached a majority, in which case it was agreed, had no effect and we can do nothing; or
        //   2) did not reach a majority, was not agreed, and was not user visible as a result
        return latestAccepted.isAfter(latestCommitted) && !latestAccepted.update.isEmpty();
    }

    static Status prepare(long deadline, Commit seekPromise, Paxos.Participants participants, ReadCommand readCommand)
    {
        PaxosPrepare prepare = new PaxosPrepare(seekPromise.update.partitionKey(), seekPromise.update.metadata(), participants.contact.size(), participants.required);

        Request request = new Request(seekPromise, readCommand);
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_PREPARE, request, requestSerializer);

        start(prepare, participants, message);
        return prepare.await(deadline);
    }

    /**
     * Submit the message to our peers, and submit it for local execution if relevant
     */
    private static <R extends Request> void start(PaxosPrepare prepare, Paxos.Participants participants, MessageOut<R> send)
    {
        for (int i = 0, size = participants.contact.size() ; i < size ; ++i)
        {
            InetAddress destination = participants.contact.get(i);
            MessagingService.instance().sendRRWithFailure(send, destination, prepare);
        }
    }

    synchronized Status await(long deadline)
    {
        try
        {
            while (!isDone)
            {
                long wait = deadline - System.nanoTime();
                if (wait <= 0)
                    break;

                wait((wait + 999999) / 1000000);
            }

            return status();
        }
        catch (InterruptedException e)
        {
            // can only normally be interrupted if the system is shutting down; should rethrow as a write failure but propagate the interrupt
            Thread.currentThread().interrupt();
            return new MaybeFailure(new Paxos.MaybeFailure(true, participants, required, 0, 0));
        }
    }

    /**
     * @return the Status as of now, which may be final or may indicate we have not received sufficient responses
     */
    private synchronized Status status()
    {
        if (isSuperseded())
            return new Superseded(supersededBy);

        if (hasInProgressCommit())
            return new FoundInProgress(latestAccepted);

        // we can only return success if we have received sufficient promises AND we know that at least that many
        // nodes have also committed the prior proposal
        if (withLatest.size() >= required)
            return new Success(readResponses);

        if (promises >= required)
            return new IncompleteCommit(latestCommitted, withoutLatest);

        return new MaybeFailure(new Paxos.MaybeFailure(participants, required, withLatest.size(), 0));
    }

    @Override
    public void response(MessageIn<Response> message)
    {
        response(message.payload, message.from);
    }

    private synchronized void response(Response response, InetAddress from)
    {
        logger.trace("Prepare response {} from {}", response, from);

        if (isDone)
            return;

        if (isSuperseded())
            return; // cannot succeed; nothing to do but start from scratch

        if (!response.isPromised)
        {
            Rejected rejected = response.rejected();
            supersededBy = rejected.supersededBy;
            signalDone();
            return;
        }

        Promised promised = response.promised();
        if (promised.latestCommitted.hasSameBallot(latestCommitted))
        {
            withLatest.add(from);
        }
        else if (promised.latestCommitted.isAfter(latestCommitted))
        {
            // move with->withoutMostRecent
            if (!withLatest.isEmpty())
            {
                if (withoutLatest == null)
                {
                    withoutLatest = withLatest;
                    withLatest = new ArrayList<>(Math.min(participants - withoutLatest.size(), required));
                }
                else
                {
                    List<InetAddress> tmp = withoutLatest;
                    withoutLatest = withLatest;
                    withLatest = tmp;
                    withLatest.clear();
                }
            }

            withLatest.add(from);
            latestCommitted = promised.latestCommitted;
        }
        else
        {
            if (withoutLatest == null)
                withoutLatest = new ArrayList<>(participants - withLatest.size());
            withoutLatest.add(from);
        }

        if (isAfter(promised.latestAcceptedButNotCommitted, latestAccepted))
            latestAccepted = promised.latestAcceptedButNotCommitted;

        addReadResponse(promised.readResponse, from);

        if (++promises >= required)
            signalDone();
    }

    @Override
    public synchronized void onFailure(InetAddress from)
    {
        logger.debug("Received paxos prepare failure response from {}", from);
        if (++failures + required == participants && !isDone)
            signalDone();
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }

    private synchronized void signalDone()
    {
        isDone = true;
        notify();
    }

    /**
     * Save a read response from a node that we know to have witnessed the most recent commit
     *
     * Must be invoked while owning lock
     */
    private void addReadResponse(ReadResponse response, InetAddress from)
    {
        if (readResponses.size() < required)
            readResponses.add(MessageIn.create(from, response, Collections.emptyMap(), REQUEST_RESPONSE, current_version));
    }

    private static class Request
    {
        final Commit seekPromise;
        final ReadCommand read;

        public Request(Commit seekPromise, ReadCommand read)
        {
            this.seekPromise = seekPromise;
            this.read = read;
        }
    }

    static class Response
    {
        final boolean isPromised;

        Response(boolean isPromised)
        {
            this.isPromised = isPromised;
        }
        Promised promised() { return (Promised) this; }
        Rejected rejected() { return (Rejected) this; }
    }

    static class Promised extends Response
    {
        // a proposal that has been accepted but not committed, i.e. must be null or > latestCommit
        @Nullable final Commit latestAcceptedButNotCommitted;
        final Commit latestCommitted;
        final ReadResponse readResponse;

        Promised(@Nullable Commit latestAcceptedButNotCommitted, Commit latestCommitted, ReadResponse readResponse)
        {
            super(true);

            this.latestAcceptedButNotCommitted = latestAcceptedButNotCommitted;
            this.latestCommitted = latestCommitted;
            this.readResponse = readResponse;
        }


        @Override
        public String toString()
        {
            return "Promised(" + latestAcceptedButNotCommitted + ',' + latestCommitted + ')';
        }
    }

    static class Rejected extends Response
    {
        final UUID supersededBy;

        Rejected(UUID supersededBy)
        {
            super(false);
            this.supersededBy = supersededBy;
        }

        @Override
        public String toString()
        {
            return "Rejected(" + supersededBy + ')';
        }
    }

    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(MessageIn<Request> message, int id)
        {
            Response response = execute(message.payload, message.from);
            if (response == null)
                Paxos.sendFailureResponse("prepare", message.from, message.payload.seekPromise.ballot, id);
            else
                MessagingService.instance().sendReply(new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer), id, message.from);
        }

        static Response execute(Request request, InetAddress from)
        {
            if (!Paxos.isInRangeAndShouldProcess(from, request.seekPromise.update.partitionKey(), request.seekPromise.update.metadata()))
                return null;

            PaxosState result = PaxosState.promiseIfNewer(request.seekPromise);

            if (request.seekPromise == result.promised)
            {
                ReadResponse readResponse;
                try (ReadOrderGroup readGroup = request.read.startOrderGroup();
                     UnfilteredPartitionIterator iterator = request.read.executeLocally(readGroup))
                {
                    readResponse = request.read.createResponse(iterator);
                }

                Commit acceptedButNotCommitted = result.accepted;
                Commit committed = result.committed;
                if (!isAfter(acceptedButNotCommitted, committed))
                    acceptedButNotCommitted = null;

                return new Promised(acceptedButNotCommitted, result.committed, readResponse);
            }
            else
            {
                assert result.promised != null;
                return new Rejected(result.promised.ballot);
            }
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            Commit.serializer.serialize(request.seekPromise, out, version);
            ReadCommand.serializer.serialize(request.read, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            Commit commit = Commit.serializer.deserialize(in, version);
            ReadCommand readCommand = ReadCommand.serializer.deserialize(in, version);
            return new Request(commit, readCommand);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return Commit.serializer.serializedSize(request.seekPromise, version)
                    + ReadCommand.serializer.serializedSize(request.read, version);
        }
    }

    public static class ResponseSerializer implements IVersionedSerializer<Response>
    {
        public void serialize(Response response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.isPromised);
            if (response.isPromised)
            {
                Promised promised = (Promised) response;
                serializeNullable(Commit.serializer, promised.latestAcceptedButNotCommitted, out, version);
                Commit.serializer.serialize(promised.latestCommitted, out, version);
                ReadResponse.serializer.serialize(promised.readResponse, out, version);
            }
            else
            {
                Rejected rejected = (Rejected) response;
                UUIDSerializer.serializer.serialize(rejected.supersededBy, out, version);
            }
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isPromised = in.readBoolean();
            if (isPromised)
            {
                Commit acceptedNotCommitted = deserializeNullable(Commit.serializer, in, version);
                Commit committed = Commit.serializer.deserialize(in, version);
                ReadResponse readResponse = ReadResponse.serializer.deserialize(in, version);
                return new Promised(acceptedNotCommitted, committed, readResponse);
            }
            else
            {
                UUID supersededBy = UUIDSerializer.serializer.deserialize(in, version);
                return new Rejected(supersededBy);
            }
        }

        public long serializedSize(Response response, int version)
        {
            if (response.isPromised)
            {
                Promised promised = (Promised) response;
                return TypeSizes.sizeof(true)
                        + serializedSizeNullable(Commit.serializer, promised.latestAcceptedButNotCommitted, version)
                        + Commit.serializer.serializedSize(promised.latestCommitted, version)
                        + ReadResponse.serializer.serializedSize(promised.readResponse, version);
            }
            else
            {
                Rejected rejected = (Rejected) response;
                return TypeSizes.sizeof(false)
                        + UUIDSerializer.serializer.serializedSize(rejected.supersededBy, version);
            }
        }
    }

}
