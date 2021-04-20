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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
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
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.verbStages;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.service.paxos.Paxos.newBallot;
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
public class PaxosPrepare implements IAsyncCallbackWithFailure<PaxosPrepare.Response>, PaxosPrepareRefresh.Callbacks
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosPrepare.class);

    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();

    /**
     * Represents the current status of a prepare action: it is a status rather than a result,
     * as the result may be unknown without sufficient responses (though in most cases it is final status).
     */
    static class Status
    {
        enum Outcome { SUCCESS, SUPERSEDED, FOUND_IN_PROGRESS, MAYBE_FAILURE}

        final Outcome outcome;
        Status(Outcome outcome)
        {
            this.outcome = outcome;
        }
        UUID supersededBy() { return ((Superseded) this).by; }
        Success success() { return (Success) this; }
        FoundInProgress foundInProgress() { return (FoundInProgress) this; }
        Paxos.MaybeFailure maybeFailure() { return ((MaybeFailure) this).info; }
    }

    static class Success extends Status
    {
        final UUID ballot;
        final List<MessageIn<ReadResponse>> responses;

        Success(UUID ballot, List<MessageIn<ReadResponse>> responses)
        {
            super(Outcome.SUCCESS);
            this.ballot = ballot;
            this.responses = responses;
        }

        public String toString() { return "Success(" + ballot + ')'; }
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
        final UUID promisedBallot;
        final Commit partiallyAcceptedProposal;

        private FoundInProgress(UUID promisedBallot, Commit partiallyAcceptedProposal)
        {
            super(Outcome.FOUND_IN_PROGRESS);
            this.promisedBallot = promisedBallot;
            this.partiallyAcceptedProposal = partiallyAcceptedProposal;
        }

        public String toString() { return "FoundInProgress(" + partiallyAcceptedProposal.ballot + ')'; }
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

    private final UUID ballot;
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
    private final Consumer<Status> onDone;

    private PaxosPrepareRefresh refreshStaleParticipants;

    PaxosPrepare(DecoratedKey partitionKey, CFMetaData metadata, UUID ballot, int participants, int required, Consumer<Status> onDone)
    {
        assert required > 0;
        this.ballot = ballot;
        this.participants = participants;
        this.required = required;
        this.readResponses = new ArrayList<>(required);
        this.withLatest = new ArrayList<>(required);
        this.latestCommitted = this.latestAccepted = Commit.emptyCommit(partitionKey, metadata);
        this.onDone = onDone;
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
        if (latestAccepted.update.isEmpty())
            return false;

        return latestAccepted.isAfter(latestCommitted);
    }

    static PaxosPrepare prepare(Paxos.Participants participants, UUID minimumBallot, SinglePartitionReadCommand readCommand)
    {
        return prepareWithBallot(participants, newBallot(minimumBallot), readCommand);
    }

    static PaxosPrepare prepareWithBallot(Paxos.Participants participants, UUID ballot, SinglePartitionReadCommand readCommand)
    {
        Tracing.trace("Preparing with read {}", ballot);
        return prepareWithBallotInternal(participants, new Request(ballot, readCommand), null);
    }

    static <T extends Consumer<Status>> T prepareWithBallot(Paxos.Participants participants, UUID ballot, DecoratedKey partitionKey, CFMetaData metadata, T onDone)
    {
        Tracing.trace("Preparing {}", ballot);
        prepareWithBallotInternal(participants, new Request(ballot, partitionKey, metadata), onDone);
        return onDone;
    }

    private static PaxosPrepare prepareWithBallotInternal(Paxos.Participants participants, Request request, Consumer<Status> onDone)
    {
        PaxosPrepare prepare = new PaxosPrepare(request.partitionKey, request.metadata, request.ballot, participants.contact.size(), participants.requiredForConsensus, onDone);
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_PREPARE, request, requestSerializer);

        start("prepare", prepare, participants, message, RequestHandler::execute);
        return prepare;
    }

    /**
     * Submit the message to our peers, and submit it for local execution if relevant
     */
    static <R extends Request> void start(String action, PaxosPrepare prepare, Paxos.Participants participants, MessageOut<R> send, BiFunction<R, InetAddress, Response> selfHandler)
    {
        boolean executeOnSelf = false;
        for (int i = 0, size = participants.contact.size() ; i < size ; ++i)
        {
            InetAddress destination = participants.contact.get(i);
            if (StorageProxy.canDoLocalRequest(destination))
                executeOnSelf = true;
            else
                MessagingService.instance().sendRRWithFailure(send, destination, prepare);
        }

        if (executeOnSelf)
            StageManager.getStage(verbStages.get(send.verb)).execute(() -> prepare.executeOnSelf(action, send.payload, selfHandler));
    }

    synchronized Status awaitUntil(long deadline)
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
            return new FoundInProgress(ballot, latestAccepted);

        // we can only return success if we have received sufficient promises AND we know that at least that many
        // nodes have also committed the prior proposal
        if (withLatest.size() >= required)
            return new Success(ballot, readResponses);

        return new MaybeFailure(new Paxos.MaybeFailure(participants, required, withLatest.size(), failures));
    }

    private <R extends Request> void executeOnSelf(String action, R request, BiFunction<R, InetAddress, Response> execute)
    {
        try
        {
            Response response = execute.apply(request, FBUtilities.getBroadcastAddress());
            if (response == null)
                return;

            response(response, FBUtilities.getBroadcastAddress());
        }
        catch (Exception ex)
        {
            if (!(ex instanceof WriteTimeoutException))
                logger.error("Failed to apply paxos {} locally", action, ex);
            onFailure(FBUtilities.getBroadcastAddress());
        }
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
        {
            if (withLatest.size() < required)
                refreshStaleParticipants();
            else
                signalDone();
        }
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
        if (onDone != null)
            onDone.accept(status());
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

    /**
     * See {@link PaxosPrepareRefresh}
     *
     * Must be invoked while owning lock
     */
    private void refreshStaleParticipants()
    {
        if (refreshStaleParticipants == null)
            refreshStaleParticipants = new PaxosPrepareRefresh(ballot, latestCommitted, this);

        refreshStaleParticipants.refresh(withoutLatest);
        withoutLatest.clear();
    }

    public void onRefreshFailure(InetAddress from)
    {
        onFailure(from);
    }

    public synchronized void onRefreshSuccess(UUID isSupersededBy, InetAddress from)
    {
        if (isDone)
            return;

        if (isSupersededBy != null)
        {
            supersededBy = isSupersededBy;
            signalDone();
        }
        else
        {
            withLatest.add(from);
            if (withLatest.size() >= required)
                signalDone();
        }
    }

    static class Request
    {
        final UUID ballot;
        final SinglePartitionReadCommand read;
        final DecoratedKey partitionKey;
        final CFMetaData metadata;

        public Request(UUID ballot, SinglePartitionReadCommand read)
        {
            this.ballot = ballot;
            this.read = read;
            this.partitionKey = read.partitionKey();
            this.metadata = read.metadata();
        }

        public Request(UUID ballot, DecoratedKey partitionKey, CFMetaData metadata)
        {
            this.ballot = ballot;
            this.partitionKey = partitionKey;
            this.metadata = metadata;
            this.read = null;
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
        @Nullable final ReadResponse readResponse;

        Promised(@Nullable Commit latestAcceptedButNotCommitted, Commit latestCommitted, @Nullable ReadResponse readResponse)
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
                Paxos.sendFailureResponse("prepare", message.from, message.payload.ballot, id);
            else
                MessagingService.instance().sendReply(new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer), id, message.from);
        }

        static Response execute(Request request, InetAddress from)
        {
            if (!Paxos.isInRangeAndShouldProcess(from, request.partitionKey, request.metadata))
                return null;

            PaxosState result = PaxosState.promiseIfNewer(request.partitionKey, request.metadata, request.ballot);

            if (request.ballot == result.promised)
            {
                ReadResponse readResponse = null;
                if (request.read != null)
                {
                    try (ReadOrderGroup readGroup = request.read.startOrderGroup();
                         UnfilteredPartitionIterator iterator = request.read.executeLocally(readGroup))
                    {
                        readResponse = request.read.createResponse(iterator);
                    }
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
                return new Rejected(result.promised);
            }
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.ballot, out, version);
            out.writeBoolean(request.read != null);
            if (request.read != null)
            {
                ReadCommand.serializer.serialize(request.read, out, version);
            }
            else
            {
                CFMetaData.serializer.serialize(request.metadata, out, version);
                DecoratedKey.serializer.serialize(request.partitionKey, out, version);
            }
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            boolean hasRead = in.readBoolean();
            if (hasRead)
            {
                SinglePartitionReadCommand readCommand = (SinglePartitionReadCommand) ReadCommand.serializer.deserialize(in, version);
                return new Request(ballot, readCommand);
            }
            else
            {
                CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
                DecoratedKey partitionKey = (DecoratedKey) DecoratedKey.serializer.deserialize(in, metadata.partitioner, version);
                return new Request(ballot, partitionKey, metadata);
            }
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.ballot, version)
                    + (request.read != null
                        ? ReadCommand.serializer.serializedSize(request.read, version)
                        : CFMetaData.serializer.serializedSize(request.metadata, version)
                            + DecoratedKey.serializer.serializedSize(request.partitionKey, version));
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
                serializeNullable(ReadResponse.serializer, promised.readResponse, out, version);
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
                ReadResponse readResponse = deserializeNullable(ReadResponse.serializer, in, version);
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
                        + serializedSizeNullable(ReadResponse.serializer, promised.readResponse, version);
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
