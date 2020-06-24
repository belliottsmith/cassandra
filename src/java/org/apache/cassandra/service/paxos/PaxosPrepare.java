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
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.concurrent.StageManager.getStage;
import static org.apache.cassandra.net.CompactEndpointSerializationHelper.*;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.verbStages;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.service.paxos.Paxos.*;
import static org.apache.cassandra.service.paxos.Paxos.newBallot;
import static org.apache.cassandra.utils.CollectionSerializer.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializer.newHashMap;
import static org.apache.cassandra.utils.CollectionSerializer.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializer.serializedSizeMap;
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
        enum Outcome { SUCCESS, SUPERSEDED, FOUND_IN_PROGRESS, MAYBE_FAILURE, ELECTORATE_MISMATCH }

        final Outcome outcome;
        final Participants participants;

        Status(Outcome outcome, Participants participants)
        {
            this.outcome = outcome;
            this.participants = participants;
        }
        UUID supersededBy() { return ((Superseded) this).by; }
        UUID previousBallot() { return ((ElectorateMismatch) this).ballot; }
        Success success() { return (Success) this; }
        FoundInProgress foundInProgress() { return (FoundInProgress) this; }
        Paxos.MaybeFailure maybeFailure() { return ((MaybeFailure) this).info; }
    }

    static class Success extends Status
    {
        final UUID ballot;
        final List<MessageIn<ReadResponse>> responses;

        Success(UUID ballot, Participants participants, List<MessageIn<ReadResponse>> responses)
        {
            super(Outcome.SUCCESS, participants);
            this.ballot = ballot;
            this.responses = responses;
        }

        public String toString() { return "Success(" + ballot + ", " + participants.electorate + ')'; }
    }

    /**
     * The ballot we sought promises for has been superseded by another proposer's
     */
    static class Superseded extends Status
    {
        final UUID by;

        Superseded(UUID by, Participants participants)
        {
            super(Outcome.SUPERSEDED, participants);
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
        final Participants participants;
        final Commit partiallyAcceptedProposal;

        private FoundInProgress(UUID promisedBallot, Participants participants, Commit partiallyAcceptedProposal)
        {
            super(Outcome.FOUND_IN_PROGRESS, participants);
            this.promisedBallot = promisedBallot;
            this.participants = participants;
            this.partiallyAcceptedProposal = partiallyAcceptedProposal;
        }

        public String toString() { return "FoundInProgress(" + partiallyAcceptedProposal.ballot + ')'; }
    }

    static class MaybeFailure extends Status
    {
        final Paxos.MaybeFailure info;
        private MaybeFailure(Paxos.MaybeFailure info, Participants participants)
        {
            super(Outcome.MAYBE_FAILURE, participants);
            this.info = info;
        }

        public String toString() { return info.toString(); }
    }

    static class ElectorateMismatch extends Status
    {
        final UUID ballot;
        private ElectorateMismatch(Participants participants, UUID ballot)
        {
            super(Outcome.ELECTORATE_MISMATCH, participants);
            this.ballot = ballot;
        }
    }

    private final AbstractRequest<?> request;
    private UUID supersededBy; // cannot be promised, as a newer promise has been made
    private boolean electorateMismatch; // cannot be promised, a participant has a different view of the electorate
    private Commit latestAccepted; // the latest latestAcceptedButNotCommitted response we have received (which may still have been committed elsewhere)
    private Commit latestCommitted; // latest actually committed proposal

    private final Participants participants; // may be modified _only_ until we receive a quorum of promises

    private final List<MessageIn<ReadResponse>> readResponses;
    private boolean haveQuorumOfPromises;
    private List<InetAddress> withLatest; // promised and have latest commit
    private List<InetAddress> needLatest; // promised without having witnessed latest commit, nor yet been refreshed by us
    private List<InetAddress> failures; // failed either on initial request or on refresh

    private boolean isDone;
    private final Consumer<Status> onDone;

    private PaxosPrepareRefresh refreshStaleParticipants;

    PaxosPrepare(Participants participants, AbstractRequest<?> request, Consumer<Status> onDone)
    {
        assert participants.requiredForConsensus > 0;
        this.participants = participants;
        this.request = request;
        this.readResponses = new ArrayList<>(participants.requiredForConsensus);
        this.withLatest = new ArrayList<>(participants.requiredForConsensus);
        this.latestCommitted = this.latestAccepted = Commit.emptyCommit(request.partitionKey, request.metadata);
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
        //   2) did not reach a majority, was not agreed, and was not user visible as a result so we can ignore it
        if (latestAccepted.update.isEmpty())
            return false;

        return latestAccepted.isAfter(latestCommitted);
    }

    static PaxosPrepare prepare(UUID minimumBallot, Participants initialParticipants, SinglePartitionReadCommand readCommand) throws UnavailableException
    {
        return prepareWithBallot(newBallot(minimumBallot), initialParticipants, readCommand);
    }

    static PaxosPrepare prepareWithBallot(UUID ballot, Participants initialParticipants, SinglePartitionReadCommand readCommand)
    {
        Tracing.trace("Preparing {} with read", ballot);
        Request request = new Request(ballot, initialParticipants.consistencyForConsensus, initialParticipants.electorate, readCommand);
        return prepareWithBallotInternal(initialParticipants, request, null);
    }

    static <T extends Consumer<Status>> T prepareWithBallot(Participants initialParticipants, UUID ballot, DecoratedKey partitionKey, CFMetaData metadata, T onDone)
    {
        Tracing.trace("Preparing {}", ballot);
        prepareWithBallotInternal(initialParticipants, new Request(ballot, initialParticipants.consistencyForConsensus, initialParticipants.electorate, partitionKey, metadata), onDone);
        return onDone;
    }

    private static PaxosPrepare prepareWithBallotInternal(Participants participants, Request request, Consumer<Status> onDone)
    {
        PaxosPrepare prepare = new PaxosPrepare(participants, request, onDone);
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_PREPARE, request, requestSerializer);

        start(prepare, participants, message, RequestHandler::execute);
        return prepare;
    }

    /**
     * Submit the message to our peers, and submit it for local execution if relevant
     */
    static <R extends AbstractRequest<R>> void start(PaxosPrepare prepare, Participants participants, MessageOut<R> send, BiFunction<R, InetAddress, Response> selfHandler)
    {
        List<InetAddress> contact = participants.poll;
        boolean executeOnSelf = false;
        for (int i = 0, size = contact.size() ; i < size ; ++i)
        {
            InetAddress destination = contact.get(i);
            boolean isPending = participants.electorate.isPending(destination);
            logger.trace("{} to {}", send.payload, destination);
            if (canExecuteOnSelf(destination))
                executeOnSelf = true;
            else
                MessagingService.instance().sendRRWithFailure(isPending ? withoutRead(send) : send, destination, prepare);
        }

        if (executeOnSelf)
            getStage(verbStages.get(send.verb)).execute(() -> prepare.executeOnSelf(send.payload, selfHandler));
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
            return new MaybeFailure(new Paxos.MaybeFailure(true, participants.poll.size(), participants.requiredForConsensus, 0, 0), participants);
        }
    }

    /**
     * @return the Status as of now, which may be final or may indicate we have not received sufficient responses
     */
    private synchronized Status status()
    {
        if (electorateMismatch)
            return new ElectorateMismatch(participants, request.ballot);

        if (isSuperseded())
            return new Superseded(supersededBy, participants);

        if (haveQuorumOfPromises)
        {
            // We must be certain to have witnessed a quorum of promises before completing any in-progress commit
            // else we may complete a stale proposal that did not reach a quorum (and may do so in preference
            // to a different in progress proposal that did reach a quorum)
            if (hasInProgressCommit())
                return new FoundInProgress(request.ballot, participants, latestAccepted);

            // we can only return success if we have received sufficient promises AND we know that at least that many
            // nodes have also committed the prior proposal
            if (withLatest() >= participants.requiredForConsensus)
                return new Success(request.ballot, participants, readResponses);
        }

        return new MaybeFailure(new Paxos.MaybeFailure(participants, withLatest(), failures()), participants);
    }

    private int withLatest()
    {
        return withLatest.size();
    }

    private int needLatest()
    {
        return needLatest == null ? 0 : needLatest.size();
    }

    private int failures()
    {
        return failures == null ? 0 : failures.size();
    }


    private <R extends AbstractRequest<R>> void executeOnSelf(R request, BiFunction<R, InetAddress, Response> execute)
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
                logger.error("Failed to apply {} locally", request, ex);
            onFailure(FBUtilities.getBroadcastAddress());
        }
    }

    @Override
    public void response(MessageIn<Response> message)
    {
        response(message.payload, message.from);
    }

    private static boolean needsGossipUpdate(Map<InetAddress, EndpointState> gossipInfo)
    {
        if (gossipInfo.isEmpty())
            return false;

        for (Map.Entry<InetAddress, EndpointState> entry : gossipInfo.entrySet())
        {
            EndpointState remote = entry.getValue();
            EndpointState local = Gossiper.instance.getEndpointStateForEndpoint(entry.getKey());
            if (local == null || local.isSupersededBy(remote))
                return true;
        }

        return false;
    }

    private synchronized void response(Response response, InetAddress from)
    {
        Promised promised;
        logger.trace("{} from {}", response, from);

        if (isDone)
            return;

        if (!response.isPromised)
        {
            Rejected rejected = response.rejected();
            supersededBy = rejected.supersededBy;
            signalDone();
            return;
        }

        promised = response.promised();

        if (!needsGossipUpdate(promised.gossipInfo))
            promise(promised, from);
        else
            // otherwise the peer has divergent beliefs about the ring, so update with the information provided by the peer
            Gossiper.runInGossipStageAsync(() -> {
                Gossiper.instance.notifyFailureDetector(promised.gossipInfo);
                Gossiper.instance.applyStateLocally(promised.gossipInfo);

                // TODO: We should also wait for schema pulls/pushes, however this would be quite an involved change to MigrationManager
                //       (which currently drops some migration tasks on the floor).
                //       Note it would be fine for us to fail to complete the migration task and simply treat this response as a failure/timeout.

                // once any pending ranges have been calculated, refresh our Participants list and submit the promise
                PendingRangeCalculatorService.instance.executeWhenFinished(() -> promiseOrTerminateAfterGossipUpdate(promised, from));
            });
    }

    private synchronized void promiseOrTerminateAfterGossipUpdate(Promised promised, InetAddress from)
    {
        // if the electorate has changed, finish so we can retry with the updated view of the ring
        if (!Paxos.Electorate.get(request.metadata, request.partitionKey, request.consistency).equals(participants.electorate))
        {
            electorateMismatch = true;
            signalDone();
            return;
        }

        // otherwise continue as normal
        promise(promised, from);
    }

    private void promise(Promised promised, InetAddress from)
    {
        if (promised.latestCommitted.hasSameBallot(latestCommitted))
        {
            withLatest.add(from);
        }
        else if (!haveQuorumOfPromises && promised.latestCommitted.isAfter(latestCommitted))
        {
            // move with->withoutMostRecent
            if (!withLatest.isEmpty())
            {
                if (needLatest == null)
                {
                    needLatest = withLatest;
                    withLatest = new ArrayList<>(Math.min(participants.poll.size() - needLatest.size(), participants.requiredForConsensus));
                }
                else
                {
                    List<InetAddress> tmp = needLatest;
                    needLatest = withLatest;
                    withLatest = tmp;
                    withLatest.clear();
                }
            }

            withLatest.add(from);
            latestCommitted = promised.latestCommitted;
        }
        else
        {
            if (haveQuorumOfPromises)
            {
                logger.error("{} serializability violation: {} promised by {} with latest {}; also by {} with latest {}",
                        request.consistency, request.ballot, withLatest, latestCommitted, from, promised.latestCommitted);
            }
            // if promised.latestCommitted.isAfter(latestCommitted) we have a consistency violation,
            // as we should not be able to witness a newer committed after receiving a quorum of responses
            if (needLatest == null)
                needLatest = new ArrayList<>(participants.poll.size() - withLatest.size());
            needLatest.add(from);
        }

        if (isAfter(promised.latestAcceptedButNotCommitted, latestAccepted))
            latestAccepted = promised.latestAcceptedButNotCommitted;

        if (promised.readResponse != null && !participants.electorate.isPending(from))
            addReadResponse(promised.readResponse, from);

        haveQuorumOfPromises |= withLatest() + needLatest() >= participants.requiredForConsensus;
        if (haveQuorumOfPromises)
        {
            if (request.read != null && readResponses.size() < participants.requiredReads)
                throw new AssertionError("Insufficient read responses: " + readResponses + "; need " + participants.requiredReads);

            if (withLatest() < participants.requiredForConsensus)
                refreshStaleParticipants();
            else
                signalDone();
        }
    }

    @Override
    public void onFailure(InetAddress from)
    {
        onFailure("Failure", from);
    }

    @Override
    public void onExpired(InetAddress from)
    {
        onFailure("Timeout", from);
    }

    private synchronized void onFailure(String kind, InetAddress from)
    {
        logger.trace("{} {} from {}", request, kind, from);

        if (isDone)
            return;

        if (failures == null)
            failures = new ArrayList<>(participants.poll.size() - withLatest());

        failures.add(from);
        if (failures() + participants.requiredForConsensus == participants.poll.size())
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
        if (readResponses.size() < participants.requiredForConsensus)
            readResponses.add(MessageIn.create(from, response, Collections.emptyMap(), MessagingService.Verb.REQUEST_RESPONSE, current_version));
    }

    /**
     * See {@link PaxosPrepareRefresh}
     *
     * Must be invoked while owning lock
     */
    private void refreshStaleParticipants()
    {
        if (refreshStaleParticipants == null)
            refreshStaleParticipants = new PaxosPrepareRefresh(request.ballot, latestCommitted, this);

        refreshStaleParticipants.refresh(needLatest);
        needLatest.clear();
    }

    public void onRefreshFailure(InetAddress from, boolean isTimeout)
    {
        onFailure(isTimeout ? "Timeout" : "Failure", from);
    }

    public synchronized void onRefreshSuccess(UUID isSupersededBy, InetAddress from)
    {
        logger.trace("Refresh {} from {}", isSupersededBy == null ? "Success" : "SupersededBy(" + isSupersededBy + ')', from);

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
            if (withLatest.size() >= participants.requiredForConsensus)
                signalDone();
        }
    }

    static abstract class AbstractRequest<R extends AbstractRequest<R>>
    {
        final UUID ballot;
        final ConsistencyLevel consistency;
        final Electorate electorate;
        final SinglePartitionReadCommand read;
        final DecoratedKey partitionKey;
        final CFMetaData metadata;

        AbstractRequest(UUID ballot, ConsistencyLevel consistency, Electorate electorate, SinglePartitionReadCommand read)
        {
            this.ballot = ballot;
            this.consistency = consistency;
            this.electorate = electorate;
            this.read = read;
            this.partitionKey = read.partitionKey();
            this.metadata = read.metadata();
        }

        AbstractRequest(UUID ballot, ConsistencyLevel consistency, Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata)
        {
            this.ballot = ballot;
            this.consistency = consistency;
            this.electorate = electorate;
            this.partitionKey = partitionKey;
            this.metadata = metadata;
            this.read = null;
        }

        abstract R withoutRead();

        public String toString()
        {
            return "Prepare(" + ballot + ')';
        }
    }

    static class Request extends AbstractRequest<Request>
    {
        Request(UUID ballot, ConsistencyLevel consistency, Electorate electorate, SinglePartitionReadCommand read)
        {
            super(ballot, consistency, electorate, read);
        }

        Request(UUID ballot, ConsistencyLevel consistency, Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata)
        {
            super(ballot, consistency, electorate, partitionKey, metadata);
        }

        Request withoutRead()
        {
            return read == null ? this : new Request(ballot, consistency, electorate, partitionKey, metadata);
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
        final Map<InetAddress, EndpointState> gossipInfo;

        Promised(@Nullable Commit latestAcceptedButNotCommitted, Commit latestCommitted, @Nullable ReadResponse readResponse, Map<InetAddress, EndpointState> gossipInfo)
        {
            super(true);
            this.latestAcceptedButNotCommitted = latestAcceptedButNotCommitted;
            this.latestCommitted = latestCommitted;
            this.readResponse = readResponse;
            this.gossipInfo = gossipInfo;
        }


        @Override
        public String toString()
        {
            return "Promised(" + latestAcceptedButNotCommitted + ", " + latestCommitted + ", " + gossipInfo + ')';
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
                sendFailureResponse("Prepare", message.from, message.payload.ballot, id);
            else
                MessagingService.instance().sendReply(new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, responseSerializer), id, message.from);
        }

        static Response execute(AbstractRequest<?> request, InetAddress from)
        {
            if (!isInRangeAndShouldProcess(from, request.partitionKey, request.metadata))
                return null;

            PaxosState result = PaxosState.promiseIfNewer(request.partitionKey, request.metadata, request.ballot);

            if (request.ballot == result.promised)
            {
                Electorate localElectorate = Electorate.get(request.metadata, request.partitionKey, request.consistency);

                // verify electorates; if they differ, send back gossip info for superset of two participant sets
                Map<InetAddress, EndpointState> gossipInfo = Paxos.verifyElectorate(request.electorate, localElectorate);
                ReadResponse readResponse = null;
                if (request.read != null && !localElectorate.pending.contains(FBUtilities.getBroadcastAddress()))
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

                return new Promised(acceptedButNotCommitted, result.committed, readResponse, gossipInfo);
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
            out.writeUnsignedVInt(request.consistency.code);
            Electorate.serializer.serialize(request.electorate, out, version);
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
            ConsistencyLevel consistency = ConsistencyLevel.fromCode((int) in.readUnsignedVInt());
            Electorate electorate = Electorate.serializer.deserialize(in, version);
            boolean hasRead = in.readBoolean();
            if (hasRead)
            {
                SinglePartitionReadCommand readCommand = (SinglePartitionReadCommand) ReadCommand.serializer.deserialize(in, version);
                return new Request(ballot, consistency, electorate, readCommand);
            }
            else
            {
                CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
                DecoratedKey partitionKey = (DecoratedKey) DecoratedKey.serializer.deserialize(in, metadata.partitioner, version);
                return new Request(ballot, consistency, electorate, partitionKey, metadata);
            }
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.ballot, version)
                    + VIntCoding.computeUnsignedVIntSize(request.consistency.code)
                    + Electorate.serializer.serializedSize(request.electorate, version)
                    + 1 + (request.read != null
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
                serializeMap(endpointSerializer, EndpointState.nullableSerializer, promised.gossipInfo, out, version);
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
                Map<InetAddress, EndpointState> gossipInfo = deserializeMap(endpointSerializer, EndpointState.nullableSerializer, newHashMap(), in, version);
                return new Promised(acceptedNotCommitted, committed, readResponse, gossipInfo);
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
                        + serializedSizeNullable(ReadResponse.serializer, promised.readResponse, version)
                        + serializedSizeMap(endpointSerializer, EndpointState.nullableSerializer, promised.gossipInfo, version);
            }
            else
            {
                Rejected rejected = (Rejected) response;
                return TypeSizes.sizeof(false)
                        + UUIDSerializer.serializer.serializedSize(rejected.supersededBy, version);
            }
        }
    }

    static <R extends AbstractRequest<R>> MessageOut<R> withoutRead(MessageOut<R> send)
    {
        if (send.payload.read == null)
            return send;

        return send.withPayload(send.payload.withoutRead());
    }

}
