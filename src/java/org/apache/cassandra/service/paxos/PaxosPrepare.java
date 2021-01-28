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
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
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
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.verbStages;
import static org.apache.cassandra.service.paxos.Commit.*;
import static org.apache.cassandra.service.paxos.Paxos.*;
import static org.apache.cassandra.service.paxos.PaxosPrepare.Status.Outcome.*;
import static org.apache.cassandra.service.paxos.PaxosState.*;
import static org.apache.cassandra.service.paxos.PaxosState.MaybePromise.Outcome.*;
import static org.apache.cassandra.utils.CollectionSerializer.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializer.newHashMap;
import static org.apache.cassandra.utils.CollectionSerializer.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializer.serializedSizeMap;
import static org.apache.cassandra.utils.concurrent.WaitManager.Global.waits;

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
public class PaxosPrepare implements IAsyncCallbackWithFailure<PaxosPrepare.Response>, PaxosPrepareRefresh.Callbacks, Paxos.Async<PaxosPrepare.Status>
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
        enum Outcome { READ_PERMITTED, PROMISED, SUPERSEDED, FOUND_INCOMPLETE_ACCEPTED, FOUND_INCOMPLETE_COMMITTED, MAYBE_FAILURE, ELECTORATE_MISMATCH }

        final Outcome outcome;
        final Participants participants;

        Status(Outcome outcome, Participants participants)
        {
            this.outcome = outcome;
            this.participants = participants;
        }
        @Nullable UUID retryWithAtLeast()
        {
            switch (outcome)
            {
                case READ_PERMITTED: return ((Success) this).supersededBy;
                case SUPERSEDED: return ((Superseded) this).by;
                default: return null;
            }
        }
        Success success() { return (Success) this; }
        FoundIncompleteAccepted incompleteAccepted() { return (FoundIncompleteAccepted) this; }
        FoundIncompleteCommitted incompleteCommitted() { return (FoundIncompleteCommitted) this; }
        Paxos.MaybeFailure maybeFailure() { return ((MaybeFailure) this).info; }
    }

    static class Success extends WithRequestedBallot
    {
        final List<MessageIn<ReadResponse>> responses;
        final boolean isReadSafe; // read responses constitute a linearizable read (though short read protection would invalidate that)
        final @Nullable UUID supersededBy; // if known and READ_SUCCESS

        Success(Outcome outcome, UUID ballot, Participants participants, List<MessageIn<ReadResponse>> responses, boolean isReadSafe, @Nullable UUID supersededBy)
        {
            super(outcome, participants, ballot);
            this.responses = responses;
            this.isReadSafe = isReadSafe;
            this.supersededBy = supersededBy;
        }

        static Success read(UUID ballot, Participants participants, List<MessageIn<ReadResponse>> responses, @Nullable UUID supersededBy)
        {
            return new Success(Status.Outcome.READ_PERMITTED, ballot, participants, responses, true, supersededBy);
        }

        static Success readOrWrite(UUID ballot, Participants participants, List<MessageIn<ReadResponse>> responses, boolean isReadConsistent)
        {
            return new Success(Status.Outcome.PROMISED, ballot, participants, responses, isReadConsistent, null);
        }

        public String toString() { return "Success(" + ballot + ", " + participants.electorate + ')'; }
    }

    /**
     * The ballot we sought promises for has been superseded by another proposer's
     *
     * Note: we extend this for Success, so that supersededBy() can be called for ReadSuccess
     */
    static class Superseded extends Status
    {
        final UUID by;

        Superseded(UUID by, Participants participants)
        {
            super(SUPERSEDED, participants);
            this.by = by;
        }

        public String toString() { return "Superseded(" + by + ')'; }
    }

    static class WithRequestedBallot extends Status
    {
        final UUID ballot;

        WithRequestedBallot(Outcome outcome, Participants participants, UUID ballot)
        {
            super(outcome, participants);
            this.ballot = ballot;
        }
    }

    static class FoundIncomplete extends WithRequestedBallot
    {
        private FoundIncomplete(Outcome outcome, Participants participants, UUID promisedBallot)
        {
            super(outcome, participants, promisedBallot);
        }
    }

    /**
     * We have been informed of a promise made by one of the replicas we contacted, that was not accepted by all replicas
     * (though may have been accepted by a majority; we don't know).
     * In this case we cannot readily know if we have prevented this proposal from being completed, so we attempt
     * to finish it ourselves (unfortunately leaving the proposer to timeout, given the current semantics)
     * TODO: we should consider waiting for more responses in case we encounter any successful commit, or a majority
     *       of acceptors?
     */
    static class FoundIncompleteAccepted extends FoundIncomplete
    {
        final Accepted accepted;

        private FoundIncompleteAccepted(UUID promisedBallot, Participants participants, Accepted accepted)
        {
            super(FOUND_INCOMPLETE_ACCEPTED, participants, promisedBallot);
            this.accepted = accepted;
        }

        public String toString()
        {
            return "FoundIncomplete" + accepted;
        }
    }

    /**
     * We have been informed of a proposal that was accepted by a majority, but we do not know has been
     * committed to a majority, and we failed to read from a single natural replica that had witnessed this
     * commit when we performed the read.
     * Since this is an edge case, we simply start again, to keep the control flow more easily understood;
     * the commit shouldld be committed to a majority as part of our re-prepare.
     */
    static class FoundIncompleteCommitted extends FoundIncomplete
    {
        final Committed committed;

        private FoundIncompleteCommitted(UUID promisedBallot, Participants participants, Committed committed)
        {
            super(FOUND_INCOMPLETE_COMMITTED, participants, promisedBallot);
            this.committed = committed;
        }

        public String toString()
        {
            return "FoundIncomplete" + committed;
        }
    }

    static class MaybeFailure extends Status
    {
        final Paxos.MaybeFailure info;
        private MaybeFailure(Paxos.MaybeFailure info, Participants participants)
        {
            super(MAYBE_FAILURE, participants);
            this.info = info;
        }

        public String toString() { return info.toString(); }
    }

    static class ElectorateMismatch extends WithRequestedBallot
    {
        private ElectorateMismatch(Participants participants, UUID ballot)
        {
            super(ELECTORATE_MISMATCH, participants, ballot);
        }
    }

    private final boolean acceptEarlyReadPermission;
    private final AbstractRequest<?> request;
    private UUID supersededBy; // cannot be promised, as a newer promise has been made
    private Accepted latestAccepted; // the latest latestAcceptedButNotCommitted response we have received (which may still have been committed elsewhere)
    private Committed latestCommitted; // latest actually committed proposal

    private final Participants participants;

    private final List<MessageIn<ReadResponse>> readResponses;
    private boolean haveReadResponseWithLatest;
    private boolean haveQuorumOfPermissions; // permissions => SUCCESS or READ_SUCCESS
    private List<InetAddress> withLatest; // promised and have latest commit
    private List<InetAddress> needLatest; // promised without having witnessed latest commit, nor yet been refreshed by us
    private List<InetAddress> failures; // failed either on initial request or on refresh
    private boolean hasProposalStability = true; // no successful modifying proposal could have raced with us and not been seen
    private boolean hasOnlyPromises = true;
    private long maxLowBound;

    private Status outcome;
    private final Consumer<Status> onDone;

    private PaxosPrepareRefresh refreshStaleParticipants;

    PaxosPrepare(Participants participants, AbstractRequest<?> request, boolean acceptEarlyReadPermission, Consumer<Status> onDone)
    {
        this.acceptEarlyReadPermission = acceptEarlyReadPermission;
        assert participants.requiredForConsensus > 0;
        this.participants = participants;
        this.request = request;
        this.readResponses = new ArrayList<>(participants.requiredForConsensus);
        this.withLatest = new ArrayList<>(participants.requiredForConsensus);
        this.latestAccepted = Accepted.none(request.partitionKey, request.metadata);
        this.latestCommitted = Committed.none(request.partitionKey, request.metadata);
        this.onDone = onDone;
    }

    private boolean hasInProgressProposal()
    {
        // no need to commit a no-op; either it
        //   1) reached a majority, in which case it was agreed, had no effect and we can do nothing; or
        //   2) did not reach a majority, was not agreed, and was not user visible as a result so we can ignore it
        if (latestAccepted.update.isEmpty())
            return false;

        // If we aren't newer than latestCommitted, then we're done
        if (!latestAccepted.isAfter(latestCommitted))
            return false;

        if (latestAccepted.ballot.timestamp() <= maxLowBound)
            return false;

        // We can be a re-proposal of latestCommitted, in which case we do not need to re-propose it
        return !latestAccepted.isReproposalOf(latestCommitted);
    }

    static PaxosPrepare prepare(Participants participants, SinglePartitionReadCommand readCommand, boolean isWrite, boolean acceptEarlyReadPermission) throws UnavailableException
    {
        return prepare(null, participants, readCommand, isWrite, acceptEarlyReadPermission);
    }

    static PaxosPrepare prepare(UUID minimumBallot, Participants participants, SinglePartitionReadCommand readCommand, boolean isWrite, boolean acceptEarlyReadPermission) throws UnavailableException
    {
        return prepareWithBallot(newBallot(minimumBallot, participants.consistencyForConsensus), participants, readCommand, isWrite, acceptEarlyReadPermission);
    }

    static PaxosPrepare prepareWithBallot(UUID ballot, Participants participants, SinglePartitionReadCommand readCommand, boolean isWrite, boolean acceptEarlyReadPermission)
    {
        Tracing.trace("Preparing {} with read", ballot);
        Request request = new Request(ballot, participants.electorate, readCommand, isWrite);
        return prepareWithBallotInternal(participants, request, acceptEarlyReadPermission, null);
    }

    @SuppressWarnings("SameParameterValue")
    static <T extends Consumer<Status>> T prepareWithBallot(UUID ballot, Participants participants, DecoratedKey partitionKey, CFMetaData metadata, boolean isWrite, boolean acceptEarlyReadPermission, T onDone)
    {
        Tracing.trace("Preparing {}", ballot);
        prepareWithBallotInternal(participants, new Request(ballot, participants.electorate, partitionKey, metadata, isWrite), acceptEarlyReadPermission, onDone);
        return onDone;
    }

    private static PaxosPrepare prepareWithBallotInternal(Participants participants, Request request, boolean acceptEarlyReadPermission, Consumer<Status> onDone)
    {
        PaxosPrepare prepare = new PaxosPrepare(participants, request, acceptEarlyReadPermission, onDone);
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_PREPARE_REQ, request, requestSerializer)
                .permitsArtificialDelay(participants.consistencyForConsensus);

        start(prepare, participants, message, RequestHandler::execute);
        return prepare;
    }

    /**
     * Submit the message to our peers, and submit it for local execution if relevant
     */
    static <R extends AbstractRequest<R>> void start(PaxosPrepare prepare, Participants participants, MessageOut<R> send, BiFunction<R, InetAddress, Response> selfHandler)
    {
        boolean executeOnSelf = false;
        for (int i = 0, size = participants.sizeOfPoll() ; i < size ; ++i)
        {
            InetAddress destination = participants.electorateToPoll.get(i);
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

    public synchronized Status awaitUntil(long deadline)
    {
        try
        {
            //noinspection StatementWithEmptyBody
            while (!isDone() && waits().waitUntil(this, deadline)) {}

            if (!isDone())
                signalDone(MAYBE_FAILURE);

            return outcome;
        }
        catch (InterruptedException e)
        {
            // can only normally be interrupted if the system is shutting down; should rethrow as a write failure but propagate the interrupt
            Thread.currentThread().interrupt();
            return new MaybeFailure(new Paxos.MaybeFailure(true, participants.sizeOfPoll(), participants.requiredForConsensus, 0, 0), participants);
        }
    }

    private boolean isDone()
    {
        return outcome != null;
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
        if (logger.isTraceEnabled())
            logger.trace("{} for {} from {}", response, request.ballot, from);

        if (isDone())
            return;

        if (response.isRejected())
        {
            Rejected rejected = response.rejected();
            supersededBy = rejected.supersededBy;
            signalDone(SUPERSEDED);
            return;
        }

        Permitted permitted = response.permitted();
        if (permitted.gossipInfo.isEmpty())
            // we agree about the electorate, so can simply accept the promise/permission
            permitted(permitted, from);
        else if (!needsGossipUpdate(permitted.gossipInfo))
            // our gossip is up-to-date, but our original electorate could have been built with stale gossip, so verify it
            permittedOrTerminateIfElectorateMismatch(permitted, from);
        else
            // otherwise our beliefs about the ring potentially diverge, so update gossip with the peer's information
            Gossiper.runInGossipStageAsync(() -> {
                Gossiper.instance.notifyFailureDetector(permitted.gossipInfo);
                Gossiper.instance.applyStateLocally(permitted.gossipInfo);

                // TODO: We should also wait for schema pulls/pushes, however this would be quite an involved change to MigrationManager
                //       (which currently drops some migration tasks on the floor).
                //       Note it would be fine for us to fail to complete the migration task and simply treat this response as a failure/timeout.

                // once any pending ranges have been calculated, refresh our Participants list and submit the promise
                PendingRangeCalculatorService.instance.executeWhenFinished(() -> permittedOrTerminateIfElectorateMismatch(permitted, from));
            });
    }

    private synchronized void permittedOrTerminateIfElectorateMismatch(Permitted permitted, InetAddress from)
    {
        if (isDone()) // this execution is asynchronous wrt promise arrival, so must recheck done status
            return;

        // if the electorate has changed, finish so we can retry with the updated view of the ring
        if (!Electorate.get(request.metadata, request.partitionKey, decodeConsistency(request.ballot)).equals(participants.electorate))
        {
            signalDone(ELECTORATE_MISMATCH);
            return;
        }

        // otherwise continue as normal
        permitted(permitted, from);
    }

    private void permitted(Permitted permitted, InetAddress from)
    {
        hasOnlyPromises &= permitted.outcome == PROMISE;

        if (permitted.lowBound > maxLowBound)
            maxLowBound = permitted.lowBound;

        if (!haveQuorumOfPermissions)
        {
            CompareResult compareLatest = permitted.latestCommitted.compareWith(latestCommitted);
            switch (compareLatest)
            {
                default: throw new IllegalStateException();
                case IS_REPROPOSAL:
                    latestCommitted = permitted.latestCommitted;
                case WAS_REPROPOSED_BY:
                case SAME:
                    withLatest.add(from);
                    haveReadResponseWithLatest |= permitted.readResponse != null;
                    break;
                case BEFORE:
                    if (needLatest == null)
                        needLatest = new ArrayList<>(participants.sizeOfPoll() - withLatest.size());
                    needLatest.add(from);
                    break;
                case AFTER:
                    // move with->need
                    if (!withLatest.isEmpty())
                    {
                        if (needLatest == null)
                        {
                            needLatest = withLatest;
                            withLatest = new ArrayList<>(Math.min(participants.sizeOfPoll() - needLatest.size(), participants.requiredForConsensus));
                        }
                        else
                        {
                            needLatest.addAll(withLatest);
                            withLatest.clear();
                        }
                    }

                    withLatest.add(from);
                    haveReadResponseWithLatest = permitted.readResponse != null;
                    latestCommitted = permitted.latestCommitted;
            }

            if (isAfter(permitted.latestAcceptedButNotCommitted, latestAccepted))
                latestAccepted = permitted.latestAcceptedButNotCommitted;

            hasProposalStability &= permitted.hadProposalStability;
            if (permitted.readResponse != null)
                addReadResponse(permitted.readResponse, from);
        }
        else
        {
            switch (permitted.latestCommitted.compareWith(latestCommitted))
            {
                default: throw new IllegalStateException();
                case SAME:
                case IS_REPROPOSAL:
                case WAS_REPROPOSED_BY:
                    withLatest.add(from);
                    break;

                case AFTER:
                    maybeLogLinearizabilityViolation(permitted, from);
                    // witnessing future commit doesn't imply have seen prior, so add to refresh list

                case BEFORE:
                    if (needLatest == null)
                        needLatest = new ArrayList<>(participants.sizeOfPoll() - withLatest.size());
                    needLatest.add(from);
            }
        }

        haveQuorumOfPermissions |= withLatest() + needLatest() >= participants.requiredForConsensus;
        if (haveQuorumOfPermissions)
        {
            if (request.read != null && readResponses.size() < participants.requiredReads)
                throw new IllegalStateException("Insufficient read responses: " + readResponses + "; need " + participants.requiredReads);

            // We must be certain to have witnessed a quorum of responses before completing any in-progress proposal
            // else we may complete a stale proposal that did not reach a quorum (and may do so in preference
            // to a different in progress proposal that did reach a quorum).

            // We should also be sure to return any in progress proposal in preference to any incompletely committed
            // earlier commits (since, while we should encounter it next round, any commit that is incomplete in the
            // presence of an incomplete proposal can be ignored, as either the proposal is a re-proposal of the same
            // commit or the commit has already reached a quorum
            if (hasInProgressProposal())
                signalDone(FOUND_INCOMPLETE_ACCEPTED);

            // If we've reached a quorum of responses that witnessed the latest commmit,
            // our read response should be correct, as will future readers
            else if (withLatest() >= participants.requiredForConsensus)
                signalDone(hasOnlyPromises ? Status.Outcome.PROMISED : Status.Outcome.READ_PERMITTED);

            // otherwise if we have any read response with the latest commit,
            // try to simply ensure it has been persisted to a consensus group
            else if (haveReadResponseWithLatest)
            {
                refreshStaleParticipants();
                // if an optimistic read is possible, and we are performing a read,
                // we can safely answer immediately without waiting for the refresh
                if (hasProposalStability && acceptEarlyReadPermission)
                    signalDone(Status.Outcome.READ_PERMITTED);
            }

            // otherwise we need to run our reads again anyway,
            // and the chance of receiving another response with latest may be slim.
            // so we just start again
            else
                signalDone(FOUND_INCOMPLETE_COMMITTED);
        }
    }

    private void maybeLogLinearizabilityViolation(Permitted promised, InetAddress from)
    {
        // if we witness a newer commit AND are accepted something has gone wrong, except:

        // if we have raced with an ongoing commit, having missed all of them initially
        if (promised.latestCommitted.hasSameBallot(latestAccepted))
            return;

        // or in the case that we have an empty proposal accepted, since that will not be committed
        // in theory in this case we could now restart refreshStaleParticipants, but this would
        // unnecessarily complicate the logic so instead we accept that we will unnecessarily re-propose
        if (latestAccepted != null && latestAccepted.update.isEmpty() && latestAccepted.isAfter(promised.latestCommitted))
            return;

        // or in the case that both are older than the most recent repair low bound), in which case a topology change
        // could have ocurred that means not all paxos state tables know of the accept/commit, though it is persistent
        // in theory in this case we could ignore this entirely and call ourselves done
        // TODO: consider this more; is it possible we cause problems by reproposing an old accept?
        //  shouldn't be, as any newer accept that reaches a quorum will supersede
        if (promised.latestCommitted.ballot.timestamp() <= maxLowBound)
            return;

        logger.error("Linearizability violation: {} witnessed {} of latest {}({}) (withLatest: {}, readResponses: {}); {} promised with latest {}({})",
                request.ballot, decodeConsistency(request.ballot), latestCommitted, latestCommitted.ballot.timestamp(),
                withLatest, readResponses
                        .stream()
                        .map(r -> r.from)
                        .map(Object::toString)
                        .collect(Collectors.joining(", ", "[", "]")),
                from, promised.latestCommitted, promised.latestCommitted.ballot.timestamp());
    }

    /**
     * Save a read response from a node that we know to have witnessed the most recent commit
     *
     * Must be invoked while owning lock
     */
    private void addReadResponse(ReadResponse response, InetAddress from)
    {
        readResponses.add(MessageIn.create(from, response, Collections.emptyMap(), REQUEST_RESPONSE, current_version));
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
        if (logger.isTraceEnabled())
            logger.trace("{} {} from {}", request, kind, from);

        if (isDone())
            return;

        if (failures == null)
            failures = new ArrayList<>(participants.sizeOfPoll() - withLatest());

        failures.add(from);
        if (failures() + participants.requiredForConsensus == 1 + participants.sizeOfPoll())
            signalDone(MAYBE_FAILURE);
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }

    private void signalDone(Status.Outcome kindOfOutcome)
    {
        if (isDone())
            throw new IllegalStateException();

        Status outcome = toStatus(kindOfOutcome);
        this.outcome = outcome;
        if (onDone != null)
            onDone.accept(outcome);
        waits().notify(this);
    }

    private Status toStatus(Status.Outcome outcome)
    {
        switch (outcome)
        {
            case ELECTORATE_MISMATCH:
                return new ElectorateMismatch(participants, request.ballot);
            case SUPERSEDED:
                return new Superseded(supersededBy, participants);
            case FOUND_INCOMPLETE_ACCEPTED:
                return new FoundIncompleteAccepted(request.ballot, participants, latestAccepted);
            case FOUND_INCOMPLETE_COMMITTED:
                return new FoundIncompleteCommitted(request.ballot, participants, latestCommitted);
            case PROMISED:
                return Success.readOrWrite(request.ballot, participants, readResponses, hasProposalStability);
            case READ_PERMITTED:
                if (!hasProposalStability)
                    throw new IllegalStateException();
                return Success.read(request.ballot, participants, readResponses, supersededBy);
            case MAYBE_FAILURE:
                return new MaybeFailure(new Paxos.MaybeFailure(participants, withLatest(), failures()), participants);
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * See {@link PaxosPrepareRefresh}
     *
     * Must be invoked while owning lock
     */
    private void refreshStaleParticipants()
    {
        if (refreshStaleParticipants == null)
            refreshStaleParticipants = new PaxosPrepareRefresh(request.ballot, participants, latestCommitted, this);

        refreshStaleParticipants.refresh(needLatest);
        needLatest.clear();
    }

    public void onRefreshFailure(InetAddress from, boolean isTimeout)
    {
        onFailure(isTimeout ? "Timeout" : "Failure", from);
    }

    public synchronized void onRefreshSuccess(UUID isSupersededBy, InetAddress from)
    {
        if (logger.isTraceEnabled())
            logger.trace("Refresh {} from {}", isSupersededBy == null ? "Success" : "SupersededBy(" + isSupersededBy + ')', from);

        if (isDone())
            return;

        if (isSupersededBy != null)
        {
            supersededBy = isSupersededBy;
            if (hasProposalStability) signalDone(Status.Outcome.READ_PERMITTED);
            else signalDone(SUPERSEDED);
        }
        else
        {
            withLatest.add(from);
            if (withLatest.size() >= participants.requiredForConsensus)
                signalDone(hasOnlyPromises ? Status.Outcome.PROMISED : Status.Outcome.READ_PERMITTED);
        }
    }

    static abstract class AbstractRequest<R extends AbstractRequest<R>>
    {
        final UUID ballot;
        final Electorate electorate;
        final SinglePartitionReadCommand read;
        final boolean isForWrite;
        final DecoratedKey partitionKey;
        final CFMetaData metadata;

        AbstractRequest(UUID ballot, Electorate electorate, SinglePartitionReadCommand read, boolean isForWrite)
        {
            this.ballot = ballot;
            this.electorate = electorate;
            this.read = read;
            this.isForWrite = isForWrite;
            this.partitionKey = read.partitionKey();
            this.metadata = read.metadata();
        }

        AbstractRequest(UUID ballot, Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata, boolean isForWrite)
        {
            this.ballot = ballot;
            this.electorate = electorate;
            this.partitionKey = partitionKey;
            this.metadata = metadata;
            this.read = null;
            this.isForWrite = isForWrite;
        }

        abstract R withoutRead();

        public String toString()
        {
            return "Prepare(" + ballot + ')';
        }
    }

    static class Request extends AbstractRequest<Request>
    {
        Request(UUID ballot, Electorate electorate, SinglePartitionReadCommand read, boolean isWrite)
        {
            super(ballot, electorate, read, isWrite);
        }

        private Request(UUID ballot, Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata, boolean isWrite)
        {
            super(ballot, electorate, partitionKey, metadata, isWrite);
        }

        Request withoutRead()
        {
            return read == null ? this : new Request(ballot, electorate, partitionKey, metadata, isForWrite);
        }

        public String toString()
        {
            return "Prepare(" + ballot + ')';
        }
    }

    static class Response
    {
        final MaybePromise.Outcome outcome;

        Response(MaybePromise.Outcome outcome)
        {
            this.outcome = outcome;
        }
        Permitted permitted() { return (Permitted) this; }
        Rejected rejected() { return (Rejected) this; }

        public boolean isRejected()
        {
            return outcome == REJECT;
        }
    }

    static class Permitted extends Response
    {
        final long lowBound;
        // a proposal that has been accepted but not committed, i.e. must be null or > latestCommit
        @Nullable final Accepted latestAcceptedButNotCommitted;
        final Committed latestCommitted;
        @Nullable final ReadResponse readResponse;
        // latestAcceptedButNotCommitted and latestCommitted were the same before and after the read occurred, and no incomplete promise was witnessed
        final boolean hadProposalStability;
        final Map<InetAddress, EndpointState> gossipInfo;

        Permitted(MaybePromise.Outcome outcome, long lowBound, @Nullable Accepted latestAcceptedButNotCommitted, Committed latestCommitted, @Nullable ReadResponse readResponse, boolean hadProposalStability, Map<InetAddress, EndpointState> gossipInfo)
        {
            super(outcome);
            this.lowBound = lowBound;
            this.latestAcceptedButNotCommitted = latestAcceptedButNotCommitted;
            this.latestCommitted = latestCommitted;
            this.hadProposalStability = hadProposalStability;
            this.readResponse = readResponse;
            this.gossipInfo = gossipInfo;
        }

        @Override
        public String toString()
        {
            return "Promise(" + latestAcceptedButNotCommitted + ", " + latestCommitted + ", " + hadProposalStability + ", " + gossipInfo + ')';
        }
    }

    static class Rejected extends Response
    {
        final UUID supersededBy;

        Rejected(UUID supersededBy)
        {
            super(REJECT);
            this.supersededBy = supersededBy;
        }

        @Override
        public String toString()
        {
            return "RejectPromise(supersededBy=" + supersededBy + ')';
        }
    }

    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(MessageIn<Request> message, int id)
        {
            Response response = execute(message.payload, message.from);
            if (response == null)
                sendFailureResponse("Prepare", message.from, message.payload.ballot, id, message);
            else
                MessagingService.instance().sendReply(new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer)
                        .permitsArtificialDelay(message), id, message.from);
        }

        static Response execute(AbstractRequest<?> request, InetAddress from)
        {
            if (!isInRangeAndShouldProcess(from, request.partitionKey, request.metadata))
                return null;

            long start = System.nanoTime();
            try (PaxosState state = get(request.partitionKey, request.metadata, request.ballot))
            {
                return execute(request, state);
            }
            finally
            {
                Keyspace.openAndGetStore(request.metadata).metric.casPrepare.addNano(System.nanoTime() - start);
            }
        }

        static Response execute(AbstractRequest<?> request, PaxosState state)
        {
            MaybePromise result = state.promiseIfNewer(request.ballot, request.isForWrite);
            switch (result.outcome)
            {
                case PROMISE:
                case PERMIT_READ:
                    // verify electorates; if they differ, send back gossip info for superset of two participant sets
                    Map<InetAddress, EndpointState> gossipInfo = verifyElectorate(request.electorate, Electorate.get(request.metadata, request.partitionKey, decodeConsistency(request.ballot)));
                    ReadResponse readResponse = null;
                    // Check we cannot race with a proposal, i.e. that we have not made a promise that
                    // could be in the process of making a proposal. If a majority of nodes have made no such promise
                    // then either we must have witnessed it (since it must have been committed), or the proposal
                    // will now be rejected by our promises.
                    // This is logicaly complicated a bit by reading from a subset of the consensus group when there are
                    // pending nodes, however electorate verification we will cause us to retry if the pending status changes
                    // during execution; otherwise if the most recent commit we witnessed wasn't witnessed by a read response
                    // we will abort and retry, and we must witness it by the above argument.
                    boolean hasProposalStability = result.before.committed.hasBallot(result.before.promisedWrite)
                            && (result.before.accepted == null || result.before.accepted.update.isEmpty());

                    if (request.read != null)
                    {
                        try (ReadOrderGroup readGroup = request.read.startOrderGroup();
                             UnfilteredPartitionIterator iterator = request.read.executeLocally(readGroup))
                        {
                            readResponse = request.read.createResponse(iterator);
                        }

                        if (hasProposalStability)
                        {
                            Snapshot now = state.current();
                            hasProposalStability = now.promisedWrite == result.after.promisedWrite
                                    && now.committed == result.after.committed
                                    && now.accepted == result.after.accepted;
                        }
                    }

                    Accepted acceptedButNotCommitted = result.after.accepted;
                    Committed committed = result.after.committed;

                    ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(request.metadata.cfId);
                    long lowBound = cfs.getPaxosRepairLowBound(request.partitionKey).timestamp();
                    return new Permitted(result.outcome, lowBound, acceptedButNotCommitted, committed, readResponse, hasProposalStability, gossipInfo);

                case REJECT:
                    return new Rejected(result.supersededBy());

                default:
                    throw new IllegalStateException();
            }
        }
    }

    static abstract class AbstractRequestSerializer<R extends AbstractRequest<R>, T> implements IVersionedSerializer<R>
    {
        abstract R construct(T param, UUID ballot, Electorate electorate, SinglePartitionReadCommand read, boolean isWrite);
        abstract R construct(T param, UUID ballot, Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata, boolean isWrite);

        @Override
        public void serialize(R request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.ballot, out, version);
            Electorate.serializer.serialize(request.electorate, out, version);
            out.writeByte((request.read != null ? 1 : 0) | (request.isForWrite ? 0 : 2));
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

        public R deserialize(T param, DataInputPlus in, int version) throws IOException
        {
            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            Electorate electorate = Electorate.serializer.deserialize(in, version);
            byte flag = in.readByte();
            if ((flag & 1) != 0)
            {
                SinglePartitionReadCommand readCommand = (SinglePartitionReadCommand) ReadCommand.serializer.deserialize(in, version);
                return construct(param, ballot, electorate, readCommand, (flag & 2) == 0);
            }
            else
            {
                CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
                DecoratedKey partitionKey = (DecoratedKey) DecoratedKey.serializer.deserialize(in, metadata.partitioner, version);
                return construct(param, ballot, electorate, partitionKey, metadata, (flag & 2) == 0);
            }
        }

        @Override
        public long serializedSize(R request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.ballot, version)
                    + Electorate.serializer.serializedSize(request.electorate, version)
                    + 1 + (request.read != null
                        ? ReadCommand.serializer.serializedSize(request.read, version)
                        : CFMetaData.serializer.serializedSize(request.metadata, version)
                            + DecoratedKey.serializer.serializedSize(request.partitionKey, version));
        }
    }

    public static class RequestSerializer extends AbstractRequestSerializer<Request, Object>
    {
        Request construct(Object ignore, UUID ballot, Electorate electorate, SinglePartitionReadCommand read, boolean isWrite)
        {
            return new Request(ballot, electorate, read, isWrite);
        }

        Request construct(Object ignore, UUID ballot, Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata, boolean isWrite)
        {
            return new Request(ballot, electorate, partitionKey, metadata, isWrite);
        }

        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(null, in, version);
        }
    }

    public static class ResponseSerializer implements IVersionedSerializer<Response>
    {
        public void serialize(Response response, DataOutputPlus out, int version) throws IOException
        {
            if (response.isRejected())
            {
                out.writeByte(0);
                Rejected rejected = (Rejected) response;
                UUIDSerializer.serializer.serialize(rejected.supersededBy, out, version);
            }
            else
            {
                Permitted promised = (Permitted) response;
                out.writeByte(1
                        | (promised.latestAcceptedButNotCommitted != null ? 2  : 0)
                        | (promised.readResponse != null                  ? 4  : 0)
                        | (promised.hadProposalStability                  ? 8  : 0)
                        | (promised.outcome == PERMIT_READ ? 16 : 0)
                );
                out.writeUnsignedVInt(promised.lowBound);
                if (promised.latestAcceptedButNotCommitted != null)
                    Accepted.serializer.serialize(promised.latestAcceptedButNotCommitted, out, version);
                Committed.serializer.serialize(promised.latestCommitted, out, version);
                if (promised.readResponse != null)
                    ReadResponse.serializer.serialize(promised.readResponse, out, version);
                serializeMap(endpointSerializer, EndpointState.nullableSerializer, promised.gossipInfo, out, version);
            }
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            byte flags = in.readByte();
            if (flags == 0)
            {
                UUID supersededBy = UUIDSerializer.serializer.deserialize(in, version);
                return new Rejected(supersededBy);
            }
            else
            {
                long lowBound = in.readUnsignedVInt();
                Accepted acceptedNotCommitted = (flags & 2) != 0 ? Accepted.serializer.deserialize(in, version) : null;
                Committed committed = Committed.serializer.deserialize(in, version);
                ReadResponse readResponse = (flags & 4) != 0 ? ReadResponse.serializer.deserialize(in, version) : null;
                Map<InetAddress, EndpointState> gossipInfo = deserializeMap(endpointSerializer, EndpointState.nullableSerializer, newHashMap(), in, version);
                boolean hasProposalStability = (flags & 8) != 0;
                MaybePromise.Outcome outcome = (flags & 16) != 0 ? PERMIT_READ : PROMISE;
                // TODO: should we serialize supersededBy? probably not necessary, and not worth breaking serialization
                return new Permitted(outcome, lowBound, acceptedNotCommitted, committed, readResponse, hasProposalStability, gossipInfo);
            }
        }

        public long serializedSize(Response response, int version)
        {
            if (response.isRejected())
            {
                Rejected rejected = (Rejected) response;
                return 1 + UUIDSerializer.serializer.serializedSize(rejected.supersededBy, version);
            }
            else
            {
                Permitted promised = (Permitted) response;
                return 1
                        + VIntCoding.computeUnsignedVIntSize(promised.lowBound)
                        + (promised.latestAcceptedButNotCommitted == null ? 0 : Accepted.serializer.serializedSize(promised.latestAcceptedButNotCommitted, version))
                        + Committed.serializer.serializedSize(promised.latestCommitted, version)
                        + (promised.readResponse == null ? 0 : ReadResponse.serializer.serializedSize(promised.readResponse, version))
                        + serializedSizeMap(endpointSerializer, EndpointState.nullableSerializer, promised.gossipInfo, version);
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
