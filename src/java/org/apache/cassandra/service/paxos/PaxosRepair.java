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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.PaxosRepair.Result.Outcome;
import org.apache.cassandra.service.paxos.cleanup.PaxosTableRepairs;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.service.paxos.Commit.*;
import static org.apache.cassandra.service.paxos.Paxos.*;
import static org.apache.cassandra.service.paxos.PaxosPrepare.*;
import static org.apache.cassandra.service.paxos.PaxosRepair.Result.Outcome.*;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;
import static org.apache.cassandra.utils.concurrent.WaitManager.Global.waits;

/**
 * Facility to finish any in-progress paxos transaction, and ensure that a quorum of nodes agree on the most recent operation.
 * Semantically, we simply ensure that any side effects that were "decided" before repair was initiated have been committed
 * to a quorum of nodes.
 * This means:
 *   - any prepare that has _possibly_ reached a quorum of nodes will be invalidated
 *   - any proposal that has been accepted by at least one node, but not known to be committed to any, will be proposed again
 *   - any proposal that has been committed to at least one node, but not committed to all, will be committed to a quorum
 *
 * Note that once started, this continues to try to repair any ongoing operations for the partition up to 4 times.
 * In a functioning cluster this should always be possible, but during a network partition this might cause the repair
 * to fail.
 *
 * Requirements for correction:
 * - If performed during a range movement, we depend on a quorum (of the new topology) have been informed of the new
 *   topology _prior_ to initiating this repair, and this node to have been a member of a quorum of nodes verifying
 *    - If a quorum of nodes is unaware of the new topology prior to initiating repair, an operation could simply occur
 *      after repair completes that permits a linearization failure, such as with CASSANDRA-15745.
 *   their topology is up-to-date.
 * - Paxos prepare rounds must also verify the topology being used with their peers
 *    - If prepare rounds do not verify their topology, a node that is not a member of the quorum who have agreed
 *      the latest topology could still perform an operation without being aware of the topology change, and permit a
 *      linearization failure, such as with CASSANDRA-15745.
 *
 * With these issues addressed elsewhere, our algorithm is fairly simple.
 * In brief:
 *   1) Query all replicas for any promises or proposals they have witnessed that have not been committed,
 *      and their most recent commit. Wait for a quorum of responses.
 *   2) If this is the first time we have queried other nodes, we take a note of the most recent ballot we see;
 *      If this is not the first time we have queried other nodes, and we have committed a newer ballot than the one
 *      we previously recorded, we terminate (somebody else has done the work for us).
 *   3) If we see an in-progress operation that is very recent, we wait for it to complete and try again
 *   4) If we see a previously accepted operation, we attempt to complete it, or
 *      if we see a prepare with no proposal, we propose an empty update to invalidate it;
 *      otherwise we have nothing to do, as there is no operation that can have produced a side-effect before we began.
 *   5) We prepare a paxos round to agree the new commit using a higher ballot than the one witnessed,
 *      but a lower than one we would propose a new operation with. This permits newer operations to "beat" us so
 *      that we do not interfere with normal paxos operations.
 *   6) If we are "beaten" we start again (without delay, as (2) manages delays where necessary)
 */
public class PaxosRepair implements PaxosTableRepairs.QueueableRepair
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosRepair.class);

    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();
    private static final long RETRY_TIMEOUT_NANOS = getRetryTimeoutNanos();

    private static long getRetryTimeoutNanos()
    {
        long retryMillis = Long.getLong("cassandra.paxos_repair_retry_timeout_millis", 60000);
        return TimeUnit.MILLISECONDS.toNanos(retryMillis);
    }

    public static class Result extends State
    {
        enum Outcome { DONE, CANCELLED, FAILURE }
        final Outcome outcome;
        public Result(Outcome outcome)
        {
            this.outcome = outcome;
        }
        public String toString() { return outcome.toString(); }

        public boolean wasSuccessful()
        {
            return outcome == Outcome.DONE;
        }
    }

    public static final Result DONE = new Result(Outcome.DONE);
    public static final Result CANCELLED = new Result(Outcome.CANCELLED);
    public static final class Failure extends Result
    {
        public final Throwable failure;
        private Failure(Throwable failure)
        {
            super(FAILURE);
            this.failure = failure;
        }

        public String toString()
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            if (failure != null)
                failure.printStackTrace(pw);
            return outcome.toString() + ": " + sw.toString();
        }
    }

    private final DecoratedKey partitionKey;
    private final CFMetaData metadata;
    private final ConsistencyLevel consistency;
    private final Consumer<Result> onDone;
    private Participants participants;

    private UUID successCriteria;
    private UUID prevSupersededBy;
    private State state;
    private long startedNanos;
    private boolean started = false;

    private static class State {}

    private abstract class ConsumerState<T> extends State implements Consumer<T>
    {
        public void accept(T input)
        {
            synchronized (PaxosRepair.this)
            {
                if (state != this)
                    return;

                try
                {
                    setState(execute(input));
                }
                catch (Throwable t)
                {
                    setState(new Failure(t));
                }
            }
        }

        abstract State execute(T input) throws Throwable;
    }

    /**
     * Waiting for responses to APPLE_PAXOS_REPAIR messages.
     *
     * This state may be entered multiple times; every time we fail for any reason, we restart from this state
     */
    private class Querying extends State implements IAsyncCallbackWithFailure<PaxosRepair.Response>
    {
        private int successes;
        private int failures;

        private UUID latestWitnessed;
        private @Nullable Accepted latestAccepted;
        private Committed latestCommitted;
        private UUID oldestCommitted;

        @Override
        public void onFailure(InetAddress from)
        {
            synchronized (PaxosRepair.this)
            {
                if (state != this)
                    return;

                if (++failures + participants.requiredForConsensus > participants.sizeOfPoll())
                    setState(maybeRetry());
            }
        }

        @Override
        public void response(MessageIn<Response> msg)
        {
            logger.trace("PaxosRepair {} from {}", msg.payload, msg.from);
            synchronized (PaxosRepair.this)
            {
                try
                {
                    if (state != this)
                        return;

                    latestWitnessed = latest(latestWitnessed, msg.payload.latestWitnessedOrLowBound);
                    latestAccepted = latest(latestAccepted, msg.payload.acceptedButNotCommitted);
                    latestCommitted = latest(latestCommitted, msg.payload.committed);
                    if (oldestCommitted == null || isAfter(oldestCommitted, msg.payload.committed))
                        oldestCommitted = msg.payload.committed.ballot;

                    // once we receive the requisite number, we can simply proceed, and ignore future responses
                    if (++successes == participants.requiredForConsensus)
                        setState(execute());
                }
                catch (Throwable t)
                {
                    setState(new Failure(t));
                }
            }
        }

        private State execute()
        {
            // Save as success criteria the latest promise seen by our first round; if we ever see anything
            // newer committed, we know at least one paxos round has been completed since we started, which is all we need
            // or newer than this committed we know we're done, so to avoid looping indefinitely in competition
            // with others, we store this ballot for future retries so we can terminate based on other proposers' work
            if (successCriteria == null)
            {
                if (logger.isTraceEnabled())
                    logger.trace("PaxosRepair of {} setting success criteria to {}", partitionKey, Ballot.toString(latestWitnessed));

                successCriteria = latestWitnessed;
            }

            boolean hasCommittedSuccessCriteria = isAfter(latestCommitted, successCriteria) || latestCommitted.hasBallot(successCriteria);
            boolean isPromisedButNotAccepted    = isAfter(latestWitnessed, latestAccepted); // not necessarily promised - may be lowBound
            boolean isAcceptedButNotCommitted   = isAfter(latestAccepted, latestCommitted);
            boolean reproposalMayBeRejected     = timestampsClash(latestCommitted, latestWitnessed)
                                               || timestampsClash(latestAccepted, latestWitnessed)
                                               || !isAfter(latestWitnessed, prevSupersededBy);


            if (hasCommittedSuccessCriteria)
            {
                if (logger.isTraceEnabled())
                    logger.trace("PaxosRepair witnessed {} newer than success criteria {} (oldest: {})", latestCommitted, Ballot.toString(successCriteria), Ballot.toString(oldestCommitted));

                // we have a new enough commit, but it might not have reached enough participants; make sure it has before terminating
                // note: we could send to only those we know haven't witnessed it, but this is a rare operation so a small amount of redundant work is fine
                return oldestCommitted.equals(latestCommitted.ballot)
                        ? DONE
                        : PaxosCommit.commit(latestCommitted, participants, consistency, true,
                            new CommittingRepair());
            }
            else if (isAcceptedButNotCommitted && !isPromisedButNotAccepted && !reproposalMayBeRejected)
            {
                if (logger.isTraceEnabled())
                    logger.trace("PaxosRepair of {} completing {}", partitionKey, latestAccepted);
                // We need to complete this in-progress accepted proposal, which may not have been seen by a majority
                // However, since we have not sought any promises, we can simply complete the existing proposal
                // since this is an idempotent operation - both us and the original proposer (and others) can
                // all do it at the same time without incident

                // If ballots with same timestamp have been both accepted and rejected by different nodes,
                // to avoid a livelock we simply try to poison, knowing we will fail but use a new ballot
                // (note there are alternative approaches but this is conservative)

                return PaxosPropose.propose(latestAccepted, participants, false,
                        new ProposingRepair(latestAccepted));
            }
            else if (isAcceptedButNotCommitted || isPromisedButNotAccepted)
            {
                UUID ballot = staleBallotNewerThan(latestWitnessed, consistency);
                // We need to propose a no-op > latestPromised, to ensure we don't later discover
                // that latestPromised had already been accepted (by a minority) and repair it
                // This means starting a new ballot, but we choose to use one that is likely to lose a contention battle
                // Since this operation is not urgent, and we can piggy-back on other paxos operations
                if (logger.isTraceEnabled())
                    logger.trace("PaxosRepair of {} found incomplete promise or proposal; preparing stale ballot {}", partitionKey, Ballot.toString(ballot));

                return prepareWithBallot(ballot, participants, partitionKey, metadata,
                        new PoisonProposals());
            }
            else
            {
                logger.error("PaxosRepair illegal state latestWitnessed={}, latestAcceptedButNotCommitted={}, latestCommitted={}, oldestCommitted={}", latestWitnessed, latestAccepted, latestCommitted, oldestCommitted);
                throw new IllegalStateException(); // should be logically impossible
            }
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }

    /**
     * We found either an incomplete promise or proposal, so we need to start a new paxos round to complete them
     */
    private class PoisonProposals extends ConsumerState<Status>
    {
        @Override
        public State execute(Status input) throws Throwable
        {
            switch (input.outcome)
            {
                case MAYBE_FAILURE:
                    return maybeRetry();
                case SUPERSEDED:
                    if (isAfter(input.supersededBy(), prevSupersededBy))
                        prevSupersededBy = input.supersededBy();
                    return restart();

                case FOUND_INCOMPLETE_ACCEPTED:
                {
                    // finish the in-progress proposal
                    // cannot simply restart, as our latest promise is newer than the proposal
                    // so we require a promise before we decide which proposal to complete
                    // (else an "earlier" operation can sneak in and invalidate us while we're proposing
                    // with a newer ballot)
                    FoundIncompleteAccepted incomplete = input.incompleteAccepted();
                    Proposal propose = new Proposal(incomplete.promisedBallot, incomplete.accepted.update);
                    logger.trace("PaxosRepair of {} found incomplete {}", partitionKey, incomplete.accepted);
                    return PaxosPropose.propose(propose, participants, false,
                            new ProposingRepair(propose)); // we don't know if we're done, so we must restart
                }

                case FOUND_INCOMPLETE_COMMITTED:
                {
                    // finish the in-progress commit
                    FoundIncompleteCommitted incomplete = input.incompleteCommitted();
                    logger.trace("PaxosRepair of {} found in progress {}", partitionKey, incomplete.committed);
                    return PaxosCommit.commit(incomplete.committed, participants, consistency, true,
                            new CommitAndRestart()); // we don't know if we're done, so we must restart
                }

                case SUCCESS:
                {
                    // propose the empty ballot
                    logger.trace("PaxosRepair of {} submitting empty proposal", partitionKey);
                    Proposal proposal = Proposal.empty(input.success().ballot, partitionKey, metadata);
                    return PaxosPropose.propose(proposal, participants, false,
                            new ProposingRepair(proposal));
                }

                default:
                    throw new IllegalStateException();
            }
        }
    }

    private class ProposingRepair extends ConsumerState<PaxosPropose.Status>
    {
        final Proposal proposal;
        private ProposingRepair(Proposal proposal)
        {
            this.proposal = proposal;
        }

        @Override
        public State execute(PaxosPropose.Status input)
        {
            switch (input.outcome)
            {
                case MAYBE_FAILURE:
                    return maybeRetry();

                case SUPERSEDED:
                    if (isAfter(input.superseded().by, prevSupersededBy))
                        prevSupersededBy = input.superseded().by;
                    return restart();

                case SUCCESS:
                    if (proposal.update.isEmpty())
                    {
                        logger.trace("PaxosRepair of {} complete after successful empty proposal", partitionKey);
                        return DONE;
                    }

                    logger.trace("PaxosRepair of {} committing successful proposal {}", partitionKey, proposal);
                    return PaxosCommit.commit(proposal.agreed(), participants, consistency, true,
                            new CommittingRepair());

                default:
                    throw new IllegalStateException();
            }
        }
    }

    private class CommittingRepair extends ConsumerState<PaxosCommit.Status>
    {
        @Override
        public State execute(PaxosCommit.Status input)
        {
            logger.trace("PaxosRepair of {} {}", partitionKey, input);
            return input.isSuccess() ? DONE : maybeRetry();
        }
    }

    private class CommitAndRestart extends ConsumerState<PaxosCommit.Status>
    {
        @Override
        public State execute(PaxosCommit.Status input)
        {
            return restart();
        }
    }

    private PaxosRepair(DecoratedKey partitionKey, CFMetaData metadata, ConsistencyLevel consistency, Consumer<Result> onDone)
    {
        this.partitionKey = partitionKey;
        this.metadata = metadata;
        this.consistency = consistency;
        this.onDone = onDone;
    }

    public static PaxosRepair create(ConsistencyLevel consistency, DecoratedKey partitionKey, CFMetaData metadata, Consumer<Result> onDone)
    {
        return new PaxosRepair(partitionKey, metadata, consistency, onDone);
    }

    public synchronized PaxosRepair start()
    {
        Preconditions.checkState(!started);
        startedNanos = System.nanoTime();
        started = true;
        setState(restart());
        return this;
    }

    public synchronized void cancel()
    {
        setState(CANCELLED);
    }

    @Override
    public DecoratedKey key()
    {
        return partitionKey;
    }

    public synchronized Result await() throws InterruptedException
    {
        while (!(state instanceof Result))
            waits().wait(this);
        return (Result) state;
    }

    private State maybeRetry()
    {
        assert started;
        if (state instanceof Result)
            return state;

        if (System.nanoTime() - startedNanos > RETRY_TIMEOUT_NANOS)
            return new Failure(null);

        return restart();
    }

    private State restart()
    {
        if (state instanceof Result)
            return state;

        // TODO: assureSufficientLiveNodes?
        participants = Participants.get(metadata, partitionKey, consistency);
        Querying querying = new Querying();
        MessageOut<Request> message = new MessageOut<>(MessagingService.Verb.APPLE_PAXOS_REPAIR_REQ, new Request(partitionKey, metadata), requestSerializer)
                .permitsArtificialDelay(participants.consistencyForConsensus);
        for (int i = 0, size = participants.sizeOfPoll(); i < size ; ++i)
            MessagingService.instance().sendRRWithFailure(message, participants.electorateToPoll.get(i), querying);
        return querying;
    }

    private void setState(State newState)
    {
        if (state instanceof Result)
            return; // already terminated; probably due to user-invoked cancellation

        state = newState;
        if (newState instanceof Result)
        {
            if (onDone != null)
                onDone.accept((Result) newState);
            waits().notify(this);
        }
    }

    static class Request
    {
        final DecoratedKey partitionKey;
        final CFMetaData metadata;
        Request(DecoratedKey partitionKey, CFMetaData metadata)
        {
            this.partitionKey = partitionKey;
            this.metadata = metadata;
        }
    }

    /**
     * The response to a proposal, indicating success (if {@code supersededBy == null},
     * or failure, alongside the ballot that beat us
     */
    static class Response
    {
        @Nonnull final UUID latestWitnessedOrLowBound;
        @Nullable final Accepted acceptedButNotCommitted;
        @Nonnull final Committed committed;

        Response(UUID latestWitnessedOrLowBound, @Nullable Accepted acceptedButNotCommitted, Committed committed)
        {
            this.latestWitnessedOrLowBound = latestWitnessedOrLowBound;
            this.acceptedButNotCommitted = acceptedButNotCommitted;
            this.committed = committed;
        }

        public String toString()
        {
            return String.format("Response(%s, %s, %s", latestWitnessedOrLowBound, acceptedButNotCommitted, committed);
        }
    }

    private static Map<String, Set<InetAddress>> mapToDc(Collection<InetAddress> endpoints, Function<InetAddress, String> dcFunc)
    {
        Map<String, Set<InetAddress>> map = new HashMap<>();
        endpoints.forEach(e -> map.computeIfAbsent(dcFunc.apply(e), k -> new HashSet<>()).add(e));
        return map;
    }

    private static boolean hasQuorumOrSingleDead(Collection<InetAddress> all, Collection<InetAddress> live, boolean requireQuorum)
    {
        Preconditions.checkArgument(all.size() >= live.size());
        return live.size() >= (all.size() / 2) + 1 || (!requireQuorum && live.size() >= all.size() - 1);
    }

    @VisibleForTesting
    static boolean hasSufficientLiveNodesForTopologyChange(Collection<InetAddress> allEndpoints, Collection<InetAddress> liveEndpoints, Function<InetAddress, String> dcFunc, boolean onlyQuorumRequired, boolean strictQuorum)
    {

        Map<String, Set<InetAddress>> allDcMap = mapToDc(allEndpoints, dcFunc);
        Map<String, Set<InetAddress>> liveDcMap = mapToDc(liveEndpoints, dcFunc);

        if (!hasQuorumOrSingleDead(allEndpoints, liveEndpoints, strictQuorum))
            return false;

        if (onlyQuorumRequired)
            return true;

        for (Map.Entry<String, Set<InetAddress>> entry : allDcMap.entrySet())
        {
            Set<InetAddress> all = entry.getValue();
            Set<InetAddress> live = liveDcMap.getOrDefault(entry.getKey(), Collections.emptySet());
            if (!hasQuorumOrSingleDead(all, live, strictQuorum))
                return false;
        }
        return true;
    }

    /**
     * checks if we have enough live nodes to perform a paxos repair for topology repair. Generally, this means that we need enough
     * live participants to reach EACH_QUORUM, with a few exceptions. The EACH_QUORUM requirement is meant to support workload using either
     * SERIAL or LOCAL_SERIAL
     *
     * if paxos_topology_repair_strict_each_quorum is set to false (the default), we will accept either a quorum or n-1 live nodes
     * in the cluster and per dc. If paxos_topology_repair_no_dc_checks is true, we only check the live nodes in the cluster,
     * and do not do any per-dc checks.
     */
    public static boolean hasSufficientLiveNodesForTopologyChange(Keyspace keyspace, Range<Token> range, Collection<InetAddress> liveEndpoints)
    {
        return hasSufficientLiveNodesForTopologyChange(StorageService.instance.getNaturalEndpoints(keyspace.getName(), range.right),
                                                       liveEndpoints,
                                                       DatabaseDescriptor.getEndpointSnitch()::getDatacenter,
                                                       DatabaseDescriptor.paxoTopologyRepairNoDcChecks(),
                                                       DatabaseDescriptor.paxoTopologyRepairStrictEachQuorum());
    }

    /**
     * The proposal request handler, i.e. receives a proposal from a peer and responds with either acccept/reject
     */
    public static class RequestHandler implements IVerbHandler<PaxosRepair.Request>
    {
        @Override
        public void doVerb(MessageIn<PaxosRepair.Request> message, int id)
        {
            PaxosRepair.Request request = message.payload;
            if (!isInRangeAndShouldProcess(message.from, request.partitionKey, request.metadata))
            {
                sendFailureResponse("repair", message.from, null, id, message);
                return;
            }

            UUID latestWitnessed;
            Accepted acceptedButNotCommited;
            Committed committed;
            int nowInSec = FBUtilities.nowInSeconds();
            try (PaxosState state = PaxosState.get(request.partitionKey, request.metadata, nowInSec))
            {
                PaxosState.Snapshot snapshot = state.current();
                latestWitnessed = snapshot.latestWitnessedOrLowBound();
                acceptedButNotCommited = snapshot.accepted;
                committed = snapshot.committed;
            }

            Response response = new Response(latestWitnessed, acceptedButNotCommited, committed);
            MessageOut<Response> reply = new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer)
                    .permitsArtificialDelay(message);
            MessagingService.instance().sendReply(reply, id, message.from);
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            CFMetaData.serializer.serialize(request.metadata, out, version);
            DecoratedKey.serializer.serialize(request.partitionKey, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
            DecoratedKey partitionKey = (DecoratedKey) DecoratedKey.serializer.deserialize(in, metadata.partitioner, version);
            return new Request(partitionKey, metadata);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return CFMetaData.serializer.serializedSize(request.metadata, version) +
                    DecoratedKey.serializer.serializedSize(request.partitionKey, version);
        }
    }

    public static class ResponseSerializer implements IVersionedSerializer<Response>
    {
        public void serialize(Response response, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(response.latestWitnessedOrLowBound, out, version);
            serializeNullable(Accepted.serializer, response.acceptedButNotCommitted, out, version);
            Committed.serializer.serialize(response.committed, out, version);
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID latestWitnessed = UUIDSerializer.serializer.deserialize(in, version);
            Accepted acceptedButNotCommitted = deserializeNullable(Accepted.serializer, in, version);
            Committed committed = Committed.serializer.deserialize(in, version);
            return new Response(latestWitnessed, acceptedButNotCommitted, committed);
        }

        public long serializedSize(Response response, int version)
        {
            return UUIDSerializer.serializer.serializedSize(response.latestWitnessedOrLowBound, version)
                    + serializedSizeNullable(Accepted.serializer, response.acceptedButNotCommitted, version)
                    + Committed.serializer.serializedSize(response.committed, version);
        }
    }
}
