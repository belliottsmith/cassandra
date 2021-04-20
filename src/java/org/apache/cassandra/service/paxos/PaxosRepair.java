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
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.paxos.PaxosRepair.Result.Outcome;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.service.paxos.Paxos.*;
import static org.apache.cassandra.service.paxos.PaxosPrepare.prepareWithBallot;
import static org.apache.cassandra.service.paxos.PaxosRepair.Result.Outcome.*;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

/**
 * Facility to finish any in-progress paxos transaction, and ensure that a quorum of nodes agree on the most recent operation.
 * Semantically, we simply ensure that any side effects that were "decided" before repair was initiated have been committed
 * to a quorum of nodes.
 * This means:
 *   - any prepare that has _possibly_ reached a quorum of nodes will be invalidated
 *   - any proposal that has been accepted by at least one node, but not known to be committed to any, will be proposed again
 *   - any proposal that has been committed to at least one node, but not committed to all, will be committed to a quorum
 *
 * Note that once started, this continues to try to repair any ongoing operations for the partition until success is achieved;
 * in a functioning cluster this should always be possible, but during a network partition this might continue indefinitely.
 * It is up to the caller to cancel any ongoing repairs if they exceed a reasonable time frame.
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
public class PaxosRepair
{
    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();

    public static class Result extends State
    {
        enum Outcome { DONE, CANCELLED, FAILURE }
        final Outcome outcome;
        public Result(Outcome outcome)
        {
            this.outcome = outcome;
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
    }

    private final DecoratedKey partitionKey;
    private final CFMetaData metadata;
    private final ConsistencyLevel consistency;
    private final Participants participants;
    private final int required;
    private final Consumer<Result> onDone;

    private UUID successCriteria;
    private State state;

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
        private Commit latestAccepted;
        private Commit latestCommitted;
        private UUID oldestCommitted;

        @Override
        public void onFailure(InetAddress from)
        {
            synchronized (PaxosRepair.this)
            {
                if (state != this)
                    return;

                if (++failures + required > participants.poll.size())
                    restart();
            }
        }

        @Override
        public void response(MessageIn<Response> msg)
        {
            synchronized (PaxosRepair.this)
            {
                try
                {
                    if (state != this)
                        return;

                    if (isAfter(msg.payload.latestWitnessed, latestWitnessed))
                        latestWitnessed = msg.payload.latestWitnessed;
                    if (isAfter(msg.payload.acceptedButNotCommitted, latestAccepted))
                        latestAccepted = msg.payload.acceptedButNotCommitted;
                    if (isAfter(msg.payload.committed, latestCommitted))
                        latestCommitted = msg.payload.committed;
                    if (oldestCommitted == null || isAfter(oldestCommitted, msg.payload.committed))
                        oldestCommitted = msg.payload.committed.ballot;

                    // once we receive the requisite number, we can simply proceed, and ignore future responses
                    if (++successes == required)
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
                successCriteria = latestWitnessed;

            boolean hasCommittedSuccessCriteria = isAfter(latestCommitted, successCriteria) || latestCommitted.ballot.equals(successCriteria);
            boolean isPromisedButNotAccepted    = isAfter(latestWitnessed, latestAccepted);
            boolean isAcceptedButNotCommitted   = isAfter(latestAccepted, latestCommitted);

            if (hasCommittedSuccessCriteria)
            {
                // we have a new enough commit, but it might not have reached enough participants; make sure it has before terminating
                // note: we could send to only those we know haven't witnessed it, but this is a rare operation so a small amount of redundant work is fine
                return oldestCommitted.equals(latestCommitted.ballot)
                        ? DONE
                        : PaxosCommit.async(latestCommitted, participants, consistency, true,
                            new CommittingRepair());
            }
            else if (isPromisedButNotAccepted)
            {
                // We need to propose a no-op > latestPromised, to ensure we don't later discover
                // that latestPromised had already been accepted (by a minority) and repair it
                // This means starting a new ballot, but we choose to use one that is likely to lose a contention battle
                // Since this operation is not urgent, and we can piggy-back on other paxos operations
                UUID ballot = staleBallotNewerThan(latestWitnessed, consistency);
                PartitionUpdate proposal = PartitionUpdate.emptyUpdate(metadata, partitionKey);

                return prepareWithBallot(ballot, participants, proposal.partitionKey(), proposal.metadata(),
                        new PoisonProposals(ballot, proposal));
            }
            else if (isAcceptedButNotCommitted)
            {
                // We need to complete this in-progress accepted proposal, which may not have been seen by a majority
                // However, since we have not sought any promises, we can simply complete the existing proposal
                // since this is an idempotent operation - both us and the original proposer (and others) can
                // all do it at the same time without incident

                return PaxosPropose.async(latestAccepted, participants, false,
                        new ProposingRepair(latestAccepted));
            }
            else
            {
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
    private class PoisonProposals extends ConsumerState<PaxosPrepare.Status>
    {
        final UUID ballot;
        final PartitionUpdate proposal;

        private PoisonProposals(UUID ballot, PartitionUpdate proposal)
        {
            this.ballot = ballot;
            this.proposal = proposal;
        }

        @Override
        public State execute(PaxosPrepare.Status input) throws Throwable
        {
            switch (input.outcome)
            {
                case MAYBE_FAILURE:
                case SUPERSEDED:
                case FOUND_IN_PROGRESS:
                    // start again
                    return restart();

                case SUCCESS:
                    // propose the in-progress operation
                    Commit proposal = Commit.newProposal(ballot, this.proposal);
                    return PaxosPropose.async(proposal, participants, false,
                            new ProposingRepair(proposal));

                default:
                    throw new IllegalStateException();
            }
        }
    }

    private class ProposingRepair extends ConsumerState<PaxosPropose.Status>
    {
        final Commit proposal;
        private ProposingRepair(Commit proposal)
        {
            this.proposal = proposal;
        }

        @Override
        public State execute(PaxosPropose.Status input)
        {
            switch (input.outcome)
            {
                case MAYBE_FAILURE:
                case SUPERSEDED:
                    return restart();

                case SUCCESS:
                    if (proposal.update.isEmpty())
                        return DONE;

                    return PaxosCommit.async(proposal, participants, consistency, true,
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
            return input.isSuccess() ? DONE : restart();
        }
    }

    public PaxosRepair(DecoratedKey partitionKey, CFMetaData metadata, ConsistencyLevel consistency, Participants participants, int required, Consumer<Result> onDone)
    {
        this.partitionKey = partitionKey;
        this.metadata = metadata;
        this.consistency = consistency;
        this.participants = participants;
        this.required = required;
        this.onDone = onDone;
    }

    public static PaxosRepair async(ConsistencyLevel consistency, DecoratedKey partitionKey, CFMetaData metadata, Consumer<Result> onDone)
    {
        return async(consistency, partitionKey, metadata, Participants.get(metadata, partitionKey, consistency), onDone);
    }

    public static PaxosRepair async(ConsistencyLevel consistency, DecoratedKey partitionKey, CFMetaData metadata, Participants participants, Consumer<Result> onDone)
    {
        PaxosRepair repair = new PaxosRepair(partitionKey, metadata, consistency, participants, participants.requiredForConsensus, onDone);
        repair.start();
        return repair;
    }

    private synchronized void start()
    {
        setState(restart());
    }

    public synchronized void cancel()
    {
        setState(CANCELLED);
    }

    public synchronized void await() throws InterruptedException
    {
        while (!(state instanceof Result))
            WAIT.wait(this);
    }

    private State restart()
    {
        if (state instanceof Result)
            return state;

        Querying querying = new Querying();
        MessageOut<Request> message = new MessageOut<>(MessagingService.Verb.APPLE_PAXOS_REPAIR_REQ, new Request(partitionKey, metadata), requestSerializer);
        for (int i = 0, size = participants.poll.size(); i < size ; ++i)
            MessagingService.instance().sendRR(message, participants.poll.get(i), querying);
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
            WAIT.notify(this);
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
        final UUID latestWitnessed;
        @Nullable final Commit acceptedButNotCommitted;
        final Commit committed;

        Response(UUID latestWitnessed, Commit acceptedButNotCommitted, Commit committed)
        {
            this.latestWitnessed = latestWitnessed;
            this.acceptedButNotCommitted = acceptedButNotCommitted;
            this.committed = committed;
        }
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
                sendFailureResponse("repair", message.from, null, id);
                return;
            }

            PaxosState current = PaxosState.read(request.partitionKey, request.metadata, (int)(System.currentTimeMillis() / 1000L));

            UUID latestWitnessed = current.promised;
            Commit acceptedButNotCommited = current.accepted;
            Commit committed = current.committed;

            if (isAfter(acceptedButNotCommited, committed))
            {
                if (isAfter(acceptedButNotCommited, latestWitnessed))
                    latestWitnessed = acceptedButNotCommited.ballot;
            }
            else
            {
                if (isAfter(committed, latestWitnessed))
                    latestWitnessed = committed.ballot;
                acceptedButNotCommited = null;
            }

            Response response = new Response(latestWitnessed, acceptedButNotCommited, committed);
            MessageOut<Response> reply = new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer);
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
            UUIDSerializer.serializer.serialize(response.latestWitnessed, out, version);
            serializeNullable(Commit.serializer, response.acceptedButNotCommitted, out, version);
            Commit.serializer.serialize(response.committed, out, version);
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID latestWitnessed = UUIDSerializer.serializer.deserialize(in, version);
            Commit acceptedButNotCommitted = deserializeNullable(Commit.serializer, in, version);
            Commit committed = Commit.serializer.deserialize(in, version);
            return new Response(latestWitnessed, acceptedButNotCommitted, committed);
        }

        public long serializedSize(Response response, int version)
        {
            return UUIDSerializer.serializer.serializedSize(response.latestWitnessed, version)
                    + serializedSizeNullable(Commit.serializer, response.acceptedButNotCommitted, version)
                    + Commit.serializer.serializedSize(response.committed, version);
        }
    }
}
