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
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit.Proposal;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.CollectionSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.concurrent.WaitMonitor;
import org.apache.cassandra.service.paxos.PaxosPrepare.FoundIncompleteAccepted;
import org.apache.cassandra.service.paxos.PaxosPrepare.FoundIncompleteCommitted;

import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.size;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.CompactEndpointSerializationHelper.*;
import static org.apache.cassandra.config.Config.PaxosVariant.*;
import static org.apache.cassandra.config.DatabaseDescriptor.*;
import static org.apache.cassandra.db.ConsistencyLevel.*;
import static org.apache.cassandra.gms.FailureDetector.isAlivePredicate;
import static org.apache.cassandra.net.MessagingService.FAILURE_RESPONSE_PARAM;
import static org.apache.cassandra.net.MessagingService.ONE_BYTE;
import static org.apache.cassandra.service.StorageProxy.casReadMetrics;
import static org.apache.cassandra.service.StorageProxy.casWriteMetrics;
import static org.apache.cassandra.service.StorageProxy.readMetrics;
import static org.apache.cassandra.service.StorageProxy.readMetricsMap;
import static org.apache.cassandra.service.StorageProxy.writeMetricsMap;
import static org.apache.cassandra.service.StorageProxy.verifyAgainstBlacklist;
import static org.apache.cassandra.service.paxos.PaxosCommit.commit;
import static org.apache.cassandra.service.paxos.PaxosCommitAndPrepare.commitAndPrepare;
import static org.apache.cassandra.service.paxos.PaxosPrepare.prepare;
import static org.apache.cassandra.service.paxos.PaxosPropose.propose;
import static org.apache.cassandra.utils.CollectionSerializer.newHashSet;
import static org.apache.cassandra.utils.NoSpamLogger.Level.WARN;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddress;

public class Paxos
{
    private static volatile Config.PaxosVariant PAXOS_VARIANT = DatabaseDescriptor.getPaxosVariant();
    private static final boolean USE_SELF_EXECUTION = Boolean.getBoolean("cassandra.paxos.use_apple_paxos_self_execution");
    static WaitMonitor WAIT = WaitMonitor.NONE;
    private static BallotGenerator BALLOT_GENERATOR = new BallotGenerator.Default();

    private static final MessageOut<?> failureResponse = WriteResponse.createMessage()
            .withParameter(FAILURE_RESPONSE_PARAM, ONE_BYTE);

    private static final Logger logger = LoggerFactory.getLogger(Paxos.class);

    static class Electorate
    {
        static final Serializer serializer = new Serializer();

        // all replicas, including pending, but without those in a remote DC if consistency is local
        final Set<InetAddress> all;

        // pending subset of electorate
        final Set<InetAddress> pending;

        public Electorate(Set<InetAddress> all, Set<InetAddress> pending)
        {
            this.all = all;
            this.pending = pending;
        }

        static Electorate get(CFMetaData cfm, DecoratedKey key, ConsistencyLevel consistency)
        {
            Token token = key.getToken();
            List<InetAddress> natural = StorageService.instance.getNaturalEndpoints(cfm.ksName, token);
            Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(token, cfm.ksName);
            Collection<InetAddress> all = pending.isEmpty() ? natural :
                    new AbstractCollection<InetAddress>() {
                        public Iterator<InetAddress> iterator() { return Iterators.concat(natural.iterator(), pending.iterator()); }
                        public int size() { return natural.size() + pending.size(); }
                    };
            return get(consistency, all, natural, pending);
        }

        static Electorate get(ConsistencyLevel consistency, Collection<InetAddress> all, List<InetAddress> allNatural, Collection<InetAddress> allPending)
        {
            Set<InetAddress> electorate, pendingElectorate;
            if (consistency == LOCAL_SERIAL || consistency == UNSAFE_DELAY_LOCAL_SERIAL)
            {
                // Restrict naturalEndpoints and pendingEndpoints to node in the local DC only
                Predicate<InetAddress> isLocalDc = getEndpointSnitch().isSameDcAs(getBroadcastAddress());

                int countNatural = 0;
                for (int i = 0 ; i < allNatural.size() ; ++i)
                    if (isLocalDc.apply(allNatural.get(i)))
                        ++countNatural;

                int countPending = allPending.size();
                if (countPending > 0)
                    countPending = size(filter(allPending.iterator(), isLocalDc));

                int count = countNatural + countPending;
                electorate = copyAsSet(filter(all.iterator(), isLocalDc), count);
                pendingElectorate = copyAsSet(filter(allPending.iterator(), isLocalDc), countPending);
            }
            else
            {
                electorate = copyAsSet(all);
                pendingElectorate = copyAsSet(allPending);
            }

            return new Electorate(electorate, pendingElectorate);
        }

        boolean hasPending()
        {
            return !pending.isEmpty();
        }

        boolean isPending(InetAddress endpoint)
        {
            return hasPending() && pending.contains(endpoint);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Electorate that = (Electorate) o;
            return all.equals(that.all) && pending.equals(that.pending);
        }

        public int hashCode()
        {
            return Objects.hash(all, pending);
        }

        public String toString()
        {
            return "{" + all + ", " + pending + '}';
        }

        static class Serializer implements IVersionedSerializer<Electorate>
        {
            public void serialize(Electorate electorate, DataOutputPlus out, int version) throws IOException
            {
                CollectionSerializer.serializeCollection(endpointSerializer, electorate.all, out, version);
                CollectionSerializer.serializeCollection(endpointSerializer, electorate.pending, out, version);
            }

            public Electorate deserialize(DataInputPlus in, int version) throws IOException
            {
                Set<InetAddress> endpoints = CollectionSerializer.deserializeCollection(endpointSerializer, newHashSet(), in, version);
                Set<InetAddress> pending = CollectionSerializer.deserializeCollection(endpointSerializer, newHashSet(), in, version);
                return new Electorate(endpoints, pending);
            }

            public long serializedSize(Electorate electorate, int version)
            {
                return CollectionSerializer.serializedSizeCollection(endpointSerializer, electorate.all, version) +
                       CollectionSerializer.serializedSizeCollection(endpointSerializer, electorate.pending, version);
            }
        }
    }

    /**
     * Encapsulates the peers we will talk to for this operation.
     */
    static class Participants
    {
        /**
         * SERIAL or LOCAL_SERIAL
         */
        final ConsistencyLevel consistencyForConsensus;

        /**
         * Those members that vote for {@link #consistencyForConsensus}
         */
        final Electorate electorate;

        /**
         * Those members of {@link #electorate} that we will 'poll' for their vote
         * i.e. {@link #electorate} with down nodes removed
         */
        final List<InetAddress> electorateToPoll;

        /**
         * All endpoints for the token, regardless of location (DC) or status: natural, pending, live or otherwise
         */
        final List<InetAddress> all;

        /**
         * {@link #all} but limited to those nodes that are "pending" (joining) endpoints
         */
        final Collection<InetAddress> allPending;

        /**
         * The number of responses we require to reach desired consistency from members of {@code contact}
         */
        final int requiredForConsensus;

        /**
         * The number of read responses we require to reach desired consistency from members of {@code contact}
         * Note that this should always be met if {@link #requiredForConsensus} is met, but we supply it separately
         * for corroboration.
         */
        final int requiredReads;

        private Participants(ConsistencyLevel consistencyForConsensus, List<InetAddress> all, Collection<InetAddress> allPending, Electorate electorate, List<InetAddress> electorateToPoll, int requiredForConsensus, int requiredReads)
        {
            this.consistencyForConsensus = consistencyForConsensus;
            this.all = all;
            this.allPending = allPending;
            this.electorate = electorate;
            this.electorateToPoll = electorateToPoll;
            this.requiredForConsensus = requiredForConsensus;
            this.requiredReads = requiredReads;
        }

        static Participants get(CFMetaData cfm, DecoratedKey key, ConsistencyLevel consistency)
        {
            Token token = key.getToken();
            List<InetAddress> natural = StorageService.instance.getNaturalEndpoints(cfm.ksName, token);
            Collection<InetAddress> allPending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(token, cfm.ksName);

            List<InetAddress> all = allPending.isEmpty()
                    ? natural
                    : ImmutableList.<InetAddress>builder().addAll(natural).addAll(allPending).build();

            Electorate electorate = Electorate.get(consistency, all, natural, allPending);
            int countPending = electorate.pending.size();
            int countNatural = electorate.all.size() - countPending;

            List<InetAddress> contact = ImmutableList.copyOf(filter(electorate.all.iterator(), isAlivePredicate));

            // we need a quorum of natural replicas + pending replicas to partipate in the paxos operation, but we
            // can only read from a quorum of natural replicas
            int readsRequired = countNatural/2 + 1;
            // Since we read from the same group we use for consensus, we can simply increment the size of our quorum
            // (i.e. we no longer need to limit ourselves due to the problems highlighted by CASSANDRA-8346 or CASSANDRA-833)
            int requiredForConsensus = readsRequired + countPending;

            return new Participants(consistency, all, allPending, electorate, contact, requiredForConsensus, readsRequired);
        }

        int sizeOfPoll()
        {
            return electorateToPoll.size();
        }

        void assureSufficientLiveNodes(boolean isWrite) throws UnavailableException
        {
            if (requiredForConsensus > sizeOfPoll())
            {
                mark(isWrite, m -> m.unavailables, consistencyForConsensus);
                throw new UnavailableException(consistencyForConsensus, requiredForConsensus, sizeOfPoll());
            }
        }

        int requiredFor(ConsistencyLevel consistency, CFMetaData metadata)
        {
            if (consistency == Paxos.nonSerial(consistencyForConsensus))
                return requiredForConsensus;

            int requiredPending = allPending.size();
            if (requiredPending > 0 && consistency.isDatacenterLocal())
            {
                requiredPending = size(filter(allPending.iterator(), getEndpointSnitch().isSameDcAs(getBroadcastAddress())));
            }
            return consistency.blockFor(Keyspace.open(metadata.ksName)) + requiredPending;
        }
    }

    /**
     * Encapsulates information about a failure to reach Success, either because of explicit failure responses
     * or insufficient responses (in which case the status is not final)
     */
    static class MaybeFailure
    {
        final boolean isFailure;
        final int contacted;
        final int required;
        final int successes;
        final int failures;

        MaybeFailure(Participants contacted, int successes, int failures)
        {
            this(contacted.sizeOfPoll() - failures < contacted.requiredForConsensus,
                    contacted.sizeOfPoll(), contacted.requiredForConsensus, successes, failures);
        }

        MaybeFailure(int contacted, int required, int successes, int failures)
        {
            this(contacted - failures < required, contacted, required, successes, failures);
        }

        MaybeFailure(boolean isFailure, int contacted, int required, int successes, int failures)
        {
            this.isFailure = isFailure;
            this.contacted = contacted;
            this.required = required;
            this.successes = successes;
            this.failures = failures;
        }

        /**
         * update relevant counters and throw the relevant exception
         */
        RequestExecutionException markAndThrowAsTimeoutOrFailure(boolean isWrite, ConsistencyLevel consistency)
        {
            if (isFailure)
            {
                mark(isWrite, m -> m.failures, consistency);
                throw isWrite
                        ? new WriteFailureException(consistency, successes, failures, required, WriteType.CAS)
                        : new ReadFailureException(consistency, successes, failures, required, false);
            }
            else
            {
                mark(isWrite, m -> m.timeouts, consistency);
                throw isWrite
                        ? new WriteTimeoutException(WriteType.CAS, consistency, successes, required)
                        : new ReadTimeoutException(consistency, successes, required, false);
            }
        }

        public String toString()
        {
            return (isFailure ? "Failure(" : "Timeout(") + successes + ',' + failures + ')';
        }
    }

    public interface Async<Result>
    {
        Result awaitUntil(long until);
    }

    static class ConditionAsConsumer<T> extends SimpleCondition implements Consumer<T>
    {
        public void accept(T o)
        {
            WAIT.signal(this);
        }
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any partition.
     *
     * The Paxos electorate consists only of the replicas for the partition key.
     * We expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (TimeUUID in our case) and asks replicas to
     *     - promise not to accept updates from older ballots and
     *     - tell us about the latest ballots it has already _promised_, _accepted_, or _committed_
     *     - reads the necessary data to evaluate our CAS condition
     *
     *  2. Propose: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
     *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     *     value.
     *
     *  Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     *  so here is our approach:
     *   3a. The coordinator sends a commit message to all replicas with the ballot and value.
     *   3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     *       and send it with subsequent promise replies.  This allows us to discard acceptance records
     *       for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     *       later on.
     *
     *  Note that since we are performing a CAS rather than a simple update, when nodes respond positively to
     *  Prepare, they include read response of commited values that will be reconciled on the coordinator
     *  and checked against CAS precondition between the prepare and accept phases. This gives us a slightly
     *  longer window for another coordinator to come along and trump our own promise with a newer one but
     *  is otherwise safe.
     *
     *  Any successful prepare phase yielding a read that rejects the condition must be followed by the proposal of
     *  an empty update, to ensure the evaluation of the condition is linearized with respect to other reads and writes.
     *
     * @param key the row key for the row to CAS
     * @param request the conditions for the CAS to apply as well as the update to perform if the conditions hold.
     * @param consistencyForPaxos the consistency for the paxos prepare and propose round. This can only be either SERIAL or LOCAL_SERIAL.
     * @param consistencyForCommit the consistency for write done during the commit phase. This can be anything, except SERIAL or LOCAL_SERIAL.
     *
     * @return null if the operation succeeds in updating the row, or the current values corresponding to conditions.
     * (since, if the CAS doesn't succeed, it means the current value do not match the conditions).
     */
    public static RowIterator cas(DecoratedKey key,
                                  CASRequest request,
                                  ConsistencyLevel consistencyForPaxos,
                                  ConsistencyLevel consistencyForCommit)
            throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException
    {
        final long start = System.nanoTime();
        final long proposeDeadline = start + MILLISECONDS.toNanos(getCasContentionTimeout());
        final long commitDeadline = Math.max(proposeDeadline, start + MILLISECONDS.toNanos(getWriteRpcTimeout()));
        return cas(key, request, consistencyForPaxos, consistencyForCommit, start, proposeDeadline, commitDeadline);
    }
    public static RowIterator cas(DecoratedKey key,
                                  CASRequest request,
                                  ConsistencyLevel consistencyForPaxos,
                                  ConsistencyLevel consistencyForCommit,
                                  long proposeDeadline,
                                  long commitDeadline
                                  )
            throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException
    {
        return cas(key, request, consistencyForPaxos, consistencyForCommit, System.nanoTime(), proposeDeadline, commitDeadline);
    }
    private static RowIterator cas(DecoratedKey key,
                                  CASRequest request,
                                  ConsistencyLevel consistencyForPaxos,
                                  ConsistencyLevel consistencyForCommit,
                                  long start,
                                  long proposeDeadline,
                                  long commitDeadline
                                  )
            throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException
    {
        SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
        CFMetaData metadata = readCommand.metadata();

        consistencyForPaxos.validateForCas();
        consistencyForCommit.validateForCasCommit(metadata.ksName);
        verifyAgainstBlacklist(metadata.ksName, metadata.cfName, key);

        UUID minimumBallot = null;
        int failedAttemptsDueToContention = 0;
        try
        {

            while (true)
            {
                // read the current values and check they validate the conditions
                Tracing.trace("Reading existing values for CAS precondition");

                BeginResult begin = begin(proposeDeadline, readCommand, consistencyForPaxos,
                        true, minimumBallot, failedAttemptsDueToContention);
                UUID ballot = begin.ballot;
                Participants participants = begin.participants;
                failedAttemptsDueToContention = begin.failedAttemptsDueToContention;

                FilteredPartition current;
                try (RowIterator iter = PartitionIterators.getOnlyElement(begin.readResponse, readCommand))
                {
                    current = FilteredPartition.create(iter);
                }

                Proposal proposal;
                boolean conditionMet = request.appliesTo(current);
                if (!conditionMet)
                {
                    // If we failed to meet our condition, it does not mean we can do nothing: if we do not propose
                    // anything that is accepted by a quorum, it is possible for our !conditionMet state
                    // to not be serialized wrt other operations.
                    // If a later read encounters an "in progress" write that did not reach a majority,
                    // but that would have permitted conditionMet had it done so (and hence we evidently did not witness),
                    // that operation will complete the in-progress proposal before continuing, so that this and future
                    // reads will perceive conditionMet without any intervening modification from the time at which we
                    // assured a conditional write that !conditionMet.
                    // So our evaluation is only serialized if we invalidate any in progress operations by proposing an empty update
                    // See also CASSANDRA-12126
                    Tracing.trace("CAS precondition does not match current values {}; proposing empty update", current);
                    proposal = Proposal.empty(ballot, key, metadata);
                }
                else
                {
                    // finish the paxos round w/ the desired updates
                    // TODO "turn null updates into delete?" - what does this TODO even mean?
                    PartitionUpdate updates = request.makeUpdates(current);

                    // Apply triggers to cas updates. A consideration here is that
                    // triggers emit Mutations, and so a given trigger implementation
                    // may generate mutations for partitions other than the one this
                    // paxos round is scoped for. In this case, TriggerExecutor will
                    // validate that the generated mutations are targetted at the same
                    // partition as the initial updates and reject (via an
                    // InvalidRequestException) any which aren't.
                    updates = TriggerExecutor.instance.execute(updates);

                    proposal = Proposal.from(ballot, updates);
                    Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                }

                PaxosPropose.Status propose = propose(proposal, participants, true).awaitUntil(proposeDeadline);
                switch (propose.outcome)
                {
                    default: throw new IllegalStateException();

                    case MAYBE_FAILURE:
                        throw propose.maybeFailure().markAndThrowAsTimeoutOrFailure(true, consistencyForPaxos);

                    case SUCCESS:
                    {
                        if (conditionMet)
                        {
                            // no need to commit a no-op; either it
                            //   1) reached a majority, in which case it was agreed, had no effect and we can do nothing; or
                            //   2) did not reach a majority, was not agreed, and was not user visible as a result so we can ignore it
                            if (!proposal.update.isEmpty())
                            {
                                PaxosCommit.Status commit = commit(proposal.agreed(), participants, consistencyForCommit, true).awaitUntil(commitDeadline);
                                if (!commit.isSuccess())
                                    throw commit.maybeFailure().markAndThrowAsTimeoutOrFailure(true, consistencyForCommit);
                            }

                            Tracing.trace("CAS successful");
                            return null;
                        }
                        else
                        {
                            Tracing.trace("CAS precondition rejected", current);
                            casWriteMetrics.conditionNotMet.inc();
                            return current.rowIterator();
                        }
                    }

                    case SUPERSEDED:
                    {
                        switch (propose.superseded().hadSideEffects)
                        {
                            default: throw new IllegalStateException();

                            case MAYBE:
                                // We don't know if our update has been applied, as the competing ballot may have completed
                                // our proposal.  We yield our uncertainty to the caller via timeout exception.
                                // TODO: should return more useful result to client, and should also avoid this situation where possible
                                throw new MaybeFailure(false, participants.sizeOfPoll(), participants.requiredForConsensus, 0, 0)
                                        .markAndThrowAsTimeoutOrFailure(true, consistencyForPaxos);

                            case NO:
                                minimumBallot = propose.superseded().by;
                                // We have been superseded without our proposal being accepted by anyone, so we can safely retry
                                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                                failedAttemptsDueToContention++;
                                if (!waitForContention(proposeDeadline, ++failedAttemptsDueToContention))
                                    throw new MaybeFailure(participants, 0, 0).markAndThrowAsTimeoutOrFailure(true, consistencyForPaxos);
                        }
                    }
                }
                // continue to retry
            }
        }
        finally
        {
            if (failedAttemptsDueToContention > 0)
            {
                casWriteMetrics.contention.update(failedAttemptsDueToContention);
                casWriteMetrics.contentionEstimatedHistogram.add(failedAttemptsDueToContention);
            }
            final long latency = System.nanoTime() - start;
            Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).metric.topCasPartitionContention.addSample(key.getKey(), failedAttemptsDueToContention);

            casWriteMetrics.addNano(latency);
            writeMetricsMap.get(consistencyForPaxos).addNano(latency);
        }
    }

    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyForPaxos)
            throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        long start = System.nanoTime();
        long deadline = start + MILLISECONDS.toNanos(DatabaseDescriptor.getReadRpcTimeout());
        return read(group, consistencyForPaxos, start, deadline);
    }

    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyForPaxos, long deadline)
            throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        return read(group, consistencyForPaxos, System.nanoTime(), deadline);
    }

    private static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyForPaxos, long start, long deadline)
            throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        if (group.commands.size() > 1)
            throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");

        int failedAttemptsDueToContention = 0;
        UUID minimumBallot = null;
        SinglePartitionReadCommand read = group.commands.get(0);
        try
        {
            while (true)
            {
                // does the work of applying in-progress writes; throws UAE or timeout if it can't
                final BeginResult begin = begin(deadline, read, consistencyForPaxos, false, minimumBallot, failedAttemptsDueToContention);
                if (PAXOS_VARIANT == apple_norrl)
                    return begin.readResponse;

                Proposal proposal = Proposal.empty(begin.ballot, read.partitionKey(), read.metadata());
                if (begin.isOptimisticReadSafe && PAXOS_VARIANT == apple_rrl)
                {
                    propose(proposal, begin.participants, false, null);
                    return begin.readResponse;
                }

                PaxosPropose.Status propose = propose(proposal, begin.participants, false).awaitUntil(deadline);
                switch (propose.outcome)
                {
                    default: throw new IllegalStateException();

                    case MAYBE_FAILURE:
                        throw propose.maybeFailure().markAndThrowAsTimeoutOrFailure(true, consistencyForPaxos);

                    case SUCCESS:
                        return begin.readResponse;

                    case SUPERSEDED:
                    {
                        switch (propose.superseded().hadSideEffects)
                        {
                            default: throw new IllegalStateException();

                            case MAYBE:
                                // We don't know if our update has been applied, as the competing ballot may have completed
                                // our proposal.  We yield our uncertainty to the caller via timeout exception.
                                // TODO: should return more useful result to client, and should also avoid this situation where possible
                                throw new MaybeFailure(false, begin.participants.sizeOfPoll(), begin.participants.requiredForConsensus, 0, 0)
                                        .markAndThrowAsTimeoutOrFailure(true, consistencyForPaxos);

                            case NO:
                                minimumBallot = propose.superseded().by;
                                // We have been superseded without our proposal being accepted by anyone, so we can safely retry
                                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                                failedAttemptsDueToContention++;
                                if (!waitForContention(deadline, ++failedAttemptsDueToContention))
                                    throw new MaybeFailure(begin.participants, 0, 0).markAndThrowAsTimeoutOrFailure(true, consistencyForPaxos);
                        }
                    }
                }
            }
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            casReadMetrics.addNano(latency);
            readMetricsMap.get(consistencyForPaxos).addNano(latency);
            CFMetaData metadata = read.metadata();
            Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
            if (failedAttemptsDueToContention > 0)
            {
                casReadMetrics.contention.update(failedAttemptsDueToContention);
                casReadMetrics.contentionEstimatedHistogram.add(failedAttemptsDueToContention);
            }
        }
    }

    static class BeginResult
    {
        final UUID ballot;
        final Participants participants;
        final int failedAttemptsDueToContention;
        final PartitionIterator readResponse;
        final boolean isOptimisticReadSafe;

        public BeginResult(UUID ballot, Participants participants, int failedAttemptsDueToContention, PartitionIterator readResponse, boolean isOptimisticReadSafe)
        {
            this.ballot = ballot;
            this.participants = participants;
            this.failedAttemptsDueToContention = failedAttemptsDueToContention;
            this.readResponse = readResponse;
            this.isOptimisticReadSafe = isOptimisticReadSafe;
        }
    }

    /**
     * Begin a Paxos operation by seeking promises from our electorate to be completed with proposals by our caller; and:
     *
     *  - Completing any in-progress proposals witnessed, that are not known to have reached the commit phase
     *  - Completing any in-progress commits witnessed, that are not known to have reached a quorum of the electorate
     *  - Retrying and backing-off under contention
     *  - Detecting electorate mismatches with our peers and retrying to avoid non-overlapping
     *    electorates agreeing operations
     *  - Returning a resolved read response, and knowledge of if it is linearizable to read without proposing an empty update
     *
     * Optimisations:
     *    - If the promises report an incomplete commit (but have been able to witness it in a read response)
     *      we will submit the commit to those nodes that have not witnessed while waiting for those that have,
     *      returning as soon as a quorum is known to have witnessed the commit
     *    - If we witness an in-progress commit to complete, we batch the commit together with a new prepare
     *      restarting our operation.
     *    - If we witness an in-progress proposal to complete, after successfully proposing it we batch its
     *      commit together with a new prepare restarting our operation.
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static BeginResult begin(long deadline,
                                     SinglePartitionReadCommand readCommand,
                                     ConsistencyLevel consistencyForConsensus,
                                     final boolean isWrite,
                                     UUID minimumBallot,
                                     int failedAttemptsDueToContention)
            throws WriteTimeoutException, WriteFailureException, ReadTimeoutException, ReadFailureException
    {
        Participants initialParticipants = Participants.get(readCommand.metadata(), readCommand.partitionKey(), consistencyForConsensus);
        initialParticipants.assureSufficientLiveNodes(isWrite);
        PaxosPrepare preparing = prepare(minimumBallot, initialParticipants, readCommand, !isWrite);
        while (true)
        {
            // prepare
            PaxosPrepare retry;
            PaxosPrepare.Status prepare = preparing.awaitUntil(deadline);
            retry: switch (prepare.outcome)
            {
                default: throw new IllegalStateException();

                case FOUND_INCOMPLETE_COMMITTED:
                {
                    FoundIncompleteCommitted incomplete = prepare.incompleteCommitted();
                    Tracing.trace("Repairing replicas that missed the most recent commit");
                    retry = commitAndPrepare(incomplete.committed, incomplete.participants, readCommand, !isWrite);
                    break;
                }
                case FOUND_INCOMPLETE_ACCEPTED:
                {
                    FoundIncompleteAccepted inProgress = prepare.incompleteAccepted();
                    Tracing.trace("Finishing incomplete paxos round {}", inProgress.accepted);
                    if (isWrite)
                        casWriteMetrics.unfinishedCommit.inc();
                    else
                        casReadMetrics.unfinishedCommit.inc();

                    // we DO NOT need to change the timestamp of this commit - either we or somebody else will finish it
                    // and the original timestamp is correctly linearised. By not updatinig the timestamp we leave enough
                    // information for nodes to avoid competing re-proposing the same proposal; if an in progress accept
                    // is equal to the latest commit (even if the ballots aren't) we're done and can abort earlier,
                    // and in fact it's possible for a CAS to sometimes determine if side effects occurred by reading
                    // the underlying data and not witnessing the timestamp of its ballot (or any newer for the relevant data).
                    Proposal repropose = new Proposal(inProgress.promisedBallot, inProgress.accepted.update);
                    PaxosPropose.Status proposeResult = propose(repropose, inProgress.participants, false).awaitUntil(deadline);
                    switch (proposeResult.outcome)
                    {
                        default: throw new IllegalStateException();

                        case MAYBE_FAILURE:
                            throw proposeResult.maybeFailure().markAndThrowAsTimeoutOrFailure(isWrite, consistencyForConsensus);

                        case SUCCESS:
                            retry = commitAndPrepare(repropose.agreed(), inProgress.participants, readCommand, !isWrite);
                            break retry;

                        case SUPERSEDED:
                            // since we are proposing a previous value that was maybe superseded by us before completion
                            // we don't need to test the side effects, as we just want to start again, and fall through
                            // to the superseded section below
                            prepare = new PaxosPrepare.Superseded(proposeResult.superseded().by, inProgress.participants);

                    }
                }

                case SUPERSEDED:
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    if (!waitForContention(deadline, ++failedAttemptsDueToContention))
                        throw new MaybeFailure(prepare.participants, 0, 0).markAndThrowAsTimeoutOrFailure(true, consistencyForConsensus);
                    retry = prepare(prepare.supersededBy(), prepare.participants, readCommand, !isWrite);
                    break;
                }

                case SUCCESS:
                {
                    // We have received a quorum of promises that have all witnessed the commit of the prior paxos
                    // round's proposal (if any).
                    PaxosPrepare.Success success = prepare.success();

                    DataResolver resolver = new DataResolver(Keyspace.open(readCommand.metadata().ksName), readCommand, nonSerial(consistencyForConsensus), success.responses.size());
                    for (int i = 0 ; i < success.responses.size() ; ++i)
                        resolver.preprocess(success.responses.get(i));
                    class WasRun implements Runnable { boolean v; public void run() { v = true; } }
                    WasRun hadShortRead = new WasRun();
                    PartitionIterator result = resolver.resolve(hadShortRead);

                    return new BeginResult(success.ballot, success.participants, failedAttemptsDueToContention, result, !hadShortRead.v && success.isReadConsistent);
                }

                case MAYBE_FAILURE:
                    throw prepare.maybeFailure().markAndThrowAsTimeoutOrFailure(isWrite, consistencyForConsensus);

                case ELECTORATE_MISMATCH:
                    Participants participants = Participants.get(readCommand.metadata(), readCommand.partitionKey(), consistencyForConsensus);
                    participants.assureSufficientLiveNodes(isWrite);
                    retry = prepare(prepare.previousBallot(), participants, readCommand, !isWrite);
                    break;

            }

            preparing = retry;
        }
    }

    static boolean waitForContention(long deadline, int failedAttemptsDueToContention)
    {
        // should have at most 2 of 3 messages to complete, and our latency counts retries and this is just a lower bound
        long minimumWait = 10000;
        long maximumWait = 100000;
        try
        {
            minimumWait = (casWriteMetrics.recentLatencyHistogram.percentile(0.5) * 2)/3;
            maximumWait = failedAttemptsDueToContention * casWriteMetrics.recentLatencyHistogram.percentile(0.95);
        }
        catch (Throwable t)
        {
            NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES).error("", t);
        }

        if (maximumWait <= 0 || maximumWait > 100000)
            maximumWait = 100000;
        if (minimumWait > maximumWait)
            minimumWait = maximumWait - 1;

        long wait = MICROSECONDS.toNanos(ThreadLocalRandom.current().nextLong(minimumWait, maximumWait));
        long until = System.nanoTime() + wait;
        if (until >= deadline)
            return false;

        try
        {
            WAIT.waitUntil(until);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    static void sendFailureResponse(String action, InetAddress to, UUID ballot, int messageId, MessageIn<?> respondingTo)
    {
        if (Tracing.isTracing())
            Tracing.trace("Sending {} failure response to {} for {}", action, ballot, to);
        MessagingService.instance().sendReply(failureResponse.permitsArtificialDelay(respondingTo), messageId, to);
    }

    static boolean isInRangeAndShouldProcess(InetAddress from, DecoratedKey key, CFMetaData metadata)
    {
        boolean outOfRangeTokenLogging = StorageService.instance.isOutOfTokenRangeRequestLoggingEnabled();
        boolean outOfRangeTokenRejection = StorageService.instance.isOutOfTokenRangeRequestRejectionEnabled();

        if ((outOfRangeTokenLogging || outOfRangeTokenRejection) && isOutOfRangeCommit(metadata.ksName, key))
        {
            StorageService.instance.incOutOfRangeOperationCount();
            Keyspace.open(metadata.ksName).metric.outOfRangeTokenPaxosRequests.inc();

            // Log at most 1 message per second
            if (outOfRangeTokenLogging)
                NoSpamLogger.log(logger, WARN, 1, SECONDS, "Received paxos request from {} for {} outside valid range for keyspace {}", from, key, metadata.ksName);

            return !outOfRangeTokenRejection;
        }

        return true;
    }

    private static boolean isOutOfRangeCommit(String keyspace, DecoratedKey key)
    {
        return !StorageService.instance.isEndpointValidForWrite(keyspace, key.getToken());
    }

    static ConsistencyLevel nonSerial(ConsistencyLevel serial)
    {
        switch (serial)
        {
            default: throw new IllegalStateException();
            case SERIAL: return QUORUM;
            case LOCAL_SERIAL: return LOCAL_QUORUM;
        }
    }

    private static void mark(boolean isWrite, Function<ClientRequestMetrics, Meter> toMark, ConsistencyLevel consistency)
    {
        if (isWrite)
        {
            toMark.apply(casWriteMetrics).mark();
            toMark.apply(writeMetricsMap.get(consistency)).mark();
        }
        else
        {
            toMark.apply(casReadMetrics).mark();
            toMark.apply(readMetricsMap.get(consistency)).mark();
        }
    }

    static UUID newBallot(@Nullable UUID minimumBallot, ConsistencyLevel consistency)
    {
        // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
        // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
        // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
        // out-of-order (#7801).
        long minTimestampMicros = minimumBallot == null ? Long.MIN_VALUE : 1 + UUIDGen.microsTimestamp(minimumBallot);
        long timestampMicros = BALLOT_GENERATOR.nextTimestamp(minTimestampMicros);
        // Note that ballotMicros is not guaranteed to be unique if two proposal are being handled concurrently by the same coordinator. But we still
        // need ballots to be unique for each proposal so we have to use getRandomTimeUUIDFromMicros.
        return BALLOT_GENERATOR.generate(timestampMicros, consistency == SERIAL);
    }

    static UUID staleBallotNewerThan(UUID than, ConsistencyLevel consistency)
    {
        long minTimestampMicros = 1 + UUIDGen.microsTimestamp(than);
        long maxTimestampMicros = BALLOT_GENERATOR.prevTimestamp();
        maxTimestampMicros -= Math.min((maxTimestampMicros - minTimestampMicros) / 2, SECONDS.toMicros(5L));
        if (maxTimestampMicros <= minTimestampMicros)
            return BALLOT_GENERATOR.generate(minTimestampMicros, consistency == SERIAL);

        return BALLOT_GENERATOR.generate(minTimestampMicros, maxTimestampMicros, consistency == SERIAL);
    }

    /**
     * Create a ballot uuid with the consistency level encoded in the timestamp.
     *
     * UUIDGen.getRandomTimeUUIDFromMicros timestamps are always a multiple of 10, so we add a 1 or 2 to indicate
     * the consistency level of the operation. This should have no effect in practice (except preferring a serial
     * operation over a local serial if there's a timestamp collision), but lets us avoid adding CL to the paxos
     * table and messages, which should make backcompat easier if a different solution is committed upstream.
     */
    public static UUID ballotForConsistency(long whenInMicros, ConsistencyLevel consistency)
    {
        Preconditions.checkArgument(consistency.isSerialConsistency());
        return BALLOT_GENERATOR.generate(whenInMicros, consistency == SERIAL);
    }

    public static ConsistencyLevel decodeConsistency(UUID ballot)
    {
        switch ((int) (ballot.timestamp() % 10))
        {
            case 1: return LOCAL_SERIAL;
            case 2: return SERIAL;
            default: return null;
        }
    }

    static Map<InetAddress, EndpointState> verifyElectorate(Electorate remoteElectorate, Electorate localElectorate)
    {
        // verify electorates; if they differ, send back gossip info for superset of two participant sets
        if (remoteElectorate.equals(localElectorate))
            return Collections.emptyMap();

        Map<InetAddress, EndpointState> endpoints = Maps.newHashMapWithExpectedSize(remoteElectorate.all.size() + localElectorate.all.size());
        for (InetAddress host : remoteElectorate.all)
        {
            endpoints.put(host, Gossiper.instance.getEndpointStateForEndpoint(host));
        }
        for (InetAddress host : localElectorate.all)
        {
            if (!endpoints.containsKey(host))
                endpoints.put(host, Gossiper.instance.getEndpointStateForEndpoint(host));
        }

        return endpoints;
    }

    private static <V> Set<V> copyAsSet(Collection<V> collection)
    {
        return collection.isEmpty() ? Collections.emptySet() : new HashSet<>(collection);
    }
    private static <V> Set<V> copyAsSet(Iterator<V> iterator, int count)
    {
        if (count == 0)
            return Collections.emptySet();

        return new HashSet<>(new AbstractCollection<V>()
        {
            public Iterator<V> iterator() { return iterator; }
            public int size() { return count; }
        });
    }

    public static boolean useApplePaxos()
    {
        switch (PAXOS_VARIANT)
        {
            case apple_norrl:
            case apple_rrl:
            case apple_rrl2rt:
                return true;
            case legacy:
                return false;
            default:
                throw new AssertionError();
        }
    }

    public static void setPaxosVariant(Config.PaxosVariant paxosVariant)
    {
        Preconditions.checkNotNull(paxosVariant);
        PAXOS_VARIANT = paxosVariant;
    }

    public static String getPaxosVariant()
    {
        return PAXOS_VARIANT.toString();
    }

    static boolean canExecuteOnSelf(InetAddress replica)
    {
        return USE_SELF_EXECUTION && replica.equals(FBUtilities.getBroadcastAddress());
    }

    public static interface BallotGenerator
    {
        static class Default implements BallotGenerator
        {
            public UUID generate(long whenInMicros, boolean isSerial)
            {
                return UUIDGen.getRandomTimeUUIDFromMicros(whenInMicros, isSerial ? 2 : 1);
            }

            public UUID generate(long fromInMicros, long toInMicros, boolean isSerial)
            {
                long timestampMicros = ThreadLocalRandom.current().nextLong(fromInMicros, toInMicros);
                return generate(timestampMicros, isSerial);
            }

            public long nextTimestamp(long minTimestamp)
            {
                return ClientState.getTimestampForPaxos(minTimestamp);
            }

            public long prevTimestamp()
            {
                return ClientState.getLastTimestampMicros();
            }
        }

        UUID generate(long whenInMicros, boolean isSerial);
        UUID generate(long fromInMicros, long toInMicros, boolean isSerial);
        long nextTimestamp(long minWhenInMicros);
        long prevTimestamp();
    }

    public static void unsafeSetWaitMonitor(WaitMonitor wait)
    {
        WAIT = wait;
    }

    public static void unsafeSetBallotGenerator(BallotGenerator ballotGenerator)
    {
        BALLOT_GENERATOR = ballotGenerator;
    }

}
