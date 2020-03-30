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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
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
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.DataResolver;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UUIDGen;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.MessagingService.FAILURE_RESPONSE_PARAM;
import static org.apache.cassandra.net.MessagingService.ONE_BYTE;
import static org.apache.cassandra.net.MessagingService.instance;
import static org.apache.cassandra.service.StorageProxy.casReadMetrics;
import static org.apache.cassandra.service.StorageProxy.casWriteMetrics;
import static org.apache.cassandra.service.StorageProxy.readMetrics;
import static org.apache.cassandra.service.StorageProxy.readMetricsMap;
import static org.apache.cassandra.service.StorageProxy.sameDCPredicateFor;
import static org.apache.cassandra.service.StorageProxy.verifyAgainstBlacklist;
import static org.apache.cassandra.service.StorageProxy.writeMetricsMap;
import static org.apache.cassandra.service.paxos.PaxosPrepare.prepare;
import static org.apache.cassandra.utils.NoSpamLogger.Level.WARN;

public class Paxos
{
    public static final boolean USE_APPLE_PAXOS = Boolean.getBoolean("cassandra.paxos.use_apple_paxos");

    private static final MessageOut<?> failureResponse = WriteResponse.createMessage()
            .withParameter(FAILURE_RESPONSE_PARAM, ONE_BYTE);

    private static final Logger logger = LoggerFactory.getLogger(Paxos.class);

    /**
     * Encapsulates the peers we will talk to for this operation.
     */
    static class Participants
    {
        /**
         * All natural endpoints for the token, regardless of status and location
         */
        final List<InetAddress> allNatural;

        /**
         * All pending endpoints for the token, regardless of status and location
         */
        final Collection<InetAddress> allPending;

        /**
         * allNatural++allPending, without down nodes or remote DC if consistency is local
         */
        final List<InetAddress> contact;
        /**
         *
         * The number of responses we require to reach desired consistency from members of {@code contact}
         */
        final int required;

        private Participants(List<InetAddress> allNatural, Collection<InetAddress> allPending, List<InetAddress> contact, int required)
        {
            this.allNatural = allNatural;
            this.allPending = allPending;
            this.contact = contact;
            this.required = required;
        }

        static Participants get(boolean isWrite, CFMetaData cfm, DecoratedKey key, ConsistencyLevel consistencyForPaxos) throws UnavailableException
        {
            Token tk = key.getToken();
            List<InetAddress> natural = StorageService.instance.getNaturalEndpoints(cfm.ksName, tk);
            Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, cfm.ksName);

            Predicate<InetAddress> filter = FailureDetector.isAlivePredicate;
            int countNatural = natural.size(), countPending = pending.size();
            if (consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL)
            {
                // Restrict naturalEndpoints and pendingEndpoints to node in the local DC only
                String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
                Predicate<InetAddress> isLocalDc = sameDCPredicateFor(localDc);

                countNatural = 0;
                for (int i = 0 ; i < natural.size() ; ++i)
                    if (isLocalDc.apply(natural.get(i)))
                        ++countNatural;

                if (countPending > 0)
                    countPending = Iterators.size(Iterators.filter(pending.iterator(), isLocalDc));

                filter = Predicates.and(filter, isLocalDc);
            }

            List<InetAddress> contact; {
                ImmutableList.Builder<InetAddress> builder = ImmutableList.builder();
                builder.addAll(Iterators.filter(natural.iterator(), filter));
                if (countPending > 0)
                    builder.addAll(Iterators.filter(pending.iterator(), filter));
                contact = builder.build();
            }

            int participants = countNatural + countPending;
            int required = participants / 2 + 1; // See CASSANDRA-8346, CASSANDRA-833

            if (countPending > 1 || required > contact.size())
            {
                mark(isWrite, m -> m.unavailables, consistencyForPaxos);

                if (countPending > 1)
                {
                    // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
                    // Note that we fake an impossible number of required nodes in the unavailable exception
                    // to nail home the point that it's an impossible operation no matter how many nodes are live.
                    throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) relevant pending range movement", countPending),
                            consistencyForPaxos,
                            participants + 1,
                            contact.size());
                }
                else
                {
                    throw new UnavailableException(consistencyForPaxos, required, contact.size());
                }
            }

            return new Participants(natural, pending, contact, required);
        }
    }

    /**
     * Encapsulates information about a failure to reach Success, either because of explicit failure responses
     * or insufficient responses (in which case the status is not final)
     */
    static class MaybeFailure
    {
        final boolean isFailure;
        final int participants;
        final int required;
        final int successes;
        final int failures;

        MaybeFailure(Participants participants, int successes, int failures)
        {
            this(participants.contact.size() - failures < participants.required,
                    participants.contact.size(), participants.required, successes, failures);
        }

        MaybeFailure(int participants, int required, int successes, int failures)
        {
            this(participants - failures < required, participants, required, successes, failures);
        }

        MaybeFailure(boolean isFailure, int participants, int required, int successes, int failures)
        {
            this.isFailure = isFailure;
            this.participants = participants;
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

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any row,
     * which (that is, the row) is the unit of values being proposed, not single columns.
     *
     * The Paxos cohort is only the replicas for the given key, not the entire cluster.
     * So we expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
     *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
     *     accepted.
     *  2. Accept: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
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
     *  Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
     *  values) between the prepare and accept phases.  This gives us a slightly longer window for another
     *  coordinator to come along and trump our own promise with a newer one but is otherwise safe.
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
        final long proposeDeadline = start + MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());
        final long commitDeadline = Math.max(proposeDeadline, start + MILLISECONDS.toNanos(DatabaseDescriptor.getWriteRpcTimeout()));

        SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
        CFMetaData metadata = readCommand.metadata();

        consistencyForPaxos.validateForCas();
        consistencyForCommit.validateForCasCommit(metadata.ksName);
        verifyAgainstBlacklist(metadata.ksName, metadata.cfName, key);

        int failedAttemptsDueToContention = 0;
        try
        {

            while (true)
            {
                // for simplicity, we'll do a single liveness check at the start of each attempt
                Participants participants = Participants.get(true, metadata, key, consistencyForPaxos);

                BeginResult begin = begin(proposeDeadline, key, metadata, participants, consistencyForPaxos, consistencyForCommit, true, readCommand);
                UUID ballot = begin.ballot;
                failedAttemptsDueToContention += begin.failedAttemptsDueToContention;

                FilteredPartition current;
                try (RowIterator iter = PartitionIterators.getOnlyElement(begin.readResponse, readCommand))
                {
                    current = FilteredPartition.create(iter);
                }

                if (!request.appliesTo(current))
                {
                    Tracing.trace("CAS precondition does not match current values {}", current);
                    casWriteMetrics.conditionNotMet.inc();
                    return current.rowIterator();
                }

                // finish the paxos round w/ the desired updates
                // TODO turn null updates into delete?
                PartitionUpdate updates = request.makeUpdates(current);

                // Apply triggers to cas updates. A consideration here is that
                // triggers emit Mutations, and so a given trigger implementation
                // may generate mutations for partitions other than the one this
                // paxos round is scoped for. In this case, TriggerExecutor will
                // validate that the generated mutations are targetted at the same
                // partition as the initial updates and reject (via an
                // InvalidRequestException) any which aren't.
                updates = TriggerExecutor.instance.execute(updates);

                Commit proposal = Commit.newProposal(ballot, updates);
                Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                PaxosPropose.Status propose = PaxosPropose.sync(proposeDeadline, proposal, participants, true);
                switch (propose.outcome)
                {
                    default: throw new IllegalStateException();

                    case MAYBE_FAILURE:
                        throw propose.maybeFailure().markAndThrowAsTimeoutOrFailure(true, consistencyForPaxos);

                    case SUCCESS:
                    {
                        PaxosCommit.Status commit = PaxosCommit.sync(commitDeadline, proposal, participants, consistencyForCommit, true);
                        if (!commit.isSuccess())
                            throw commit.maybeFailure().markAndThrowAsTimeoutOrFailure(true, consistencyForCommit);

                        Tracing.trace("CAS successful");
                        return null;
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
                                throw new MaybeFailure(false, participants.contact.size(), participants.required, 0, 0)
                                        .markAndThrowAsTimeoutOrFailure(true, consistencyForPaxos);

                            case NO:
                                // We have been superseded without our proposal being accepted by anyone, so we can safely retry
                                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                                failedAttemptsDueToContention++;
                                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
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

    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel)
            throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        if (group.commands.size() > 1)
            throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");

        long start = System.nanoTime();
        long deadline = start + MILLISECONDS.toNanos(DatabaseDescriptor.getReadRpcTimeout());
        SinglePartitionReadCommand command = group.commands.get(0);
        CFMetaData metadata = command.metadata();
        DecoratedKey key = command.partitionKey();

        try
        {
            // make sure any in-progress paxos writes are done (i.e., committed to a majority of replicas), before performing a quorum read
            Participants participants = Participants.get(false, metadata, key, consistencyLevel);

            // does the work of applying in-progress writes; throws UAE or timeout if it can't
            final BeginResult begin = begin(deadline, key, metadata, participants, consistencyLevel, nonSerial(consistencyLevel), false, group.commands.get(0));
            if (begin.failedAttemptsDueToContention > 0)
            {
                casReadMetrics.contention.update(begin.failedAttemptsDueToContention);
                casReadMetrics.contentionEstimatedHistogram.add(begin.failedAttemptsDueToContention);
            }

            return begin.readResponse;
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            casReadMetrics.addNano(latency);
            readMetricsMap.get(consistencyLevel).addNano(latency);
            Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }
    }

    static class BeginResult
    {
        final UUID ballot;
        final int failedAttemptsDueToContention;
        final PartitionIterator readResponse;

        public BeginResult(UUID ballot, int failedAttemptsDueToContention, PartitionIterator readResponse)
        {
            this.ballot = ballot;
            this.failedAttemptsDueToContention = failedAttemptsDueToContention;
            this.readResponse = readResponse;
        }
    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static BeginResult begin(long deadline,
                                     DecoratedKey key,
                                     CFMetaData metadata,
                                     Participants participants,
                                     ConsistencyLevel consistencyForPaxos,
                                     ConsistencyLevel consistencyForCommit,
                                     final boolean isWrite,
                                     ReadCommand readCommand)
            throws WriteTimeoutException, WriteFailureException, ReadTimeoutException, ReadFailureException
    {
        UUID minimumBallot = null;
        int failedAttemptsDueToContention = 0;
        while (true)
        {
            // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
            // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
            // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
            // out-of-order (#7801).
            long minTimestampMicrosToUse = minimumBallot == null ? Long.MIN_VALUE : 1 + UUIDGen.microsTimestamp(minimumBallot);
            long ballotMicros = ClientState.getTimestampForPaxos(minTimestampMicrosToUse);
            // Note that ballotMicros is not guaranteed to be unique if two proposal are being handled concurrently by the same coordinator. But we still
            // need ballots to be unique for each proposal so we have to use getRandomTimeUUIDFromMicros.
            UUID ballot = UUIDGen.getRandomTimeUUIDFromMicros(ballotMicros);

            // prepare
            Tracing.trace("Preparing {} with read", ballot);
            Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
            PaxosPrepare.Status prepare = prepare(deadline, toPrepare, participants, readCommand);
            switch (prepare.outcome)
            {
                default: throw new IllegalStateException();

                case SUPERSEDED:
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    minimumBallot = prepare.supersededBy();
                    failedAttemptsDueToContention++;
                    // sleep a random amount to give the other proposer a chance to finish
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                    continue;
                }

                case FOUND_IN_PROGRESS:
                {
                    Commit inProgress = prepare.foundInProgress().partiallyAcceptedProposal;
                    Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                    if(isWrite)
                        casWriteMetrics.unfinishedCommit.inc();
                    else
                        casReadMetrics.unfinishedCommit.inc();
                    Commit refreshedInProgress = Commit.newProposal(ballot, inProgress.update);
                    PaxosPropose.Status proposeResult = PaxosPropose.sync(deadline, refreshedInProgress, participants, false);
                    switch (proposeResult.outcome)
                    {
                        default: throw new IllegalStateException();

                        case MAYBE_FAILURE:
                            throw proposeResult.maybeFailure().markAndThrowAsTimeoutOrFailure(isWrite, consistencyForPaxos);

                        case SUPERSEDED:
                            minimumBallot = proposeResult.superseded().by;
                            Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                            // sleep a random amount to give the other proposer a chance to finish
                            failedAttemptsDueToContention++;
                            Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                            continue;

                        case SUCCESS:
                            // TODO: this commit consistency is wrong, as we must reach consistencyForPaxos in next prepare round
                            PaxosCommit.Status commitResult = PaxosCommit.sync(deadline, refreshedInProgress, participants, consistencyForCommit, false);
                            if (!commitResult.isSuccess())
                                throw commitResult.maybeFailure().markAndThrowAsTimeoutOrFailure(isWrite, consistencyForCommit);
                            continue;
                    }
                }

                case INCOMPLETE_COMMIT:
                {
                    // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
                    // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
                    // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
                    // mean we lost messages), we pro-actively "repair" those nodes, and retry.
                    PaxosPrepare.IncompleteCommit incompleteCommit = prepare.incompleteCommit();
                    Tracing.trace("Repairing replicas that missed the most recent commit");
                    PaxosCommit.async(incompleteCommit.incompleteCommit, incompleteCommit.missingFrom);
                    // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                    // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                    // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                    // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                    continue;
                }

                case SUCCESS:
                {
                    // We have received a quorum of promises that have all witnessed the commit of the prior paxos
                    // round's proposal (if any).
                    PaxosPrepare.Success success = prepare.success();

                    DataResolver resolver = new DataResolver(Keyspace.open(metadata.ksName), readCommand, nonSerial(consistencyForPaxos), success.responses.size());
                    for (int i = 0 ; i < success.responses.size() ; ++i)
                        resolver.preprocess(success.responses.get(i));

                    return new BeginResult(ballot, failedAttemptsDueToContention, resolver.resolve());
                }

                case MAYBE_FAILURE:
                    throw prepare.maybeFailure().markAndThrowAsTimeoutOrFailure(isWrite, consistencyForPaxos);
            }
        }
    }

    static void sendFailureResponse(String action, InetAddress to, UUID ballot, int messageId)
    {
        Tracing.trace("Sending {} failure response to {} for {}", action, ballot, to);
        instance().sendReply(failureResponse, messageId, to);
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
            case SERIAL: return ConsistencyLevel.QUORUM;
            case LOCAL_SERIAL: return ConsistencyLevel.LOCAL_QUORUM;
            default: throw new IllegalStateException();
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
            toMark.apply(readMetrics).mark();
            toMark.apply(casReadMetrics).mark();
            toMark.apply(readMetricsMap.get(consistency)).mark();
        }
    }

}
