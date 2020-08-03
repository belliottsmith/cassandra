/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.service.paxos;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.service.paxos.Commit.Accepted;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTracker;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.service.paxos.Commit.hasSameBallot;
import static org.apache.cassandra.service.paxos.Commit.isAfter;

public class PaxosState
{
    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    @Nonnull final UUID promised;
    @Nullable final Accepted accepted; // if already committed, this will be null
    @Nonnull final Committed committed;

    private static PaxosUncommittedTracker loadTracker()
    {
        File directory = new File(Directories.dataDirectories[0].location, "paxos");
        FileUtils.createDirectory(directory);
        return PaxosUncommittedTracker.load(directory);
    }

    private static class TrackerHandle
    {
        static final PaxosUncommittedTracker instance = loadTracker();
    }

    public static PaxosUncommittedTracker tracker()
    {
        return TrackerHandle.instance;
    }

    public static void initializeTracker()
    {
        Preconditions.checkState(TrackerHandle.instance != null);
    }

    public @Nonnull UUID latestWitnessed()
    {
        Commit proposal = accepted == null ? committed : accepted;
        // if proposal has same timestamp as promised, we should prefer accepted since (if different) it reached a quorum of promises
        return proposal.isBefore(promised) ? promised : proposal.ballot;
    }

    public PaxosState(UUID promised, Accepted accepted, Committed committed)
    {
        assert accepted == null || accepted.update.partitionKey().equals(committed.update.partitionKey());
        assert accepted == null || accepted.update.metadata().equals(committed.update.metadata());

        this.promised = promised;
        this.accepted = accepted;
        this.committed = committed;
    }

    public static class PromiseResult extends PaxosState
    {
        final UUID prevPromised;
        final UUID supersededBy;

        public PromiseResult(PaxosState previous, UUID promised, UUID supersededBy)
        {
            super(promised, previous.accepted, previous.committed);
            this.prevPromised = previous.promised;
            this.supersededBy = supersededBy;
        }

        public boolean isPromised()
        {
            return supersededBy == null;
        }

        static PromiseResult promised(PaxosState previous, UUID promised)
        {
            return new PromiseResult(previous, promised, null);
        }

        static PromiseResult rejected(PaxosState previous, UUID supersededBy)
        {
            return new PromiseResult(previous, previous.promised, supersededBy);
        }
    }

    /**
     * Record the requested ballot as promised if it is newer than our current promise; otherwise do nothing.
     * @return a PaxosState object representing the state after this operation completes
     */
    public static PromiseResult promiseIfNewer(DecoratedKey key, CFMetaData metadata, UUID ballot)
    {
        long start = System.nanoTime();
        try
        {
            Lock lock = LOCKS.get(key);
            lock.lock();
            try
            {
                // When preparing, we need to use the same time as "now" (that's the time we use to decide if something
                // is expired or not) accross nodes otherwise we may have a window where a Most Recent Commit shows up
                // on some replica and not others during a new proposal (in StorageProxy.beginAndRepairPaxos()), and no
                // amount of re-submit will fix this (because the node on which the commit has expired will have a
                // tombstone that hides any re-submit). See CASSANDRA-12043 for details.
                int nowInSec = UUIDGen.unixTimestampInSec(ballot);
                PaxosState state = SystemKeyspace.loadPaxosState(key, metadata, nowInSec, true);
                // state.promised can be null, because it is invalidated by committed;
                // we may also have accepted a newer proposal than we promised, so we confirm that we are the absolute newest
                UUID latest = state.latestWitnessed();
                if (isAfter(ballot, latest))
                {
                    Tracing.trace("Promising ballot {}", ballot);
                    SystemKeyspace.savePaxosPromise(key, metadata, ballot);
                    return PromiseResult.promised(state, ballot);
                }
                else
                {
                    Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", ballot, latest);
                    return PromiseResult.rejected(state, latest);
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        finally
        {
            Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfId).metric.casPrepare.addNano(System.nanoTime() - start);
        }
    }

    static PaxosState read(DecoratedKey partitionKey, CFMetaData metadata, int nowInSec)
    {
        return SystemKeyspace.loadPaxosState(partitionKey, metadata, nowInSec, true);
    }

    /**
     * Record an acceptance of the proposal if there is no newer promise; otherwise inform the caller of the newer ballot
     */
    public static UUID acceptIfLatest(Commit proposal)
    {
        long start = System.nanoTime();
        try
        {
            Lock lock = LOCKS.get(proposal.update.partitionKey());
            lock.lock();
            try
            {
                int nowInSec = UUIDGen.unixTimestampInSec(proposal.ballot);
                PaxosState state = SystemKeyspace.loadPaxosState(proposal.update.partitionKey(), proposal.update.metadata(), nowInSec, true);
                // state.promised can be null, because it is invalidated by committed;
                // we may also have accepted a newer proposal than we promised, so we confirm that we are the absolute newest
                // (or that we have the exact same ballot as our promise, which is the typical case)
                UUID latest;
                if (proposal.hasBallot(state.promised)
                        || hasSameBallot(state.accepted, proposal)  // PaxosRepair may re-propose the same commit to reach a majority
                        || proposal.isAfter(latest = state.latestWitnessed())
                        )
                {
                    Tracing.trace("Accepting proposal {}", proposal);
                    SystemKeyspace.savePaxosProposal(proposal);
                    return null;
                }
                else
                {
                    Tracing.trace("Rejecting proposal for {} because latest is now {}", proposal, latest);
                    return latest;
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        finally
        {
            Keyspace.open(proposal.update.metadata().ksName).getColumnFamilyStore(proposal.update.metadata().cfId).metric.casPropose.addNano(System.nanoTime() - start);
        }
    }

    public static void commit(Commit proposal)
    {
        long start = System.nanoTime();
        try
        {
            // There is no guarantee we will see commits in the right order, because messages
            // can get delayed, so a proposal can be older than our current most recent ballot/commit.
            // Committing it is however always safe due to column timestamps, so always do it. However,
            // if our current in-progress ballot is strictly greater than the proposal one, we shouldn't
            // erase the in-progress update.
            // The table may have been truncated since the proposal was initiated. In that case, we
            // don't want to perform the mutation and potentially resurrect truncated data
            if (UUIDGen.unixTimestamp(proposal.ballot) >= SystemKeyspace.getTruncatedAt(proposal.update.metadata().cfId))
            {
                Tracing.trace("Committing proposal {}", proposal);
                Mutation mutation = proposal.makeMutation();
                Keyspace.open(mutation.getKeyspaceName()).apply(mutation, true);
            }
            else
            {
                Tracing.trace("Not committing proposal {} as ballot timestamp predates last truncation time", proposal);
            }
            // We don't need to lock, we're just blindly updating
            SystemKeyspace.savePaxosCommit(proposal);
        }
        finally
        {
            Keyspace.open(proposal.update.metadata().ksName).getColumnFamilyStore(proposal.update.metadata().cfId).metric.casCommit.addNano(System.nanoTime() - start);
        }
    }

    public static PrepareResponse legacyPrepare(Commit toPrepare)
    {
        long start = System.nanoTime();
        try
        {
            Lock lock = LOCKS.get(toPrepare.update.partitionKey());
            lock.lock();
            try
            {
                // When preparing, we need to use the same time as "now" (that's the time we use to decide if something
                // is expired or not) accross nodes otherwise we may have a window where a Most Recent Commit shows up
                // on some replica and not others during a new proposal (in StorageProxy.beginAndRepairPaxos()), and no
                // amount of re-submit will fix this (because the node on which the commit has expired will have a
                // tombstone that hides any re-submit). See CASSANDRA-12043 for details.
                int nowInSec = UUIDGen.unixTimestampInSec(toPrepare.ballot);
                PaxosState state = SystemKeyspace.loadPaxosState(toPrepare.update.partitionKey(), toPrepare.update.metadata(), nowInSec, false);
                assert state.accepted != null;
                if (toPrepare.isAfter(state.promised))
                {
                    Tracing.trace("Promising ballot {}", toPrepare.ballot);
                    SystemKeyspace.savePaxosPromise(toPrepare.update.partitionKey(), toPrepare.update.metadata(), toPrepare.ballot);
                    return new PrepareResponse(true, state.accepted, state.committed);
                }
                else
                {
                    Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPrepare, state.promised);
                    // return the currently promised ballot (not the last accepted one) so the coordinator can make sure it uses newer ballot next time (#5667)
                    return new PrepareResponse(false, new Commit(state.promised, toPrepare.update), state.committed);
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        finally
        {
            Keyspace.open(toPrepare.update.metadata().ksName).getColumnFamilyStore(toPrepare.update.metadata().cfId).metric.casPrepare.addNano(System.nanoTime() - start);
        }

    }

    public static Boolean legacyPropose(Commit proposal)
    {
        long start = System.nanoTime();
        try
        {
            Lock lock = LOCKS.get(proposal.update.partitionKey());
            lock.lock();
            try
            {
                int nowInSec = UUIDGen.unixTimestampInSec(proposal.ballot);
                PaxosState state = SystemKeyspace.loadPaxosState(proposal.update.partitionKey(), proposal.update.metadata(), nowInSec, false);
                if (proposal.hasBallot(state.promised) || proposal.isAfter(state.promised))
                {
                    Tracing.trace("Accepting proposal {}", proposal);
                    SystemKeyspace.savePaxosProposal(proposal);
                    return true;
                }
                else
                {
                    Tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, state.promised);
                    return false;
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        finally
        {
            Keyspace.open(proposal.update.metadata().ksName).getColumnFamilyStore(proposal.update.metadata().cfId).metric.casPropose.addNano(System.nanoTime() - start);
        }
    }

    public static void completeFor(UUID cfId, Collection<Range<Token>> ranges, int before)
    {
        if (ranges == null || ranges.isEmpty())
        {
            Token min = DatabaseDescriptor.getPartitioner().getMinimumToken();
            ranges = Collections.singleton(new Range<>(min, min));
        }

    }
}
