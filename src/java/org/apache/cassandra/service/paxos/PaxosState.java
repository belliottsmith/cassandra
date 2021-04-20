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
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.paxos.Commit.Accepted;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.paxos.uncommitted.PaxosBallotTracker;
import org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTracker;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.config.Config.PaxosVariant.legacy;
import static org.apache.cassandra.config.Config.PaxosVariant.legacy_fixed;
import static org.apache.cassandra.db.SystemKeyspace.loadPaxosState;
import static org.apache.cassandra.service.paxos.Commit.*;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.utils.concurrent.WaitManager.Global.waits;

/**
 * We save to memory the result of each operation before persisting to disk, however each operation that performs
 * the update does not return a result to the coordinator until the result is fully persisted.
 */
public class PaxosState implements AutoCloseable
{
    public static final ConcurrentHashMap<Key, PaxosState> ACTIVE = new ConcurrentHashMap<>();
    public static final ConcurrentLinkedHashMap<Key, Snapshot> RECENT = new ConcurrentLinkedHashMap.Builder<Key, Snapshot>()
            .weigher(s -> Ints.saturatedCast(
                    (s.accepted != null ? s.accepted.update.unsharedHeapSize() : 0L) + s.committed.update.unsharedHeapSize()))
            .maximumWeightedCapacity(DatabaseDescriptor.getPaxosCacheSizeInMB() << 20)
            .build();

    private static class TrackerHandle
    {
        static final PaxosUncommittedTracker uncommittedInstance;
        static final PaxosBallotTracker ballotInstance;

        static
        {
            File directory = new File(Directories.dataDirectories[0].location, "paxos");
            FileUtils.createDirectory(directory);
            uncommittedInstance = PaxosUncommittedTracker.load(directory);
            try
            {
                ballotInstance = PaxosBallotTracker.load(directory);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static PaxosUncommittedTracker uncommittedTracker()
    {
        return TrackerHandle.uncommittedInstance;
    }

    public static PaxosBallotTracker ballotTracker()
    {
        return TrackerHandle.ballotInstance;
    }

    public static void initializeTrackers()
    {
        Preconditions.checkState(TrackerHandle.uncommittedInstance != null);
        Preconditions.checkState(TrackerHandle.ballotInstance != null);
    }

    public static class Key
    {
        final DecoratedKey partitionKey;
        final CFMetaData metadata;

        public Key(DecoratedKey partitionKey, CFMetaData metadata)
        {
            this.partitionKey = partitionKey;
            this.metadata = metadata;
        }

        public int hashCode()
        {
            return partitionKey.hashCode() * 31 + metadata.cfId.hashCode();
        }

        public boolean equals(Object that)
        {
            return that instanceof Key && equals((Key) that);
        }

        public boolean equals(Key that)
        {
            return this.partitionKey.equals(that.partitionKey)
                    && this.metadata.cfId.equals(that.metadata.cfId);
        }
    }

    public static class Snapshot
    {
        public final @Nonnull  UUID      promised; // TODO: logically apply delete here too, making it null when < accepted or committed?
        public final @Nullable Accepted  accepted; // if already committed, this will be null
        public final @Nonnull  Committed committed;

        public Snapshot(@Nonnull UUID promised, @Nullable Accepted accepted, @Nonnull Committed committed)
        {
            assert accepted == null || accepted.update.partitionKey().equals(committed.update.partitionKey());
            assert accepted == null || accepted.update.metadata().cfId.equals(committed.update.metadata().cfId);
            assert accepted == null || committed.isBefore(accepted.ballot);

            this.promised = promised;
            this.accepted = accepted;
            this.committed = committed;
        }

        public @Nonnull UUID latestWitnessed()
        {
            Commit proposal = accepted == null ? committed : accepted;
            // if proposal has same timestamp as promised, we should prefer accepted since (if different) it reached a quorum of promises
            UUID latest = proposal.isBefore(promised) ? promised : proposal.ballot;
            UUID lowerBound = ballotTracker().getLowBound();
            return isAfter(lowerBound, latest) ? lowerBound : latest;
        }

        public static Snapshot merge(Snapshot a, Snapshot b)
        {
            if (a == null || b == null)
                return a == null ? b : a;

            Committed committed = isAfter(a.committed, b.committed) ? a.committed : b.committed;
            if (a instanceof UnsafeSnapshot && b instanceof UnsafeSnapshot)
                return new UnsafeSnapshot(committed);

            Accepted accepted;
            UUID promised;
            if (a instanceof UnsafeSnapshot || b instanceof UnsafeSnapshot)
            {
                if (a instanceof UnsafeSnapshot)
                    a = b; // we already have the winning Committed saved above, so just want the full snapshot (if either)

                if (committed == a.committed)
                    return a;

                promised = a.promised;
                accepted = isAfter(a.accepted, committed) ? a.accepted : null;
            }
            else
            {
                accepted = isAfter(a.accepted, b.accepted) ? a.accepted : b.accepted;
                accepted = isAfter(accepted, committed) ? accepted : null;
                promised = isAfter(a.promised, b.promised) ? a.promised : b.promised;
            }

            return new Snapshot(promised, accepted, committed);
        }
    }

    // used to permit recording Committed outcomes without waiting for initial read
    public static class UnsafeSnapshot extends Snapshot
    {
        public UnsafeSnapshot(@Nonnull Committed committed)
        {
            super(Ballot.none(), null, committed);
        }

        public UnsafeSnapshot(@Nonnull Commit committed)
        {
            this(new Committed(committed.ballot, committed.update));
        }
    }

    @VisibleForTesting
    public static class PromiseResult
    {
        final Snapshot before;
        final Snapshot after;
        final UUID supersededBy;

        PromiseResult(Snapshot before, Snapshot after, UUID supersededBy)
        {
            this.before = before;
            this.after = after;
            this.supersededBy = supersededBy;
        }

        static PromiseResult promise(Snapshot before, Snapshot after)
        {
            return new PromiseResult(before, after, null);
        }

        static PromiseResult reject(Snapshot snapshot, UUID supersededBy)
        {
            return new PromiseResult(snapshot, snapshot, supersededBy);
        }

        public boolean isPromised()
        {
            return supersededBy == null;
        }

        public UUID supersededBy()
        {
            return supersededBy;
        }
    }

    private static final AtomicReferenceFieldUpdater<PaxosState, Snapshot> currentUpdater = AtomicReferenceFieldUpdater.newUpdater(PaxosState.class, Snapshot.class, "current");

    final Key key;
    private int active;
    private int total;
    private volatile Snapshot current;

    private PaxosState(Key key, Snapshot current)
    {
        this.key = key;
        this.current = current;
    }

    @VisibleForTesting
    public static PaxosState get(Commit commit)
    {
        return get(commit.update.partitionKey(), commit.update.metadata(), commit.ballot, 0);
    }

    @VisibleForTesting
    public static PaxosState get(DecoratedKey partitionKey, CFMetaData metadata, UUID ballot)
    {
        return get(partitionKey, metadata, ballot, 0);
    }

    static PaxosState get(DecoratedKey partitionKey, CFMetaData metadata, int nowInSec)
    {
        return get(partitionKey, metadata, null, nowInSec);
    }

    private static PaxosState get(DecoratedKey partitionKey, CFMetaData metadata, UUID ballot, int nowInSec)
    {
        // TODO would be nice to refactor verb handlers to support re-submitting to executor if waiting for another thread to read state
        return getUnsafe(partitionKey, metadata).maybeLoad(ballot, nowInSec);
    }

    private static PaxosState tryGetUnsafe(DecoratedKey partitionKey, CFMetaData metadata)
    {
        return ACTIVE.compute(new Key(partitionKey, metadata), (key, cur) -> {
            if (cur == null)
            {
                Snapshot saved = RECENT.remove(key);
                if (saved != null)
                    //noinspection resource
                    cur = new PaxosState(key, saved);
            }
            if (cur != null)
                ++cur.active;
            return cur;
        });
    }

    private static PaxosState getUnsafe(DecoratedKey partitionKey, CFMetaData metadata)
    {
        return ACTIVE.compute(new Key(partitionKey, metadata), (key, cur) -> {
            if (cur == null)
            {
                //noinspection resource
                cur = new PaxosState(key, RECENT.remove(key));
            }
            ++cur.active;
            ++cur.total;
            return cur;
        });
    }

    private PaxosState maybeLoad(UUID ballot, int nowInSec)
    {
        // CASSANDRA-12043 is not an issue for Apple Paxos, as we perform Commit+Prepare and PrepareRefresh
        // which are able to make progress whether or not the old commit is shadowed by the TTL (since they
        // depend only on the write being successful, not the data being read again later).
        // We still need to use a nowInSec for reading, so we just use that of the first ballot we see this time around,
        // which would anyway protect against C-12043 so long as there isn't perpetual competition on the partition.
        if (ballot != null)
            nowInSec = UUIDGen.unixTimestampInSec(ballot);

        try
        {
            Snapshot current = this.current;
            if (current == null || current instanceof UnsafeSnapshot)
            {
                synchronized (this)
                {
                    current = this.current;
                    if (current == null || current instanceof UnsafeSnapshot)
                    {
                        Snapshot snapshot = SystemKeyspace.loadPaxosState(key.partitionKey, key.metadata, nowInSec);
                        currentUpdater.accumulateAndGet(this, snapshot, Snapshot::merge);
                    }
                }
            }
        }
        catch (Throwable t)
        {
            try { close(); } catch (Throwable t2) { t.addSuppressed(t2); }
            throw t;
        }

        return this;
    }

    private void load(UUID ballot)
    {
        int nowInSec = UUIDGen.unixTimestampInSec(ballot);
        synchronized (this)
        {
            Snapshot snapshot = SystemKeyspace.loadPaxosState(key.partitionKey, key.metadata, nowInSec);
            currentUpdater.accumulateAndGet(this, snapshot, Snapshot::merge);
        }
    }

    public void close()
    {
        ACTIVE.compute(key, (key, cur) ->
        {
            assert cur != null;
            if (--cur.active > 0)
                return cur;

            RECENT.put(key, cur.current);
            return null;
        });
        int active = 1 + this.active, total = this.total;
        TableMetrics metrics = Keyspace.openAndGetStore(key.metadata).metric;
        metrics.casLocalConcurrency.update(active);
        metrics.casLocalRecentOverlaps.update(total);
    }

    Snapshot current()
    {
        Snapshot current = this.current;
        if (current == null || current.getClass() != Snapshot.class)
            throw new IllegalStateException();
        return current;
    }

    /**
     * Record the requested ballot as promised if it is newer than our current promise; otherwise do nothing.
     * @return a PromiseResult containing the before and after state for this operation
     */
    public PromiseResult promiseIfNewer(UUID ballot)
    {
        Snapshot before, after;
        while (true)
        {
            before = current;
            UUID latest = before.latestWitnessed();
            if (!isAfter(ballot, latest))
            {
                Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", ballot, latest);
                return PromiseResult.reject(before, latest);
            }

            after = new Snapshot(ballot, before.accepted, before.committed);
            if (currentUpdater.compareAndSet(this, before, after))
                break;
        }

        waits().nemesis();
        // It doesn't matter if a later operation witnesses this before it's persisted,
        // as it can only lead to rejecting a promise which leaves no persistent state
        // (and it's anyway safe to arbitrarily reject promises)
        Tracing.trace("Promising ballot {}", ballot);
        SystemKeyspace.savePaxosPromise(key.partitionKey, key.metadata, ballot);
        return PromiseResult.promise(before, after);
    }

    /**
     * Record an acceptance of the proposal if there is no newer promise; otherwise inform the caller of the newer ballot
     */
    public UUID acceptIfLatest(Proposal proposal)
    {
        // state.promised can be null, because it is invalidated by committed;
        // we may also have accepted a newer proposal than we promised, so we confirm that we are the absolute newest
        // (or that we have the exact same ballot as our promise, which is the typical case)
        Snapshot before, after;
        while (true)
        {
            before = current;
            UUID latest = before.latestWitnessed();
            if (!proposal.isSameOrAfter(latest))
            {
                Tracing.trace("Rejecting proposal for {} because latest is now {}", proposal, latest);
                return latest;
            }

            if (proposal.hasSameBallot(before.committed)) // TODO: consider not answering
                return null; // no need to save anything, or indeed answer at all

            after = new Snapshot(before.promised, proposal.accepted(), before.committed);
            if (currentUpdater.compareAndSet(this, before, after))
                break;
        }

        waits().nemesis();
        // It is more worrisome to permit witnessing an accepted proposal before we have persisted it
        // because this has more tangible effects on the recipient, but again it is safe: either it is
        //  - witnessed to reject (which is always safe, as it prevents rather than creates an outcome); or
        //  - witnessed as an in progress proposal
        // in the latter case, for there to be any effect on the state the proposal must be re-proposed, or not,
        // on its own terms, and must
        // be persisted by the re-proposer, and so it remains a non-issue
        // though this
        Tracing.trace("Accepting proposal {}", proposal);
        SystemKeyspace.savePaxosProposal(proposal);
        return null;
    }

    public void commit(Agreed commit)
    {
        applyCommit(commit, this, (apply, to) ->
            currentUpdater.accumulateAndGet(to, new UnsafeSnapshot(apply), Snapshot::merge)
        );
    }

    public static void commitDirect(Commit commit)
    {
        applyCommit(commit, null, (apply, ignore) -> {
            try (PaxosState state = tryGetUnsafe(apply.update.partitionKey(), apply.update.metadata()))
            {
                if (state != null)
                    currentUpdater.accumulateAndGet(state, new UnsafeSnapshot(apply), Snapshot::merge);
            }
        });
    }

    private static void applyCommit(Commit commit, PaxosState state, BiConsumer<Commit, PaxosState> postCommit)
    {
        long start = System.nanoTime();
        try
        {
            // The table may have been truncated since the proposal was initiated. In that case, we
            // don't want to perform the mutation and potentially resurrect truncated data
            if (UUIDGen.unixTimestamp(commit.ballot) >= SystemKeyspace.getTruncatedAt(commit.update.metadata().cfId))
            {
                Tracing.trace("Committing proposal {}", commit);
                Mutation mutation = commit.makeMutation();
                Keyspace.open(mutation.getKeyspaceName()).apply(mutation, true);
            }
            else
            {
                Tracing.trace("Not committing proposal {} as ballot timestamp predates last truncation time", commit);
            }

            waits().nemesis();
            // for commits we save to disk first, because we can; even here though it is safe to permit later events to
            // witness the state before it is persisted. The only tricky situation is that we use the witnessing of
            // a quorum of nodes having witnessed the latest commit to decide if we need to disseminate a commit
            // again before proceeding with any new operation, but in this case we have already persisted the relevant
            // information, namely the base table mutation.  So this fact is persistent, even if knowldge of this fact
            // is not (and if this is lost, it may only lead to a future operation unnecessarily committing again)
            SystemKeyspace.savePaxosCommit(commit);
            waits().nemesis();
            postCommit.accept(commit, state);
        }
        finally
        {
            Keyspace.open(commit.update.metadata().ksName).getColumnFamilyStore(commit.update.metadata().cfId).metric.casCommit.addNano(System.nanoTime() - start);
        }
    }

    public static PrepareResponse legacyPrepare(Commit toPrepare)
    {
        long start = System.nanoTime();
        try (PaxosState unsafeState = get(toPrepare))
        {
            synchronized (unsafeState)
            {
                // When preparing, we need to use the same time as "now" (that's the time we use to decide if something
                // is expired or not) across nodes otherwise we may have a window where a Most Recent Commit shows up
                // on some replica and not others during a new proposal (in StorageProxy.beginAndRepairPaxos()), and no
                // amount of re-submit will fix this (because the node on which the commit has expired will have a
                // tombstone that hides any re-submit). See CASSANDRA-12043 for details.
                Config.PaxosVariant variant = Paxos.getPaxosVariant();
                if (variant == legacy)
                    unsafeState.load(toPrepare.ballot);
                else
                    unsafeState.maybeLoad(toPrepare.ballot, 0);

                while (true)
                {
                    // ignore nowInSec when merging as this can only be an issue during the transition period, so the unbounded
                    // problem of CASSANDRA-12043 is not an issue
                    Snapshot before = unsafeState.current;
                    UUID latest = variant == legacy_fixed ? before.latestWitnessed() : before.promised;
                    if (toPrepare.isAfter(latest))
                    {
                        Snapshot after = new Snapshot(toPrepare.ballot, before.accepted, before.committed);
                        if (currentUpdater.compareAndSet(unsafeState, before, after))
                        {
                            Tracing.trace("Promising ballot {}", toPrepare.ballot);
                            DecoratedKey partitionKey = toPrepare.update.partitionKey();
                            CFMetaData metadata = toPrepare.update.metadata();
                            SystemKeyspace.savePaxosPromise(partitionKey, metadata, toPrepare.ballot);
                            return new PrepareResponse(true, before.accepted == null ? Accepted.none(partitionKey, metadata) : before.accepted, before.committed);
                        }
                    }
                    else
                    {
                        Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPrepare, before.promised);
                        // return the currently promised ballot (not the last accepted one) so the coordinator can make sure it uses newer ballot next time (#5667)
                        return new PrepareResponse(false, new Commit(before.promised, toPrepare.update), before.committed);
                    }
                }
            }
        }
        finally
        {
            Keyspace.openAndGetStore(toPrepare.update.metadata()).metric.casPrepare.addNano(System.nanoTime() - start);
        }
    }

    public static Boolean legacyPropose(Commit proposal)
    {
        long start = System.nanoTime();
        try (PaxosState unsafeState = get(proposal))
        {
            synchronized (unsafeState)
            {
                Config.PaxosVariant variant = Paxos.getPaxosVariant();
                if (variant == legacy)
                    unsafeState.load(proposal.ballot);
                else
                    unsafeState.maybeLoad(proposal.ballot, 0);

                while (true)
                {
                    // ignore nowInSec when merging as this can only be an issue during the transition period, so the unbounded
                    // problem of CASSANDRA-12043 is not an issue
                    Snapshot before = unsafeState.current;
                    boolean accept = variant == legacy_fixed
                            ? proposal.isSameOrAfter(before.latestWitnessed())
                            : proposal.hasBallot(before.promised) || proposal.isAfter(before.promised);

                    if (accept)
                    {
                        // maintain legacy (broken) semantics of accepting proposal older than committed without breaking contract for Apple Paxos
                        boolean acceptWithLegacyBug = variant != legacy_fixed && !proposal.isAfter(before.committed);
                        if (acceptWithLegacyBug || currentUpdater.compareAndSet(unsafeState, before, new Snapshot(before.promised, new Accepted(proposal), before.committed)))
                        {
                            Tracing.trace("Accepting proposal {}", proposal);
                            SystemKeyspace.savePaxosProposal(proposal);
                            return true;
                        }
                    }
                    else
                    {
                        Tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, before.promised);
                        return false;
                    }
                }
            }
        }
        finally
        {
            Keyspace.openAndGetStore(proposal.update.metadata()).metric.casPropose.addNano(System.nanoTime() - start);
        }
    }

    public static void unsafeReset()
    {
        ACTIVE.clear();
        RECENT.clear();
        ballotTracker().truncate();
    }
}
