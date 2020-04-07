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

import java.util.UUID;
import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDGen;

public class PaxosState
{
    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    final Commit promised;
    final Commit accepted;
    final Commit committed;

    public static PaxosState empty(DecoratedKey key, CFMetaData metadata)
    {
        Commit empty = Commit.emptyCommit(key, metadata);
        return new PaxosState(empty, empty, empty);
    }

    public PaxosState(Commit promised, Commit accepted, Commit committed)
    {
        assert promised.update.partitionKey().equals(accepted.update.partitionKey()) && accepted.update.partitionKey().equals(committed.update.partitionKey());
        assert promised.update.metadata() == accepted.update.metadata() && accepted.update.metadata() == committed.update.metadata();

        this.promised = promised;
        this.accepted = accepted;
        this.committed = committed;
    }

    public static PrepareResponse legacyPrepare(Commit toPrepare)
    {
        PaxosState state = promiseIfNewer(toPrepare);
        if (toPrepare == state.promised)
            return new PrepareResponse(true, state.accepted, state.committed);
        else
            return new PrepareResponse(false, state.promised, state.committed);
    }

    /**
     * Record the requested ballot as promised if it is newer than our current promise; otherwise do nothing.
     * @return a PaxosState object representing the state after this operation completes
     */
    public static PaxosState promiseIfNewer(Commit toPromise)
    {
        long start = System.nanoTime();
        try
        {
            Lock lock = LOCKS.get(toPromise.update.partitionKey());
            lock.lock();
            try
            {
                // When preparing, we need to use the same time as "now" (that's the time we use to decide if something
                // is expired or not) accross nodes otherwise we may have a window where a Most Recent Commit shows up
                // on some replica and not others during a new proposal (in StorageProxy.beginAndRepairPaxos()), and no
                // amount of re-submit will fix this (because the node on which the commit has expired will have a
                // tombstone that hides any re-submit). See CASSANDRA-12043 for details.
                int nowInSec = UUIDGen.unixTimestampInSec(toPromise.ballot);
                PaxosState state = SystemKeyspace.loadPaxosState(toPromise.update.partitionKey(), toPromise.update.metadata(), nowInSec);
                if (toPromise.isAfter(state.promised))
                {
                    Tracing.trace("Promising ballot {}", toPromise.ballot);
                    SystemKeyspace.savePaxosPromise(toPromise);
                    return new PaxosState(toPromise, state.accepted, state.committed);
                }
                else
                {
                    Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPromise, state.promised);
                    // return the currently promised ballot (not the last accepted one) so the coordinator can make sure it uses newer ballot next time (#5667)
                    return state;
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        finally
        {
            Keyspace.open(toPromise.update.metadata().ksName).getColumnFamilyStore(toPromise.update.metadata().cfId).metric.casPrepare.addNano(System.nanoTime() - start);
        }
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
                PaxosState state = SystemKeyspace.loadPaxosState(proposal.update.partitionKey(), proposal.update.metadata(), nowInSec);
                if (proposal.hasBallot(state.promised.ballot) || proposal.isAfter(state.promised))
                {
                    Tracing.trace("Accepting proposal {}", proposal);
                    SystemKeyspace.savePaxosProposal(proposal);
                    return null;
                }
                else
                {
                    Tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, state.promised);
                    return state.promised.ballot;
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
}
