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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOException;
import java.util.UUID;

import com.google.common.collect.Lists;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.db.partitions.PartitionUpdate.PartitionUpdateSerializer.*;
import static org.apache.cassandra.service.paxos.Commit.isAfter;

class PaxosRows
{
    private static final ColumnDefinition PROMISE = paxosColumn("in_progress_ballot", TimeUUIDType.instance);
    private static final ColumnDefinition PROPOSAL = paxosColumn("proposal_ballot", TimeUUIDType.instance);
    private static final ColumnDefinition PROPOSAL_UPDATE = paxosColumn("proposal", BytesType.instance);
    private static final ColumnDefinition PROPOSAL_VERSION = paxosColumn("proposal_version", Int32Type.instance);
    private static final ColumnDefinition COMMIT = paxosColumn("most_recent_commit_at", TimeUUIDType.instance);

    private PaxosRows() {}

    private static ColumnDefinition paxosColumn(String name, AbstractType<?> type)
    {
        return ColumnDefinition.regularDef(SystemKeyspace.NAME, SystemKeyspace.PAXOS, name, type);
    }

    static UUID getCfId(Row row)
    {
        return UUIDType.instance.compose(row.clustering().get(0));
    }

    private static UUID getBallot(Row row, ColumnDefinition cdef)
    {
        Cell cell = row.getCell(cdef);
        if (cell == null)
            return null;
        return TimeUUIDType.instance.compose(cell.value());
    }

    private static boolean proposalIsEmpty(Row row, DecoratedKey key)
    {
        try
        {
            Cell proposalVersionCell = row.getCell(PROPOSAL_VERSION);
            if (proposalVersionCell == null || !proposalVersionCell.value().hasRemaining())
                return true;
            int proposalVersion = ByteBufferUtil.toInt(proposalVersionCell.value());

            Cell proposal = row.getCell(PROPOSAL_UPDATE);
            if (proposal == null || !proposal.value().hasRemaining())
                return true;
            return isEmpty(proposal.value(), proposalVersion, DeserializationHelper.Flag.LOCAL, key);
        }
        catch (IOException e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            throw new RuntimeException(e);
        }
    }

    private static long getTimestamp(Row row, ColumnDefinition cdef)
    {
        Cell cell = row.getCell(cdef);
        if (cell == null || !cell.value().hasRemaining())
            return Long.MIN_VALUE;
        return cell.timestamp();
    }

    static PaxosKeyState getCommitState(DecoratedKey key, Row row, UUID targetCfId)
    {
        if (row == null)
            return null;

        UUID cfId = getCfId(row);

        if (targetCfId != null && !targetCfId.equals(cfId))
            return null;

        UUID promise = getBallot(row, PROMISE);
        UUID proposal = getBallot(row, PROPOSAL);
        UUID commit = getBallot(row, COMMIT);

        UUID inProgress = null;
        UUID committed = null;
        if (isAfter(promise, proposal))
        {
            if (isAfter(promise, commit))
                inProgress = promise;
            else
                committed = commit;
        }
        else if (isAfter(proposal, commit))
        {
            if (proposalIsEmpty(row, key))
                committed = proposal;
            else
                inProgress = proposal;
        }
        else
        {
            committed = commit;
        }

        return inProgress != null ?
               new PaxosKeyState(cfId, key, inProgress, false) :
               new PaxosKeyState(cfId, key, committed, true);
    }

    private static class KeyCommitStateIterator extends AbstractIterator<PaxosKeyState> implements CloseableIterator<PaxosKeyState>
    {
        private final UnfilteredPartitionIterator partitions;
        private UnfilteredRowIterator partition;
        private final UUID cfId;

        private KeyCommitStateIterator(UnfilteredPartitionIterator partitions, UUID cfId)
        {
            this.partitions = partitions;
            this.cfId = cfId;
        }

        protected PaxosKeyState computeNext()
        {
            while (true)
            {
                if (partition != null && partition.hasNext())
                {
                    PaxosKeyState commitState = PaxosRows.getCommitState(partition.partitionKey(),
                                                                         (Row) partition.next(),
                                                                         cfId);
                    if (commitState == null)
                        continue;

                    return commitState;
                }
                else if (partition != null)
                {
                    partition.close();
                    partition = null;
                }

                if (partitions.hasNext())
                {
                    partition = partitions.next();
                }
                else
                {
                    partitions.close();
                    return endOfData();
                }
            }
        }

        public void close()
        {
            if (partition != null)
                partition.close();
            partitions.close();
        }
    }

    static CloseableIterator<PaxosKeyState> toIterator(UnfilteredPartitionIterator partitions, UUID cfId, boolean forFlush)
    {
        CloseableIterator<PaxosKeyState> iter = new KeyCommitStateIterator(partitions, cfId);
        if (forFlush)
            return iter;

        try
        {
            // eagerly materialize key states for repairs so we're not referencing memtables for the entire repair
            return CloseableIterator.wrap(Lists.newArrayList(iter).iterator());
        }
        finally
        {
            iter.close();
        }
    }

    static UUID getHighBallot(Row row, UUID current)
    {
        long maxBallot = current != null ? UUIDGen.microsTimestamp(current) : Long.MIN_VALUE;
        ColumnDefinition maxCDef = null;

        long inProgress = getTimestamp(row, PROMISE);
        if (inProgress > maxBallot)
        {
            maxBallot = inProgress;
            maxCDef = PROMISE;
        }

        long proposal = getTimestamp(row, PROPOSAL);
        if (proposal > maxBallot)
        {
            maxBallot = proposal;
            maxCDef = PROPOSAL;
        }

        long commit = getTimestamp(row, COMMIT);
        if (commit > maxBallot)
            maxCDef = COMMIT;

        return maxCDef == null ? current : getBallot(row, maxCDef);
    }
}
