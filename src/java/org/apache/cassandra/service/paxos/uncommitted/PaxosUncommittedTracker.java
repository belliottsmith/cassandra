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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Tracks uncommitted paxos operations to enable operation completion as part of repair by returning an iterator of
 * partition keys with uncommitted paxos operations (and their consistency levels) for a given table and token range(s)
 *
 * There are 2 parts to the uncommitted states it tracks: operations flushed to disk, and updates still in memory. This
 * class handles merging these two sources for queries and for merging states as part of flush. In practice, in memory
 * updates are the contents of the system.paxos memtables, although this has been generalized into an "UpdateSupplier"
 * interface to accomodate testing.
 */
public class PaxosUncommittedTracker
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosUncommittedTracker.class);
    private static final Range<Token> FULL_RANGE = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                               DatabaseDescriptor.getPartitioner().getMinimumToken());

    interface UpdateSupplier
    {
        CloseableIterator<PaxosKeyState> getIterator(UUID cfId, Collection<Range<Token>> ranges);
        CloseableIterator<PaxosKeyState> flushIterator();
    }

    private final File dataDirectory;
    private volatile ImmutableMap<UUID, UncommittedKeyFileContainer> tableStates;
    private volatile UpdateSupplier updateSupplier;

    public PaxosUncommittedTracker(File dataDirectory, ImmutableMap<UUID, UncommittedKeyFileContainer> tableStates)
    {
        this.dataDirectory = dataDirectory;
        this.tableStates = tableStates;
    }

    public PaxosUncommittedTracker(File dataDirectory)
    {
        this(dataDirectory, ImmutableMap.of());
    }

    public void setUpdateSupplier(UpdateSupplier updateSupplier)
    {
        Preconditions.checkArgument(updateSupplier != null);
        this.updateSupplier = updateSupplier;
    }

    public static PaxosUncommittedTracker load(File dataDirectory)
    {
        ImmutableMap.Builder<UUID, UncommittedKeyFileContainer> builder = ImmutableMap.builder();
        for (UUID cfid : UncommittedKeyFile.listCfids(dataDirectory))
        {
            builder.put(cfid, UncommittedKeyFileContainer.load(dataDirectory, cfid));
        }

        return new PaxosUncommittedTracker(dataDirectory, builder.build());
    }

    @VisibleForTesting
    UncommittedKeyFileContainer getOrCreateTableState(UUID cfId)
    {
        UncommittedKeyFileContainer state = tableStates.get(cfId);
        if (state == null)
        {
            synchronized (this)
            {
                state = tableStates.get(cfId);
                if (state != null)
                    return state;

                state = new UncommittedKeyFileContainer(cfId, dataDirectory, null);
                tableStates = ImmutableMap.<UUID, UncommittedKeyFileContainer>builder()
                              .putAll(tableStates).put(cfId, state)
                              .build();
            }
        }
        return state;
    }

    void flushUpdates() throws IOException
    {
        Map<UUID, UncommittedKeyFileContainer.FlushWriter> flushWriters = new HashMap<>();
        try (CloseableIterator<PaxosKeyState> iterator = updateSupplier.flushIterator())
        {
            while (iterator.hasNext())
            {
                PaxosKeyState next = iterator.next();
                UncommittedKeyFileContainer.FlushWriter writer = flushWriters.get(next.cfId);
                if (writer == null)
                {
                    writer = getOrCreateTableState(next.cfId).createFlushWriter();
                    flushWriters.put(next.cfId, writer);
                }
                writer.update(next);
            }
        }
        catch (Throwable t)
        {
            for (UncommittedKeyFileContainer.FlushWriter writer : flushWriters.values())
                t = writer.abort(t);
            throw new IOException(t);
        }

        for (UncommittedKeyFileContainer.FlushWriter writer : flushWriters.values())
            writer.finish();
    }

    @VisibleForTesting
    UncommittedKeyFileContainer getTableState(UUID cfId)
    {
        return tableStates.get(cfId);
    }

    public CloseableIterator<UncommittedPaxosKey> uncommittedKeyIterator(UUID cfId, Collection<Range<Token>> ranges, UUID before)
    {
        ranges = (ranges == null || ranges.isEmpty()) ? Collections.singleton(FULL_RANGE) : Range.normalize(ranges);
        CloseableIterator<PaxosKeyState> updates = updateSupplier.getIterator(cfId, ranges);

        UncommittedKeyFileContainer state = tableStates.get(cfId);
        if (state == null)
            return PaxosKeyState.toUncommittedInfo(updates);

        CloseableIterator<PaxosKeyState> fileIter = state.iterator(ranges);
        CloseableIterator<PaxosKeyState> merged = PaxosKeyState.mergeUncommitted(before, updates, fileIter);

        return PaxosKeyState.toUncommittedInfo(merged);
    }

    synchronized void truncate()
    {
        logger.info("truncating paxos uncommitted info");
        tableStates.values().forEach(UncommittedKeyFileContainer::truncate);
        tableStates = ImmutableMap.of();
    }
}
