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
package org.apache.cassandra.db;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class ReadOrderGroup implements AutoCloseable
{
    private static final long NO_SAMPLING = Long.MIN_VALUE;

    // For every reads
    private final OpOrder.Group baseOp;
    private final CFMetaData baseMetadata;

    // For index reads
    private final OpOrder.Group indexOp;
    private final OpOrder.Group writeOp;
    private final ReadCommand command;
    static Clock clock = Clock.instance;

    private final long createdAtNanos; // Only used while sampling

    private ReadOrderGroup(ReadCommand command,
                           OpOrder.Group baseOp,
                           CFMetaData baseMetadata,
                           OpOrder.Group indexOp,
                           OpOrder.Group writeOp,
                           long createdAtNanos)
    {
        this.baseOp = baseOp;
        this.baseMetadata = baseMetadata;
        this.indexOp = indexOp;
        this.writeOp = writeOp;
        this.command = command;
        this.createdAtNanos = createdAtNanos;
    }

    public OpOrder.Group baseReadOpOrderGroup()
    {
        return baseOp;
    }

    public OpOrder.Group indexReadOpOrderGroup()
    {
        return indexOp;
    }

    public OpOrder.Group writeOpOrderGroup()
    {
        return writeOp;
    }

    public static ReadOrderGroup emptyGroup()
    {
        return new ReadOrderGroup(null, null, null, null, null, NO_SAMPLING);
    }

    @SuppressWarnings("resource") // ops closed during group close
    public static ReadOrderGroup forCommand(ReadCommand command)
    {
        ColumnFamilyStore baseCfs = Keyspace.openAndGetStore(command.metadata());
        ColumnFamilyStore indexCfs = maybeGetIndexCfs(baseCfs, command);

        long createdAtNanos = baseCfs.metric.topLocalReadQueryTime.isEnabled() ? clock.nanoTime() : NO_SAMPLING;

        if (indexCfs == null)
        {
            return new ReadOrderGroup(command, baseCfs.readOrdering.start(), baseCfs.metadata, null, null, createdAtNanos);
        }
        else
        {
            OpOrder.Group baseOp = null, indexOp = null, writeOp = null;
            // OpOrder.start() shouldn't fail, but better safe than sorry.
            try
            {
                baseOp = baseCfs.readOrdering.start();
                indexOp = indexCfs.readOrdering.start();
                // TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try* to delete stale entries, without blocking if there's no room
                // as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room being made
                writeOp = Keyspace.writeOrder.start();
                return new ReadOrderGroup(command, baseOp, baseCfs.metadata, indexOp, writeOp, createdAtNanos);
            }
            catch (RuntimeException e)
            {
                // Note that must have writeOp == null since ReadOrderGroup ctor can't fail
                assert writeOp == null;
                try
                {
                    if (baseOp != null)
                        baseOp.close();
                }
                finally
                {
                    if (indexOp != null)
                        indexOp.close();
                }
                throw e;
            }
        }
    }

    private static ColumnFamilyStore maybeGetIndexCfs(ColumnFamilyStore baseCfs, ReadCommand command)
    {
        Index index = command.getIndex(baseCfs);
        return index == null ? null : index.getBackingTable().orElse(null);
    }

    public void close()
    {
        try
        {
            if (baseOp != null)
                baseOp.close();
        }
        finally
        {
            if (indexOp != null)
            {
                try
                {
                    indexOp.close();
                }
                finally
                {
                    writeOp.close();
                }
            }
        }
        if (createdAtNanos != NO_SAMPLING)
            addSample();
    }

    private void addSample()
    {
        String cql = command.toCQLString();
        int timeMicros = (int) Math.min(TimeUnit.NANOSECONDS.toMicros(clock.nanoTime() - createdAtNanos), Integer.MAX_VALUE);
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(baseMetadata.cfId);
        if (cfs != null)
            cfs.metric.topLocalReadQueryTime.addSample(cql, timeMicros);
    }
}
