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

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Callables;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * A 2i implementation made specifically for system.paxos that listens for changes to paxos state by interpreting
 * mutations against system.paxos and updates the uncommitted tracker accordingly.
 *
 * No read expressions are supported by the index.
 *
 * This is implemented as a 2i so it can piggy back off the commit log and paxos table flushes, and avoid worrying
 * about implementing a parallel log/flush system for the tracker and potential bugs there. It also means we don't
 * have to worry about cases where the tracker can become out of sync with the paxos table due to failure/edge cases
 * outside of the PaxosTableState class itself.
 */
public class PaxosUncommittedIndex implements Index, PaxosUncommittedTracker.UpdateSupplier
{
    public final ColumnFamilyStore baseCfs;
    private final PaxosUncommittedTracker tracker = PaxosState.tracker();
    protected IndexMetadata metadata;

    private static final Collection<Range<Token>> FULL_RANGE = Collections.singleton(new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                                                                 DatabaseDescriptor.getPartitioner().getMinimumToken()));
    private final ColumnFilter memtableColumnFilter;

    public PaxosUncommittedIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
    {
        Preconditions.checkState(baseCfs.metadata.ksName.equals(SystemKeyspace.NAME));
        Preconditions.checkState(baseCfs.metadata.cfName.equals(SystemKeyspace.PAXOS));

        this.baseCfs = baseCfs;
        this.metadata = metadata;

        this.memtableColumnFilter = ColumnFilter.all(baseCfs.metadata);
        PaxosState.tracker().setUpdateSupplier(this);
    }

    public static IndexMetadata indexMetadata()
    {
        Map<String, String> options = new HashMap<>();
        options.put("class_name", PaxosUncommittedIndex.class.getName());
        options.put("target", "n/a");
        return IndexMetadata.fromSchemaMetadata("PaxosUncommittedIndex", IndexMetadata.Kind.CUSTOM, options);
    }

    public static Indexes indexes()
    {
        return Indexes.builder().add(indexMetadata()).build();
    }

    public Callable<?> getInitializationTask()
    {
        return Callables.returning(null);
    }

    public IndexMetadata getIndexMetadata()
    {
        return metadata;
    }

    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return Callables.returning(null);
    }

    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    private CloseableIterator<PaxosKeyState> getPaxosUpdates(boolean forFlush, Collection<Range<Token>> ranges, UUID cfId)
    {
        Preconditions.checkArgument((cfId == null) == forFlush);

        View view = baseCfs.getTracker().getView();
        int now = FBUtilities.nowInSeconds();

        List<Memtable> live = view.liveMemtables;
        List<Memtable> flushing = view.flushingMemtables;

        int numMemtables = flushing.size() + (forFlush ? 0 : live.size());
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(numMemtables * ranges.size());

        for (Range<Token> range: ranges)
        {
            DataRange dataRange = DataRange.forTokenRange(range);
            for (int i=0, isize=flushing.size(); i<isize; i++)
                iters.add(flushing.get(i).makePartitionIterator(memtableColumnFilter, dataRange, false));

            if (!forFlush)
            {
                for (int i=0, isize=live.size(); i<isize; i++)
                    iters.add(live.get(i).makePartitionIterator(memtableColumnFilter, dataRange, false));
            }
        }

        return PaxosRows.toIterator(UnfilteredPartitionIterators.merge(iters, now, UnfilteredPartitionIterators.MergeListener.NOOP), cfId, forFlush);
    }

    public CloseableIterator<PaxosKeyState> getIterator(UUID cfId, Collection<Range<Token>> ranges)
    {
        return getPaxosUpdates(false, ranges, cfId);
    }

    public CloseableIterator<PaxosKeyState> flushIterator()
    {
        return getPaxosUpdates(true, FULL_RANGE, null);
    }

    public Callable<?> getBlockingFlushTask()
    {
        return (Callable<Object>) () -> {
            tracker.flushUpdates();
            return null;
        };
    }

    public Callable<?> getInvalidateTask()
    {
        return (Callable<Object>) () -> {
            tracker.truncate();
            return null;
        };
    }

    public Callable<?> getTruncateTask(long truncatedAt)
    {
        return (Callable<Object>) () -> {
            tracker.truncate();
            return null;
        };
    }

    public boolean shouldBuildBlocking()
    {
        return false;
    }

    public boolean dependsOn(ColumnDefinition column)
    {
        return false;
    }

    public boolean supportsExpression(ColumnDefinition column, Operator operator)
    {
        // should prevent this from ever being used
        return false;
    }

    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return null;
    }

    public long getEstimatedResultRows()
    {
        return 0;
    }

    public void validate(PartitionUpdate update) throws InvalidRequestException
    {

    }

    public Indexer indexerFor(DecoratedKey key, PartitionColumns columns, int nowInSec, OpOrder.Group opGroup, IndexTransaction.Type transactionType)
    {
        return null;
    }

    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command)
    {
        return null;
    }

    public Searcher searcherFor(ReadCommand command)
    {
        throw new UnsupportedOperationException();
    }
}
