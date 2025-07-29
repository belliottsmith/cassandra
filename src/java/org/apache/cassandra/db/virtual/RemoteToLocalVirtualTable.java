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

package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.function.Function;

import accord.utils.Invariants;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.concurrent.SyncPromise;

import static org.apache.cassandra.db.ClusteringBound.BOTTOM;
import static org.apache.cassandra.db.ClusteringBound.TOP;
import static org.apache.cassandra.db.ClusteringBound.boundKind;
import static org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts.ALLOW;

public class RemoteToLocalVirtualTable extends AbstractLazyVirtualTable
{
    private static final int MAX_CONCURRENCY = 8;
    final TableMetadata local;
    final boolean allowFilteringImplicitly;
    final boolean allowFilteringLocalPartitionKeysImplicitly;

    public RemoteToLocalVirtualTable(String keyspace, VirtualTable virtualTable)
    {
        super(wrap(keyspace, virtualTable.name(), virtualTable.metadata()), virtualTable.sorted());
        this.local = virtualTable.metadata();
        this.allowFilteringImplicitly = virtualTable.allowFilteringImplicitly();
        this.allowFilteringLocalPartitionKeysImplicitly = virtualTable.allowFilteringPrimaryKeysImplicitly();
    }

    @Override
    public boolean allowFilteringImplicitly()
    {
        return allowFilteringImplicitly;
    }

    @Override
    public boolean allowFilteringPrimaryKeysImplicitly()
    {
        return true;
    }

    private static TableMetadata wrap(String keyspace, String name, TableMetadata local)
    {
        if (local.partitionKeyColumns().size() != 1 && !(local.partitionKeyType instanceof CompositeType))
            throw new IllegalArgumentException("Underlying table must have a single partition key, else use CompositeType for its partitioner");
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, name);
        builder.partitioner(new LocalPartitioner(Int32Type.instance));
        builder.addPartitionKeyColumn("node_id", Int32Type.instance);
        for (ColumnMetadata cm : local.partitionKeyColumns())
            builder.addClusteringColumn(cm.name, cm.type, cm.getMask(), cm.getColumnConstraints());
        for (ColumnMetadata cm : local.clusteringColumns())
            builder.addClusteringColumn(cm.name, cm.type, cm.getMask(), cm.getColumnConstraints());
        // we don't add static columns as they can't be modelled correctly with the insertion of a prefix partition column
        for (ColumnMetadata cm : local.regularColumns())
        {
            if (!cm.isComplex())
                builder.addRegularColumn(cm.name, cm.type, cm.getMask(), cm.getColumnConstraints());
        }
        builder.kind(TableMetadata.Kind.VIRTUAL);
        return builder.build();
    }

    @Override
    protected void collect(PartitionsCollector collector)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        NavigableSet<NodeId> matchingIds = cm.directory.states.keySet();
        DataRange dataRange = collector.dataRange();
        AbstractBounds<PartitionPosition> bounds = dataRange.keyRange();
        {
            NodeId start = null;
            if (!bounds.left.isMinimum())
            {
                if (!(bounds.left instanceof DecoratedKey))
                    throw new InvalidRequestException(metadata + " does not support filtering by token or incomplete partition keys");
                start = new NodeId(Int32Type.instance.compose(((DecoratedKey) bounds.left).getKey()));
            }
            NodeId end = null;
            if (!bounds.right.isMaximum())
            {
                if (!(bounds.right instanceof DecoratedKey))
                    throw new InvalidRequestException(metadata + " does not support filtering by token or incomplete partition keys");
                end = new NodeId(Int32Type.instance.compose(((DecoratedKey) bounds.right).getKey()));
            }
            if (start != null && end != null) matchingIds = matchingIds.subSet(start, bounds.isStartInclusive(), end, bounds.isEndInclusive());
            else if (start != null) matchingIds = matchingIds.tailSet(start, bounds.isStartInclusive());
            else if (end != null) matchingIds = matchingIds.headSet(end, bounds.isEndInclusive());
        }
        if (dataRange.isReversed())
            matchingIds = matchingIds.descendingSet();

        RowFilter rowFilter = rebind(local, collector.rowFilter());
        ColumnFilter columnFilter = collector.columnFilter().rebind(local);
        // TODO (expected): count this down as we progress where possible (or have AbstractLazyVirtualTable do it for us)
        DataLimits limits = collector.limits();

        Function<DecoratedKey, ByteBuffer[]> pksToCks = partitionKeyToClusterings(metadata, local);
        ArrayDeque<RequestAndResponse> pending = new ArrayDeque<>();
        matchingIds.forEach(id -> {
            InetAddressAndPort endpoint = cm.directory.endpoint(id);
            DecoratedKey remoteKey = metadata.partitioner.decorateKey(Int32Type.instance.decompose(id.id()));
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(remoteKey);
            Slices slices = filter.getSlices(metadata);

            int i = 0, advance = 1, end = slices.size();
            if (dataRange.isReversed())
            {
                i = slices.size() - 1;
                end = -1;
                advance = -1;
            }

            PartitionCollector partition = collector.partition(id.id());
            while (i != end)
            {
                List<Request> request = rebind(local, slices.get(i), dataRange.isReversed(), rowFilter, columnFilter);
                for (Request send : request)
                {
                    ReadCommand readCommand;
                    if (send.dataRange.startKey().equals(send.dataRange.stopKey()) && !send.dataRange.startKey().isMinimum())
                        readCommand = SinglePartitionReadCommand.create(local, collector.nowInSeconds(), send.columnFilter, send.rowFilter, limits, (DecoratedKey) send.dataRange.startKey(), send.dataRange.clusteringIndexFilter(remoteKey), ALLOW);
                    else
                        readCommand = PartitionRangeReadCommand.create(local, collector.nowInSeconds(), send.columnFilter, send.rowFilter, limits, send.dataRange);

                    RequestAndResponse rr = new RequestAndResponse(partition, readCommand);
                    send(rr, endpoint);
                    pending.addLast(rr);

                    boolean selectsOneRow = selectsOneRow(local, send.dataRange, remoteKey);
                    while (pending.size() >= (selectsOneRow ? 1 : MAX_CONCURRENCY))
                        collect(collector, pending.pollFirst(), pksToCks);
                }
                i += advance;
            }
        });
        while (!pending.isEmpty())
            collect(collector, pending.pollFirst(), pksToCks);
    }

    private static class RequestAndResponse extends SyncPromise<ReadResponse>
    {
        final PartitionCollector partition;
        final ReadCommand readCommand;
        private RequestAndResponse(PartitionCollector partition, ReadCommand readCommand)
        {
            this.partition = partition;
            this.readCommand = readCommand;
        }
    }

    private static class Request
    {
        final DataRange dataRange;
        final RowFilter rowFilter;
        final ColumnFilter columnFilter;

        private Request(DataRange dataRange, RowFilter rowFilter, ColumnFilter columnFilter)
        {
            this.dataRange = dataRange;
            this.rowFilter = rowFilter;
            this.columnFilter = columnFilter;
        }
    }

    private void send(RequestAndResponse rr, InetAddressAndPort endpoint)
    {
        MessagingService.instance().sendWithCallback(Message.out(Verb.READ_REQ, rr.readCommand), endpoint, new RequestCallback<ReadResponse>()
        {
            @Override public void onResponse(Message<ReadResponse> msg) { rr.trySuccess(msg.payload); }
            @Override public boolean invokeOnFailure() { return true; }
            @Override public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                if (failure.failure == null) rr.tryFailure(new RuntimeException(failure.reason.toString()));
                else rr.tryFailure(failure.failure);
            }
        });
    }

    private void collect(PartitionsCollector collector, RequestAndResponse rr, Function<DecoratedKey, ByteBuffer[]> pksToCks)
    {
        if (!rr.awaitUntilThrowUncheckedOnInterrupt(collector.deadlineNanos()))
            throw new ReadTimeoutException(ConsistencyLevel.ONE, 0, 1, false);

        rr.rethrowIfFailed();
        int pkCount = local.partitionKeyColumns().size();
        ReadResponse response = Invariants.nonNull(rr.getNow());
        try (UnfilteredPartitionIterator partitions = response.makeIterator(rr.readCommand))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator iter = partitions.next())
                {
                    ByteBuffer[] clusterings = pksToCks.apply(iter.partitionKey());
                    while (iter.hasNext())
                    {
                        Unfiltered next = iter.next();
                        if (!next.isRow())
                            throw new UnsupportedOperationException("Range tombstones not supported");

                        Row row = (Row)next;
                        {
                            Clustering<?> clustering = row.clustering();
                            for (int j = 0 ; j < clustering.size(); ++j)
                                clusterings[pkCount + j] = clustering.bufferAt(j);
                        }
                        rr.partition.row((Object[])clusterings)
                                 .lazyAdd(columns -> {
                                     row.forEach(cd -> {
                                         Invariants.require(cd instanceof Cell);
                                         columns.add(cd.column().name.toString(), ((Cell<?>) cd).buffer());
                                     });
                                 });
                    }
                }
            }
        }

    }

    private static boolean selectsOneRow(TableMetadata metadata, DataRange dataRange, DecoratedKey key)
    {
        if (dataRange.startKey().isMinimum() || !dataRange.startKey().equals(dataRange.stopKey()))
            return false;

        if (metadata.clusteringColumns().isEmpty())
            return true;

        Slices slices = dataRange.clusteringIndexFilter(key).getSlices(metadata);
        if (slices.size() != 1)
            return false;

        Slice slice = slices.get(0);
        return slice.start().equals(slice.end());
    }

    private static Function<DecoratedKey, ByteBuffer[]> partitionKeyToClusterings(TableMetadata metadata, TableMetadata local)
    {
        ByteBuffer[] cks = new ByteBuffer[metadata.clusteringColumns().size()];
        if (local.partitionKeyColumns().size() == 1)
        {
            return pk -> {
                cks[0] = pk.getKey();
                return cks.clone();
            };
        }

        CompositeType type = (CompositeType) local.partitionKeyType;
        int pkCount = type.types.size();
        return (pk) -> {
            System.arraycopy(type.split(pk.getKey()), 0, cks, 0, pkCount);
            return cks.clone();
        };
    }

    private static RowFilter rebind(TableMetadata local, RowFilter rowFilter)
    {
        if (rowFilter.isEmpty())
            return rowFilter;

        RowFilter result = RowFilter.create(false, IndexHints.NONE);
        for (RowFilter.Expression in : rowFilter.getExpressions())
        {
            RowFilter.Expression out = in.rebind(local);
            if (out != null)
                result.add(out);
        }
        return result;
    }

    private List<Request> rebind(TableMetadata local, Slice slice, boolean reversed, RowFilter rowFilter, ColumnFilter columnFilter)
    {
        ClusteringBound<?> start = slice.start();
        ClusteringBound<?> end = slice.end();
        int pkCount = local.partitionKeyColumns().size();
        // TODO (expected): we can filter by partition key by inserting a new row filter, but need to impose ALLOW FILTERING restrictions
        if (((start.size() > 0 && start.size() < pkCount) || (end.size() > 0 && end.size() < pkCount)))
        {
            if (!allowFilteringLocalPartitionKeysImplicitly)
                throw new InvalidRequestException("Must specify full partition key bounds for the underlying table");

            List<ColumnMetadata> pks = local.partitionKeyColumns();
            ByteBuffer[] starts = start.getBufferArray();
            ByteBuffer[] ends = end.getBufferArray();

            int minCount = Math.min(start.size(), end.size());
            int maxCount = Math.max(start.size(), end.size());
            int commonPrefixLength = 0;
            while (commonPrefixLength < minCount && equalPart(start, end, commonPrefixLength))
                ++commonPrefixLength;

            RowFilter commonRowFilter = rowFilter;
            if (commonPrefixLength > 0)
            {
                commonRowFilter = copy(commonRowFilter);
                for (int i = 0 ; i < commonPrefixLength ; ++i)
                    commonRowFilter.add(pks.get(i), Operator.EQ, starts[i]);
            }

            Operator lastStartOp = start.isInclusive() ? Operator.GTE : Operator.GT;
            Operator lastEndOp = end.isInclusive() ? Operator.LTE : Operator.LT;
            if (commonPrefixLength == Math.max(minCount, maxCount - 1))
            {
                // can simply add our remaining filters and continue on our way
                addExpressions(commonRowFilter, pks, commonPrefixLength, starts, Operator.GTE, lastStartOp);
                addExpressions(commonRowFilter, pks, commonPrefixLength, ends, Operator.LTE, lastEndOp);
                return List.of(new Request(DataRange.allData(local.partitioner), commonRowFilter, columnFilter));
            }

            throw new InvalidRequestException("This table currently does not support the complex partial partition key filters implied for the underlying table");
        }

        ByteBuffer[] startBuffers = start.getBufferArray();
        PartitionPosition startBound;
        if (start.size() == 0) startBound = local.partitioner.getMinimumToken().minKeyBound();
        else if (pkCount == 1) startBound = local.partitioner.decorateKey(startBuffers[0]);
        else startBound = local.partitioner.decorateKey(CompositeType.build(ByteBufferAccessor.instance, Arrays.copyOf(startBuffers, pkCount)));

        ByteBuffer[] endBuffers = end.getBufferArray();
        PartitionPosition endBound;
        if (end.size() == 0) endBound = local.partitioner.getMinimumToken().maxKeyBound();
        else if (pkCount == 1) endBound = local.partitioner.decorateKey(endBuffers[0]);
        else endBound = local.partitioner.decorateKey(CompositeType.build(ByteBufferAccessor.instance, Arrays.copyOf(endBuffers, pkCount)));

        AbstractBounds<PartitionPosition> bounds = AbstractBounds.bounds(startBound, start.isEmpty() || start.size() > pkCount || start.isInclusive(),
                                                                         endBound, end.isEmpty() || end.size() > pkCount || end.isInclusive());
        boolean hasSlices = start.size() > pkCount || end.size() > pkCount;
        if (!hasSlices)
            return List.of(new Request(new DataRange(bounds, new ClusteringIndexSliceFilter(Slices.ALL, reversed)), rowFilter, columnFilter));

        ClusteringBound<?> startSlice = ClusteringBound.BOTTOM;
        if (start.size() > pkCount)
            startSlice = BufferClusteringBound.create(boundKind(true, start.isInclusive()), Arrays.copyOfRange(startBuffers, pkCount, startBuffers.length));

        ClusteringBound<?> endSlice = ClusteringBound.TOP;
        if (end.size() > pkCount)
            endSlice = BufferClusteringBound.create(boundKind(false, end.isInclusive()), Arrays.copyOfRange(startBuffers, pkCount, startBuffers.length));

        if (startBound.equals(endBound))
            return List.of(new Request(new DataRange(bounds, filter(local, startSlice, endSlice, reversed)), rowFilter, columnFilter));

        List<Request> result = new ArrayList<>(3);
        if (startSlice != BOTTOM)
        {
            AbstractBounds<PartitionPosition> startBoundOnly = AbstractBounds.bounds(startBound, true, startBound, true);
            result.add(new Request(new DataRange(startBoundOnly, filter(local, startSlice, TOP, reversed)), rowFilter, columnFilter));
        }
        result.add(new Request(new DataRange(AbstractBounds.bounds(bounds.left, bounds.inclusiveLeft() && startSlice == BOTTOM,
                                                       bounds.right, bounds.inclusiveRight() && endSlice == TOP),
                                 new ClusteringIndexSliceFilter(Slices.ALL, reversed)), rowFilter, columnFilter)
        );
        if (endSlice != TOP)
        {
            AbstractBounds<PartitionPosition> endBoundOnly = AbstractBounds.bounds(endBound, true, endBound, true);
            result.add(new Request(new DataRange(endBoundOnly, filter(local, BOTTOM, endSlice, reversed)), rowFilter, columnFilter));
        }
        if (reversed)
            Collections.reverse(result);
        return result;
    }

    private static boolean equalPart(ClusteringBound start, ClusteringBound end, int i)
    {
        return 0 == start.accessor().compare(start.get(i), end.get(i), end.accessor());
    }

    private static RowFilter copy(RowFilter copy)
    {
        RowFilter newRowFilter = RowFilter.create(false, IndexHints.NONE);
        for (RowFilter.Expression expression : copy)
            newRowFilter.add(expression);
        return newRowFilter;
    }

    private static void addExpressions(RowFilter rowFilter, List<ColumnMetadata> cms, int start, ByteBuffer[] values, Operator op, Operator lastOp)
    {
        for (int i = start ; i < values.length ; ++i)
            rowFilter.add(cms.get(i), i + 1 == values.length ? lastOp : op, values[i]);
    }

    private static ClusteringIndexSliceFilter filter(TableMetadata metadata, ClusteringBound<?> start, ClusteringBound<?> end, boolean reversed)
    {
        return new ClusteringIndexSliceFilter(Slices.with(metadata.comparator, Slice.make(start, end)), reversed);
    }
}
