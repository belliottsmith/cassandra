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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import javax.annotation.Nullable;

import accord.utils.Invariants;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

import static org.apache.cassandra.db.ClusteringPrefix.Kind.STATIC_CLUSTERING;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * An abstract virtual table implementation that builds the resultset on demand.
 */
public abstract class AbstractLazyVirtualTable implements VirtualTable
{
    // in the special case where we know we have enough rows in the collector, throw this exception to terminate early
    static class InternalDoneException extends RuntimeException {}

    public static class FilterRange<V>
    {
        final V min, max;
        public FilterRange(V min, V max)
        {
            this.min = min;
            this.max = max;
        }
    }

    public interface PartitionsCollector
    {
        DataRange dataRange();
        RowFilter rowFilter();
        RowCollector row(Object... primaryKeys);
        PartitionCollector partition(Object... partitionKeys);
        UnfilteredPartitionIterator finish();

        default @Nullable DecoratedKey singleKey()
        {
            AbstractBounds<?> bounds = dataRange().keyRange();
            if (!bounds.isStartInclusive() || !bounds.isEndInclusive() || !bounds.left.equals(bounds.right) || !(bounds.left instanceof DecoratedKey))
                return null;

            return (DecoratedKey) bounds.left;
        }

        <I, O> FilterRange<O> filters(String column, Function<I, O> translate, UnaryOperator<O> increment, UnaryOperator<O> decrement);
    }

    public interface PartitionCollector
    {
        RowCollector row(Object... clusteringKeys);
    }

    public interface RowCollector
    {
        default PartitionsCollector lazyAdd(Consumer<ColumnsCollector> addToIfNeeded) { return eagerAdd(addToIfNeeded); }
        PartitionsCollector eagerAdd(Consumer<ColumnsCollector> addToNow);
    }

    public interface ColumnsCollector
    {
        <V> ColumnsCollector add(String columnName, V value, Function<V, Object> transform);
        default ColumnsCollector add(String columnName, Object value) { return add(columnName, value, Function.identity()); }
    }

    public static class SimplePartitionsCollector implements PartitionsCollector
    {
        final TableMetadata metadata;
        final boolean isSorted;

        final Map<String, ColumnMetadata> columnLookup = new HashMap<>();
        final NavigableMap<DecoratedKey, SimplePartition> partitions;

        final DataRange dataRange;
        final ColumnFilter columnFilter;
        final RowFilter rowFilter;
        final DataLimits limits;

        final long startedAt = Clock.Global.nanoTime();
        final long timeoutAt;

        final long nowInSeconds = Clock.Global.nowInSeconds();
        final long timestamp;

        int totalRowCount;
        int lastFilteredTotalRowCount;

        @Override public DataRange dataRange() { return dataRange; }
        @Override public RowFilter rowFilter() { return rowFilter; }
        public ColumnFilter columnFilter() { return columnFilter; }

        public SimplePartitionsCollector(TableMetadata metadata, boolean isSorted, DataRange dataRange, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
        {
            this.metadata = metadata;
            this.isSorted = isSorted;
            this.dataRange = dataRange;
            this.columnFilter = columnFilter;
            this.rowFilter = rowFilter;
            this.limits = limits;
            this.timestamp = FBUtilities.timestampMicros();
            this.timeoutAt = startedAt + DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS);
            this.partitions = new TreeMap<>(dataRange.isReversed() ? DecoratedKey.comparator.reversed() : DecoratedKey.comparator);
            for (ColumnMetadata cm : metadata.columns())
                columnLookup.put(cm.name.toString(), cm);
        }

        @Override
        public PartitionCollector partition(Object ... partitionKeys)
        {
            int pkSize = metadata.partitionKeyColumns().size();
            if (pkSize != partitionKeys.length)
                throw new IllegalArgumentException();

            DecoratedKey partitionKey = makeDecoratedKey(partitionKeys);
            if (!dataRange.contains(partitionKey))
                return dropCks -> dropRow -> this;

            return partitions.computeIfAbsent(partitionKey, SimplePartition::new);
        }

        @Override
        public UnfilteredPartitionIterator finish()
        {
            final Iterator<SimplePartition> partitions = this.partitions.values().iterator();
            return new UnfilteredPartitionIterator()
            {
                @Override public TableMetadata metadata() { return metadata; }
                @Override public void close() {}

                @Override
                public boolean hasNext()
                {
                    return partitions.hasNext();
                }

                @Override
                public UnfilteredRowIterator next()
                {
                    SimplePartition partition = partitions.next();
                    Iterator<Row> rows = partition.rows();

                    return new UnfilteredRowIterator()
                    {
                        @Override public TableMetadata metadata() { return metadata; }
                        @Override public boolean isReverseOrder() { return dataRange.isReversed(); }
                        @Override public RegularAndStaticColumns columns() { return columnFilter.fetchedColumns(); }
                        @Override public DecoratedKey partitionKey() { return partition.key; }

                        @Override public Row staticRow() { return partition.staticRow(); }
                        @Override public boolean hasNext() { return rows.hasNext(); }
                        @Override public Unfiltered next() { return rows.next(); }

                        @Override public void close() {}
                        @Override public DeletionTime partitionLevelDeletion() { return DeletionTime.LIVE; }
                        @Override public EncodingStats stats() { return EncodingStats.NO_STATS; }
                    };
                }
            };
        }

        @Override
        @Nullable
        public <I, O> FilterRange<O> filters(String columnName, Function<I, O> translate, UnaryOperator<O> increment, UnaryOperator<O> decrement)
        {
            ColumnMetadata column = columnLookup.get(columnName);
            O min = null, max = null;
            for (RowFilter.Expression expression : rowFilter().getExpressions())
            {
                if (!expression.column().equals(column))
                    continue;

                O bound = translate.apply((I)column.type.compose(expression.getIndexValue()));
                switch (expression.operator())
                {
                    default: throw new InvalidRequestException("Operator " + expression.operator() + " not supported for txn_id");
                    case EQ: min = max = bound; break;
                    case LTE: max = bound; break;
                    case LT: max = decrement.apply(bound); break;
                    case GTE: min = bound; break;
                    case GT: min = increment.apply(bound); break;
                }
            }

            return new FilterRange<>(min, max);
        }

        @Override
        public RowCollector row(Object... primaryKeys)
        {
            int pkSize = metadata.partitionKeyColumns().size();
            int ckSize = metadata.clusteringColumns().size();
            if (pkSize + ckSize != primaryKeys.length)
                throw new IllegalArgumentException();

            Object[] partitionKeyValues = new Object[pkSize];
            Object[]   clusteringValues = new Object[ckSize];

            System.arraycopy(primaryKeys, 0, partitionKeyValues, 0, pkSize);
            System.arraycopy(primaryKeys, pkSize, clusteringValues, 0, ckSize);

            DecoratedKey partitionKey = makeDecoratedKey(partitionKeyValues);
            Clustering<?> clustering = makeClustering(clusteringValues);

            if (!dataRange.contains(partitionKey) || !dataRange.clusteringIndexFilter(partitionKey).selects(clustering))
                return drop -> this;

            return partitions.computeIfAbsent(partitionKey, SimplePartition::new).row();
        }

        private DecoratedKey makeDecoratedKey(Object... partitionKeyValues)
        {
            ByteBuffer partitionKey = partitionKeyValues.length == 1
                                      ? decompose(metadata.partitionKeyType, partitionKeyValues[0])
                                      : ((CompositeType) metadata.partitionKeyType).decompose(partitionKeyValues);
            return metadata.partitioner.decorateKey(partitionKey);
        }

        private Clustering<?> makeClustering(Object... clusteringValues)
        {
            if (clusteringValues.length == 0)
                return Clustering.EMPTY;

            ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
            for (int i = 0; i < clusteringValues.length; i++)
                clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);
            return Clustering.make(clusteringByteBuffers);
        }

        private final class SimplePartition implements PartitionCollector
        {
            private final DecoratedKey key;
            // we assume no duplicate rows, and impose the condition lazily
            private SimpleRow[] rows;
            private int rowCount;
            private SimpleRow staticRow;
            private boolean dropRows;

            private SimplePartition(DecoratedKey key)
            {
                this.key = key;
                this.rows = new SimpleRow[1];
            }

            @Override
            public RowCollector row(Object... clusteringKeys)
            {
                int ckSize = metadata.clusteringColumns().size();
                if (ckSize != clusteringKeys.length)
                    throw new IllegalArgumentException();

                return row(makeClustering(clusteringKeys));
            }

            RowCollector row(Clustering<?> clustering)
            {
                if (nanoTime() > timeoutAt)
                    throw new ReadTimeoutException(ConsistencyLevel.ONE, 0, 1, false);

                if (dropRows || !dataRange.clusteringIndexFilter(key).selects(clustering))
                    return drop -> SimplePartitionsCollector.this;

                if (totalRowCount >= limits.count())
                {
                    boolean filter;
                    if (!isSorted) filter = totalRowCount / 2 >= Math.max(1024, limits.count());
                    else
                    {
                        int rowsAddedSinceLastFiltered = totalRowCount - lastFilteredTotalRowCount;
                        int threshold = Math.max(32, Math.min(1024, lastFilteredTotalRowCount / 2));
                        filter = lastFilteredTotalRowCount == 0 || rowsAddedSinceLastFiltered >= threshold;
                    }

                    if (filter)
                    {
                        // first filter within each partition
                        for (SimplePartition partition : partitions.values())
                        {
                            int curCount = partition.rowCount;
                            int newCount = Math.min(curCount, limits.perPartitionCount());
                            newCount = partition.filterAndSortAndTruncate(newCount);
                            totalRowCount -= curCount - newCount;
                        }

                        // then drop any partitions that completely fall outside our limit
                        Iterator<SimplePartition> iter = partitions.descendingMap().values().iterator();
                        SimplePartition last;
                        while (true)
                        {
                            SimplePartition next = last = iter.next();
                            if (totalRowCount - next.rowCount < limits.count())
                                break;

                            iter.remove();
                            totalRowCount -= next.rowCount;
                            if (next == this)
                                dropRows = true;
                        }

                        // possibly truncate the last partition if it partially falls outside the limit
                        int overflow = Math.max(0, totalRowCount - limits.count());
                        int curCount = last.rowCount;
                        int newCount = curCount - overflow;
                        newCount = last.filterAndSortAndTruncate(newCount);
                        totalRowCount -= curCount - newCount;
                        lastFilteredTotalRowCount = totalRowCount;

                        if (isSorted && totalRowCount >= limits.count())
                            throw new InternalDoneException();

                        if (dropRows)
                            return drop -> SimplePartitionsCollector.this;
                    }
                }

                SimpleRow result = new SimpleRow(clustering);
                if (clustering.kind() == STATIC_CLUSTERING)
                {
                    Invariants.require(staticRow == null);
                    staticRow = result;
                }
                else
                {
                    totalRowCount++;
                    if (rowCount == rows.length)
                        rows = Arrays.copyOf(rows, Math.max(8, rowCount * 2));
                    rows[rowCount++] = result;
                }
                return result;
            }

            void filterAndSort()
            {
                int newCount = 0;
                for (int i = 0 ; i < rowCount; ++i)
                {
                    if (rows[i].rowFilterIncludes())
                    {
                        if (newCount != i)
                            rows[newCount] = rows[i];
                        newCount++;
                    }
                }
                totalRowCount -= (rowCount - newCount);
                Arrays.fill(rows, newCount, rowCount, null);
                rowCount = newCount;
                Arrays.sort(rows, 0, newCount, rowComparator());
            }

            int filterAndSortAndTruncate(int newCount)
            {
                Invariants.requireArgument(newCount <= rowCount);
                filterAndSort();
                if (rowCount < newCount)
                    return rowCount;

                Arrays.fill(rows, newCount, rowCount, null);
                rowCount = newCount;
                return newCount;
            }

            private Comparator<SimpleRow> rowComparator()
            {
                Comparator<Clusterable> cmp = dataRange.isReversed() ? metadata.comparator.reversed() : metadata.comparator;
                return (a, b) -> cmp.compare(a.clustering, b.clustering);
            }

            Row staticRow()
            {
                if (staticRow == null)
                    return null;

                return staticRow.materialiseAndFilter();
            }

            Iterator<Row> rows()
            {
                filterAndSort();
                return Arrays.stream(rows, 0, rowCount).map(SimpleRow::materialiseAndFilter).iterator();
            }

            private final class SimpleRow implements RowCollector
            {
                final Clustering<?> clustering;
                SomeColumns state;

                private SimpleRow(Clustering<?> clustering)
                {
                    this.clustering = clustering;
                }

                @Override
                public PartitionsCollector lazyAdd(Consumer<ColumnsCollector> addToIfNeeded)
                {
                    Invariants.require(state == null);
                    state = new LazyColumnsCollector(addToIfNeeded);
                    return SimplePartitionsCollector.this;
                }

                @Override
                public PartitionsCollector eagerAdd(Consumer<ColumnsCollector> addToNow)
                {
                    Invariants.require(state == null);
                    state = new EagerColumnsCollector(addToNow);
                    return SimplePartitionsCollector.this;
                }

                boolean rowFilterIncludes()
                {
                    return null != materialiseAndFilter();
                }

                Row materialiseAndFilter()
                {
                    if (state == null)
                        return null;

                    FilteredRow filtered = state.materialiseAndFilter(this);
                    state = filtered;
                    return filtered == null ? null : filtered.row;
                }

                DecoratedKey partitionKey()
                {
                    return SimplePartition.this.key;
                }

                SimplePartitionsCollector collector()
                {
                    return SimplePartitionsCollector.this;
                }
            }
        }

        static abstract class SomeColumns
        {
            abstract FilteredRow materialiseAndFilter(SimplePartition.SimpleRow parent);
        }

        static class LazyColumnsCollector extends SomeColumns
        {
            final Consumer<ColumnsCollector> lazy;
            LazyColumnsCollector(Consumer<ColumnsCollector> lazy)
            {
                this.lazy = lazy;
            }

            @Override
            FilteredRow materialiseAndFilter(SimplePartition.SimpleRow parent)
            {
                return parent.collector().new EagerColumnsCollector(lazy).materialiseAndFilter(parent);
            }
        }

        class EagerColumnsCollector extends SomeColumns implements ColumnsCollector
        {
            Object[] columns = new Object[4];
            int columnCount;

            public EagerColumnsCollector(Consumer<ColumnsCollector> add)
            {
                add.accept(this);
            }

            @Override
            public <V> ColumnsCollector add(String name, V input, Function<V, Object> f)
            {
                ColumnMetadata cm = columnLookup.get(name);
                if (!columnFilter.fetches(cm))
                    return this;

                Object value = f.apply(input);
                if (value == null)
                    return this;

                if (columnCount * 2 == columns.length)
                    columns = Arrays.copyOf(columns, columnCount * 4);

                columns[columnCount * 2] = cm;
                columns[columnCount * 2 + 1] = value;
                ++columnCount;
                return this;
            }

            @Override
            FilteredRow materialiseAndFilter(SimplePartition.SimpleRow parent)
            {
                for (int i = 0 ; i < columnCount ; i++)
                {
                    ColumnMetadata cm = (ColumnMetadata) columns[i * 2];
                    columns[i] = BufferCell.live(cm, timestamp, decompose(cm.type, columns[i * 2 + 1]));
                }
                Arrays.sort(columns, 0, columnCount, (a, b) -> ColumnData.comparator.compare((BufferCell)a, (BufferCell)b));
                Object[] btree = BTree.build(BulkIterator.of(columns), columnCount, UpdateFunction.noOp);
                BTreeRow row = BTreeRow.create(parent.clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE, btree);
                if (!rowFilter.isSatisfiedBy(metadata, parent.partitionKey(), row, nowInSeconds))
                    return null;
                return new FilteredRow(row);
            }
        }

        static class FilteredRow extends SomeColumns
        {
            final Row row;
            FilteredRow(Row row)
            {
                this.row = row;
            }

            @Override
            FilteredRow materialiseAndFilter(SimplePartition.SimpleRow parent)
            {
                return this;
            }
        }
    }

    // TODO (expected): add e.g. BOTH_ASC_DESC when some vtable supports it
    public enum Sorted { UNSORTED, ASC, DESC }

    protected final TableMetadata metadata;
    private final Sorted sorted;

    protected AbstractLazyVirtualTable(TableMetadata metadata, Sorted sorted)
    {
        if (!metadata.isVirtual())
            throw new IllegalArgumentException("Cannot instantiate a non-virtual table");

        this.metadata = metadata;
        this.sorted = sorted;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    protected PartitionsCollector collector(DataRange dataRange, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
    {
        boolean isSorted = sorted == (dataRange.isReversed() ? Sorted.DESC : Sorted.ASC);
        return new SimplePartitionsCollector(metadata, isSorted, dataRange, columnFilter, rowFilter, limits);
    }

    protected abstract void collect(PartitionsCollector collector);

    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
    {
        return select(new DataRange(new Bounds<>(partitionKey, partitionKey), clusteringIndexFilter), columnFilter, rowFilter, limits);
    }

    @Override
    public final UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
    {
        PartitionsCollector collector = collector(dataRange, columnFilter, rowFilter, limits);
        try
        {
            collect(collector);
        }
        catch (InternalDoneException ignore) {}
        return collector.finish();
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    @Override
    public void truncate()
    {
        throw new InvalidRequestException("Truncation is not supported by table " + metadata);
    }

    @Override
    public String toString()
    {
        return metadata().toString();
    }

    private static ByteBuffer decompose(AbstractType<?> type, Object value)
    {
        return type.decomposeUntyped(value);
    }
}
