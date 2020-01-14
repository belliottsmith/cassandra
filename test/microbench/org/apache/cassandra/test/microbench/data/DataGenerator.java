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

package org.apache.cassandra.test.microbench.data;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.function.IntToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

import static java.lang.Long.min;
import static java.lang.Math.max;

public class DataGenerator
{
    private static final MutableDeletionInfo NO_DELETION_INFO = new MutableDeletionInfo(DeletionTime.LIVE);
    private static final ByteBuffer zero = Int32Type.instance.decompose(0);

    public enum Distribution { RANDOM, SEQUENTIAL }

    final float updateRowOverlap;
    final int updateRowCount;
    final int updatesPerPartition;
    final Distribution distribution;
    final Distribution timestamps;

    final TableMetadata schema;
    final ByteBuffer value;
    final Clustering[] clusterings;
    final CellPath[] complexPaths;
    final int averageSizeInBytesOfOneBatch;

    public DataGenerator(int clusteringCount, int columnCount, float updateRowOverlap, int updateRowCount, int valueSize, int updatesPerPartition, Distribution distribution, Distribution timestamps)
    {
        this.updateRowOverlap = updateRowOverlap;
        this.updateRowCount = updateRowCount;
        this.updatesPerPartition = updatesPerPartition;
        this.distribution = distribution;
        this.timestamps = timestamps;
        this.value = ByteBuffer.allocate(valueSize);
        this.averageSizeInBytesOfOneBatch = 50 * max(columnCount, 1) * (updateRowCount + updateRowCount/2) * updatesPerPartition;

        int rowCount = (int) (updateRowCount * (1 + (updatesPerPartition * (1f - updateRowOverlap))));
        if (columnCount >= 0)
        {
            schema = simpleMetadata(clusteringCount, rowCount);
            clusterings = generateClusterings(clusteringCount, rowCount);
            complexPaths = new CellPath[0];
        }
        else
        {
            schema = complexMetadata(1, clusteringCount);
            clusterings = generateClusterings(1, 1);
            complexPaths = generateComplexPaths(schema, clusteringCount, rowCount);
        }
    }

    // stateful; cannot be shared between threads
    public static class PartitionGenerator
    {
        final Random random;
        final TableMetadata schema;
        final RegularAndStaticColumns regularAndStaticColumns;
        final boolean isComplex;
        final ColumnMetadata[] columns;
        final Clustering[] clusterings;
        final CellPath[] complexPaths;
        final float updateRowOverlap;
        final int updatesPerPartition;
        final Distribution distribution;
        final IntToLongFunction timestamps;
        final ByteBuffer value;
        final int averageSizeInBytesOfOneBatch;

        final IntVisitor insertRowCount;
        final Row[] insertBuffer;
        final ColumnData[] rowBuffer;
        final Cell[] complexBuffer;
        int offset;

        final Function<Clustering, Row> simpleRow = this::simpleRow;
        final Function<CellPath, Cell> complexCell = this::complexCell;

        PartitionGenerator(DataGenerator parent, long seed)
        {
            this.random = new Random(seed);
            this.schema = parent.schema;
            this.averageSizeInBytesOfOneBatch = parent.averageSizeInBytesOfOneBatch;
            this.regularAndStaticColumns = parent.schema.regularAndStaticColumns();
            this.columns = parent.schema.regularColumns().toArray(new ColumnMetadata[0]);
            this.isComplex = this.columns[0].isComplex();
            this.clusterings = parent.clusterings.clone();
            this.complexPaths = parent.complexPaths.clone();
            this.value = parent.value;
            this.insertRowCount = new IntVisitor(parent.updateRowCount);
            this.updateRowOverlap = parent.updateRowOverlap;
            this.updatesPerPartition = parent.updatesPerPartition;
            this.distribution = parent.distribution;
            this.timestamps = parent.timestamps == Distribution.RANDOM ? i -> random.nextLong() : i -> i;
            this.insertBuffer = new Row[parent.updateRowCount * 2];
            this.rowBuffer = new ColumnData[columns.length];
            this.complexBuffer = new Cell[complexPaths.length];
        }

        public List<PartitionUpdate> generate(DecoratedKey key)
        {
            offset = 0;
            insertRowCount.randomise(random);
            return IntStream.range(0, updatesPerPartition)
                            .mapToObj(i -> next(key))
                            .collect(Collectors.toList());
        }

        public int averageSizeInBytesOfOneBatch() { return averageSizeInBytesOfOneBatch; }

        private PartitionUpdate next(DecoratedKey key)
        {
            int rowCount;
            if (!isComplex)
            {
                rowCount = selectSortAndTransform(insertBuffer, clusterings, schema.comparator, simpleRow);
            }
            else
            {
                rowCount = 1;
                insertBuffer[0] = complexRow();
            }
            Object[] tree = BTree.build(Arrays.asList(insertBuffer).subList(0, rowCount), rowCount, UpdateFunction.noOp());
            return PartitionUpdate.unsafeConstruct(schema, key, AbstractBTreePartition.unsafeConstructHolder(regularAndStaticColumns, tree, DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS), NO_DELETION_INFO, false);
        }

        private <I, O> int selectSortAndTransform(O[] out, I[] in, Comparator<? super I> comparator, Function<I, O> transform)
        {
            int prevRowCount = offset == 0 ? 0 : insertRowCount.cur();
            int rowCount = insertRowCount.next();
            switch (distribution)
            {
                case SEQUENTIAL:
                {
                    for (int i = 0 ; i < rowCount ; ++i)
                        out[i] = transform.apply(in[i + offset]);
                    offset += rowCount * (1f - updateRowOverlap);
                    break;
                }
                case RANDOM:
                {
                    int rowOverlap = (int) (updateRowOverlap * min(rowCount, prevRowCount));
                    shuffle(random, in, 0, rowOverlap, 0, prevRowCount);
                    shuffle(random, in, rowOverlap, rowCount - rowOverlap, prevRowCount, in.length - prevRowCount);
                    Arrays.sort(in, 0, rowCount, comparator);
                    for (int i = 0 ; i < rowCount ; ++i)
                        out[i] = transform.apply(in[i]);
                    ++offset;
                    break;
                }
            }
            return rowCount;
        }

        Row simpleRow(Clustering clustering)
        {
            for (int i = 0 ; i < columns.length ; ++i)
                rowBuffer[i] = simpleCell(columns[i]);
            return bufferToRow(clustering);
        }

        Row complexRow()
        {
            int mapCount = selectSortAndTransform(complexBuffer, complexPaths, columns[0].cellPathComparator(), complexCell);
            rowBuffer[0] = new ComplexColumnData(columns[0], BTree.build(Arrays.asList(complexBuffer).subList(0, mapCount), mapCount, UpdateFunction.noOp()), DeletionTime.LIVE);
            return bufferToRow(clusterings[0]);
        }

        Row bufferToRow(Clustering clustering)
        {
            return BTreeRow.create(clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE, BTree.build(Arrays.asList(rowBuffer), columns.length, UpdateFunction.noOp()));
        }

        Cell simpleCell(ColumnMetadata column)
        {
            return cell(column, null);
        }

        Cell complexCell(CellPath path)
        {
            return cell(columns[0], path);
        }

        Cell cell(ColumnMetadata column, CellPath path)
        {
            return new BufferCell(column, timestamps.applyAsLong(offset), Cell.NO_TTL, Cell.NO_DELETION_TIME, value, path);
        }
    }

    public TableMetadata schema()
    {
        return schema;
    }

    public PartitionGenerator newGenerator(long seed)
    {
        return new PartitionGenerator(this, seed);
    }

    private static void addSimpleColumns(TableMetadata.Builder builder, int columnCount)
    {
        addRegularColumns(builder, BytesType.instance, columnCount);
    }

    private static void addComplexColumn(TableMetadata.Builder builder, int clusteringCount)
    {
        AbstractType[] types = new AbstractType[clusteringCount];
        Arrays.fill(types, BytesType.instance);
        addRegularColumns(builder, MapType.getInstance(CompositeType.getInstance(types), BytesType.instance, true), 1);
    }

    private static void addPartitionKeyColumns(TableMetadata.Builder builder)
    {
        addColumns(builder, Int32Type.instance, ColumnMetadata.Kind.PARTITION_KEY, 1, "pk");
    }

    private static void addClusteringColumns(TableMetadata.Builder builder, int clusteringCount)
    {
        addColumns(builder, Int32Type.instance, ColumnMetadata.Kind.CLUSTERING, clusteringCount, "c");
    }

    private TableMetadata simpleMetadata(int clusteringCount, int regularCount)
    {
        TableMetadata.Builder builder = TableMetadata.builder("", "")
                                                     .partitioner(ByteOrderedPartitioner.instance);
        addPartitionKeyColumns(builder);
        addClusteringColumns(builder, clusteringCount);
        addSimpleColumns(builder, regularCount);
        return builder.build();
    }

    private TableMetadata complexMetadata(int clusteringCount, int complexKeyCount)
    {
        TableMetadata.Builder builder = TableMetadata.builder("", "")
                                                     .partitioner(ByteOrderedPartitioner.instance);
        addPartitionKeyColumns(builder);
        addClusteringColumns(builder, clusteringCount);
        addComplexColumn(builder, complexKeyCount);
        return builder.build();
    }

    private int uniqueRowCount()
    {
        return (int) (updateRowCount * (1 + (updatesPerPartition * (1f - updateRowOverlap))));
    }

    private static Clustering[] generateClusterings(int clusteringCount, int rowCount)
    {
        Clustering prefix = Clustering.make(IntStream.range(0, clusteringCount - 1).mapToObj(i -> zero).toArray(ByteBuffer[]::new));
        return generateClusterings(prefix, rowCount);
    }

    private static CellPath[] generateComplexPaths(TableMetadata schema, int complexKeyCount, int complexPathCount)
    {
        Clustering prefix = Clustering.make(IntStream.range(0, complexKeyCount - 1).mapToObj(i -> zero).toArray(ByteBuffer[]::new));
        return generateComplexPaths((CompositeType) ((MapType)schema.regularColumns().getSimple(0).type).getKeysType(), complexPathCount, prefix);
    }

    private static void addRegularColumns(TableMetadata.Builder builder, AbstractType<?> type, int count)
    {
        addColumns(builder, type, ColumnMetadata.Kind.REGULAR, count, "v");
    }

    private static void addColumns(TableMetadata.Builder builder, AbstractType<?> type, ColumnMetadata.Kind kind, int count, String prefix)
    {
        IntStream.range(0, count)
                 .mapToObj(i -> new ColumnMetadata("", "", new ColumnIdentifier(prefix + i, true), type, kind != ColumnMetadata.Kind.REGULAR ? i : ColumnMetadata.NO_POSITION, kind))
                 .forEach(builder::addColumn);
    }

    private static Clustering[] generateClusterings( Clustering prefix, int rowCount)
    {
        return IntStream.range(0, rowCount)
                        .mapToObj(i -> {
                            ByteBuffer[] values = Arrays.copyOf(prefix.getRawValues(), prefix.size() + 1);
                            values[prefix.size()] = Int32Type.instance.decompose(i);
                            return Clustering.make(values);
                        }).toArray(Clustering[]::new);
    }

    private static CellPath[] generateComplexPaths(CompositeType type, int pathCount, Clustering prefix)
    {
        return IntStream.range(0, pathCount)
                        .mapToObj(i -> {
                            Object[] values = Arrays.copyOf(prefix.getRawValues(), prefix.size() + 1);
                            values[prefix.size()] = Int32Type.instance.decompose(i);
                            return CellPath.create(type.decompose(values));
                        }).toArray(CellPath[]::new);
    }

    private static void shuffle(Random random, Object[] data, int trgOffset, int trgSize, int srcOffset, int srcSize)
    {
        for (int i = 0 ; i < trgSize ; ++i)
        {
            int swap = srcOffset + srcSize == 0 ? 0 : random.nextInt(srcSize);
            Object tmp = data[swap];
            data[swap] = data[i + trgOffset];
            data[i + trgOffset] = tmp;
        }
    }
}
