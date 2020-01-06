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
package org.apache.cassandra.db.rows;

import java.util.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
    private Rows() {}

    public static final Row EMPTY_STATIC_ROW = BTreeRow.emptyRow(Clustering.STATIC_CLUSTERING);

    /**
     * Collect statistics on a given row.
     *
     * @param row the row for which to collect stats.
     * @param collector the stats collector.
     * @return the total number of cells in {@code row}.
     */
    public static int collectStats(Row row, PartitionStatisticsCollector collector)
    {
        assert !row.isEmpty();

        collector.update(row.primaryKeyLivenessInfo());
        collector.update(row.deletion().time());

        int columnCount = 0;
        int cellCount = 0;
        for (ColumnData cd : row)
        {
            if (cd.column().isSimple())
            {
                ++columnCount;
                ++cellCount;
                Cells.collectStats((Cell) cd, collector);
            }
            else
            {
                ComplexColumnData complexData = (ComplexColumnData)cd;
                collector.update(complexData.complexDeletion());
                if (complexData.hasCells())
                {
                    ++columnCount;
                    for (Cell cell : complexData)
                    {
                        ++cellCount;
                        Cells.collectStats(cell, collector);
                    }
                }
            }

        }
        collector.updateColumnSetPerRow(columnCount);
        return cellCount;
    }

    /**
     * Given the result ({@code merged}) of merging multiple {@code inputs}, signals the difference between
     * each input and {@code merged} to {@code diffListener}.
     * <p>
     * Note that this method doesn't only emit cells etc where there's a difference. The listener is informed
     * of every corresponding entity between the merged and input rows, including those that are equal.
     *
     * @param diffListener the listener to which to signal the differences between the inputs and the merged result.
     * @param merged the result of merging {@code inputs}.
     * @param inputs the inputs whose merge yielded {@code merged}.
     */
    public static void diff(RowDiffListener diffListener, Row merged, Row...inputs)
    {
        Clustering clustering = merged.clustering();
        LivenessInfo mergedInfo = merged.primaryKeyLivenessInfo().isEmpty() ? null : merged.primaryKeyLivenessInfo();
        Row.Deletion mergedDeletion = merged.deletion().isLive() ? null : merged.deletion();
        for (int i = 0; i < inputs.length; i++)
        {
            Row input = inputs[i];
            LivenessInfo inputInfo = input == null || input.primaryKeyLivenessInfo().isEmpty() ? null : input.primaryKeyLivenessInfo();
            Row.Deletion inputDeletion = input == null || input.deletion().isLive() ? null : input.deletion();

            if (mergedInfo != null || inputInfo != null)
                diffListener.onPrimaryKeyLivenessInfo(i, clustering, mergedInfo, inputInfo);
            if (mergedDeletion != null || inputDeletion != null)
                diffListener.onDeletion(i, clustering, mergedDeletion, inputDeletion);
        }

        List<Iterator<ColumnData>> inputIterators = new ArrayList<>(1 + inputs.length);
        inputIterators.add(merged.iterator());
        for (Row row : inputs)
            inputIterators.add(row == null ? Collections.emptyIterator() : row.iterator());

        Iterator<?> iter = MergeIterator.get(inputIterators, ColumnData.comparator, new MergeIterator.Reducer<ColumnData, Object>()
        {
            ColumnData mergedData;
            ColumnData[] inputDatas = new ColumnData[inputs.length];
            public void reduce(int idx, ColumnData current)
            {
                if (idx == 0)
                    mergedData = current;
                else
                    inputDatas[idx - 1] = current;
            }

            protected Object getReduced()
            {
                for (int i = 0 ; i != inputDatas.length ; i++)
                {
                    ColumnData input = inputDatas[i];
                    if (mergedData != null || input != null)
                    {
                        ColumnDefinition column = (mergedData != null ? mergedData : input).column;
                        if (column.isSimple())
                        {
                            diffListener.onCell(i, clustering, (Cell) mergedData, (Cell) input);
                        }
                        else
                        {
                            ComplexColumnData mergedData = (ComplexColumnData) this.mergedData;
                            ComplexColumnData inputData = (ComplexColumnData) input;
                            if (mergedData == null)
                            {
                                // Everything in inputData has been shadowed
                                if (!inputData.complexDeletion().isLive())
                                    diffListener.onComplexDeletion(i, clustering, column, null, inputData.complexDeletion());
                                for (Cell inputCell : inputData)
                                    diffListener.onCell(i, clustering, null, inputCell);
                            }
                            else if (inputData == null)
                            {
                                // Everything in inputData is new
                                if (!mergedData.complexDeletion().isLive())
                                    diffListener.onComplexDeletion(i, clustering, column, mergedData.complexDeletion(), null);
                                for (Cell mergedCell : mergedData)
                                    diffListener.onCell(i, clustering, mergedCell, null);
                            }
                            else
                            {

                                if (!mergedData.complexDeletion().isLive() || !inputData.complexDeletion().isLive())
                                    diffListener.onComplexDeletion(i, clustering, column, mergedData.complexDeletion(), inputData.complexDeletion());

                                PeekingIterator<Cell> mergedCells = Iterators.peekingIterator(mergedData.iterator());
                                PeekingIterator<Cell> inputCells = Iterators.peekingIterator(inputData.iterator());
                                while (mergedCells.hasNext() && inputCells.hasNext())
                                {
                                    int cmp = column.cellPathComparator().compare(mergedCells.peek().path(), inputCells.peek().path());
                                    if (cmp == 0)
                                        diffListener.onCell(i, clustering, mergedCells.next(), inputCells.next());
                                    else if (cmp < 0)
                                        diffListener.onCell(i, clustering, mergedCells.next(), null);
                                    else // cmp > 0
                                        diffListener.onCell(i, clustering, null, inputCells.next());
                                }
                                while (mergedCells.hasNext())
                                    diffListener.onCell(i, clustering, mergedCells.next(), null);
                                while (inputCells.hasNext())
                                    diffListener.onCell(i, clustering, null, inputCells.next());
                            }
                        }
                    }

                }
                return null;
            }

            protected void onKeyChange()
            {
                mergedData = null;
                Arrays.fill(inputDatas, null);
            }
        });

        while (iter.hasNext())
            iter.next();
    }

    public static Row merge(Row existing, Row update, int nowInSec)
    {
        return merge(existing, update, ColumnData.noOp, nowInSec);
    }

    /**
     * Merges two rows into the given builder, mainly for merging memtable rows. In addition to reconciling the cells
     * in each row, the liveness info, and deletion times for the row and complex columns are also merged.
     * <p>
     * Note that this method assumes that the provided rows can meaningfully be reconciled together. That is,
     * that the rows share the same clustering value, and belong to the same partition.
     *
     * @param existing
     * @param update
     * @param nowInSec the current time in seconds (which plays a role during reconciliation
     * because deleted cells always have precedence on timestamp equality and deciding if a
     * cell is a live or not depends on the current time due to expiring cells).
     *
     * @return the smallest timestamp delta between corresponding rows from existing and update. A
     * timestamp delta being computed as the difference between the cells and DeletionTimes from {@code existing}
     * and those in {@code existing}.
     */
    public static Row merge(Row existing,
                            Row update,
                            ColumnData.ReconcileUpdateFunction onReconcile,
                            int nowInSec)
    {
        assert existing instanceof BTreeRow;
        assert update instanceof BTreeRow;
        return BTreeRow.merge((BTreeRow) existing, (BTreeRow) update, onReconcile, nowInSec);
    }

    /**
     * Returns the {@code ColumnDefinition} to use for merging the columns.
     * If the 2 column definitions are different the latest one will be returned.
     */
    private static ColumnDefinition getColumnDefinition(ColumnData cura, ColumnData curb)
    {
        if (cura == null)
            return curb.column;

        if (curb == null)
            return cura.column;

        if (AbstractTypeVersionComparator.INSTANCE.compare(cura.column.type, curb.column.type) >= 0)
            return cura.column;

        return curb.column;
    }
}
