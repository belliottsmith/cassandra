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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.util.Collection;
import java.util.List;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class ClientRequestSizeMetrics
{
    private static final String TYPE = "ClientRequestSize";

    public static final Counter totalColumnsRead = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "ColumnsRead", null));
    public static final Counter totalRowsRead = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "RowsRead", null));
    public static final Counter totalColumnsWritten = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "ColumnsWritten", null));
    public static final Counter totalRowsWritten = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "RowsWritten", null));

    public static void recordReadResponseMetrics(ResultMessage.Rows rows, StatementRestrictions restrictions)
    {
        if (!DatabaseDescriptor.getClientRequestSizeMetricsEnabled())
            return;

        int rowCount = rows.result.size();
        ClientRequestSizeMetrics.totalRowsRead.inc(rowCount);

        List<ColumnSpecification> selection = rows.result.metadata.requestNames();
        int restricted = 0;

        // SELECT_SIZE queries do not select particular columns, and they will not pass restrictions here.
        if (restrictions != null)
        {
            int maxRestrictions = restrictions.totalRestrictionCount();

            for (ColumnSpecification column : selection)
            {
                // If a column is exactly specified by the client, our metrics shouldn't give us credit for reading it.
                if (restrictions.isEqualityRestricted(column))
                    restricted++;

                // Stop checking columns if we've already accounted for all restrictions.
                if (restricted == maxRestrictions)
                    break;
            }
        }

        long columnCount = (long) rowCount * (selection.size() - restricted);
        ClientRequestSizeMetrics.totalColumnsRead.inc(columnCount);
    }

    public static void recordRowAndColumnCountMetrics(Collection<? extends IMutation> mutations)
    {
        if (!DatabaseDescriptor.getClientRequestSizeMetricsEnabled())
            return;

        int rowCount = 0;
        int columnCount = 0;

        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                columnCount += update.affectedColumnCount();
                rowCount += update.affectedRowCount();
            }
        }

        ClientRequestSizeMetrics.totalColumnsWritten.inc(columnCount);
        ClientRequestSizeMetrics.totalRowsWritten.inc(rowCount);
    }

    public static void recordRowAndColumnCountMetrics(PartitionUpdate update)
    {
        if (!DatabaseDescriptor.getClientRequestSizeMetricsEnabled())
            return;

        ClientRequestSizeMetrics.totalColumnsWritten.inc(update.affectedColumnCount());
        ClientRequestSizeMetrics.totalRowsWritten.inc(update.affectedRowCount());
    }
}
