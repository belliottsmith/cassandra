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

import java.util.Collection;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLog.CommitLogUsage;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

final class CommitLogTablesTable extends AbstractVirtualTable
{
    final static String KEYSPACE_NAME = "keyspace_name";
    final static String TABLE_NAME = "table_name";
    final static String NUM_DIRTY_SEGMENTS = "num_dirty_segments";
    final static String ON_DISK_BYTES_BLOCKED = "on_disk_bytes_blocked";
    final static String EARLIEST_SEGMENT_PATH = "earliest_segment_path";

    CommitLogTablesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "commitlog_tables")
                           .comment("tables with unflushed entries in the commitlog")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(NUM_DIRTY_SEGMENTS, Int32Type.instance)
                           .addRegularColumn(ON_DISK_BYTES_BLOCKED, LongType.instance)
                           .addRegularColumn(EARLIEST_SEGMENT_PATH, UTF8Type.instance)
                           .build());
    }

    public DataSet data()
    {
        Collection<CommitLogUsage> summaries = CommitLog.instance.activeSegmentUsageByTableId();
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (CommitLogUsage summary : summaries)
        {
            TableMetadata tmd = Schema.instance.getTableMetadata(summary.tableId);
            if (tmd == null) // skip if table was dropped
                continue;
            result.row(tmd.keyspace, tmd.name)
                  .column(NUM_DIRTY_SEGMENTS, summary.numDirtySegments)
                  .column(ON_DISK_BYTES_BLOCKED, summary.onDiskBytesBlocked)
                  .column(EARLIEST_SEGMENT_PATH, summary.earliestSegmentPath);
        }

        return result;
    }
}
