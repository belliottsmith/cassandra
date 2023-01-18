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

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

final class CommitLogSegmentsMapTable extends AbstractVirtualTable
{
    final static String SEGMENT_PATH = "segment_path";
    final static String KEYSPACE_NAME = "keyspace_name";
    final static String TABLE_NAME = "table_name";

    CommitLogSegmentsMapTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "commitlog_segments_map")
                           .comment("unflushed tables that contribute to active segments in the commitlog")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(SEGMENT_PATH, UTF8Type.instance)
                           .addClusteringColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (CommitLogSegment segment : CommitLog.instance.segmentManager.getActiveSegments())
        {
            for (TableId tId : segment.getDirtyTableIds())
            {
                TableMetadata tmd = Schema.instance.getTableMetadata(tId);
                if (tmd == null) // skip if table was dropped
                    continue;
                result.row(segment.getPath(), tmd.keyspace, tmd.name);
            }
        }

        return result;
    }
}
