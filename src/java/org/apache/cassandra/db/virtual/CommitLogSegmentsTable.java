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
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

final class CommitLogSegmentsTable extends AbstractVirtualTable
{
    final static String segment_path = "segment_path";
    final static String ON_DISK_SIZE = "on_disk_size";
    final static String CONTENT_SIZE = "content_size";
    final static String NUM_DIRTY_TABLES = "num_dirty_tables";
    final static String IS_STILL_ALLOCATING = "is_still_allocating";
    final static String CDC_STATE = "cdc_state";

    CommitLogSegmentsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "commitlog_segments")
                           .comment("information about commitlog segments")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(segment_path, UTF8Type.instance)
                           .addRegularColumn(ON_DISK_SIZE, LongType.instance)
                           .addRegularColumn(CONTENT_SIZE, LongType.instance)
                           .addRegularColumn(NUM_DIRTY_TABLES, Int32Type.instance)
                           .addRegularColumn(IS_STILL_ALLOCATING, BooleanType.instance)
                           .addRegularColumn(CDC_STATE, UTF8Type.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (CommitLogSegment segment : CommitLog.instance.segmentManager.getActiveSegments())
        {
            result.row(segment.getPath())
                  .column(ON_DISK_SIZE, segment.onDiskSize())
                  .column(CONTENT_SIZE, segment.contentSize())
                  .column(NUM_DIRTY_TABLES, segment.getDirtyTableIds().size())
                  .column(IS_STILL_ALLOCATING, segment.isStillAllocating())
                  .column(CDC_STATE, segment.getCDCState().name());
        }

        return result;
    }
}
