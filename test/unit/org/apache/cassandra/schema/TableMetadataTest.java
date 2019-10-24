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

package org.apache.cassandra.schema;

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.*;

public class TableMetadataTest
{

    @Test
    public void equalsWithoutId()
    {
        TableId id1 = TableId.generate();
        TableId id2 = TableId.generate();

        TableMetadata table1 =
          TableMetadata.builder("keyspace", "table")
                       .id(id1)
                       .partitioner(ByteOrderedPartitioner.instance)
                       .addPartitionKeyColumn("pk", BytesType.instance)
                       .addClusteringColumn("cc", Int32Type.instance)
                       .addRegularColumn("rc", UTF8Type.instance)
                       .build();
        TableMetadata table2 = table1.unbuild().id(id2).build();
        TableMetadata table3 = table2.unbuild().comment("non-empty comment").build();

        assertFalse(table1.equals(table2));
        assertTrue (table1.equalsWithoutId(table2));

        assertFalse(table2.equals(table3));
        assertFalse(table2.equalsWithoutId(table3));
    }
}
