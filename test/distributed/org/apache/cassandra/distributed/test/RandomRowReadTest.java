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

package org.apache.cassandra.distributed.test;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class RandomRowReadTest extends TestBaseImpl
{
    @Test
    public void testSelectWithUpdatedColumnOnOneNodeAndColumnDeletionOnTheOther() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v int, PRIMARY KEY (pk, ck))"));
            
            cluster.get(1).runOnInstance(() -> {
                for (ColumnFamilyStore cs : Keyspace.open(KEYSPACE).getColumnFamilyStores())
                    cs.disableAutoCompaction();
            });
            
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, '1', 1) USING TIMESTAMP 1"));
            cluster.get(1).flush(KEYSPACE);
            cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 1000 SET v = 2 WHERE pk = 1 AND ck = '1'"));
            cluster.get(1).flush(KEYSPACE);

            cluster.get(2).executeInternal(withKeyspace("DELETE v FROM %s.tbl USING TIMESTAMP 10000 WHERE pk=1 AND ck='1'"));
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row(1, "1", null));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row((Integer) null));

        }
    }
}
