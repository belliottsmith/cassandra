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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class RepairHistorySyncTest extends TestBaseImpl
{
    @Test
    public void testCorrection() throws IOException
    {
        try (Cluster cluster = init(Cluster.build().withNodes(2).withConfig(c -> c.with(Feature.NETWORK)
                                                                                  .with(Feature.GOSSIP)
                                                                                  .set("enable_christmas_patch", true))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int primary key, x int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, x) values (?,?)", ConsistencyLevel.ALL, i, i);

            cluster.forEach(i -> i.flush(KEYSPACE));

            cluster.get(1).nodetool("repair", "--full", KEYSPACE);

            Map<Range<Token>, Integer> expectedHistory = getRepHistory(cluster.get(2));
            assertFalse(expectedHistory.isEmpty());

            cluster.get(2).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").clearRepairedRangeUnsafes());
            assertTrue(getRepHistory(cluster.get(2)).isEmpty());

            cluster.get(1).nodetool("repair", "-vd", "--full", KEYSPACE);

            assertEquals(expectedHistory, getRepHistory(cluster.get(2)));
        }
    }

    private Map<Range<Token>, Integer> getRepHistory(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            Token min = cfs.metadata().partitioner.getMinimumToken();
            return cfs.getRepairHistoryForRanges(Collections.singleton(new Range<>(min, min)));
        });
    }
}
