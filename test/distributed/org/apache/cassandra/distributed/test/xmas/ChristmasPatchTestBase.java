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

package org.apache.cassandra.distributed.test.xmas;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public abstract class ChristmasPatchTestBase extends TestBaseImpl implements Serializable
{
    private final int[] insertIds;
    private final int deletionId;

    @Parameterized.Parameters()
    public static Collection<Object[]> insertionData()
    {
        return Arrays.asList(new Object[][]{
        // insert 2 entries and delete one, so we have a non deleted entry and hit SuccessfulRepairTimeHolder#gcBeforeForKey
        { new int[]{ 1, 2 }, 1 },
        // insert 1 entry and delete it so that the sstable is fully expired
        { new int[]{ 1 }, 1 }
        });
    }

    public static final Object[][] EMPTY = new Object[0][0];

    public ChristmasPatchTestBase(int[] insertIds, int deletionId)
    {
        this.insertIds = insertIds;
        this.deletionId = deletionId;
    }

    public Object[][] createRowExpectations(final boolean withDeletion)
    {
        return IntStream.of(insertIds)
                        .filter(i -> !(withDeletion && (i == deletionId)))
                        .mapToObj(i -> new Object[]{ i, "abc" })
                        .toArray(size -> new Object[size][2])
        ;
    }

    public void christmasPatchTesterUtil(final Cluster cluster,
                                         final String table,
                                         boolean expectResurection,
                                         boolean expectLastRepairLogging) throws Throwable
    {
        // Insert data into the table
        // this line would fail consistently until it didnt and we didnt change anything (couldnt fulfil Consistency ALL)
        // we believe it is due to the cluster creation not waiting for all members to be up
        // fixed in OSS here : https://github.com/apache/cassandra/blob/cassandra-3.0/test/distributed/org/apache/cassandra/distributed/impl/AbstractCluster.java#L505
        Object[][] withDeletion = createRowExpectations(true);
        Object[][] withOutDeletion = createRowExpectations(false);
        for (int i : insertIds)
        {
            cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (id, b) VALUES (%d, 'abc')", KEYSPACE, table, i),
                                           ConsistencyLevel.ALL);
        }

        // Delete the data from one of the nodes
        cluster.get(1).executeInternal(String.format("DELETE FROM %s.%s WHERE id=%d", KEYSPACE, table, deletionId));


        // flush wait for gc grace to expire
        cluster.get(1).flush(KEYSPACE);
        Thread.sleep(2000);
        // force a compaction on the node where deletion occurred
        cluster.get(1).forceCompact(KEYSPACE, table);

        // Data does not exist on the coordinating node
        Object[][] node1Output = cluster.get(1).executeInternal(String.format("SELECT * FROM %s.%s", KEYSPACE, table));
        assertRows(node1Output, withDeletion);

        // Data still exists on the peer
        Object[][] node2Output = cluster.get(2).executeInternal(String.format("SELECT * FROM %s.%s", KEYSPACE, table));
        assertRows(node2Output, withOutDeletion);

        // Repair the data
        cluster.get(1).runOnInstance(() -> {
            Map<String, String> args = new HashMap<String, String>()
            {{
                put(RepairOption.PRIMARY_RANGE_KEY, "true");
                //TODO add IR tests to make david happy
                put(RepairOption.INCREMENTAL_KEY, "false");
            }};
            int cmd = StorageService.instance.repairAsync(KEYSPACE, args);
            Assert.assertFalse("repair return status was 0, expected non-zero return status, 0 indicates repair not submitted", cmd == 0);
            List<String> status;
            do
            {
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                status = StorageService.instance.getParentRepairStatus(cmd);
            } while (status == null || status.get(0).equals(ActiveRepairService.ParentRepairStatus.IN_PROGRESS.name()));


            assertEquals(status.toString(), ActiveRepairService.ParentRepairStatus.COMPLETED, ActiveRepairService.ParentRepairStatus.valueOf(status.get(0)));
        });

        node1Output = cluster.get(1).executeInternal(String.format("SELECT * FROM %s.%s", KEYSPACE, table));
        node2Output = cluster.get(2).executeInternal(String.format("SELECT * FROM %s.%s", KEYSPACE, table));
        if (expectResurection)
        {
            // Without christmas_patch we expect the data to be resurected
            assertRows(node1Output, withOutDeletion);
            assertRows(node2Output, withOutDeletion);
        }
        else
        {
            // With the christmas_patch we expect the data to not be resurected
            assertRows(node1Output, withDeletion);
            assertRows(node2Output, withDeletion);
        }

        Object[][] rsp = cluster.coordinator(1).execute("SELECT * FROM system.repair_history", ConsistencyLevel.QUORUM);
        if (expectLastRepairLogging)
        {
            // with christmas_patch shadowing we expect to see last repair times in the repair_history
            assertTrue(Array.getLength(rsp) > 0);
            assertEquals(rsp[0][1], table);
        }
        else
        {
            assertEquals(Array.getLength(rsp), 0);
        }
    }
}

