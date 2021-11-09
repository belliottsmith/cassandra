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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import org.apache.cassandra.distributed.api.Feature;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RepairStreamingUpgradeTest extends UpgradeTestBase
{
    @Test
    public void zcsUpgradeTest() throws Throwable
    {
        new UpgradeTestBase.TestCase()
        .nodes(2)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP).set("disable_incremental_repair", false))
        .singleUpgrade(v4_0_0_30, v40)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) with compaction={'class': 'SizeTieredCompactionStrategy', 'enabled': false};");
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node == 1)
            {
                for (int i = 0; i < 10; i++)
                {
                    cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 1);", i);
                    cluster.get(1).flush(KEYSPACE); // tiny files to make sure ZCS gets used
                }
                cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
                assertTrue(cluster.get(2).logs().grep("entireSSTable=true").getResult().isEmpty());
            }
        }).runAfterClusterUpgrade((cluster) -> {
            for (int i = 0; i < 10; i++)
            {
                Object [][] res = cluster.get(2).executeInternal("SELECT pk FROM "+KEYSPACE+".tbl WHERE pk=?", i);
                assertEquals(1, res.length);
                assertEquals(i, res[0][0]);
            }
        }).run();
    }

}
