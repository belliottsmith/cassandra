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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.distributed.test.DistributedTestBase;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.impl.Versions.find;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MixedModeReadRepairTest extends UpgradeTestBase
{
    @Test
    public void mixedModeReadRepairCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .nodesToUpgrade(2)
        .setup((cluster) -> cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk ascii, b boolean, v blob, PRIMARY KEY (pk)) WITH COMPACT STORAGE"))
        .runAfterClusterUpgrade((cluster) -> {
            // now node2 is 3.0 and node1 is 2.2
            // make sure 2.2 side does not get the mutation
            cluster.get(2).executeInternal("DELETE FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                                                          "something");
            // trigger a read repair
            cluster.coordinator(1).execute("SELECT * FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                           ConsistencyLevel.ALL,
                                           "something");
            cluster.get(1).flush(DistributedTestBase.KEYSPACE);
            // upgrade node1 to 3.0
            cluster.get(1).shutdown().get();
            Versions allVersions = find();
            cluster.get(1).setVersion(allVersions.getLatest(Versions.Major.v30));
            cluster.get(1).startup();

            // and make sure the sstables are readable
            cluster.get(1).forceCompact(DistributedTestBase.KEYSPACE, "tbl");
        }).run();
    }

    @Test
    public void testReads() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .nodesToUpgrade(2)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
        .setup((cluster) ->
               {
                   cluster.disableAutoCompaction(DistributedTestBase.KEYSPACE);
                   cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck)) ");
                   for (int j = 0; j < 5000; j++)
                   {
                       for (int i = 0; i < 10; i++)
                           cluster.coordinator(1).execute("insert into " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES ("+j+", " + i + ", 'hello')", ConsistencyLevel.ALL);
                   }
                   cluster.forEach(c -> c.flush(DistributedTestBase.KEYSPACE));
                   checkDuplicates("BOTH ON 2.2");
               })
        .runAfterClusterUpgrade((cluster) -> checkDuplicates("MIXED MODE"))
        .run();
    }

    private void checkDuplicates(String message) throws InterruptedException
    {
        Thread.sleep(5000); // sometimes one node doesn't have time come up properly?
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                  .addContactPoint("127.0.0.1")
                                                                                  .withProtocolVersion(ProtocolVersion.V3)
                                                                                  .withQueryOptions(new QueryOptions().setFetchSize(101))
                                                                                  .build();
             Session s = c.connect())
        {
            Statement stmt = new SimpleStatement("select distinct token(pk) from "+DistributedTestBase.KEYSPACE +".tbl WHERE token(pk) > "+Long.MIN_VALUE+" AND token(pk) < "+Long.MAX_VALUE);
            stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
            ResultSet res = s.execute(stmt);
            Set<Object> seenTokens = new HashSet<>();
            Iterator<Row> rows = res.iterator();
            Set<Object> dupes = new HashSet<>();
            while (rows.hasNext())
            {
                Object token = rows.next().getObject(0);
                if (seenTokens.contains(token))
                    dupes.add(token);
                seenTokens.add(token);
            }
            assertEquals(message+": too few rows", 5000, seenTokens.size());
            assertTrue(message+": dupes is not empty", dupes.isEmpty());
        }
    }
}
