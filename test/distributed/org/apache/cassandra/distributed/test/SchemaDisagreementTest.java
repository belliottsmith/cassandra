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

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;

public class SchemaDisagreementTest extends TestBaseImpl
{
    /**
     * If a node receives a mutation for a column it's not aware of, it should fail, since it can't write the data.
     */
    @Test
    public void writeWithSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))"));

            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));

            // Introduce schema disagreement
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl ADD v2 int"), 1);
            try
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)"), ALL);
                fail("Should have failed because of schema disagreement.");
            }
            catch (Exception e)
            {
                assertTrue(e.getCause().getMessage().contains("Unknown column v2 during deserialization"));
            }
        }
    }


    @Test
    public void readWithSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))"));

            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));

            // Introduce schema disagreement
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl ADD v2 int"), 1);
            try
            {
                assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ALL),
                           row(1, 1, 1, null));
                fail("Should have failed because of schema disagreement.");
            }
            catch (Exception e)
            {
                assertTrue(e.getCause().getMessage().contains("Unknown column v2 during deserialization"));
            }
        }
    }

    @Test
    public void readRepair() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int,  primary key (pk, ck))");
            String name = "aaa";
            cluster.get(1).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) values (?,1,1,1)", 1);
            selectSilent(cluster, name);
            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(2).shutdown();
            cluster.get(2).startup();
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
        }
    }
    private void selectSilent(Cluster cluster, String name)
    {
        try
        {
            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"), ConsistencyLevel.ALL, 1);
        }
        catch (Exception e)
        {
            boolean causeIsUnknownColumn = false;
            Throwable cause = e;
            while (cause != null)
            {
                if (cause.getMessage() != null && cause.getMessage().contains("Unknown column "+name+" during deserialization"))
                    causeIsUnknownColumn = true;
                cause = cause.getCause();
            }
            assertTrue(causeIsUnknownColumn);
        }
    }
}
