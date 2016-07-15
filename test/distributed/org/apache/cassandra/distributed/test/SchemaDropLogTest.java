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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.schema.CIEInternalKeyspace;
import org.apache.cassandra.schema.SchemaDropLog;

public class SchemaDropLogTest extends TestBaseImpl
{
    private static final String schemaDropLogProperty = "cie-cassandra.disable_schema_drop_log";

    private void assertSchemaNotDropped(Cluster c, final String keyspace, final String table)
    {
        c.forEach(instance -> {
            Assert.assertFalse(instance.callOnInstance(() -> SchemaDropLog.schemaDropExists(keyspace, table)));
        });
    }
    private void assertSchemaDropped(Cluster c, final String keyspace, final String table)
    {
        c.forEach(instance -> {
            Assert.assertTrue(instance.callOnInstance(() -> SchemaDropLog.schemaDropExists(keyspace, table)));
        });
    }

    private void assertTableUncreatable(Cluster c,final String keyspace, final String table)
    {
        // Check re-recreation is not possible
        try
        {
            c.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY)", keyspace, table));
            Assert.fail("Should not be possible to recreate tables after dropping");
        }
        catch (RuntimeException ex)
        {
            Assert.assertTrue("Unexpected exception message: " + ex.getMessage(),
                              ex.getMessage().contains("Could not create table with name '"
                                                       + keyspace + "'.'" + table +
                                                       "' as a table with the same name was dropped earlier. This is a known limitation in Cassandra. Please create a table with a different name. For more information please visit connectme.apple.com/docs/DOC-410261#drop"));
        }
    }

    @Test
    public void testSchemaDropLog() throws IOException
    {
        final String priorDisableSchemaDropLogProperty = System.setProperty(schemaDropLogProperty, "false");

        try (Cluster cluster = init(Cluster.create(2, config -> config
                                                        .set("disable_schema_drop_check", false)
                                                        .with(Feature.NETWORK, Feature.NATIVE_PROTOCOL, Feature.GOSSIP))))
        {
            // Check dropping an individual table
            assertHasKeyspace(cluster, CIEInternalKeyspace.NAME);
            final String table = "tbl";
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY)", KEYSPACE, table));
            assertSchemaNotDropped(cluster, KEYSPACE, table);
            cluster.schemaChange(String.format("DROP TABLE %s.%s", KEYSPACE, table));
            assertSchemaDropped(cluster, KEYSPACE, table);

            assertTableUncreatable(cluster, KEYSPACE, table);

            // Check all tables in a dropped keyspace are included
            final String ks_to_drop = "ks_to_drop";
            final String tbl_in_ks_to_drop1 = "tbl_in_ks_to_drop1";
            final String tbl_in_ks_to_drop2 = "tbl_in_ks_to_drop2";
            assertSchemaNotDropped(cluster, ks_to_drop, tbl_in_ks_to_drop1);
            assertSchemaNotDropped(cluster, ks_to_drop, tbl_in_ks_to_drop2);

            cluster.schemaChange("CREATE KEYSPACE " + ks_to_drop + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + 1 + "};");
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY)", ks_to_drop, tbl_in_ks_to_drop1));
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY)", ks_to_drop, tbl_in_ks_to_drop2));

            cluster.schemaChange("DROP KEYSPACE " + ks_to_drop);
            assertSchemaDropped(cluster, ks_to_drop, tbl_in_ks_to_drop1);
            assertSchemaDropped(cluster, ks_to_drop, tbl_in_ks_to_drop2);

            // recreate the keyspace - no rules against that...
            cluster.schemaChange("CREATE KEYSPACE " + ks_to_drop + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + 1 + "};");
            assertTableUncreatable(cluster, ks_to_drop, tbl_in_ks_to_drop1);
            assertTableUncreatable(cluster, ks_to_drop, tbl_in_ks_to_drop2);
        }
        finally
        {
            if (priorDisableSchemaDropLogProperty == null)
                System.clearProperty(schemaDropLogProperty);
            else
                System.setProperty(schemaDropLogProperty, priorDisableSchemaDropLogProperty);
        }
    }
}
