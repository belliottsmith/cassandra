/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLTester;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class DeterministicSchemaTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws  Throwable
    {
        SchemaLoader.loadSchema();
        Schema.instance.loadFromDisk();
    }

    @Test
    public void deterministicSchema() throws  Throwable
    {
        // Figure out the initial version
        UUID initialVersion = Schema.instance.getVersion();
        UUID lastVersion;

        String sr99 = "99PERCENTILE";
        String srNone = "NONE";

        // A schema change better change the version
        schemaChange("CREATE KEYSPACE schema_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        Schema.instance.updateVersion();
        assertNotSame(initialVersion, Schema.instance.getVersion());

        // Now we'll make a table, which we can modify
        schemaChange ("CREATE TABLE schema_test.test (id int primary key);");
        Schema.instance.updateVersion();

        // We'll write it twice with the same value, and it should be identical
        schemaChange(String.format("ALTER TABLE schema_test.test WITH speculative_retry='%s' ", sr99));
        Schema.instance.updateVersion();
        lastVersion = Schema.instance.getVersion();
        schemaChange(String.format("ALTER TABLE schema_test.test WITH speculative_retry='%s' ", sr99));
        Schema.instance.updateVersion();
        assertEquals(lastVersion, Schema.instance.getVersion());

        // Now write it again with a different value, and ensure it's changed
        schemaChange(String.format("ALTER TABLE schema_test.test WITH speculative_retry='%s' ", srNone));
        Schema.instance.updateVersion();
        assertNotSame(lastVersion, Schema.instance.getVersion());
    }
}
