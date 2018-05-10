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

package org.apache.cassandra.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.exceptions.ConfigurationException;

public class CompressionValidationTest extends CQLTester
{

    @Before
    public void setUp()
    {
        System.setProperty(SchemaAlteringStatement.SYSTEM_PROPERTY_ALLOW_DISABLED_COMPRESSION, "true");
    }

    private static int nextTableNum = 0;
    private static synchronized String nextTableName()
    {
        return String.format("tbl%s", nextTableNum++);
    }

    private static void assertConfigException(String query)
    {
        try
        {
            schemaChange(query);
            Assert.fail("Expecting exception");
        }
        catch (RuntimeException e)
        {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof ConfigurationException);
            Assert.assertTrue(cause.getMessage().contains("sstable compression cannot be disabled"));
        }
    }

    /**
     * If the system prop has been set, creating tables with compression disabled should work
     */
    @Test
    public void createSuccess()
    {
        String table = nextTableName();
        schemaChange(String.format("CREATE TABLE %s.%s (k int primary key, v int) WITH compression={'sstable_compression':''}", KEYSPACE, table));
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE, table);
        Assert.assertFalse(cfm.params.compression.isEnabled());
    }

    /**
     * If the system prop hasn't been set, creating tables with compression disabled should fail
     */
    @Test
    public void createFailure()
    {
        System.clearProperty(SchemaAlteringStatement.SYSTEM_PROPERTY_ALLOW_DISABLED_COMPRESSION);
        String table = nextTableName();
        assertConfigException(String.format("CREATE TABLE %s.%s (k int primary key, v int) WITH compression={'sstable_compression':''}", KEYSPACE, table));
    }

    @Test
    public void alterSuccess()
    {
        String table = nextTableName();
        schemaChange(String.format("CREATE TABLE %s.%s (k int primary key, v int)", KEYSPACE, table));
        schemaChange(String.format("ALTER TABLE %s.%s WITH compression = {'sstable_compression': ''}", KEYSPACE, table));
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE, table);
        Assert.assertFalse(cfm.params.compression.isEnabled());
    }

    @Test
    public void alterFailure()
    {
        System.clearProperty(SchemaAlteringStatement.SYSTEM_PROPERTY_ALLOW_DISABLED_COMPRESSION);
        String table = nextTableName();
        schemaChange(String.format("CREATE TABLE %s.%s (k int primary key, v int)", KEYSPACE, table));
        assertConfigException(String.format("ALTER TABLE %s.%s WITH compression = {'sstable_compression': ''}", KEYSPACE, table));
    }
}
