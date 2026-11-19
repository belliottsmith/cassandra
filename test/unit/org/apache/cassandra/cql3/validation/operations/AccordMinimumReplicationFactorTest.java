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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.ACCORD_TEST_ALLOW_MODES;

/**
 * Accord tables require a replication factor of at least two, unless tests opt out.
 */
public class AccordMinimumReplicationFactorTest extends CQLTester
{
    private static final String REJECTED = "Accord tables require a replication factor of at least 2";

    private String keyspace(int rf)
    {
        return createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + '}');
    }

    @Test
    public void createTable() throws Throwable
    {
        String rf1 = keyspace(1), rf2 = keyspace(2);
        try (WithProperties properties = new WithProperties().set(ACCORD_TEST_ALLOW_MODES, false))
        {
            assertInvalidMessage(REJECTED, "CREATE TABLE " + rf1 + ".t (k int PRIMARY KEY, v int) WITH transactional_mode = 'full'");
            execute("CREATE TABLE " + rf1 + ".plain (k int PRIMARY KEY, v int)");
            execute("CREATE TABLE " + rf2 + ".t (k int PRIMARY KEY, v int) WITH transactional_mode = 'full'");
        }
    }

    @Test
    public void alterTable() throws Throwable
    {
        String rf1 = keyspace(1);
        // created while tests may use a single replica, as a table created before the rule would be
        execute("CREATE TABLE " + rf1 + ".existing (k int PRIMARY KEY, v int) WITH transactional_mode = 'full'");
        execute("CREATE TABLE " + rf1 + ".plain (k int PRIMARY KEY, v int)");
        try (WithProperties properties = new WithProperties().set(ACCORD_TEST_ALLOW_MODES, false))
        {
            assertInvalidMessage(REJECTED, "ALTER TABLE " + rf1 + ".plain WITH transactional_mode = 'full'");
            // unrelated changes to a table already using accord are not rejected
            execute("ALTER TABLE " + rf1 + ".existing WITH comment = 'unrelated'");
        }
    }

    @Test
    public void alterKeyspace() throws Throwable
    {
        String rf2 = keyspace(2);
        execute("CREATE TABLE " + rf2 + ".t (k int PRIMARY KEY, v int) WITH transactional_mode = 'full'");
        try (WithProperties properties = new WithProperties().set(ACCORD_TEST_ALLOW_MODES, false))
        {
            assertInvalidMessage(REJECTED, "ALTER KEYSPACE " + rf2 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            execute("ALTER KEYSPACE " + rf2 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        }

        String rf1 = keyspace(1);
        execute("CREATE TABLE " + rf1 + ".t (k int PRIMARY KEY, v int) WITH transactional_mode = 'full'");
        try (WithProperties properties = new WithProperties().set(ACCORD_TEST_ALLOW_MODES, false))
        {
            // a change that does not reduce the replication factor is not rejected
            execute("ALTER KEYSPACE " + rf1 + " WITH durable_writes = false");
        }
    }

    @Test
    public void testsMayOptOut() throws Throwable
    {
        String rf1 = keyspace(1);
        try (WithProperties properties = new WithProperties().set(ACCORD_TEST_ALLOW_MODES, true))
        {
            execute("CREATE TABLE " + rf1 + ".t (k int PRIMARY KEY, v int) WITH transactional_mode = 'full'");
        }
    }
}
