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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IMessageFilters;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PROPOSE;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_COMMIT;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_PROPOSE;

public class CASTestBase extends DistributedTestBase
{
    @Test
    public void simpleUpdate() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200000L).set("cas_contention_timeout_in_ms", 200000L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 1));
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 3 WHERE pk = 1 and ck = 1 IF v = 2", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 1));
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 2));
        }
    }

    @Test
    public void incompletePrepare() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IMessageFilters.Filter drop = cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal()).from(1).to(2, 3).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            drop.off();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL));
        }
    }

    @Test
    public void incompletePropose() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IMessageFilters.Filter drop1 = cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(2, 3).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            drop1.off();
            // make sure we encounter one of the in-progress proposals so we complete it
            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal()).from(1).to(2).drop();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 2));
        }
    }

    @Test
    public void incompleteCommit() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IMessageFilters.Filter drop1 = cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(1).to(2, 3).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            drop1.off();
            // make sure we see one of the successful commits
            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(2).drop();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 2));
        }
    }

//    @Test
//    public void test12126() throws Throwable
//    {
//        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
//        {
//            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
//
//            IMessageFilters.Filter drop1 = cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(2, 3).drop();
//            try
//            {
//                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
//                Assert.assertTrue(false);
//            }
//            catch (RuntimeException wrapped)
//            {
//                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
//            }
//            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), APPLE_PAXOS_PROPOSE.ordinal()).from(2).to(1).drop();
//            assertRows(cluster.coordinator(2).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM),
//                    row(false));
//            drop1.off();
//            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal()).from(1).to(2).drop();
//            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL));
//        }
//    }
//
}
