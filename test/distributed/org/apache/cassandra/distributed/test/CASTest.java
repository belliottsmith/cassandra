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
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;

import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PROPOSE;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_PROPOSE;
import static org.apache.cassandra.net.MessagingService.Verb.READ;

public class CASTest extends CASTestBase
{
    @BeforeClass
    public static void beforeAll()
    {
        System.setProperty("cassandra.paxos.use_apple_paxos", "true");
    }

    /**
     * A write and a read that are able to witness different (i.e. non-linearizable) histories
     * See CASSANDRA-12126
     *
     *  - A Promised by {1, 2, 3}
     *  - A Acccepted by {1}
     *  - B (=>!A) Promised and Proposed to {2, 3}
     *  - Read from (or attempt C (=>!B)) to {1, 2} -> witness either A or B, not both
     */
    @Test
    public void testIncompleteWriteSupersededByConflictingRejectedCondition() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IMessageFilters.Filter drop1 = cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(2, 3).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", QUORUM);
                Assert.fail();
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            drop(cluster, 2, to(1), to(1), to());
            assertRows(cluster.coordinator(2).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", QUORUM),
                    row(false));
            drop1.off();
            drop(cluster, 1, to(2), to(), to());
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL));
        }
    }

    /**
     * Two reads that are able to witness different (i.e. non-linearizable) histories
     *  - A Promised by {1, 2, 3}
     *  - A Accepted by {1}
     *  - Read from {2, 3} -> do not witness A?
     *  - Read from {1, 2} -> witnesses A?
     * See CASSANDRA-12126
     */
    @Ignore
    @Test
    public void testIncompleteWriteSupersededByRead() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IMessageFilters.Filter drop1 = cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(2, 3).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", QUORUM);
                Assert.fail();
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            drop(cluster, 2, to(1), to(), to());
            assertRows(cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL));
            drop1.off();

            drop(cluster, 1, to(2), to(), to());
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL));
        }
    }

    // failed write (by node that did not yet witness a range movement via gossip) is witnessed later as successful
    // conflicting with another successful write performed by a node that did witness the range movement
    // A Promised, Accepted and Committed by {1, 2}
    // Range moves to {2, 3, 4}
    // B (=> !A) Promised and Proposed to {3, 4}
    @Test
    public void testSuccessfulWriteBeforeRangeMovement() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {1} is unaware (yet) that {4} is an owner of the token
            cluster.get(1).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {1} promises and accepts on !{3} => {1, 2}; commits on !{2,3} => {1}
            drop(cluster, 1, to(3), to(3), to(2, 3));
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ONE, pk),
                    row(true));

            for (int i = 1 ; i <= 3 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {4} reads from !{2} => {3, 4}
            drop(cluster, 4, to(2), to(2), to());
            assertRows(cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk),
                    row(false, pk, 1, 1, null));
        }
    }

    /**
     * Failed write (by node that did not yet witness a range movement via gossip) is witnessed later as successful
     * conflicting with another successful write performed by a node that did witness the range movement
     *  - Range moves from {1, 2, 3} to {2, 3, 4}, witnessed by X (not by !X)
     *  -  X: A Promised, Accepted and Committed by {3, 4}
     *  - !X: B (=>!A) Promised and Proposed to {1, 2}
     */
    @Test
    public void testConflictingWritesWithStaleRingInformation() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {1} is unaware (yet) that {4} is an owner of the token
            cluster.get(1).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));

            // {4} promises, accepts and commits on !{2} => {3, 4}
            int pk = pk(cluster, 1, 2);
            drop(cluster, 4, to(2), to(2), to(2));
            assertRows(cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ONE, pk),
                    row(true));

            // {1} promises, accepts and commmits on !{3} => {1, 2}
            drop(cluster, 1, to(3), to(3), to(3));
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk),
                    row(false, pk, 1, 1, null));
        }
    }

    /**
     * Successful write during range movement, not witnessed by read after range movement.
     * Very similar to {@link #testConflictingWritesWithStaleRingInformation}.
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -  !X: Promised and Accepted by {1, 2}
     *  - Range movement witnessed by !X
     *  - Any: Promised and Read from {3, 4}
     */
    @Test
    public void testSucccessfulWriteDuringRangeMovementFollowedByRead() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", 300L)
                .set("cas_contention_timeout_in_ms", 300L)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // make it so {4} is bootstrapping, and this has propagated to only a quorum of other nodes
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            for (int i = 2 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {1} promises and accepts on !{3} => {1, 2}; commmits on !{2, 3} => {1}
            drop(cluster, 1, to(3), to(3), to(2, 3));
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", ONE, pk),
                    row(true));

            // finish topology change
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {3} reads from !{2} => {3, 4}
            drop(cluster, 3, to(2), to(), to());
            assertRows(cluster.coordinator(3).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", SERIAL, pk),
                    row(pk, 1, 1));
        }
    }

    /**
     * Successful write during range movement not witnessed by write after range movement
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -  !X: Promised and Accepted by {1, 2}
     *  - Range movement witnessed by !X
     *  - Any: Promised and Propose to {3, 4}
     */
    @Test
    public void testSuccessfulWriteDuringRangeMovementFollowedByConflicting() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {4} is bootstrapping, and this has propagated to only a quorum of other nodes
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            for (int i = 2 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
            drop(cluster, 1, to(3), to(3), to(2, 3));
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ONE, pk),
                    row(true));

            // finish topology change
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {3} reads from !{2} => {3, 4}
            drop(cluster, 3, to(2), to(), to());
            assertRows(cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk),
                    row(false, pk, 1, 1, null));

            // TODO: repair and verify base table state
        }
    }

    /**
     * During a range movement, a CAS may fail leaving side effects that are not witnessed by another operation
     * being performed with stale ring information.
     * This is a particular special case of stale ring information sequencing, which probably would be resolved
     * by fixing each of the more isolated cases (but is unique, so deserving of its own test case).
     * See CASSANDRA-15745
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -   X: Promised by {2, 3, 4}
     *  -   X: Accepted by {4}
     *  -  !X: Promised and Accepted by {1, 2}
     *  - Range move visible by !X
     *  - Any: Promised and Propose to {3, 4}
     */
    @Test
    public void testIncompleteWriteFollowedBySuccessfulWriteWithStaleRingDuringRangeMovementFollowedByRead() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {4} is bootstrapping, and this has propagated to only a quorum of other nodes
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            for (int i = 2 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {4} promises !{1} => {2, 3, 4}, accepts on !{1, 2, 3} => {4}
            drop(cluster, 4, to(1), to(1, 2, 3), to());
            try
            {
                cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", QUORUM, pk);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }

            // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
            drop(cluster, 1, to(3), to(3), to(2, 3));
            // two options: either we can invalidate the previous operation and succeed, or we can complete the previous operation
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk);
            Object[] expectRow;
            if (result[0].length == 1)
            {
                assertRows(result, row(true));
                expectRow = row(pk, 1, null, 2);
            }
            else
            {
                assertRows(result, row(false, pk, 1, 1, null));
                expectRow = row(pk, 1, 1, null);
            }

            // finish topology change
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {3} reads from !{2} => {3, 4}
            drop(cluster, 3, to(2), to(2), to());
            assertRows(cluster.coordinator(3).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", SERIAL, pk),
                    expectRow);
        }
    }

    /**
     * During a range movement, a CAS may fail leaving side effects that are not witnessed by another operation
     * being performed with stale ring information.
     * This is a particular special case of stale ring information sequencing, which probably would be resolved
     * by fixing each of the more isolated cases (but is unique, so deserving of its own test case).
     * See CASSANDRA-15745
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -   X: Promised by {2, 3, 4}
     *  -   X: Accepted by {4}
     *  -  !X: Promised and Accepted by {1, 2}
     *  - Range move visible by !X
     *  - Any: Promised and Propose to {3, 4}
     */
    @Test
    public void testIncompleteWriteFollowedBySuccessfulWriteWithStaleRingDuringRangeMovementFollowedByWrite() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {4} is bootstrapping, and this has propagated to only a quorum of other nodes
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            for (int i = 2 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {4} promises and accepts on !{1} => {2, 3, 4}; commits on !{1, 2, 3} => {4}
            drop(cluster, 4, to(1), to(1, 2, 3), to());
            try
            {
                cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", QUORUM, pk);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }

            // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
            drop(cluster, 1, to(3), to(3), to(2, 3));
            // two options: either we can invalidate the previous operation and succeed, or we can complete the previous operation
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk);
            Object[] expectRow;
            if (result[0].length == 1)
            {
                assertRows(result, row(true));
                expectRow = row(false, pk, 1, null, 2);
            }
            else
            {
                assertRows(result, row(false, pk, 1, 1, null));
                expectRow = row(false, pk, 1, 1, null);
            }

            // finish topology change
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {3} reads from !{2} => {3, 4}
            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(3).to(2).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk),
                    expectRow);
        }
    }

    /**
     * Range movements do not necessarily complete; they may be aborted.
     * CAS consistency should not be affected by this.
     *
     *  - Range moving from {1, 2} to {2, 3}; witnessed by all
     *  - Promised and Accepted on {2, 3}; Commits are delayed and arrive after next commit (or perhaps vanish)
     *  - Range move cancelled; a new one starts moving {1, 2} to {2, 4}; witnessed by all
     *  - Promised, Accepted and Committed on {1, 4}
     */
    @Test
    public void testAbortedRangeMovement() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");
            int pk = pk(cluster, 1, 2);

            // set {3} bootstrapping, {4} not in ring
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(3));
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(3));

            // {3} promises and accepts on !{1} => {2, 3}
            // {3} commits do not YET arrive on either of {1, 2} (must be either due to read quorum differing on legacy Paxos)
            drop(cluster, 3, to(1), to(), to(1), to(1, 2));
            assertRows(cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ANY, pk),
                    row(true));

            // abort {3} bootstrap, start {4} bootstrap
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(3));
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            // {4} promises and accepts on !{2} => {1, 4}
            // {4} commits on {1, 2, 4}
            drop(cluster, 4, to(2), to(), to(2), to());
            cluster.filters().verbs(PAXOS_PREPARE.ordinal()).from(4).to(2).drop();
            cluster.filters().verbs(PAXOS_PROPOSE.ordinal()).from(4).to(2).drop();
            assertRows(cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", QUORUM, pk),
                    row(false, pk, 1, 1, null));
        }
    }

    // TODO: RF changes
    // TODO: Leaving ring


}
