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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.paxos.ContentionStrategy;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.*;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ANY;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.MessagingService.Verb.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CASTest extends CASCommonTestCases
{
    private static Cluster THREE_NODES;
    private static Cluster FOUR_NODES;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        System.setProperty("cassandra.paxos.use_apple_paxos_self_execution", "false");
        TestBaseImpl.beforeClass();
        Consumer<IInstanceConfig> conf = config -> config
                                                   .set("paxos_variant", "apple_rrl")
                                                   .set("write_request_timeout_in_ms", 200L)
                                                   .set("cas_contention_timeout_in_ms", 200L)
                                                   .set("request_timeout_in_ms", 200L);
        THREE_NODES = init(Cluster.create(3, conf));
        FOUR_NODES = init(Cluster.create(4, conf), 3);
    }

    @AfterClass
    public static void afterClass()
    {
        if (THREE_NODES != null)
            THREE_NODES.close();
        if (FOUR_NODES != null)
            FOUR_NODES.close();
    }

    @Before
    public void before()
    {
        THREE_NODES.filters().reset();
        FOUR_NODES.filters().reset();
        // tests add/remove nodes from the ring, so attempt to add them back
        for (int i = 1 ; i <= 4 ; ++i)
        {
            for (int j = 1; j <= 4; j++)
            {
                FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingNormal).accept(FOUR_NODES.get(j));
            }
        }
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
        String tableName = tableName("tbl");
        THREE_NODES.schemaChange("CREATE TABLE " + KEYSPACE + "." + tableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        IMessageFilters.Filter drop1 = THREE_NODES.filters().verbs(APPLE_PAXOS_PROPOSE_REQ.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(2, 3).drop();
        try
        {
            THREE_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", QUORUM);
            fail();
        }
        catch (RuntimeException wrapped)
        {
            Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
        }
        drop(THREE_NODES, 2, to(1), to(1), to());
        assertRows(THREE_NODES.coordinator(2).execute("UPDATE " + KEYSPACE + "." + tableName + " SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", QUORUM),
                   row(false));
        drop1.off();
        drop(THREE_NODES, 1, to(2), to(), to());
        assertRows(THREE_NODES.coordinator(1).execute("SELECT * FROM " + KEYSPACE + "." + tableName + " WHERE pk = 1", SERIAL));
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
        String tableName = tableName();
        String fullTableName = KEYSPACE + "." + tableName;
        THREE_NODES.schemaChange("CREATE TABLE " + fullTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        IMessageFilters.Filter drop1 = THREE_NODES.filters().verbs(APPLE_PAXOS_PROPOSE_REQ.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(2, 3).drop();
        try
        {
            THREE_NODES.coordinator(1).execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", QUORUM);
            fail();
        }
        catch (RuntimeException wrapped)
        {
            Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
        }
        drop(THREE_NODES, 2, to(1), to(), to());
        assertRows(THREE_NODES.coordinator(2).execute("SELECT * FROM " + fullTableName + " WHERE pk = 1", SERIAL));
        drop1.off();

        drop(THREE_NODES, 1, to(2), to(), to());
        assertRows(THREE_NODES.coordinator(1).execute("SELECT * FROM " + fullTableName + " WHERE pk = 1", SERIAL));
    }

    private static int[] paxosAndReadVerbs()
    {
        return new int[] {
            MessagingService.Verb.PAXOS_PREPARE.ordinal(),
            MessagingService.Verb.APPLE_PAXOS_PREPARE_REQ.ordinal(),
            MessagingService.Verb.APPLE_PAXOS_PREPARE_REFRESH_REQ.ordinal(),
            MessagingService.Verb.APPLE_PAXOS_COMMIT_AND_PREPARE_REQ.ordinal(),
            MessagingService.Verb.PAXOS_PROPOSE.ordinal(),
            MessagingService.Verb.APPLE_PAXOS_PROPOSE_REQ.ordinal(),
            MessagingService.Verb.PAXOS_COMMIT.ordinal(),
            MessagingService.Verb.READ.ordinal()
        };
    }

    /**
     * Tests that if a write timeouts and a following serial read does not see that write, then no following reads sees
     * it, even if some nodes still have the write in their paxos state.
     *
     * <p>This specifically test for the inconsistency described/fixed by CASSANDRA-12126.
     */
    @Test
    public void readConsistencyAfterWriteTimeoutTest() throws IOException
    {
        BiConsumer<String, ICoordinator> operation =
                (table, coordinator) -> assertRows(coordinator.execute("SELECT * FROM " + table + " WHERE k=0",
                        SERIAL));

        consistencyAfterWriteTimeoutTest(operation, operation, false, THREE_NODES);
        consistencyAfterWriteTimeoutTest(operation, operation, true, THREE_NODES);
    }

    /**
     * Tests that if a write timeouts, then a following CAS succeed but does not apply in a way that indicate the write
     * has not applied, then no following CAS can see that initial insert , even if some nodes still have the write in
     * their paxos state.
     *
     * <p>This specifically test for the inconsistency described/fixed by CASSANDRA-12126.
     */
    @Test
    public void nonApplyingCasConsistencyAfterWriteTimeout() throws IOException
    {
        // Note: we use CL.ANY so that the operation don't timeout in the case where we "lost" the operation1 commits.
        // The commit CL shouldn't have impact on this test anyway, so this doesn't diminishes the test.
        BiConsumer<String, ICoordinator> operation =
                (table, coordinator) -> assertCasNotApplied(coordinator.execute("UPDATE " + table + " SET v = 1 WHERE k = 0 IF v = 0",
                        ANY));
        consistencyAfterWriteTimeoutTest(operation, operation, false, THREE_NODES);
        consistencyAfterWriteTimeoutTest(operation, operation, true, THREE_NODES);
    }

    /**
     * Tests that if a write timeouts and a following serial read does not see that write, then no following CAS see
     * that initial insert, even if some nodes still have the write in their paxos state.
     *
     * <p>This specifically test for the inconsistency described/fixed by CASSANDRA-12126.
     */
    @Test
    public void mixedReadAndNonApplyingCasConsistencyAfterWriteTimeout() throws IOException
    {
        BiConsumer<String, ICoordinator> operation1 =
                (table, coordinator) -> assertRows(coordinator.execute("SELECT * FROM " + table + " WHERE k=0",
                        SERIAL));
        BiConsumer<String, ICoordinator> operation2 =
                (table, coordinator) -> assertCasNotApplied(coordinator.execute("UPDATE " + table + " SET v = 1 WHERE k = 0 IF v = 0",
                        QUORUM));
        consistencyAfterWriteTimeoutTest(operation1, operation2, false, THREE_NODES);
        consistencyAfterWriteTimeoutTest(operation1, operation2, true, THREE_NODES);
    }

    /**
     * Tests that if a write timeouts and a following CAS succeed but does not apply in a way that indicate the write
     * has not applied, then following serial reads do no see that write, even if some nodes still have the write in
     * their paxos state.
     *
     * <p>This specifically test for the inconsistency described/fixed by CASSANDRA-12126.
     */
    @Test
    public void mixedNonApplyingCasAndReadConsistencyAfterWriteTimeout() throws IOException
    {
        // Note: we use CL.ANY so that the operation don't timeout in the case where we "lost" the operation1 commits.
        // The commit CL shouldn't have impact on this test anyway, so this doesn't diminishes the test.
        BiConsumer<String, ICoordinator> operation1 =
                (table, coordinator) -> assertCasNotApplied(coordinator.execute("UPDATE " + table + " SET v = 1 WHERE k = 0 IF v = 0",
                        ANY));
        BiConsumer<String, ICoordinator> operation2 =
                (table, coordinator) -> assertRows(coordinator.execute("SELECT * FROM " + table + " WHERE k=0",
                        SERIAL));
        consistencyAfterWriteTimeoutTest(operation1, operation2, false, THREE_NODES);
        consistencyAfterWriteTimeoutTest(operation1, operation2, true, THREE_NODES);
    }

    // TODO: this shoud probably be moved into the dtest API.
    private void assertCasNotApplied(Object[][] resultSet)
    {
        assertFalse("Expected a CAS resultSet (with at least application result) but got an empty one.",
                resultSet.length == 0);
        assertFalse("Invalid empty first row in CAS resultSet.", resultSet[0].length == 0);
        Object wasApplied = resultSet[0][0];
        assertTrue("Expected 1st column of CAS resultSet to be a boolean, but got a " + wasApplied.getClass(),
                wasApplied instanceof Boolean);
        assertFalse("Expected CAS to not be applied, but was applied.", (Boolean)wasApplied);
    }

    // failed write (by node that did not yet witness a range movement via gossip) is witnessed later as successful
    // conflicting with another successful write performed by a node that did witness the range movement
    // A Promised, Accepted and Committed by {1, 2}
    // Range moves to {2, 3, 4}
    // B (=> !A) Promised and Proposed to {3, 4}
    @Test
    public void testSuccessfulWriteBeforeRangeMovement() throws Throwable
    {
        String tableName = tableName("tbl");
        FOUR_NODES.schemaChange("CREATE TABLE " + KEYSPACE + "." + tableName + " (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        // make it so {1} is unaware (yet) that {4} is an owner of the token
        FOUR_NODES.get(1).acceptsOnInstance(CASTestBase::removeFromRing).accept(FOUR_NODES.get(4));

        int pk = pk(FOUR_NODES, 1, 2);

        // {1} promises and accepts on !{3} => {1, 2}; commits on !{2,3} => {1}
        drop(FOUR_NODES, 1, to(3), to(3), to(2, 3));
        assertRows(FOUR_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ONE, pk),
                   row(true));

        for (int i = 1; i <= 3; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingNormal).accept(FOUR_NODES.get(4));

        // {4} reads from !{2} => {3, 4}
        drop(FOUR_NODES, 4, to(2), to(2), to());
        assertRows(FOUR_NODES.coordinator(4).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk),
                   row(false, pk, 1, 1, null));
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
        String tableName = tableName("tbl");
        FOUR_NODES.schemaChange("CREATE TABLE " + KEYSPACE + "." + tableName + " (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        // make it so {1} is unaware (yet) that {4} is an owner of the token
        FOUR_NODES.get(1).acceptsOnInstance(CASTestBase::removeFromRing).accept(FOUR_NODES.get(4));

        // {4} promises, accepts and commits on !{2} => {3, 4}
        int pk = pk(FOUR_NODES, 1, 2);
        drop(FOUR_NODES, 4, to(2), to(2), to(2));
        assertRows(FOUR_NODES.coordinator(4).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ONE, pk),
                   row(true));

        // {1} promises, accepts and commmits on !{3} => {1, 2}
        drop(FOUR_NODES, 1, to(3), to(3), to(3));
        assertRows(FOUR_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk),
                   row(false, pk, 1, 1, null));
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
        String tableName = tableName("tbl");
        FOUR_NODES.schemaChange("CREATE TABLE " + KEYSPACE + "." + tableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        // make it so {4} is bootstrapping, and this has propagated to only a quorum of other nodes
        for (int i = 1 ; i <= 4 ; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::removeFromRing).accept(FOUR_NODES.get(4));
        for (int i = 2 ; i <= 4 ; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingBootstrapping).accept(FOUR_NODES.get(4));

        int pk = pk(FOUR_NODES, 1, 2);

        // {1} promises and accepts on !{3} => {1, 2}; commmits on !{2, 3} => {1}
        drop(FOUR_NODES, 1, to(3), to(3), to(2, 3));
        assertRows(FOUR_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", ONE, pk),
                   row(true));

        // finish topology change
        for (int i = 1 ; i <= 4 ; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingNormal).accept(FOUR_NODES.get(4));

        // {3} reads from !{2} => {3, 4}
        drop(FOUR_NODES, 3, to(2), to(), to());
        assertRows(FOUR_NODES.coordinator(3).execute("SELECT * FROM " + KEYSPACE + "." + tableName + " WHERE pk = ?", SERIAL, pk),
                   row(pk, 1, 1));
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
        FOUR_NODES.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        // make it so {4} is bootstrapping, and this has propagated to only a quorum of other nodes
        for (int i = 1 ; i <= 4 ; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::removeFromRing).accept(FOUR_NODES.get(4));
        for (int i = 2 ; i <= 4 ; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingBootstrapping).accept(FOUR_NODES.get(4));

        int pk = pk(FOUR_NODES, 1, 2);

        // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
        drop(FOUR_NODES, 1, to(3), to(3), to(2, 3));
        assertRows(FOUR_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ONE, pk),
                   row(true));

        // finish topology change
        for (int i = 1 ; i <= 4 ; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingNormal).accept(FOUR_NODES.get(4));

        // {3} reads from !{2} => {3, 4}
        drop(FOUR_NODES, 3, to(2), to(), to());
        assertRows(FOUR_NODES.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk),
                   row(false, pk, 1, 1, null));

        // TODO: repair and verify base table state
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
        String tableName = tableName("tbl");
        FOUR_NODES.schemaChange("CREATE TABLE " + KEYSPACE + "." + tableName + " (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        // make it so {4} is bootstrapping, and this has propagated to only a quorum of other nodes
        for (int i = 1 ; i <= 4 ; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::removeFromRing).accept(FOUR_NODES.get(4));
        for (int i = 2 ; i <= 4 ; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingBootstrapping).accept(FOUR_NODES.get(4));

        int pk = pk(FOUR_NODES, 1, 2);

        // {4} promises !{1} => {2, 3, 4}, accepts on !{1, 2, 3} => {4}
        drop(FOUR_NODES, 4, to(1), to(1, 2, 3), to());
        try
        {
            FOUR_NODES.coordinator(4).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", QUORUM, pk);
            Assert.assertTrue(false);
        }
        catch (RuntimeException wrapped)
        {
            Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
        }

        // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
        drop(FOUR_NODES, 1, to(3), to(3), to(2, 3));
        // two options: either we can invalidate the previous operation and succeed, or we can complete the previous operation
        Object[][] result = FOUR_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk);
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
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingNormal).accept(FOUR_NODES.get(4));

        // {3} reads from !{2} => {3, 4}
        drop(FOUR_NODES, 3, to(2), to(2), to());
        assertRows(FOUR_NODES.coordinator(3).execute("SELECT * FROM " + KEYSPACE + "." + tableName + " WHERE pk = ?", SERIAL, pk),
                   expectRow);
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
        String tableName = tableName("tbl");
        FOUR_NODES.schemaChange("CREATE TABLE " + KEYSPACE + "." + tableName + " (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        // make it so {4} is bootstrapping, and this has propagated to only a quorum of other nodes
        for (int i = 1; i <= 4; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::removeFromRing).accept(FOUR_NODES.get(4));
        for (int i = 2; i <= 4; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingBootstrapping).accept(FOUR_NODES.get(4));

        int pk = pk(FOUR_NODES, 1, 2);

        // {4} promises and accepts on !{1} => {2, 3, 4}; commits on !{1, 2, 3} => {4}
        drop(FOUR_NODES, 4, to(1), to(1, 2, 3), to());
        try
        {
            FOUR_NODES.coordinator(4).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", QUORUM, pk);
            Assert.assertTrue(false);
        }
        catch (RuntimeException wrapped)
        {
            Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
        }

        // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
        drop(FOUR_NODES, 1, to(3), to(3), to(2, 3));
        // two options: either we can invalidate the previous operation and succeed, or we can complete the previous operation
        Object[][] result = FOUR_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk);
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
        for (int i = 1; i <= 4; ++i)
            FOUR_NODES.get(i).acceptsOnInstance(CASTestBase::addToRingNormal).accept(FOUR_NODES.get(4));

        // {3} reads from !{2} => {3, 4}
        FOUR_NODES.filters().verbs(APPLE_PAXOS_PREPARE_REQ.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(3).to(2).drop();
        FOUR_NODES.filters().verbs(APPLE_PAXOS_PROPOSE_REQ.ordinal(), PAXOS_PROPOSE.ordinal()).from(3).to(2).drop();
        assertRows(FOUR_NODES.coordinator(3).execute("INSERT INTO " + KEYSPACE + "." + tableName + " (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ONE, pk),
                   expectRow);
    }

    // TODO: RF changes
    // TODO: Aborted range movements
    // TODO: Leaving ring

    static void consistencyAfterWriteTimeoutTest(BiConsumer<String, ICoordinator> postTimeoutOperation1, BiConsumer<String, ICoordinator> postTimeoutOperation2, boolean loseCommitOfOperation1, Cluster cluster)
    {
        String tableName = tableName("t");
        String table = KEYSPACE + "." + tableName;
        cluster.schemaChange("CREATE TABLE " + table + " (k int PRIMARY KEY, v int)");

        // We do a CAS insertion, but have with the PROPOSE message dropped on node 1 and 2. The CAS will not get
        // through and should timeout. Importantly, node 3 does receive and answer the PROPOSE.
        IMessageFilters.Filter dropProposeFilter = cluster.filters()
                                                          .verbs(PAXOS_PROPOSE.ordinal(), APPLE_PAXOS_PROPOSE_REQ.ordinal())
                                                          .to(1, 2)
                                                          .drop();

        // Prepare A to {1, 2, 3}
        // Propose A to {3}
        // Timeout
        try
        {
            // NOTE: the consistency below is the "commit" one, so it doesn't matter at all here.
            cluster.coordinator(1)
                   .execute("INSERT INTO " + table + "(k, v) VALUES (0, 0) IF NOT EXISTS", ONE);
            fail("The insertion should have timed-out");
        }
        catch (Exception e)
        {
            // We expect a write timeout. If we get one, the test can continue, otherwise, we rethrow. Note that we
            // look at the root cause because the dtest framework effectively wrap the exception in a RuntimeException
            // (we could just look at the immediate cause, but this feel a bit more resilient this way).
            // TODO: we can't use an instanceof below because the WriteTimeoutException we get is from a different class
            //  loader than the one the test run under, and that's our poor-man work-around. This kind of things should
            //  be improved at the dtest API level.
            if (!e.getCause().getClass().getSimpleName().equals("WriteTimeoutException"))
                throw e;
        }
        finally
        {
            dropProposeFilter.off();
        }

        debugPaxosState(cluster, tableName, 0);

        // Prepare and Propose to {1, 2}
        // Commit(?) to either {1, 2, 3} or {3}
        // Isolates node 3 and executes the SERIAL operation. As neither node 1 or 2 got the initial insert proposal,
        // there is nothing to "replay" and the operation should assert the table is still empty.
        IMessageFilters.Filter ignoreNode3Filter = cluster.filters().verbs(paxosAndReadVerbs()).to(3).drop();
        IMessageFilters.Filter dropCommitFilter = null;
        if (loseCommitOfOperation1)
        {
            dropCommitFilter = cluster.filters().verbs(PAXOS_COMMIT.ordinal()).to(1, 2).drop();
        }
        try
        {
            postTimeoutOperation1.accept(table, cluster.coordinator(1));
        }
        finally
        {
            ignoreNode3Filter.off();
            if (dropCommitFilter != null)
                dropCommitFilter.off();
        }

        debugPaxosState(cluster, tableName, 0);

        // Node 3 is now back and we isolate node 2 to ensure the next read hits node 1 and 3.
        // What we want to ensure is that despite node 3 having the initial insert in its paxos state in a position of
        // being replayed, that insert is _not_ replayed (it would contradict serializability since the previous
        // operation asserted nothing was inserted). It is this execution that failed before CASSANDRA-12126.
        IMessageFilters.Filter ignoreNode2Filter = cluster.filters().verbs(paxosAndReadVerbs()).to(2).drop();
        try
        {
            postTimeoutOperation2.accept(table, cluster.coordinator(1));
        }
        finally
        {
            ignoreNode2Filter.off();
        }
    }

    protected Cluster getCluster()
    {
        return THREE_NODES;
    }
}
