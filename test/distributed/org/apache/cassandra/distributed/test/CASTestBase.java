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

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PROPOSE;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_REPAIR;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_COMMIT;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.PAXOS_PROPOSE;
import static org.apache.cassandra.net.MessagingService.Verb.READ;

public class CASTestBase extends DistributedTestBase
{
    void repair(Cluster cluster, int pk, int repairWith, int repairWithout)
    {
        IMessageFilters.Filter filter = cluster.filters().verbs(
                APPLE_PAXOS_REPAIR.ordinal(),
                APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(repairWith).to(repairWithout).drop();
        cluster.get(repairWith).runOnInstance(() -> {
            CFMetaData schema = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata;
            DecoratedKey key = schema.decorateKey(Int32Type.instance.decompose(pk));
            try
            {
                PaxosRepair.async(ConsistencyLevel.SERIAL, key, schema, null).await();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        });
        filter.off();
    }

    @Test
    public void simpleUpdate() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
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

    /**
     * A write and a read that are able to witness different (i.e. non-linearizable) histories
     * See CASSANDRA-12126
     * 
     *  - Prepare A to {1, 2, 3}
     *  - Propose A to {1}
     *  - Propose B (=>!A) to {2, 3}
     *  - Read from (or attempt C (=>!B)) to {1, 2} -> witness either A or B, not both
     */
    @Ignore
    @Test
    public void testIncompleteWriteSupersededByConflictingRejectedCondition() throws Throwable
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
            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(2).to(1).drop();
            assertRows(cluster.coordinator(2).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM),
                    row(false));
            drop1.off();
            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal()).from(1).to(2).drop();
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL));
        }
    }

    /**
     * Two reads that are able to witness different (i.e. non-linearizable) histories
     *  - Prepare A to {1, 2, 3}
     *  - Propose A to {1}
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
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(2).to(1).drop();
            assertRows(cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL));
            drop1.off();
            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(1).to(2).drop();
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL));
        }
    }

    // failed write (by node that did not yet witness a range movement via gossip) is witnessed later as successful
    // conflicting with another successful write performed by a node that did witness the range movement
    // Prepare, Propose and Commit A to {1, 2}
    // Range moves to {2, 3, 4}
    // Prepare and Propose B (=> !A) to {3, 4}
    @Ignore
    @Test
    public void testSuccessfulWriteBeforeRangeMovement() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .with(Feature.NO_RING)
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            IInstance[] initialRing = new IInstance[] { cluster.get(1), cluster.get(2), cluster.get(3) };
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, initialRing);
            cluster.get(4).acceptOnInstance(Instance::addToRingNormal, new IInstance[] { cluster.get(4) });

            cluster.schemaChange(4, "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", false);
            cluster.schemaChange(4, "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))", false);
            cluster.waitForSchemaAgreement();

            int pk = pk(cluster, 1, 2);

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            for (int i = 1 ; i <= 3 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, new IInstance[] { cluster.get(4) });

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(4).to(2).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(4).to(2).drop();
            assertRows(cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(false, pk, 1, 1, null));

            // TODO: repair and verify base table state
        }
    }

    /**
     * Failed write (by node that did not yet witness a range movement via gossip) is witnessed later as successful
     * conflicting with another successful write performed by a node that did witness the range movement
     *  - Range moves from {1, 2, 3} to {2, 3, 4}, witnessed by X (not by !X)
     *  -  X: Prepare, Propose and Commit A to {3, 4}
     *  - !X: Prepare and Propose B (=>!A) to {1, 2}
     */
    @Ignore
    @Test
    public void testConflictingWritesWithStaleRingInformation() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .with(Feature.NO_RING)
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            IInstance[] initialRing = new IInstance[] { cluster.get(1), cluster.get(2), cluster.get(3) };
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, initialRing);
            for (int i = 2 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, new IInstance[] { cluster.get(4) });

            cluster.schemaChange(4, "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", false);
            cluster.schemaChange(4, "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))", false);
            cluster.waitForSchemaAgreement();

            int pk = pk(cluster, 1, 2);
            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(4).to(2).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(4).to(2).drop();
            cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(4).to(2).drop();
            assertRows(cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(1).to(3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(false, pk, 1, 1, null));

            // TODO: repair and verify base table state
        }
    }

    /**
     * Successful write during range movement, not witnessed by read after range movement.
     * Very similar to {@link #testConflictingWritesWithStaleRingInformation}.
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -  !X: Prepare and Propose to {1, 2}
     *  - Range movement witnessed by !X
     *  - Any: Prepare and Read from {3, 4}
     */
    @Ignore
    @Test
    public void testSucccessfulWriteDuringRangeMovementFollowedByRead() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .with(Feature.NO_RING)
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            IInstance[] initialRing = new IInstance[] { cluster.get(1), cluster.get(2), cluster.get(3) };
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, initialRing);
            cluster.get(4).acceptOnInstance(Instance::addToRingBootstrapping, new IInstance[] { cluster.get(4) });

            cluster.schemaChange(4, "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", false);
            cluster.schemaChange(4, "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))", false);
            cluster.waitForSchemaAgreement();
            cluster.get(4).runOnInstance(() -> PendingRangeCalculatorService.instance.update());
            cluster.get(4).runOnInstance(() -> PendingRangeCalculatorService.instance.blockUntilFinished());

            int pk = pk(cluster, 1, 2);

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // set topology on all nodes
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, new IInstance[] { cluster.get(4) });

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", ConsistencyLevel.SERIAL, pk),
                    row(pk, 1, 1));
        }
    }

    /**
     * Successful write during range movement not witnessed by write after range movement
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -  !X: Prepare and Propose to {1, 2}
     *  - Range movement witnessed by !X
     *  - Any: Prepare and Propose to {3, 4}
     */
    @Ignore
    @Test
    public void testSuccessfulWriteDuringRangeMovementFollowedByConflicting() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .with(Feature.NO_RING)
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            IInstance[] initialRing = new IInstance[] { cluster.get(1), cluster.get(2), cluster.get(3) };
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, initialRing);
            cluster.get(4).acceptOnInstance(Instance::addToRingBootstrapping, new IInstance[] { cluster.get(4) });

            cluster.schemaChange(4, "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", false);
            cluster.schemaChange(4, "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))", false);
            cluster.waitForSchemaAgreement();
            cluster.get(4).runOnInstance(() -> PendingRangeCalculatorService.instance.update());
            cluster.get(4).runOnInstance(() -> PendingRangeCalculatorService.instance.blockUntilFinished());

            int pk = pk(cluster, 1, 2);

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // set topology on all nodes
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, new IInstance[] { cluster.get(4) });

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
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
     *  -   X: Prepare to {2, 3, 4}
     *  -   X: Propose to {4}
     *  -  !X: Prepare and Propose to {1, 2}
     *  - Range move visible by !X
     *  - Any: Prepare and Read from {3, 4}
     */
    @Ignore
    @Test
    public void testIncompleteWriteFollowedBySuccessfulWriteWithStaleRingDuringRangeMovementFollowedByRead() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .with(Feature.NO_RING)
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            IInstance[] initialRing = new IInstance[] { cluster.get(1), cluster.get(2), cluster.get(3) };
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, initialRing);
            cluster.get(4).acceptOnInstance(Instance::addToRingBootstrapping, new IInstance[] { cluster.get(4) });

            cluster.schemaChange(4, "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", false);
            cluster.schemaChange(4, "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))", false);
            cluster.waitForSchemaAgreement();
            cluster.get(4).runOnInstance(() -> PendingRangeCalculatorService.instance.update());
            cluster.get(4).runOnInstance(() -> PendingRangeCalculatorService.instance.blockUntilFinished());

            int pk = pk(cluster, 1, 2);

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(4).to(1).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(4).to(1, 2, 3).drop();
            try
            {
                cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM, pk);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // set topology on all nodes
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, new IInstance[] { cluster.get(4) });

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(3).to(2).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", ConsistencyLevel.SERIAL, pk),
                    row(pk, 1, null, 2));
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
     *  -   X: Prepare to {2, 3, 4}
     *  -   X: Propose to {4}
     *  -  !X: Prepare and Propose to {1, 2}
     *  - Range move visible by !X
     *  - Any: Prepare and Propose to {3, 4}
     */
    @Ignore
    @Test
    public void testIncompleteWriteFollowedBySuccessfulWriteWithStaleRingDuringRangeMovementFollowedByWrite() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .with(Feature.NO_RING)
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)))
        {
            IInstance[] initialRing = new IInstance[] { cluster.get(1), cluster.get(2), cluster.get(3) };
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, initialRing);
            cluster.get(4).acceptOnInstance(Instance::addToRingBootstrapping, new IInstance[] { cluster.get(4) });

            cluster.schemaChange(4, "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", false);
            cluster.schemaChange(4, "CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))", false);
            cluster.waitForSchemaAgreement();
            cluster.get(4).runOnInstance(() -> PendingRangeCalculatorService.instance.update());
            cluster.get(4).runOnInstance(() -> PendingRangeCalculatorService.instance.blockUntilFinished());

            int pk = pk(cluster, 1, 2);

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(4).to(1).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(4).to(1, 2, 3).drop();
            try
            {
                cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM, pk);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // set topology on all nodes
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptOnInstance(Instance::addToRingNormal, new IInstance[] { cluster.get(4) });

            cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(3).to(2).drop();
            cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(false, 5, 1, null, 2));
        }
    }

    /**
     *  - Prepare A to {1, 2, 3}
     *  - Propose A to {1}
     */
    @Test
    public void testRepairIncompletePropose() throws Throwable
    {
        for (int repairWithout = 1 ; repairWithout <= 3 ; ++repairWithout)
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
                int repairWith = repairWithout == 3 ? 2 : 3;
                repair(cluster, 1, repairWith, repairWithout);
                cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(repairWith).to(repairWithout).drop();
                Object[][] rows = cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.QUORUM);
                if (repairWithout == 1) assertRows(rows); // invalidated
                else assertRows(rows, row(1, 1, 1)); // finished
            }
        }
    }

    /**
     *  - Prepare A to {1, 2, 3}
     *  - Propose A to {1, 2}
     *  -  Commit A to {1}
     *  - Repair using {2, 3}
     */
    @Test
    public void testRepairIncompleteCommit() throws Throwable
    {
        for (int repairWithout = 1 ; repairWithout <= 3 ; ++repairWithout)
        {
            try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
            {
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

                cluster.filters().verbs(PAXOS_PROPOSE.ordinal()).from(1).to(3).drop();
                cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(1).to(2, 3).drop();
                try
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                    Assert.assertTrue(false);
                }
                catch (RuntimeException wrapped)
                {
                    Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
                }

                int repairWith = repairWithout == 3 ? 2 : 3;
                debugPaxosState(cluster, 1);
                repair(cluster, 1, repairWith, repairWithout);
                cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(repairWith).to(repairWithout).drop();
                assertRows("" + repairWithout, cluster.coordinator(repairWith).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.QUORUM),
                        row(1, 1, 1));
            }

        }
    }

    private static int pk(Cluster cluster, int lb, int ub)
    {
        return pk(cluster.get(lb), cluster.get(ub));
    }

    private static int pk(IInstance lb, IInstance ub)
    {
        return pk(Murmur3Partitioner.instance.getTokenFactory().fromString(lb.config().getString("initial_token")),
                Murmur3Partitioner.instance.getTokenFactory().fromString(ub.config().getString("initial_token")));
    }

    private static int pk(Token lb, Token ub)
    {
        int pk = 0;
        Token pkt;
        while (lb.compareTo(pkt = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk))) >= 0 || ub.compareTo(pkt) < 0)
            ++pk;
        return pk;
    }

    private static void debugOwnership(Cluster cluster, int pk)
    {
        for (int i = 1 ; i <= cluster.size() ; ++i)
            System.out.println(i + ": " + cluster.get(i).appliesOnInstance((Integer v) -> StorageService.instance.getNaturalAndPendingEndpoints(KEYSPACE, Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(v))))
                    .apply(pk));
    }

    private static void debugPaxosState(Cluster cluster, int pk)
    {
        UUID cfid = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata.cfId);
        for (int i = 1 ; i <= cluster.size() ; ++i)
            for (Object[] row : cluster.get(i).executeInternal("select in_progress_ballot, proposal_ballot, most_recent_commit_at from system.paxos where row_key = ? and cf_id = ?", Int32Type.instance.decompose(pk), cfid))
                System.out.println(i + ": " + (row[0] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[0])) + ", " + (row[1] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[1])) + ", " + (row[2] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[2])));
    }

}
