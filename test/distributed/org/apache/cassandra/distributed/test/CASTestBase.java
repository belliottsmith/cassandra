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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.db.ConsistencyLevel.*;
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
                PaxosRepair.async(SERIAL, key, schema, null).await();
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

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL),
                    row(1, 1, 1));
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 3 WHERE pk = 1 and ck = 1 IF v = 2", QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL),
                    row(1, 1, 1));
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL),
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
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", QUORUM);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            drop.off();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL));
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
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", QUORUM);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            drop1.off();
            // make sure we encounter one of the in-progress proposals so we complete it
            drop(cluster, 1, to(2), to(), to());
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL),
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
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", QUORUM);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }
            drop1.off();
            // make sure we see one of the successful commits
            drop(cluster, 1, to(2), to(2), to());
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", SERIAL),
                    row(1, 1, 2));
        }
    }

    /**
     *  - Prepare A to {1, 2, 3}
     *  - Propose A to {1}
     */
    @Test
    public void testRepairIncompletePropose() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            for (int repairWithout = 1 ; repairWithout <= 3 ; ++repairWithout)
            {
                try (AutoCloseable drop = drop(cluster, 1, to(), to(2, 3), to()))
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", QUORUM, repairWithout);
                    Assert.assertTrue(false);
                }
                catch (RuntimeException wrapped)
                {
                    Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
                }
                int repairWith = repairWithout == 3 ? 2 : 3;
                repair(cluster, repairWithout, repairWith, repairWithout);

                try (AutoCloseable drop = drop(cluster, repairWith, to(repairWithout), to(), to()))
                {
                    Object[][] rows = cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", QUORUM, repairWithout);
                    if (repairWithout == 1) assertRows(rows); // invalidated
                    else assertRows(rows, row(repairWithout, 1, 1)); // finished
                }
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
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 200L).set("cas_contention_timeout_in_ms", 200L))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            for (int repairWithout = 1 ; repairWithout <= 3 ; ++repairWithout)
            {
                try (AutoCloseable drop = drop(cluster, 1, to(), to(3), to(2, 3)))
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", QUORUM, repairWithout);
                    Assert.assertTrue(false);
                }
                catch (RuntimeException wrapped)
                {
                    Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
                }

                int repairWith = repairWithout == 3 ? 2 : 3;
                repair(cluster, repairWithout, repairWith, repairWithout);
                try (AutoCloseable drop = drop(cluster, repairWith, to(repairWithout), to(), to()))
                {
                    assertRows("" + repairWithout, cluster.coordinator(repairWith).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", QUORUM, repairWithout),
                            row(repairWithout, 1, 1));
                }
            }

        }
    }

    static int pk(Cluster cluster, int lb, int ub)
    {
        return pk(cluster.get(lb), cluster.get(ub));
    }

    static int pk(IInstance lb, IInstance ub)
    {
        return pk(Murmur3Partitioner.instance.getTokenFactory().fromString(lb.config().getString("initial_token")),
                Murmur3Partitioner.instance.getTokenFactory().fromString(ub.config().getString("initial_token")));
    }

    static int pk(Token lb, Token ub)
    {
        int pk = 0;
        Token pkt;
        while (lb.compareTo(pkt = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk))) >= 0 || ub.compareTo(pkt) < 0)
            ++pk;
        return pk;
    }

    int[] to(int ... nodes)
    {
        return nodes;
    }

    AutoCloseable drop(Cluster cluster, int from, int[] toPrepareAndRead, int[] toPropose, int[] toCommit)
    {
        IMessageFilters.Filter filter1 = cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal(), READ.ordinal()).from(from).to(toPrepareAndRead).drop();
        IMessageFilters.Filter filter2 = cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(from).to(toPropose).drop();
        IMessageFilters.Filter filter3 = cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(from).to(toCommit).drop();
        return () -> {
            filter1.off();
            filter2.off();
            filter3.off();
        };
    }

    AutoCloseable drop(Cluster cluster, int from, int[] toPrepare, int[] toRead, int[] toPropose, int[] toCommit)
    {
        IMessageFilters.Filter filter1 = cluster.filters().verbs(APPLE_PAXOS_PREPARE.ordinal(), PAXOS_PREPARE.ordinal()).from(from).to(toPrepare).drop();
        IMessageFilters.Filter filter2 = cluster.filters().verbs(READ.ordinal()).from(from).to(toRead).drop();
        IMessageFilters.Filter filter3 = cluster.filters().verbs(APPLE_PAXOS_PROPOSE.ordinal(), PAXOS_PROPOSE.ordinal()).from(from).to(toPropose).drop();
        IMessageFilters.Filter filter4 = cluster.filters().verbs(PAXOS_COMMIT.ordinal()).from(from).to(toCommit).drop();
        return () -> {
            filter1.off();
            filter2.off();
            filter3.off();
            filter4.off();
        };
    }

    static void debugOwnership(Cluster cluster, int pk)
    {
        for (int i = 1 ; i <= cluster.size() ; ++i)
            System.out.println(i + ": " + cluster.get(i).appliesOnInstance((Integer v) -> StorageService.instance.getNaturalAndPendingEndpoints(KEYSPACE, Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(v))))
                    .apply(pk));
    }

    static void debugPaxosState(Cluster cluster, int pk)
    {
        UUID cfid = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata.cfId);
        for (int i = 1 ; i <= cluster.size() ; ++i)
            for (Object[] row : cluster.get(i).executeInternal("select in_progress_ballot, proposal_ballot, most_recent_commit_at from system.paxos where row_key = ? and cf_id = ?", Int32Type.instance.decompose(pk), cfid))
                System.out.println(i + ": " + (row[0] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[0])) + ", " + (row[1] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[1])) + ", " + (row[2] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[2])));
    }

}
