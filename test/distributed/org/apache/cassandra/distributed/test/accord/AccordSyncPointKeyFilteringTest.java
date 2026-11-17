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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.ExecuteSyncPoint;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.RedundantStatus;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.durability.DurabilityResult;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.primitives.KeyDeps;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.execution.SaferCommandStore;

import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Once an exclusive sync point is decided for a range, a later exclusive sync point over that range should neither
 * load nor depend on any key with no new transaction since: the earlier sync point already covers those keys.
 * <p>
 * The sync points are deliberately not made durable, and background durability is disabled, so that GC cannot remove
 * the earlier writes. This test checks the {@link accord.local.MaxDecidedRX} filtering, not GC.
 */
public class AccordSyncPointKeyFilteringTest extends TestBaseImpl
{
    private static final int KEY_COUNT = 32;
    private static final int TOUCHED_PK = 0;
    private static final long TIMEOUT_SECONDS = 30;

    /** the sync point last coordinated by {@link #syncPoint} on this instance */
    private static volatile SyncPoint lastSyncPoint;

    enum Mode
    {
        /** every write is informed of its durability */
        INFORM_DURABLE,
        /** a quorum, but not every replica, applies the first sync point */
        QUORUM_APPLIED
    }

    /**
     * With durability informed for every write, every write before the first sync point can be omitted from the
     * second sync point's dependencies.
     */
    @Test
    public void syncPointDoesNotDependOnDurablySyncedKeys() throws Throwable
    {
        try (Cluster cluster = cluster(Mode.INFORM_DURABLE))
        {
            Synced synced = setup(cluster, Mode.INFORM_DURABLE);
            String tableId = synced.tableId;
            List<String> second = cluster.get(1).callOnInstance(() -> syncPoint(tableId));
            assertEquals("the second sync point should depend only on the write since " + synced.firstId + "; found " + second,
                         List.of(synced.touched), second.subList(1, second.size()));
        }
    }

    /**
     * Single-key writes with few dependencies are not informed of their durability (the default), but can still be
     * omitted once a quorum has applied the first sync point, as their recovery will find their outcome. The sync
     * point is applied by only a quorum, so nothing is GC'd.
     */
    @Test
    public void syncPointDoesNotDependOnQuorumAppliedSyncedKeys() throws Throwable
    {
        try (Cluster cluster = cluster(Mode.QUORUM_APPLIED))
        {
            Synced synced = setup(cluster, Mode.QUORUM_APPLIED);
            String tableId = synced.tableId;
            List<String> second = cluster.get(1).callOnInstance(() -> syncPoint(tableId));
            assertEquals("the second sync point should depend only on the write since " + synced.firstId + "; found " + second,
                         List.of(synced.touched), second.subList(1, second.size()));
        }
    }

    /**
     * Once a quorum has applied the first sync point, a sync point scanning the table should load only the
     * {@code CommandsForKey} of keys with new work, whether they are found in the cache or on disk.
     */
    @Test
    public void syncPointDoesNotLoadSyncedKeys() throws Throwable
    {
        try (Cluster cluster = cluster(Mode.QUORUM_APPLIED))
        {
            Synced synced = setup(cluster, Mode.QUORUM_APPLIED);
            String tableId = synced.tableId;
            for (IInvokableInstance instance : cluster)
            {
                // a visibility sync point does not filter by MaxDecidedRX, so loads every key that still has a write
                Map<Integer, List<String>> retained = instance.callOnInstance(() -> loadedKeys(tableId, Txn.Kind.VisibilitySyncPoint));
                Map<Integer, List<String>> loaded = instance.callOnInstance(() -> loadedKeys(tableId, Txn.Kind.ExclusiveSyncPoint));
                assertEquals("precondition: node " + instance.config().num() + " must still have every write before " + synced.firstId + "; found " + retained,
                             KEY_COUNT, retained.values().stream().mapToInt(List::size).sum());
                for (Map.Entry<Integer, List<String>> e : loaded.entrySet())
                {
                    List<String> expect = retained.get(e.getKey()).contains(synced.touched) ? List.of(synced.touched) : List.of();
                    assertEquals("node " + instance.config().num() + " store " + e.getKey() + " loaded keys with no new work since " + synced.firstId,
                                 expect, e.getValue());
                }
            }
        }
    }

    private static Cluster cluster(Mode mode) throws IOException
    {
        // no background durability, so the only sync points are ours and nothing is GC'd
        return Cluster.build(3)
                      .withoutVNodes()
                      .withConfig(c -> {
                          c.with(GOSSIP, NETWORK)
                           .set("accord.shard_durability_cycle", "1h")
                           .set("accord.global_durability_cycle", "1h");
                          if (mode == Mode.INFORM_DURABLE)
                              c.set("accord.always_inform_durable_single_key", true);
                      })
                      .start();
    }

    static class Synced
    {
        final String tableId, firstId, touched;

        Synced(String tableId, String firstId, String touched)
        {
            this.tableId = tableId;
            this.firstId = firstId;
            this.touched = touched;
        }
    }

    /**
     * Write every key, decide a sync point over the table on every replica, then write {@link #TOUCHED_PK} again.
     * Before the second write, wait until the earlier writes are durably committed or the sync point is quorum applied
     * as {@code mode} requires.
     */
    private static Synced setup(Cluster cluster, Mode mode)
    {
        fixDistributedSchemas(cluster);
        init(cluster);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int) WITH transactional_mode='full'"));
        String tableId = ClusterUtils.tableId(cluster, KEYSPACE, "tbl").toString();

        for (int pk = 0 ; pk < KEY_COUNT ; ++pk)
            write(cluster, pk);

        List<String> first = cluster.get(1).callOnInstance(() -> syncPoint(tableId));
        String firstId = first.get(0);
        assertEquals("precondition: the first sync point must depend on every key; found " + first, KEY_COUNT, first.size() - 1);

        for (IInvokableInstance instance : cluster)
        {
            instance.runOnInstance(() -> awaitDecided(tableId, firstId));
            if (mode == Mode.INFORM_DURABLE)
                instance.runOnInstance(() -> awaitDurablyCommitted(tableId));
        }

        if (mode == Mode.QUORUM_APPLIED)
            makeQuorumApplied(cluster, tableId, firstId);

        write(cluster, TOUCHED_PK);
        return new Synced(tableId, firstId, cluster.get(1).callOnInstance(() -> token(TOUCHED_PK)));
    }

    private static void write(Cluster cluster, int pk)
    {
        cluster.coordinator(1).execute(wrapInTxn(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?);")), ConsistencyLevel.ANY, pk, pk);
    }

    private static Ranges ranges(String tableId)
    {
        Node node = AccordService.instance().node();
        Ranges table = Ranges.single(TokenRange.fullRange(TableId.fromString(tableId), getPartitioner()));
        return node.topology().current().ranges().slice(table, Minimal);
    }

    private static String token(int pk)
    {
        return getPartitioner().getToken(Int32Type.instance.decompose(pk)).toString();
    }

    private static String token(RoutingKey key)
    {
        return ((TokenKey) key).token().toString();
    }

    /**
     * Coordinate an exclusive sync point over the table.
     * @return its id followed by the token of each key it depends on, repeated for each dependency on that key
     */
    private static List<String> syncPoint(String tableId)
    {
        Node node = AccordService.instance().node();
        SyncPoint syncPoint = getBlocking(CoordinateSyncPoint.exclusive(node, ranges(tableId)));
        KeyDeps keyDeps = syncPoint.waitFor.keyDeps;
        List<String> result = new ArrayList<>();
        lastSyncPoint = syncPoint;
        result.add(syncPoint.syncId.toString());
        for (int i = 0 ; i < keyDeps.txnIdCount() ; ++i)
        {
            for (RoutingKey key : keyDeps.participants(i))
                result.add(token(key));
        }
        return result;
    }

    /**
     * Have a quorum, but not node 3, report the sync point last coordinated by node 1 as applied, so that every
     * replica marks it QUORUM_APPLIED but not SHARD_APPLIED (which would GC the earlier writes).
     */
    private static void makeQuorumApplied(Cluster cluster, String tableId, String syncId)
    {
        // every replica applies the sync point itself, so can accept the durability report
        for (IInvokableInstance instance : cluster)
            instance.runOnInstance(() -> awaitStatus(tableId, syncId, LOCALLY_APPLIED.name()));

        cluster.filters().verbs(Verb.ACCORD_APPLY_AND_WAIT_REQ.id).to(3).drop();
        try
        {
            String remote = cluster.get(1).callOnInstance(() -> {
                DurabilityResult result = getBlocking(ExecuteSyncPoint.coordinate(AccordService.instance().node(), lastSyncPoint, 1).onDone());
                return result.min.remote.name();
            });
            assertEquals("precondition: only a quorum should apply " + syncId, SyncRemote.QuorumAndWaitedForAll.name(), remote);
        }
        finally
        {
            cluster.filters().reset();
        }

        for (IInvokableInstance instance : cluster)
            instance.runOnInstance(() -> awaitStatus(tableId, syncId, QUORUM_APPLIED.name()));
    }

    /**
     * Wait until every command store's {@code RedundantBefore} has {@code property} for {@code syncId} on the table
     * ranges it owns.
     */
    private static void awaitStatus(String tableId, String syncIdString, String propertyName)
    {
        TxnId syncId = TxnId.parse(syncIdString);
        RedundantStatus.Property property = RedundantStatus.Property.valueOf(propertyName);
        Ranges ranges = ranges(tableId);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        for (CommandStore store : AccordService.instance().node().commandStores().all())
        {
            Ranges owned = store.unsafeGetRangesForEpoch().all().slice(ranges, Minimal);
            if (owned.isEmpty())
                continue;

            while (!getBlocking(store.chain(() -> store.unsafeGetRedundantBefore().status(syncId, null, owned).all(property))))
            {
                assertTrue(store + " did not reach " + property + " for " + syncId + ": " + store.unsafeGetRedundantBefore(), System.nanoTime() < deadline);
                try { Thread.sleep(10); }
                catch (InterruptedException e) { throw new AssertionError(e); }
            }
        }
    }

    /**
     * Wait until every command store has recorded {@code syncId} as decided for the table ranges it owns.
     */
    private static void awaitDecided(String tableId, String syncIdString)
    {
        TxnId syncId = TxnId.parse(syncIdString);
        Ranges ranges = ranges(tableId);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        for (CommandStore store : AccordService.instance().node().commandStores().all())
        {
            Ranges owned = store.unsafeGetRangesForEpoch().all().slice(ranges, Minimal);
            while (!getBlocking(store.chain(() -> store.unsafeGetMaxDecidedRX().foldlWithDefault(owned, (v, prev) -> prev && v != null && v.any.compareTo(syncId) >= 0, null, true))))
            {
                assertTrue(store + " did not decide " + syncId + ": " + store.unsafeGetMaxDecidedRX(), System.nanoTime() < deadline);
                try { Thread.sleep(10); }
                catch (InterruptedException e) { throw new AssertionError(e); }
            }
        }
    }

    /**
     * Wait until every write in every {@code CommandsForKey} of the table is known to be durably committed.
     */
    private static void awaitDurablyCommitted(String tableId)
    {
        Node node = AccordService.instance().node();
        Ranges ranges = ranges(tableId);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        for (CommandStore store : node.commandStores().all())
        {
            Ranges owned = store.unsafeGetRangesForEpoch().all().slice(ranges, Minimal);
            if (owned.isEmpty())
                continue;

            while (true)
            {
                TxnId probe = node.nextTxnIdWithDefaultFlags(owned, Txn.Kind.VisibilitySyncPoint, Range, cardinality(owned));
                ExecutionContext context = ExecutionContext.unsequencedReadWrite(probe, owned, "Test await durably committed");
                String notDurable = getBlocking(store.chain(context, safeStore -> {
                    for (SafeCommandsForKey safeCfk : ((SaferCommandStore) safeStore).safeCommandsForKeys())
                    {
                        CommandsForKey cfk = safeCfk.current();
                        for (int i = 0 ; i < cfk.size() ; ++i)
                        {
                            if (!cfk.get(i).isDurablyCommitted())
                                return cfk.get(i).toString();
                        }
                    }
                    return null;
                }));
                if (notDurable == null)
                    break;
                assertTrue(store + " was not informed " + notDurable + " is durable", System.nanoTime() < deadline);
                try { Thread.sleep(10); }
                catch (InterruptedException e) { throw new AssertionError(e); }
            }
        }
    }

    /**
     * Run a task as a new sync point of {@code kind} over the table on every command store, as a sync point computing
     * its dependencies would.
     * @return per command store, the sorted tokens of the non-empty {@code CommandsForKey} the task loaded
     */
    private static Map<Integer, List<String>> loadedKeys(String tableId, Txn.Kind kind)
    {
        Node node = AccordService.instance().node();
        Ranges ranges = ranges(tableId);
        Map<Integer, List<String>> result = new TreeMap<>();
        for (CommandStore store : node.commandStores().all())
        {
            Ranges owned = store.unsafeGetRangesForEpoch().all().slice(ranges, Minimal);
            if (owned.isEmpty())
                continue;

            TxnId probe = node.nextTxnIdWithDefaultFlags(owned, kind, Range, cardinality(owned));
            ExecutionContext context = ExecutionContext.unsequencedReadWrite(probe, owned, "Test sync point key filtering");
            List<String> keys = getBlocking(store.chain(context, safeStore -> {
                TreeSet<String> loaded = new TreeSet<>();
                for (SafeCommandsForKey safeCfk : ((SaferCommandStore) safeStore).safeCommandsForKeys())
                {
                    if (safeCfk.current().size() > 0)
                        loaded.add(token(safeCfk.key()));
                }
                return new ArrayList<>(loaded);
            }));
            result.put(store.id(), keys);
        }
        return result;
    }
}
