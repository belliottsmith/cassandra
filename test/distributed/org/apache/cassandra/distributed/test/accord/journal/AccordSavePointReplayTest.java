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

package org.apache.cassandra.distributed.test.accord.journal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.ExecuteSyncPoint;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.RedundantStatus;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.MaxUncachedTxnId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.execution.AccordExecutor;
import org.apache.cassandra.service.accord.execution.SaferCommandStore;
import org.apache.cassandra.service.accord.journal.ReplayMarkers;

import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * State restored from a save point reflects only what was durable as of the save point, so replay must decide what to
 * replay by the save point's {@code RedundantBefore}, not the current one: otherwise, if a later save point is missing
 * (as after an unclean shutdown), commands written since the save point that have since become durable are skipped,
 * and are missing from the restored state.
 * <p>
 * We check this with {@link MaxUncachedTxnId}, which bounds the {@code CommandsForKey} not in the cache: it is valid
 * on restore only if replay brings into the cache every {@code CommandsForKey} changed since the save point.
 */
public class AccordSavePointReplayTest extends TestBaseImpl
{
    private static final int KEY_COUNT = 16;
    private static final long TIMEOUT_SECONDS = 30;

    @Test
    public void replayFromEarlierSavePoint() throws Throwable
    {
        try (Cluster cluster = Cluster.build().withNodes(1)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(1))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(1, "dc0", "rack0"))
                                      .withConfig(config -> config.set("accord.shard_durability_cycle", "1h")
                                                                  .set("accord.global_durability_cycle", "1h")
                                                                  .set("accord.catchup_on_start", "false")
                                                                  .with(NETWORK, GOSSIP))
                                      .start())
        {
            IInvokableInstance instance = cluster.get(1);
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':1}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int PRIMARY KEY, v int) WITH transactional_mode='full'");
            String tableId = ClusterUtils.tableId(cluster, "ks", "tbl").toString();
            String saveDirectory = instance.callOnInstance(() -> ReplayMarkers.saveDirectory().absolutePath());

            // write every key, and make the writes irrelevant to later sync points, which a scan then bounds
            write(cluster, 0);
            instance.runOnInstance(() -> syncPoint(tableId, true));
            assertEquals(Map.of(), instance.callOnInstance(() -> loadedKeys(tableId)));
            // a clean restart saves those bounds
            instance.shutdown().get();
            instance.startup();
            assertTrue("precondition: no bounds restored", instance.callOnInstance(() -> hasBounds(tableId)));

            // write every key again, relevant to later sync points (as not yet quorum applied), and durable on shutdown
            write(cluster, 1);
            String syncId = instance.callOnInstance(() -> syncPoint(tableId, false));
            List<String> written = instance.callOnInstance(() -> latestTxnIds(tableId));
            assertEquals("precondition: not every key written " + written, KEY_COUNT, written.size());
            instance.shutdown().get();

            // lose the latest save point, as though we had crashed before writing it
            List<Path> lost = deleteLatestSavePoints(saveDirectory);
            assertTrue("precondition: no save point to lose", !lost.isEmpty());

            instance.startup();
            instance.runOnInstance(() -> assertDurable(tableId, syncId));
            // replay must load every command not durable as of the save point we restored from
            assertEquals(List.of(), instance.callOnInstance(() -> notReplayed(written)));
            // replay must have brought every key written since the save point into the cache, as the restored bounds
            // do not cover them (with max_uncached_txnid_mode VALIDATE, as for all dtests, we fail if we would skip any)
            Map<Integer, List<String>> loaded = instance.callOnInstance(() -> loadedKeys(tableId));
            assertEquals("loaded " + loaded, KEY_COUNT, loaded.values().stream().mapToInt(List::size).sum());
        }
    }

    private static void write(Cluster cluster, int value)
    {
        for (int k = 0 ; k < KEY_COUNT ; ++k)
            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO ks.tbl (k, v) VALUES (?, ?);"), ConsistencyLevel.ANY, k, value);
    }

    private static Ranges ranges(String tableId)
    {
        Node node = AccordService.instance().node();
        Ranges table = Ranges.single(TokenRange.fullRange(TableId.fromString(tableId), getPartitioner()));
        return node.topology().current().ranges().slice(table, Minimal);
    }

    /**
     * Coordinate an exclusive sync point over the table and wait for it to be applied locally, and if {@code durable}
     * for it to be applied by a quorum
     */
    private static String syncPoint(String tableId, boolean durable)
    {
        Node node = AccordService.instance().node();
        SyncPoint syncPoint = getBlocking(CoordinateSyncPoint.exclusive(node, ranges(tableId)));
        awaitStatus(tableId, syncPoint.syncId, LOCALLY_APPLIED);
        if (durable)
        {
            getBlocking(ExecuteSyncPoint.coordinate(node, syncPoint, 1).onQuorumOrDone());
            awaitStatus(tableId, syncPoint.syncId, QUORUM_APPLIED);
        }
        return syncPoint.syncId.toString();
    }

    private static void awaitStatus(String tableId, TxnId syncId, RedundantStatus.Property property)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        forEachStore(tableId, (store, owned) -> {
            while (!getBlocking(store.chain(() -> store.unsafeGetRedundantBefore().status(syncId, null, owned).all(property))))
            {
                assertTrue(store + " did not reach " + property + " for " + syncId + ": " + store.unsafeGetRedundantBefore(), System.nanoTime() < deadline);
                try { Thread.sleep(10); }
                catch (InterruptedException e) { throw new AssertionError(e); }
            }
        });
    }

    /**
     * Every write precedes {@code syncId}, so is durable to the command store, so replaying by the current
     * {@code RedundantBefore} would skip it
     */
    private static void assertDurable(String tableId, String syncIdString)
    {
        TxnId syncId = TxnId.parse(syncIdString);
        forEachStore(tableId, (store, owned) -> {
            assertTrue("precondition: writes not durable to " + store + ": " + store.unsafeGetRedundantBefore(),
                       getBlocking(store.chain(() -> store.unsafeGetRedundantBefore().foldl(owned, (bounds, durable) -> {
                           return durable && bounds != null && bounds.maxBound(LOCALLY_DURABLE_TO_COMMAND_STORE).compareTo(syncId) >= 0;
                       }, true))));
        });
    }

    private static boolean hasBounds(String tableId)
    {
        boolean[] result = { true };
        forEachStore(tableId, (store, owned) -> {
            try (AccordExecutor.ExclusiveGlobalCaches ignore = ((AccordCommandStore) store).executor().lockCaches())
            {
                result[0] &= !((AccordCommandStore) store).unsafeGetMaxUncachedTxnId().isEmpty();
            }
        });
        return result[0];
    }

    /**
     * Run a task as a new exclusive sync point over the table on every command store, as a sync point computing its
     * dependencies would.
     * @return per command store, the sorted tokens of the non-empty {@code CommandsForKey} the task loaded
     */
    private static Map<Integer, List<String>> loadedKeys(String tableId)
    {
        Node node = AccordService.instance().node();
        Map<Integer, List<String>> result = new TreeMap<>();
        forEachStore(tableId, (store, owned) -> {
            TxnId probe = node.nextTxnIdWithDefaultFlags(owned, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, cardinality(owned));
            ExecutionContext context = ExecutionContext.unsequencedReadWrite(probe, owned, "Test save point replay");
            List<String> keys = getBlocking(store.chain(context, safeStore -> {
                TreeSet<String> loaded = new TreeSet<>();
                for (SafeCommandsForKey safeCfk : ((SaferCommandStore) safeStore).safeCommandsForKeys())
                {
                    if (safeCfk.current().size() > 0)
                        loaded.add(((TokenKey) safeCfk.key()).token().toString());
                }
                return new ArrayList<>(loaded);
            }));
            if (!keys.isEmpty())
                result.put(store.id(), keys);
        });
        return result;
    }

    /**
     * @return the latest transaction of each {@code CommandsForKey} of the table
     */
    private static List<String> latestTxnIds(String tableId)
    {
        Node node = AccordService.instance().node();
        List<String> result = new ArrayList<>();
        forEachStore(tableId, (store, owned) -> {
            TxnId probe = node.nextTxnIdWithDefaultFlags(owned, Txn.Kind.VisibilitySyncPoint, Routable.Domain.Range, cardinality(owned));
            ExecutionContext context = ExecutionContext.unsequencedReadWrite(probe, owned, "Test save point replay");
            result.addAll(getBlocking(store.chain(context, safeStore -> {
                List<String> latest = new ArrayList<>();
                for (SafeCommandsForKey safeCfk : ((SaferCommandStore) safeStore).safeCommandsForKeys())
                {
                    CommandsForKey cfk = safeCfk.current();
                    if (cfk.size() > 0)
                        latest.add(cfk.get(cfk.size() - 1).plainTxnId().toString());
                }
                return latest;
            })));
        });
        return result;
    }

    /**
     * @return those of {@code txnIds} that are not in their command store's cache
     */
    private static List<String> notReplayed(List<String> txnIds)
    {
        List<String> result = new ArrayList<>();
        for (String txnIdString : txnIds)
        {
            TxnId txnId = TxnId.parse(txnIdString);
            boolean found = false;
            for (CommandStore store : AccordService.instance().node().commandStores().all())
            {
                AccordCommandStore commandStore = (AccordCommandStore) store;
                try (AccordExecutor.ExclusiveGlobalCaches ignore = commandStore.executor().lockCaches())
                {
                    found |= commandStore.cachesExclusive().commands().getUnsafe(txnId) != null;
                }
            }
            if (!found)
                result.add(txnIdString);
        }
        return result;
    }

    private static void forEachStore(String tableId, BiConsumer<CommandStore, Ranges> forEach)
    {
        Ranges ranges = ranges(tableId);
        for (CommandStore store : AccordService.instance().node().commandStores().all())
        {
            Ranges owned = store.unsafeGetRangesForEpoch().all().slice(ranges, Minimal);
            if (!owned.isEmpty())
                forEach.accept(store, owned);
        }
    }

    /**
     * Delete the latest save point of each command store
     * @return the save points deleted
     */
    private static List<Path> deleteLatestSavePoints(String saveDirectory) throws IOException
    {
        List<Path> deleted = new ArrayList<>();
        List<Path> storeDirs;
        try (Stream<Path> list = Files.list(Paths.get(saveDirectory)))
        {
            storeDirs = list.filter(Files::isDirectory).collect(Collectors.toList());
        }
        for (Path storeDir : storeDirs)
        {
            List<Path> savePoints;
            try (Stream<Path> list = Files.list(storeDir))
            {
                savePoints = list.filter(p -> p.getFileName().toString().matches("[0-9]+"))
                                 .sorted(Comparator.comparingLong(p -> Long.parseLong(p.getFileName().toString())))
                                 .collect(Collectors.toList());
            }
            // keep the earlier save point to restore from
            if (savePoints.size() < 2)
                continue;

            Path latest = savePoints.get(savePoints.size() - 1);
            try (Stream<Path> walk = Files.walk(latest))
            {
                for (Path p : walk.sorted(Comparator.reverseOrder()).collect(Collectors.toList()))
                    Files.delete(p);
            }
            deleted.add(latest);
        }
        return deleted;
    }
}
