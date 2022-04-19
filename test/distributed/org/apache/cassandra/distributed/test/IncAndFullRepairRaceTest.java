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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertNull;

public class IncAndFullRepairRaceTest extends TestBaseImpl
{
    /*
    problem is basically a race between inc repair starting and full repair finishing
    * tombstone needs to be gcgs-expired when full repair finishes & inc repair starts
      - requires low gcgs (zone_meta_sync_history has 10h gcgs)

    (_unrepaired/_pending/_repaired indicates which data set the sstable is in, not xmas patch repaired status)

    1. node1 contains sstable <t>_unrepaired (tombstone with gcgs expired)
    2. node2 contains sstable <d>_unrepaired (data, for the deleted partition in (1), data is older than tombstone)
    3. full repair is running
    4. inc repair is starting
    5. inc repair picks <d>_unrepaired on node2, starts anticompaction
    6. full repair finishes:
       - publish streamed <d>_unrepaired on node1
       - publish streamed <t>_unrepaired on node2
       - updates xmas patch repaired history, making <t>_unrepaired purgeable
       *** after patch *** - inc repair is aborted due to being in PREPARING state
       now node1 has <t>_unrepaired + <d>_unrepaired
           node2 has <t>_unrepaired + <d>_pending
    7. inc repair picks <t>_unrepaired + <d>_unrepaired on node1
    8. anticompaction on node1 compacts sstables together, dropping both <d>_unrepaired and <t>_unrepaired
    9. now we have:
       node1: nothing
       node2: <t>_unrepaired, <d>_pending
    10. inc repair streams <d>_pending to node1 and finishes
    11. state:
        node1: <d>_repaired
        node2: <d>_repaired, <t>_unrepaired
    12. new inc repair runs, node1 has nothing unrepaired, node2 has <t>_unrepaired
        - nothing is streamed, unrepaired data matches (due to <t>_unrepaired being purgeable according to xmas patch)
    13. <t>_unrepaired on node2 gets marked repaired
        state:
        node1: <d>_repaired
        node2: <d>_repaired, <t>_repaired -> compaction purges <t>_repaired + <d>_repaired
    14. data is resurrected on node1, next full repair will copy it everywhere
    */
    @Test
    public void testIncAndFullRepairRace() throws IOException, InterruptedException, TimeoutException, ExecutionException
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withInstanceInitializer(BB::install)
                                          .withConfig(config ->
                                                      config.set("disable_incremental_repair", false)
                                                            .set("snapshot_on_repaired_data_mismatch", true)
                                                            .set("enable_christmas_patch", true)
                                                            .with(GOSSIP)
                                                            .with(NETWORK)).start()))
        {
            ExecutorService es = Executors.newFixedThreadPool(2);
            // not strictly needed - the later compactions among pending sstables could just as easily purge this tombstone + data:
            cluster.forEach((i) -> i.runOnInstance(() -> CompactionManager.setAvoidAnticompactingMutatingMetadataUnsafe(true)));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, x int) with compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled':false } and gc_grace_seconds=0"));
            cluster.get(2).executeInternal(withKeyspace("insert into %s.tbl (id, x) values (50, 1)"));
            Thread.sleep(1000);
            cluster.get(1).executeInternal(withKeyspace("delete x from %s.tbl where id = 50"));
            cluster.stream().forEach(instance -> instance.flush(KEYSPACE));
            Thread.sleep(1000); // wait for gcgs to expire

            /*
            state:
            node1: gcgs-expired tombstone
            node2: data
             */

            /*
            start full repair, this will block before publishing streamed sstables on node 2
             */
            Future<?> fullRepair = es.submit(() -> cluster.get(1).nodetoolResult("repair", KEYSPACE, "tbl", "-pr", "-full").asserts().success());

            /*
            wait until node1 has published its sstables
             */
            cluster.get(1).logs().watchFor("Adding sstables");

            /*
             now stream has published sstable on node1, but not on node2, start inc repair

             before the patch node1 will anticompact the tombstone-containing sstable + stream-published data-sstable
             after the patch this incremental repair will fail
             */
            Future<?> incRepair = es.submit(() -> cluster.get(1).nodetoolResult("repair", KEYSPACE, "-pr", "tbl").asserts().failure());
            cluster.get(2).logs().watchFor("Prepare phase for incremental repair session");
            /*
            we have now anticompacted the sstable containing the data, allow node2 to publish streamed files
            */
            cluster.get(2).runOnInstance(() -> BB.allowStreamPublishSSTables.countDown());
            fullRepair.get();

            /*
            start anticompaction on node1
             */
            cluster.get(1).runOnInstance(() -> BB.doAntiCompaction.countDown());

            incRepair.get();

            cluster.forEach(i -> {
                try
                {
                    i.logs().watchFor("Aborting inc repair session");
                }
                catch (TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
            });
            // just move sstables to repaired, needed since autocompaction is disabled:
            cluster.get(1).forceCompact(KEYSPACE, "tbl");
            cluster.get(2).forceCompact(KEYSPACE, "tbl");

            /*
            * now we have the current state:
            node1 contains the data + no tombstone, the data was streamed in by the incremental repair
            node2 contains data + tombstone, data is repaired, tombstone not, *right now* preview repair is ok
             - run another inc repair. this will not stream anything since the tombstone is purgeable (nothing == purgeable tombstone)
               now the tombstone has moved to repaired on node2,
             */
            cluster.get(1).nodetoolResult("repair", KEYSPACE, "-pr", "tbl").asserts().success();

            cluster.get(1).forceCompact(KEYSPACE, "tbl");
            cluster.get(2).forceCompact(KEYSPACE, "tbl");

            cluster.get(1).nodetoolResult("repair", "-vd",
                                           KEYSPACE, "tbl").asserts().success().stdoutContains("Repaired data is in sync");

            cluster.forEach((i) -> assertNull(i.executeInternal(withKeyspace("select x from %s.tbl where id = 50"))[0][0]));
        }
    }

    public static class BB
    {
        static CountDownLatch allowStreamPublishSSTables = new CountDownLatch(1);
        static CountDownLatch doAntiCompaction = new CountDownLatch(1);

        public static void install(ClassLoader cl, int instance)
        {
            if (instance != 1)
            {
                new ByteBuddy().rebase(ColumnFamilyStore.class)
                               .method(named("addSSTables"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
            else
            {
                new ByteBuddy().rebase(PendingAntiCompaction.class)
                               .method(named("run"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }


        }

        public static void addSSTables(Collection<SSTableReader> sstables, @SuperCall Callable<Void> zuper) throws Exception
        {
            waitFor(allowStreamPublishSSTables);
            zuper.call();
        }

        public static org.apache.cassandra.utils.concurrent.Future<List<Void>> run(@SuperCall Callable<org.apache.cassandra.utils.concurrent.Future<List<Void>>> zuper) throws Exception
        {
            waitFor(doAntiCompaction);
            return zuper.call();
        }

    }

    private static void waitFor(CountDownLatch cdl)
    {
        try
        {
            cdl.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
