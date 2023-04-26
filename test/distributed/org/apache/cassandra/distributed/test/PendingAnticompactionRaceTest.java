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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;


public class PendingAnticompactionRaceTest extends TestBaseImpl
{
    @Test
    public void testIncAndFullRepairRace() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withInstanceInitializer(BB::install)
                                          .withConfig(config ->
                                                      config.set("disable_incremental_repair", false)
                                                            .with(GOSSIP)
                                                            .with(NETWORK)).start()))
        {
            ExecutorService es = Executors.newFixedThreadPool(1);
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, x int) with compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}"));
            for (int j = 0; j < 5; j++)
            {
                for (int i = 0; i < 20; i++)
                    cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, x) values (?, 1)"), ConsistencyLevel.ALL, i);
                cluster.stream().forEach(instance -> instance.flush(KEYSPACE));
            }

            Future<NodeToolResult> res = es.submit(() -> cluster.get(1).nodetoolResult("repair", "-st", "0", "-et", "100"));
            cluster.get(1).runOnInstance(() -> waitFor(BB.waitingForPredicate));
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                cfs.getCompactionStrategyManager().resume();
                cfs.enableAutoCompaction(true);
            });
            cluster.get(1).runOnInstance(() -> BB.allowPredicate.countDown());
            res.get().asserts().failure(); // since the sstables have been compacted away we can't mark them compacting, causing the repair to fail.
            cluster.get(1).logs().watchFor("Could not mark compacting");
        }
    }

    @Test
    public void testIncAndFullRepairNoRace() throws IOException
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config ->
                                                      config.set("disable_incremental_repair", false)
                                                            .with(GOSSIP)
                                                            .with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, x int) with compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}"));
            for (int j = 0; j < 5; j++)
            {
                for (int i = 0; i < 20; i++)
                    cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, x) values (?, 1)"), ConsistencyLevel.ALL, i);
                cluster.stream().forEach(instance -> instance.flush(KEYSPACE));
            }

            cluster.get(1).nodetoolResult("repair", "-st", "0", "-et", "100").asserts().success();
            cluster.forEach(i -> {
                try
                {
                    i.logs().watchFor("Starting anticompaction for distributed_test_keyspace.tbl on 0/");
                }
                catch (TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public static class BB
    {
        static CountDownLatch allowPredicate = new CountDownLatch(1);
        static CountDownLatch waitingForPredicate = new CountDownLatch(1);

        public static void install(ClassLoader cl, int instance)
        {
            if (instance == 1)
            {
                new ByteBuddy().rebase(PendingAntiCompaction.AntiCompactionPredicate.class)
                               .method(named("apply"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
                new ByteBuddy().rebase(PendingAntiCompaction.AcquisitionCallable.class)
                               .method(named("acquireSSTables"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);            }

        }

        public static boolean apply(SSTableReader sstable, @SuperCall Callable<Boolean> zuper)
        {
            waitingForPredicate.countDown();
            waitFor(allowPredicate);
            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        // Prevent the anti-compaction failures from retrying - simulates a long anticompaction that holds
        // sstables for a long time withoug having to wait.
        public static PendingAntiCompaction.AcquireResult acquireSSTables(@SuperCall Callable<PendingAntiCompaction.AcquireResult> zuper)
        {
            try
            {
                return zuper.call();
            }
            catch (PendingAntiCompaction.SSTableAcquisitionException ex)
            {
                return null; //undo the retry mechanism
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
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
