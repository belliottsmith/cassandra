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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.RepairResult;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.consistent.PendingAntiCompaction;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.insert;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.options;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.repair;
import static org.junit.Assert.assertFalse;

public class IncRepairTruncationTest extends TestBaseImpl
{
    /**
     * Lots of sleeps here to make it possible to reproduce, basic flow is
     *
     * 1. insert data & repair
     * 2. start thread to continuosly insert data
     * 3. run a new inc repair, on node2 we delay the anticompaction start for 3s
     * 4. wait 1.5s
     * 5. start truncation, on node2 delay the truncation execution for 5s
     * 5. wait for truncation and inc repair
     * 6. run preview repair to make sure everything is ok
     *
     * Idea is that since we pick sstables for anticompaction after truncation has picked its truncatedAt
     * value, those sstables will be kept around after the truncation, making preview repair fail.
     *
     */
    @Test
    public void testTruncateDuringIncRepair() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newFixedThreadPool(3);
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.set("disable_incremental_repair", false)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .withInstanceInitializer(BBHelper::install)
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            Thread.sleep(1000);
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            // everything repaired
            cluster.get(1).callOnInstance(repair(options(false, false)));

            AtomicBoolean end = new AtomicBoolean(false);
            Future<?> writes = es.submit(() -> {
                int i = 0;
                while (!end.get())
                {
                    insert(cluster.coordinator(1), i++ * 20, 20);
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                }
            });
            SimpleCondition blockMerkleTreeResponses = new SimpleCondition();
            cluster.filters().verbs(MessagingService.Verb.REPAIR_MESSAGE.getId()).messagesMatching((from, to, message) -> {
                try
                {
                    RepairMessage repairMessage = (RepairMessage)Instance.deserializeMessage(message).left.payload;
                    if (repairMessage.messageType == RepairMessage.Type.VALIDATION_COMPLETE)
                        blockMerkleTreeResponses.await();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                return false;
            }).drop();
            Future<RepairResult> rsFuture = es.submit(() -> cluster.get(1).callOnInstance(repair(options(false, false))));

            cluster.setUncaughtExceptionsFilter(
                (instanceNum, throwable) -> instanceNum == 1 &&
                                            throwable.getMessage().contains("Parent repair session ") &&
                                            throwable.getMessage().contains("has failed."));

            Future<?> f = es.submit( () -> cluster.coordinator(1).execute("TRUNCATE "+KEYSPACE+".tbl", ConsistencyLevel.ALL));
            blockMerkleTreeResponses.signalAll();

            f.get();
            rsFuture.get();
            end.set(true);
            writes.get();

            assertFalse(cluster.get(1).callOnInstance(repair(options(true, false))).wasInconsistent);

        }
        finally
        {
            es.shutdown();
        }
    }

    public static class BBHelper
    {
        public static PendingAntiCompaction.AcquisitionCallable getAcquisitionCallable(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, UUID prsId, int acquireRetrySeconds, int acquireSleepMillis)
        {
            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
            return new PendingAntiCompaction.AcquisitionCallable(cfs, ranges, prsId, acquireRetrySeconds, acquireSleepMillis);
        }

        public static <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews)
        {
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            return cfs.runWithCompactionsDisabled(callable, (sstable) -> true, interruptValidation, interruptViews, true);
        }

        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num == 2)
            {
                new ByteBuddy().redefine(PendingAntiCompaction.class)
                               .method(named("getAcquisitionCallable"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().redefine(ColumnFamilyStore.class)
                               .method(named("runWithCompactionsDisabled").and(takesArguments(3)))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }
    }
}
