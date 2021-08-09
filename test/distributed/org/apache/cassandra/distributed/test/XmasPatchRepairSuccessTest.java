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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.RepairResult;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.insert;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.options;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.repair;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XmasPatchRepairSuccessTest extends TestBaseImpl
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        TestBaseImpl.beforeClass();

        // Load the conf in the main classloader so that conf.cross_node_timeout is populated
        // as it gets used when messages are deserialized in PreviewRepairTest.DelayFirstRepairTypeMessageFilter
        // for message filtering
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * This tests when we receive a repair success (a finished full repair) during a preview repair
     *
     * 1. we start a preview repair
     * 2. pause the validation requests from node1 -> node2
     * 3. node1 starts its validation
     * 4. run a full repair which completes fine & sends repair success
     * 5. preview repair should have failed
     *
     */
    @Test
    public void testFinishFullRepairDuringPreviewRepair() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .set("enable_christmas_patch", true)
                                                                     .set("incremental_updates_last_repaired", false)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            Thread.sleep(2000);
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).callOnInstance(repair(options(false, false)));

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            Condition previewRepairStarted = newOneTimeCondition();
            Condition continuePreviewRepair = newOneTimeCondition();
            PreviewRepairTest.DelayFirstRepairTypeMessageFilter filter = PreviewRepairTest.DelayFirstRepairTypeMessageFilter.validationRequest(previewRepairStarted, continuePreviewRepair);
            // this pauses the validation request sent from node1 to node2 until we have completed the inc repair below
            cluster.filters().outbound().verbs(Verb.VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();
            Future<RepairResult> rsFuture = es.submit(() -> cluster.get(1).callOnInstance(repair(options(true, true))));
            previewRepairStarted.await();
            // this needs to finish before the preview repair is unpaused on node2
            cluster.get(1).callOnInstance(repair(options(false, true)));
            continuePreviewRepair.signalAll();
            RepairResult rs = rsFuture.get();
            assertFalse(rs.success); // preview repair should have failed
            assertFalse(rs.wasInconsistent); // and no mismatches should have been reported
            assertTrue(getRepairTimeFor(cluster.get(1), "0:0") > 0);
        }
        finally
        {
            es.shutdown();
        }
    }

    /**
     * same as testFinishFullRepairDuringPreviewRepair but here we repair non-intersecting ranges so everything should succeed
     */
    @Test
    public void testFinishNonIntersectingFullRepairDuringPreviewRepair() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("disable_incremental_repair", false)
                                                                     .set("enable_christmas_patch", true)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            Thread.sleep(1000);
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            assertTrue(cluster.get(1).callOnInstance(repair(options(false, false))).success);

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // pause preview repair validation messages on node2 until node1 has finished
            Condition previewRepairStarted = newOneTimeCondition();
            Condition continuePreviewRepair = newOneTimeCondition();
            PreviewRepairTest.DelayFirstRepairTypeMessageFilter filter = PreviewRepairTest.DelayFirstRepairTypeMessageFilter.validationRequest(previewRepairStarted, continuePreviewRepair);
            cluster.filters().outbound().verbs(Verb.VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();

            // get local ranges to repair two separate ranges:
            List<String> localRanges = cluster.get(1).callOnInstance(() -> {
                List<String> res = new ArrayList<>();
                for (Range<Token> r : StorageService.instance.getLocalReplicas(KEYSPACE).ranges())
                    res.add(r.left.getTokenValue()+ ":"+ r.right.getTokenValue());
                return res;
            });

            assertEquals(2, localRanges.size());
            String previewedRange = localRanges.get(0);
            String repairedRange = localRanges.get(1);
            Future<RepairResult> repairStatusFuture = es.submit(() -> cluster.get(1).callOnInstance(repair(options(true, true, previewedRange))));
            previewRepairStarted.await();
            // this needs to finish before the preview repair is unpaused on node2
            assertTrue(cluster.get(1).callOnInstance(repair(options(false, true, repairedRange))).success);

            continuePreviewRepair.signalAll();
            RepairResult rs = repairStatusFuture.get();
            assertTrue(rs.success); // repair should succeed
            assertFalse(rs.wasInconsistent); // and no mismatches
            int repairedRepairTime = getRepairTimeFor(cluster.get(1), repairedRange);
            int unrepairedRepairTime = getRepairTimeFor(cluster.get(1), previewedRange);

            assertTrue(repairedRepairTime > 0);
            assertEquals(Integer.MIN_VALUE, unrepairedRepairTime);
        }
        finally
        {
            es.shutdown();
        }
    }

    int getRepairTimeFor(IInvokableInstance instance, String tokens)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            String [] startEnd = tokens.split(":");
            Token t = cfs.getPartitioner().getTokenFactory().fromString(startEnd[1]);
            return cfs.getRepairTimeSnapshot().getLastSuccessfulRepairTimeFor(t);
        });
    }

}
