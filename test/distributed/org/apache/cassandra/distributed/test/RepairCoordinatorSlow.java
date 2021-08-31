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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairParallelism;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairFailedWithMessageContains;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairNotExist;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.getRepairExceptions;

public abstract class RepairCoordinatorSlow extends RepairCoordinatorBase
{
    public RepairCoordinatorSlow(RepairType repairType, RepairParallelism parallelism, boolean withNotifications)
    {
        super(repairType, parallelism, withNotifications);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void prepareRPCTimeout()
    {
        String table = tableName("preparerpctimeout");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        IMessageFilters.Filter filter = CLUSTER.filters()
                                               .inbound()
                                               .messagesMatching(ClusterUtils.repairMatcher(RepairMessage.Type.PREPARE_MESSAGE))
                                               .drop();
        try
        {
            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            result.asserts()
                  .failure()
                  .errorContains("Did not get replies from all endpoints.");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(NodeToolResult.ProgressEventType.START, "Starting repair command")
                      .notificationContains(NodeToolResult.ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Did not get replies from all endpoints.")
                      .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
            }

            if (repairType != RepairType.PREVIEW)
            {
                assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Did not get replies from all endpoints.");
            }
            else
            {
                assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
            }

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
        }
        finally
        {
            filter.off();
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void neighbourDown() throws InterruptedException, ExecutionException
    {
        String table = tableName("neighbourdown");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        String downNodeAddress = CLUSTER.get(2).callOnInstance(() -> FBUtilities.getBroadcastAddress().getHostAddress());
        Future<Void> shutdownFuture = CLUSTER.get(2).shutdown();
        try
        {
            // wait for the node to stop
            shutdownFuture.get();
            // wait for the failure detector to detect this
            CLUSTER.get(1).runOnInstance(() -> {
                InetAddress neighbor;
                try
                {
                    neighbor = InetAddress.getByName(downNodeAddress);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                while (FailureDetector.instance.isAlive(neighbor))
                    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            });

            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            result.asserts()
                  .failure()
                  .errorContains("Endpoint not alive");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(NodeToolResult.ProgressEventType.START, "Starting repair command")
                      .notificationContains(NodeToolResult.ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Endpoint not alive")
                      .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
            }

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
        }
        finally
        {
            CLUSTER.get(2).startup();
        }

        // make sure to call outside of the try/finally so the node is up so we can actually query
        if (repairType != RepairType.PREVIEW)
        {
            assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Endpoint not alive");
        }
        else
        {
            assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
        }
    }

    @Test(timeout = 1 * 60 * 1000)
    public void validationParticipentCrashesAndComesBack()
    {
        // Test what happens when a participant restarts in the middle of validation
        // Currently this isn't recoverable but could be.
        // TODO since this is a real restart, how would I test "long pause"? Can't send SIGSTOP since same procress
        String table = tableName("validationparticipentcrashesandcomesback");
        CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
        AtomicReference<Future<Void>> participantShutdown = new AtomicReference<>();

        IMessageFilters.Filter filter = CLUSTER.filters()
                                               .inbound()
                                               .to(2)
                                               .messagesMatching(ClusterUtils.repairMatcher(RepairMessage.Type.VALIDATION_REQUEST, (from, to, m) -> {
                                                   // the nice thing about this is that this lambda is "capturing" and not "transfer", what this means is that
                                                   // this lambda isn't serialized and any object held isn't copied.
                                                   participantShutdown.set(CLUSTER.get(2).shutdown());
                                                   return true; // drop it so this node doesn't reply before shutdown.
                                               }))
                                               .drop();
        try
        {
            // since nodetool is blocking, need to handle participantShutdown in the background
            CompletableFuture<Void> recovered = CompletableFuture.runAsync(() -> {
                try {
                    while (participantShutdown.get() == null) {
                        // event not happened, wait for it
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                    Future<Void> f = participantShutdown.get();
                    f.get(); // wait for shutdown to complete
                    CLUSTER.get(2).startup();
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException(e);
                }
            });

            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            recovered.join(); // if recovery didn't happen then the results are not what are being tested, so block here first
            result.asserts()
                  .failure()
                  .errorContains("/127.0.0.2 died");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(NodeToolResult.ProgressEventType.ERROR, "/127.0.0.2 died")
                      .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
            }

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
            if (repairType != RepairType.PREVIEW)
            {
                assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Endpoint /127.0.0.2 died");
            }
            else
            {
                assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
            }
        }
        finally
        {
            filter.off();
            try {
                CLUSTER.get(2).startup();
            } catch (Exception e) {
                // if you call startup twice it is allowed to fail, so ignore it... hope this didn't brike the other tests =x
            }
        }
    }
}
