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
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import org.apache.cassandra.service.StorageService;

public class IRHistorySyncTimeoutTest extends TestBaseImpl implements Serializable
{
    @Test
    public void testHistorySyncTimeout() throws Exception
    {
        try (Cluster cluster = init(Cluster.build().withNodes(2).withConfig(
                c -> c.with(Feature.NETWORK)
                      .with(Feature.GOSSIP)
                      .set("repair_history_sync_timeout", "1s")
                ).start()))
        {
            // alter the keyspace to look like Workflows
            cluster.schemaChange("ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}");

            // drop to make sure that the request times out
            cluster.filters().verbs(Verb.APPLE_QUERY_REPAIR_HISTORY_REQ.id).drop();

            // run a repair
            // for some reason the schema change is flakey on instance 1, but is stable on instance 2
            cluster.get(2).runOnInstance(() -> {
                String ranges = "0:1";
                Map<String, String> options = new HashMap<String, String>() {{
                    put(RepairOption.RANGES_KEY, ranges);
                }};

                int cmd = StorageService.instance.repairAsync("system_auth", options);
                Assert.assertTrue("Command could not be registered; given " + cmd, cmd > 0);

                List<String> status;
                do
                {
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    status = StorageService.instance.getParentRepairStatus(cmd);
                } while (status == null || status.get(0).equals(ParentRepairStatus.IN_PROGRESS.name()));

                // repair complete, make sure it failed
                Assert.assertEquals(status.toString(), 3, status.size());
                Assert.assertEquals(ParentRepairStatus.FAILED.name(), status.get(0));
                Assert.assertTrue("Expected message to contain \"Timeout waiting for task.\" but does not: given " + status.get(1), status.get(1).contains("java.util.concurrent.TimeoutException"));
            });
        }
    }
}
