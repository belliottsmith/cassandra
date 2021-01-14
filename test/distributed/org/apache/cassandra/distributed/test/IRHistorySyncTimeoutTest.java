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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.net.MessagingService;

public class IRHistorySyncTimeoutTest extends TestBaseImpl implements Serializable
{
    @Test
    public void testHistorySyncTimeout() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c ->
                                                       c.with(Feature.NETWORK)
                                                        .with(Feature.GOSSIP)
                                                       .set("repair_history_sync_timeout", "1s")
                                           )
                                           .start()))
        {
            cluster.setUncaughtExceptionsFilter(t -> t.getMessage().contains("Timeout waiting for task"));

            // alter the keyspace to look like Workflows
            cluster.schemaChange("ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}");

            // drop to make sure that the request times out
            cluster.filters().verbs(MessagingService.Verb.APPLE_QUERY_REPAIR_HISTORY.ordinal()).drop();

            // run a repair
            // for some reason the schema change is flakey on instance 1, but is stable on instance 2
            cluster.get(2).nodetoolResult("repair", "system_auth", "--full", "--partitioner-range")
                   .asserts()
                   .failure()
                   .errorContains("Timeout waiting for task");
        }
    }
}
