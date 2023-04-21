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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class SlowDecommissionTest extends TestBaseImpl
{
    @Test
    public void testSlowShutdown() throws IOException
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            cluster.get(4).runOnInstance(() -> MessagingService.shutdownTimeoutMinutes = 0);
            cluster.get(4).nodetoolResult("decommission");
            cluster.get(4).runOnInstance(() -> {
                assertEquals("DECOMMISSIONED", StorageService.instance.getOperationMode());
            });
        }
    }
}