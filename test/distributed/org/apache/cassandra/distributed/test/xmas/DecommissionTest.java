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

package org.apache.cassandra.distributed.test.xmas;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.populate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DecommissionTest extends TestBaseImpl
{
    @Test
    public void testSendRepairedRangesWhenLeaving() throws IOException
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP).set("enable_shadow_christmas_patch", true))
                                        .start())
        {
            populate(cluster,0, 1000);
            // check last repair time for the test table according to node3, there should be no value at start
            assertLastSuccessfulRepair(cluster.get(3), null);
            // artificially set last repair time for the table on the leaving node
            long artificialRepairTime = System.currentTimeMillis();
            cluster.get(4).runOnInstance(() -> {
                IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
                SystemKeyspace.updateLastSuccessfulRepair("distributed_test_keyspace", "tbl",
                                                          new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken()),
                                                          artificialRepairTime);
            });
            cluster.get(4).nodetoolResult("decommission");
            cluster.get(4).runOnInstance(() -> {
                assertEquals("DECOMMISSIONED", StorageService.instance.getOperationMode());
            });
            // verify that the last repair time was sent from the leaving node to the peer taking over its range
            assertLastSuccessfulRepair(cluster.get(3), (int)(artificialRepairTime / 1000));
        }
    }

    static void assertLastSuccessfulRepair(IInvokableInstance inst, Integer expected)
    {
        boolean matches = inst.callOnInstance(() -> {
            Map<Range<Token>, Integer> repairTimes = SystemKeyspace.getLastSuccessfulRepair("distributed_test_keyspace", "tbl");
            if (repairTimes.isEmpty() && expected == null)
                return true;
            if (repairTimes.size() != 1)
                return false;
            return expected.equals(repairTimes.values().iterator().next());
        });
        assertTrue("Last repair time didn't match expected: " + expected, matches);
    }
}
