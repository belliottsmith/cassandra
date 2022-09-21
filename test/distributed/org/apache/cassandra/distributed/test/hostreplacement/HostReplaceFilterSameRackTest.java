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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.test.hostreplacement.HostReplacementTest.setupCluster;

// see rdar://95209681 (Host replacement failing waiting on schema of host to be replaced)
public class HostReplaceFilterSameRackTest extends TestBaseImpl
{
    private static final int TO_REPLACE = 2;

    @Test
    public void schemaDisagreementOnSingleRack() throws IOException
    {
        TokenSupplier ts = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set("host_replacement_filter_same_rack_enabled", true))
                                      .withTokenSupplier(node -> ts.token(node == 4 ? TO_REPLACE : node))
                                      .withRack("DC1", "R1", 1)
                                      .withRack("DC1", "R2", 2)
                                      .start())
        {
            IInvokableInstance loner = cluster.get(1);
            // both instances have the same rack, only loner has a different rack...
            IInvokableInstance nodeToRemove = cluster.get(TO_REPLACE);
            IInvokableInstance downedNode = cluster.get(3);
            InetSocketAddress downedNodeAddress = downedNode.config().broadcastAddress();

            setupCluster(cluster);

            stopUnchecked(nodeToRemove);
            stopUnchecked(downedNode);

            // trigger a schema disagreement by modifying the SCHEMA of the downed node
            loner.runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(InetAddressAndPort.getByAddress(downedNodeAddress));
                state.addApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(UUID.randomUUID()));
            }));

            IInvokableInstance node = replaceHostAndStart(cluster, nodeToRemove);
            Assertions.assertThat(node.logs().grep("Excluding .* due to ignore filter").getResult())
                      .hasSize(2)
                      .describedAs("Unable to find 127.0.0.2")
                      .anySatisfy(a -> Assertions.assertThat(a).contains("/127.0.0.2:7012"))
                      .describedAs("Unable to find 127.0.0.3")
                      .anySatisfy(a -> Assertions.assertThat(a).contains("/127.0.0.3:7012"));
        }
    }
}
