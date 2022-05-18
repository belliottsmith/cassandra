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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.DistributedTestSnitch;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.gms.Gossiper;
import org.assertj.core.api.Assertions;

/**
 * We have H1 which goes down and needs to be replaced with H2.
 * H2 fails to complete bootstrap, and a replacement of H2 -> H3 is triggered
 */
public class H1H2H3FatClientRemovalTest extends H1H2H3
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = createCluster())
        {
            haltH1(cluster);
            IInvokableInstance h2 = failH2(cluster);

            // clean up gossip state; this mimics the fat-client logic which evicts H2
            InetSocketAddress endpoint = h2.config().broadcastAddress();
            for (IInvokableInstance i : Arrays.asList(cluster.get(PEER_1), cluster.get(PEER_2)))
            {
                // mimic fat-client timeout
                i.runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
                    Gossiper.instance.removeEndpoint(DistributedTestSnitch.toCassandraInetAddressAndPort(endpoint));
                    Gossiper.instance.evictFromMembership(DistributedTestSnitch.toCassandraInetAddressAndPort(endpoint));
                }));
            }
            ClusterUtils.assertGossipInfoMissing(cluster, h2.config().broadcastAddress());

            Assertions.assertThatThrownBy(() -> h3Replace(cluster, h2, true))
                      .hasMessage("Cannot replace_address /127.0.0.4:7012 because it doesn't exist in gossip");
        }
    }
}
