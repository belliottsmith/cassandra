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

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.assertj.core.api.Assertions;

// This test shows that attempting to replace H2 when present in gossip no longer causes the following error
// Node /127.0.0.5 is trying to replace node /127.0.0.4 with tokens [] with a different set of tokens...
// by disabling our checks, this happens
public class HostReplacementAllowedTest extends H1H2H3
{
    @Test
    public void disableCheckFromYaml() throws IOException
    {
        // this test is mostly the same as org.apache.cassandra.distributed.test.hostreplacement.H1H2H3StatusBootReplaceTest
        // but with the status allow set disabled, which will allow the gossip message enough time to be sent to peers
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withTokenSupplier(n -> n > 2 ? tokenSupplier.token(3) : tokenSupplier.token(n))
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false)
                                                        .set("host_replacement_allow_status", ImmutableSet.of()))
                                      .withInstanceInitializer(H1H2H3.BBHelper::install)
                                      .start())
        {
            attachBugExceptionHandler(cluster);
            haltH1(cluster);
            IInvokableInstance h2 = failH2(cluster);

            Assertions.assertThatThrownBy(() -> h3Replace(cluster, h2, false))
                      .hasMessageStartingWith("Node /127.0.0.5:7012 is trying to replace node /127.0.0.4:7012 with tokens [] with a different set of tokens");
        }
    }

    @Test
    public void disableCheckFromProperties() throws IOException
    {
        try (Cluster cluster = createCluster())
        {
            attachBugExceptionHandler(cluster);
            haltH1(cluster);
            IInvokableInstance h2 = failH2(cluster);

            // due to CASSANDRA-15170, configs are defined on init and not updated, so need to define settings here early
            // which impact configs
            try (WithProperties p = new WithProperties("cassandra.settings.host_replacement_allow_status", ""))
            {
                IInvokableInstance h3 = ClusterUtils.addInstance(cluster, h2.config(), c -> c.set("auto_bootstrap", true));
                Assertions.assertThatThrownBy(() ->
                                              ClusterUtils.startHostReplacement(h2, h3, (ignore, props) ->
                                                                                        props.setProperty("cassandra.consistent.rangemovement", "false")))
                          .hasMessageStartingWith("Node /127.0.0.5:7012 is trying to replace node /127.0.0.4:7012 with tokens [] with a different set of tokens");
            }
        }
    }
}
