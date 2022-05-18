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
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.assertj.core.api.Assertions;

public class HostReplacementDisabledTest extends TestBaseImpl
{
    @Test
    public void disableCheckFromYaml() throws IOException
    {
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(n -> n > 2 ? tokenSupplier.token(2) : tokenSupplier.token(n))
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false)
                                                        .set("host_replacement_allow_status", ImmutableSet.of("NOMORE")))
                                      .start())
        {
            IInvokableInstance toReplace = cluster.get(2);
            ClusterUtils.stopAbrupt(cluster, toReplace);

            Assertions.assertThatThrownBy(() -> ClusterUtils.replaceHostAndStart(cluster, toReplace))
                      .hasMessage("Cannot replace_address /127.0.0.2:7012 because its status NORMAL is not in the allowed set [NOMORE]. To update the allowed set update yaml host_replacement_allow_status or system property -Dcassandra.settings.host_replacement_allow_status=NOMORE");
        }
    }

    @Test
    public void disableCheckFromProperties() throws IOException
    {
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(n -> n > 2 ? tokenSupplier.token(2) : tokenSupplier.token(n))
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .start())
        {
            IInvokableInstance toReplace = cluster.get(2);
            ClusterUtils.stopAbrupt(cluster, toReplace);

            // due to CASSANDRA-15170, configs are defined on init and not updated, so need to define settings here early
            // which impact configs
            try (WithProperties ignore = new WithProperties("cassandra.settings.host_replacement_allow_status", "NOMORE"))
            {
                Assertions.assertThatThrownBy(() -> ClusterUtils.replaceHostAndStart(cluster, toReplace))
                          .hasMessage("Cannot replace_address /127.0.0.2:7012 because its status NORMAL is not in the allowed set [NOMORE]. To update the allowed set update yaml host_replacement_allow_status or system property -Dcassandra.settings.host_replacement_allow_status=NOMORE");
            }
        }
    }
}
