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
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getDirectories;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;

public class IPMembershipSameIPFailsWithoutReplaceTest extends TestBaseImpl
{
    /**
     * Port of replace_address_test.py::fail_without_replace_test to jvm-dtest
     */
    @Test
    public void sameIPFailWithoutReplace() throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK, Feature.NATIVE_PROTOCOL)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .start())
        {
            IInvokableInstance nodeToReplace = cluster.get(3);

            ToolRunner.invokeCassandraStress("write", "n=10000", "-schema", "replication(factor=3)", "-port", "native=9042").assertOnExitCode();

            for (boolean auto_bootstrap : Arrays.asList(true, false))
            {
                stopUnchecked(nodeToReplace);
                getDirectories(nodeToReplace).forEach(FileUtils::deleteRecursive);

                nodeToReplace.config().set("auto_bootstrap", auto_bootstrap);

                Assertions.assertThatThrownBy(() -> nodeToReplace.startup())
                          .hasRootCauseMessage("A node with address /127.0.0.3 already exists, cancelling join. Use cassandra.replace_address if you want to replace this node.");
            }
        }
    }
}
