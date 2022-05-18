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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.assertj.core.api.Assertions;

public class H1H2H3ConsistentTest extends H1H2H3
{
    @Test
    public void test() throws IOException
    {
        try (WithProperties ignore = new WithProperties("cassandra.settings.host_replacement_allow_status", ""); // disable status checks
             Cluster cluster = createCluster())
        {
            IInvokableInstance h1 = haltH1(cluster);
            IInvokableInstance h2 = failH2(cluster);

            Assertions.assertThatThrownBy(() -> h3Replace(cluster, h2, true))
                      .hasMessageStartingWith("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");

            Assertions.assertThatThrownBy(() -> h3Replace(cluster, h1, true))
                      .hasMessageStartingWith("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");

        }
    }
}
