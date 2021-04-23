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

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.shared.DistributedTestBase;

import static org.apache.cassandra.cql3.statements.SchemaAlteringStatement.SYSTEM_PROPERTY_ALLOW_DISABLED_COMPRESSION;

public class TestBaseImpl extends DistributedTestBase
{
    @Rule
    public TestName testName = new TestName();

    @Before
    public void beforeEachBase()
    {
        System.setProperty("cassandra.testtag", getClass().getName());
        System.setProperty("suitename", testName.getMethodName());
    }


    @After
    public void afterEach() {
        super.afterEach();
    }

    @BeforeClass
    public static void beforeClass() throws Throwable {
        System.setProperty("log4j2.disableJmx", "true"); // setting both ways as changes between versions
        System.setProperty("log4j2.disable.jmx", "true");
        System.setProperty("log4j.shutdownHookEnabled", "false");
        System.setProperty("cassandra.test.logConfigProperty", "log4j.configurationFile");
        System.setProperty("cassandra.test.logConfigPath", "test/conf/log4j2-dtest.xml");
        System.setProperty("cassandra.allow_simplestrategy", "true"); // makes easier to share OSS tests without RF limits
        System.setProperty("cassandra.minimum_replication_factor", "0"); // makes easier to share OSS tests without RF limits
        System.setProperty(SYSTEM_PROPERTY_ALLOW_DISABLED_COMPRESSION, "true");
        ICluster.setup();
    }

    @Override
    public Cluster.Builder builder() {
        // This is definitely not the smartest solution, but given the complexity of the alternatives and low risk, we can just rely on the
        // fact that this code is going to work accross _all_ versions.
        return Cluster.build();
    }

    public static void fixDistributedSchemas(Cluster cluster)
    {
        // These keyspaces are under replicated by default, so must be updated when doing a mulit-node cluster;
        // else bootstrap will fail with 'Unable to find sufficient sources for streaming range <range> in keyspace <name>'
        for (String ks : Arrays.asList("system_auth", "system_traces"))
        {
            cluster.schemaChange("ALTER KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(cluster.size(), 3) + "}");
        }

        // in real live repair is needed in this case, but in the test case it doesn't matter if the tables loose
        // anything, so ignoring repair to speed up the tests.
    }
}
