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

package org.apache.cassandra.distributed.upgrade;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement.SYSTEM_PROPERTY_ALLOW_DISABLED_COMPRESSION;
import static org.apache.cassandra.distributed.shared.Versions.Major;
import static org.apache.cassandra.distributed.shared.Versions.Version;
import static org.apache.cassandra.distributed.shared.Versions.find;

public class UpgradeTestBase extends DistributedTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(UpgradeTestBase.class);

    @After
    public void afterEach()
    {
        System.runFinalization();
        System.gc();
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
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


    public UpgradeableCluster.Builder builder()
    {
        return UpgradeableCluster.build();
    }

    public static interface RunOnCluster
    {
        public void run(UpgradeableCluster cluster) throws Throwable;
    }

    public static interface RunOnClusterAndNode
    {
        public void run(UpgradeableCluster cluster, int node) throws Throwable;
    }

    public static class TestVersions
    {
        final Version initial;
        final Version[] upgrade;

        public TestVersions(Version initial, Version ... upgrade)
        {
            this.initial = initial;
            this.upgrade = upgrade;
        }
    }

    public static class TestCase implements Instance.ThrowingRunnable
    {
        private final Versions versions;
        private final List<TestVersions> upgrade = new ArrayList<>();
        private int nodeCount = 3;
        private RunOnCluster setup;
        private RunOnClusterAndNode runAfterNodeUpgrade;
        private RunOnCluster runAfterClusterUpgrade;
        private final Set<Integer> nodesToUpgrade = new HashSet<>();
        private Consumer<IInstanceConfig> configConsumer;
        private boolean allowSkipingMajorMissing = true;

        public TestCase()
        {
            this(find());
        }

        public TestCase(Versions versions)
        {
            this.versions = versions;
        }

        public TestCase allowSkipingMajorMissing(boolean value)
        {
            this.allowSkipingMajorMissing = value;
            return this;
        }

        public TestCase nodes(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public TestCase upgrade(Major initial, Major ... upgrade)
        {
            List<Major> majors = new ArrayList<>(upgrade.length + 1);
            majors.add(initial);
            majors.addAll(Arrays.asList(upgrade));
            List<Version> order = majors.stream()
                                               .map(m -> {
                                                   try
                                                   {
                                                       return versions.getLatest(m);
                                                   }
                                                   catch (RuntimeException e)
                                                   {
                                                       return null;
                                                   }
                                               })
                                               .filter(Objects::nonNull)
                                               .collect(Collectors.toList());
            if (order.size() < 2)
            {
                logger.warn("Skipping upgrade {}; not enough versions found", majors);
                return this;
            }
            this.upgrade.add(new TestVersions(order.get(0),
                                              order.stream().skip(1).toArray(Version[]::new)));
            return this;
        }

        public TestCase upgrade(Version initial, Version ... upgrade)
        {
            this.upgrade.add(new TestVersions(initial, upgrade));
            return this;
        }

        public TestCase setup(RunOnCluster setup)
        {
            this.setup = setup;
            return this;
        }

        public TestCase runAfterNodeUpgrade(RunOnClusterAndNode runAfterNodeUpgrade)
        {
            this.runAfterNodeUpgrade = runAfterNodeUpgrade;
            return this;
        }

        public TestCase runAfterClusterUpgrade(RunOnCluster runAfterClusterUpgrade)
        {
            this.runAfterClusterUpgrade = runAfterClusterUpgrade;
            return this;
        }

        public TestCase withConfig(Consumer<IInstanceConfig> config)
        {
            this.configConsumer = config;
            return this;
        }

        public void run() throws Throwable
        {
            if (setup == null)
                throw new AssertionError();
            Assume.assumeFalse("No upgrade tests defined", upgrade.isEmpty());
            if (runAfterClusterUpgrade == null && runAfterNodeUpgrade == null)
                throw new AssertionError();
            if (runAfterClusterUpgrade == null)
                runAfterClusterUpgrade = (c) -> {};
            if (runAfterNodeUpgrade == null)
                runAfterNodeUpgrade = (c, n) -> {};
            if (nodesToUpgrade.isEmpty())
                for (int n = 1; n <= nodeCount; n++)
                    nodesToUpgrade.add(n);

            for (TestVersions upgrade : this.upgrade)
            {
                try (UpgradeableCluster cluster = init(UpgradeableCluster.create(nodeCount, upgrade.initial, configConsumer)))
                {
                    setup.run(cluster);

                    for (Version version : upgrade.upgrade)
                    {
                        try
                        {
                            for (int n : nodesToUpgrade)
                            {
                                cluster.get(n).shutdown().get();
                                cluster.get(n).setVersion(version);
                                cluster.get(n).startup();
                                runAfterNodeUpgrade.run(cluster, n);
                            }

                            runAfterClusterUpgrade.run(cluster);
                        }
                        catch (Throwable t)
                        {
                            throw new AssertionError("Failing running test " + upgrade.initial.version + "->" + Stream.of(upgrade.upgrade).map(v -> v.version).collect(Collectors.joining(",", "[", "]")) + " against version " + version.version, t);
                        }
                    }
                }

            }
        }
        public TestCase nodesToUpgrade(int ... nodes)
        {
            for (int n : nodes)
            {
                nodesToUpgrade.add(n);
            }
            return this;
        }
    }

}
