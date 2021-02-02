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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getCommitLogDirectory;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getDataDirectories;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class SSTableValidate20Test extends TestBaseImpl
{
    private static final Versions VERSIONS;

    static
    {
        VERSIONS = Versions.find();
    }

    @Test
    public void correctColumnType() throws IOException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withVersion(VERSIONS.getLatest(Versions.Major.v22))
                                      .withConfig(config -> config.set("dtest.api.config.check", false))
                                      .start())
        {
            populate(cluster);
            IInvokableInstance node = cluster.get(1);

            // shut down to make sure state doesn't change
            stopUnchecked(node);

            List<File> sstables = findSSTables(node, KEYSPACE, "withuuid");

            verify(KEYSPACE, "withuuid",
                   "CREATE TABLE " + KEYSPACE + ".withuuid (pk int PRIMARY KEY, sver uuid)",
                   sstables).assertOnExitCode();

            // upgrade
            ((IUpgradeableInstance) node).setVersion(AbstractCluster.CURRENT_VERSION);

            node.startup();

            long afterStart = node.logs().mark();
            node.nodetoolResult("verify",
                                "--validate-data",
                                "--force", // --force is for apple
                                KEYSPACE,
                                "withuuid")
                .asserts().success();
            assertThat(node.logs().grep(afterStart, "Verify of .* succeeded").getResult())
                      .hasSize(sstables.size());
        }
    }

    @Test
    public void incorrectColumnType() throws IOException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withVersion(VERSIONS.getLatest(Versions.Major.v22))
                                      .withConfig(c -> c.set("dtest.api.config.check", false))
                                      .start())
        {
            cluster.setUncaughtExceptionsFilter(t -> t.getMessage().contains("Invalid SSTable") && t.getMessage().contains("please force a full repair"));

            populate(cluster);
            IInvokableInstance node = cluster.get(1);

            // alter schema, this is to replicate edge cases where C* validation did not block writes
            node.executeInternal("INSERT INTO system.schema_columns (keyspace_name, columnfamily_name, column_name, validator) VALUES (?, ?, ?, ?)",
                                 KEYSPACE, "withuuid", "sver", "org.apache.cassandra.db.marshal.TimeUUIDType");
            // reload schema
            node.executeInternal("ALTER TABLE " + KEYSPACE + ".withuuid WITH comment='upgrade'");

            //TODO why does jvm-dtest startup with the wrong schema without flushing o_O
            node.flush("system");

            // shut down to make sure state doesn't change
            stopUnchecked(node);
            // If we can't reply the commit log on startup then we crash, even if policy is ignore.
            // This logic isn't the reason for flushing system, that logic was added first.  This is only here
            // since we know a bad record will exist in the commit log (drain has issues in jvm-dtest), so make
            // sure to delete it.
            FileUtils.deleteRecursive(getCommitLogDirectory(node));

            List<File> sstables = findSSTables(node, KEYSPACE, "withuuid");

            ToolRunner.ToolResult result = verify(KEYSPACE, "withuuid",
                            "CREATE TABLE " + KEYSPACE + ".withuuid (pk int PRIMARY KEY, sver timeuuid)",
                            sstables);
            result.assertOnExitCodeFailure();
            // errors go to stdout and stderr....
            assertThat(result.getStdout() + "\n" + result.getCleanedStderr())
                      .contains("Failure reading partition [pk=", "Invalid version for TimeUUID type.");

            // if the schema was uuid, it should all pass
            verify(KEYSPACE, "withuuid",
                   "CREATE TABLE " + KEYSPACE + ".withuuid (pk int PRIMARY KEY, sver uuid)",
                   sstables).assertOnExitCode();

            // upgrade
            ((IUpgradeableInstance) node).setVersion(AbstractCluster.CURRENT_VERSION);

            node.startup();

            long afterStart = node.logs().mark();
            // sadly verify doesn't tell the user anything, you need to read the C* logs on failure
            NodeToolResult nodeToolResult = node.nodetoolResult("verify",
                                                                "--validate-data",
                                                                "--force", // --force is for apple
                                                                KEYSPACE,
                                                                "withuuid");
            nodeToolResult.asserts().failure(); // expect failure as the schema is timeuuid but data is uuid
            List<String> logs = node.logs().grep(afterStart, "Failure reading partition \\[pk=").getResult();
            assertThat(logs)
                      .isNotEmpty()
                      .element(0).asString().contains("Invalid version for TimeUUID type.");
        }
    }

    private static void populate(Cluster cluster)
    {
        init(cluster);
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".withuuid (pk int PRIMARY KEY, sver uuid )");

        IInvokableInstance node = cluster.get(1);
        for (int i = 0; i < 100; i++)
            node.executeInternal("INSERT INTO " + KEYSPACE + ".withuuid (pk, sver) VALUES (?, ?)", i, UUID.randomUUID());

        node.flush(KEYSPACE);
    }

    private static ToolRunner.ToolResult verify(String ks, String table, String schema, List<File> sstables)
    {
        List<String> args = new ArrayList<>();
        args.add("bin/sstableverify");
        args.add("--sstables");
        args.add(sstables.stream().map(File::getAbsolutePath).collect(Collectors.joining(",")));
        args.add("--create_statement");
        args.add(schema);
        args.add("--validate_data");
        args.add("--debug");
        args.add(ks);
        args.add(table);
        return ToolRunner.invoke(args);
    }

    private static List<File> findSSTables(IInstance instance, String ks, String cf)
    {
        List<File> sstables = new ArrayList<>();
        for (File dataDir : getDataDirectories(instance))
        {
            File ksDir = new File(dataDir, ks);
            if (ksDir.isDirectory())
            {
                File[] files = ksDir.listFiles();
                if (files == null || files.length == 0)
                    continue;
                Stream.of(files)
                      .filter(File::isDirectory)
                      .filter(f -> f.getName().startsWith(cf + "-"))
                      .flatMap(f -> {
                          File[] fs = f.listFiles();
                          return fs == null ? Stream.empty() : Stream.of(fs);
                      })
                      .filter(f -> f.getName().endsWith("-Data.db"))
                      .forEach(sstables::add);
            }
        }
        return sstables;
    }
}
