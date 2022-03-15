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

package org.apache.cassandra.tools;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.transport.ProtocolVersion;
import org.assertj.core.api.Assertions;

/**
 * This class is to monitor the JMX compatability cross different versions, and relies on a gold set of metrics which
 * were generated following the instructions below.  These tests only check for breaking changes, so will ignore any
 * new metrics added in a release.  If the latest release is not finalized yet then the latest version might fail
 * if a unrelesed metric gets renamed, if this happens then the gold set should be updated for the latest version.
 *
 * If a test fails for a previous version, then this means we have a JMX compatability regression, if the metric has
 * gone through proper deprecation then the metric can be excluded using the patterns used in other tests, if the metric
 * has not gone through proper deprecation then the change should be looked at more carfuly to avoid breaking users.
 *
 * In order to generate the dump for another version, launch a cluster then run the following
 * {@code
 * create keyspace cql_test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 * use cql_test_keyspace;
 * CREATE TABLE table_00 (pk int PRIMARY KEY);
 * insert into table_00 (pk) values (42);
 * select * from table_00 where pk=42;
 * }
 */
public class JMXCompatabilityTest extends CQLTester
{
    @ClassRule
    public static TemporaryFolder TMP = new TemporaryFolder();

    private static boolean CREATED_TABLE = false;

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
    }

    private void setupStandardTables() throws Throwable
    {
        if (CREATED_TABLE)
            return;

        // force loading mbean which CassandraDaemon creates
        GCInspector.register();
        CassandraDaemon.registerNativeAccess();

        String name = KEYSPACE + "." + createTable("CREATE TABLE %s (pk int PRIMARY KEY)");

        // use net to register everything like storage proxy
        executeNet(ProtocolVersion.CURRENT, new SimpleStatement("INSERT INTO " + name + " (pk) VALUES (?)", 42));
        executeNet(ProtocolVersion.CURRENT, new SimpleStatement("SELECT * FROM " + name + " WHERE pk=?", 42));

        String script = "tools/bin/jmxtool dump -f yaml --url service:jmx:rmi:///jndi/rmi://" + jmxHost + ":" + jmxPort + "/jmxrmi > " + TMP.getRoot().getAbsolutePath() + "/out.yaml";
        ToolRunner.invoke("bash", "-c", script).assertOnCleanExit();
        CREATED_TABLE = true;
    }

    @Test
    public void ciediff30() throws Throwable
    {
        List<String> excludeObjects = Arrays.asList("org.apache.cassandra.metrics:type=ThreadPools.*",
                                                    "org.apache.cassandra.internal:.*",
                                                    "org.apache.cassandra.metrics:type=DroppedMessage.*",
                                                    "org.apache.cassandra.metrics:type=ClientRequest,scope=CASRead,name=ConditionNotMet",
                                                    "org.apache.cassandra.metrics:type=Client,name=connectedThriftClients", // removed in CASSANDRA-11115
                                                    "org.apache.cassandra.request:type=ReadRepairStage", // removed in CASSANDRA-13910
                                                    "org.apache.cassandra.db:type=HintedHandoffManager", // removed in CASSANDRA-15939
                                                    "org.apache.cassandra.metrics:type=HintedHandOffManager.*", // removed in CASSANDRA-15939

                                                    "org.apache.cassandra.metrics:type=(StorageProxy|ColumnFamily),scope=PartitionBlacklist,name=.*",
                                                    ".*keyspace=cie_internal,(scope|table|columnfamily)=partition_blacklist.*",

                                                    // dropped tables
                                                    "org.apache.cassandra.metrics:type=Table,keyspace=system,scope=(schema_aggregates|schema_columnfamilies|schema_columns|schema_functions|schema_keyspaces|schema_triggers|schema_usertypes),name=.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=views_builds_in_progress.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=range_xfers.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=hints.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=batchlog.*",

                                                    // CIE exclude objects
                                                    "org.apache.cassandra.metrics:type=Client,name=MessageQueueLatency", // Waiting for forward port of rdar://58417519 (Native Transport Request Load Shedding) (#1474)
                                                    "org.apache.cassandra.metrics:type=ClientRequestSize,name=((Incoming|Outgoing)Bytes|Bytes(Read|Written)PerQuery)", // Was temporarily renamed PerFrame during 4.0 dev, but name reverted.
                                                    "org.apache.cassandra.metrics:type=Keyspace,keyspace=.*,name=RepairedOverread(Rows|Time)", // Renamed RepairedDataTrackingOverread(Rows|Time)
                                                    "org.apache.cassandra.metrics:type=(Keyspace|Table|ColumnFamily)(|,keyspace=.*),name=SyncTime", // Renamed RepairSyncTime when open sourced
                                                    "org.apache.cassandra.metrics:type=(Keyspace|Table|ColumnFamily)(|,keyspace=.*),name=ClientIndexSizeAborts",
                                                    "org.apache.cassandra.metrics:type=(Keyspace|Table|ColumnFamily)(|,keyspace=.*),name=ClientIndexSizeWarnings",
                                                    "org.apache.cassandra.metrics:type=(Keyspace|Table|ColumnFamily)(|,keyspace=.*),name=LargePartitionIndexBytesHistogram",
                                                    "org.apache.cassandra.metrics:type=Storage,name=ColumnIndexDownsamples", // Not forward ported
                                                    "org.apache.cassandra.request:type=(CounterMutation|RequestResponse|ViewMutation)Stage", // Lazily initialized in 4.0
                                                    ".*UNSAFE_DELAY_(LOCAL_|)(SERIAL|QUORUM).*", // consistency levels for ApplePaxos

                                                    ".*keyspace=system,(scope|table|columnfamily)=compactions_in_progress.*"

        );
        List<String> excludeAttributes = Arrays.asList("RPCServerRunning", // removed in CASSANDRA-11115
                                                       "MaxNativeProtocolVersion",

                                                       // Temporary for rebase while picking up upstream partition blocking
                                                       "EnableBlacklistRangeReads",
                                                       "EnableBlacklistReads",
                                                       "EnableBlacklistWrites",
                                                       "EnablePartitionBlacklist",
                                                       "PartitionBlacklistLoadAttempts",
                                                       "PartitionBlacklistLoadSuccesses",

                                                       // CIE exclude attributes
                                                       "AllowCompactStorage", // 3.0 only, CS removed. rdar://66166922 (Prevent new COMPACT STORAGE tables) (#1859)
                                                       "AllowUnsafeAggressiveSSTableExpiration",
                                                       "AllowZstd", // 3.0 only feature flag for ZStd, added commit bae2c44d3e1fae5de97080e91164839ea717bc60
                                                       "ArtificialLatency.*", // artificial latency for ApplePaxos
                                                       "Bytes(Read|Written)PerQueryEstimatedHistogram", // estimated version removed
                                                       "ClientLargeReadBlockThresholdKB", // client large read block/warn not ported to 4.0 yet
                                                       "ClientLargeReadWarnThresholdKB",
                                                       "ColumnIndexMaxCount",
                                                       "ColumnIndexMaxSizeInKB",
                                                       "DebugValidationPreviewEnabled", // did not forward port <rdar://problem/34533172> Add support for optionally dumping streams and merkle trees for validation preview failures (#939)
                                                       "DynamicBadnessThreshold",
                                                       "DynamicManualSeverityOnly",
                                                       "EnableOldMutationWarnings",
                                                       "ForcePagingStateLegacySerialization",
                                                       "InitialRangeTombstoneAllocationSize", // renamed InitialRangeTombstoneListAllocationSize
                                                       "LargePartitionIndexWarningThresholdKb", // large partition index warning threshold not ported to 4.0
                                                       "ManualSeverity", // waiting on forward port of rdar://62076377 ([3.0 / 4.0]: Make DynamicEndpointSnitch more configurable at runtime)
                                                       "RangeTombstoneResizeGrowthFactor", // renamed RangeTombstoneResizeFactor
                                                       "RecentSSTablesSkippedPerRead", // rdar://67084971 (MTC skipped sstable stats)
                                                       "RepairedDataTrackingExclusions(|Enabled)",
                                                       "ReplicaFilteringProtectionEnabled",
                                                       "SSTablesSkippedPerRead", // rdar://67084971 (MTC skipped sstable stats)
                                                       "SettingUnlimitedConcurrentValidatorsAllowed", // renamed enforceConcurrentValidatorsLimit
                                                       "StreamingSessionsPerHost", // Compacting stream reader not ported to 4.0 yet
                                                       "ThreadCorrectionDisabled", // backport of CASSANDRA-15059 added disable_thread_correction not present in OSS patch
                                                       "TombstoneCountGCable", // did not forward port <rdar://problem/28066902> Cass: Expired tombstones counted differently in 2.1  a
                                                       "UseStreamCompactionForBootstrap",
                                                       "UseStreamCompactionForRepair"
                                                       );
        List<String> excludeOperations = Arrays.asList("startRPCServer", "stopRPCServer", // removed in CASSANDRA-11115
                                                       // nodetool apis that were changed,
                                                       "decommission", // -> decommission(boolean)
                                                       "forceRepairAsync", // -> repairAsync
                                                       "forceRepairRangeAsync", // -> repairAsync
                                                       "beginLocalSampling", // -> beginLocalSampling(p1: java.lang.String, p2: int, p3: int): void
                                                       "finishLocalSampling", // -> finishLocalSampling(p1: java.lang.String, p2: int): java.util.List

                                                       // Temporary for rebase while picking up upstream partition blocking
                                                       "blacklistKey",
                                                       "loadPartitionBlacklist",

                                                       // CIE exclude operations
                                                       "(disable|enable)OldMutationWarnings",
                                                       "(disable|enable)RepairedDataTrackingExclusions",
                                                       "(disable|enable)ReplicaFilteringProtection",
                                                       "(reset|stop)FullQueryLogger", // moved from StorageProxy to StorageService
                                                       "Dynamic(BadnessThreshold|ManualSeverityOnly)", // waiting on forward port of rdar://62076377 ([3.0 / 4.0]: Make DynamicEndpointSnitch more configurable at runtime)
                                                       "allowUnsafeAggressiveSSTableExpiration",
                                                       "configureFullQueryLogger", // renamed enableFullQueryLogger
                                                       "getSessions", // -> getSessions(p1: boolean, p2: java.lang.String): java.util.List"
                                                       "loadNewSSTables", // deprecated CASSANDRA-6719, removed in 4.0
                                                       "startSamplePartitions", //renamed startSamplingPartitions
                                                       "stopSamplePartitions", // renamed stopSamplingPartitions
                                                       "verify" // have not forward ported [rdar://71055206] Added new tool to validate a SSTable that supports all supported SSTable versions (#2042) (#2137)
        );

        diff(excludeObjects, excludeAttributes, excludeOperations, "test/data/jmxdump/cie-3.0.24.36-jmx.yaml");
    }

    @Test
    public void diff30() throws Throwable
    {
        List<String> excludeObjects = Arrays.asList("org.apache.cassandra.metrics:type=ThreadPools.*",
                                                    "org.apache.cassandra.internal:.*",
                                                    "org.apache.cassandra.metrics:type=DroppedMessage.*",
                                                    "org.apache.cassandra.metrics:type=ClientRequest,scope=CASRead,name=ConditionNotMet",
                                                    "org.apache.cassandra.metrics:type=Client,name=connectedThriftClients", // removed in CASSANDRA-11115
                                                    "org.apache.cassandra.request:type=ReadRepairStage", // removed in CASSANDRA-13910
                                                    "org.apache.cassandra.db:type=HintedHandoffManager", // removed in CASSANDRA-15939

                                                    // dropped tables
                                                    "org.apache.cassandra.metrics:type=Table,keyspace=system,scope=(schema_aggregates|schema_columnfamilies|schema_columns|schema_functions|schema_keyspaces|schema_triggers|schema_usertypes),name=.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=views_builds_in_progress.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=range_xfers.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=hints.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=batchlog.*");
        List<String> excludeAttributes = Arrays.asList("RPCServerRunning", // removed in CASSANDRA-11115
                                                       "MaxNativeProtocolVersion");
        List<String> excludeOperations = Arrays.asList("startRPCServer", "stopRPCServer", // removed in CASSANDRA-11115
                                                       // nodetool apis that were changed,
                                                       "decommission", // -> decommission(boolean)
                                                       "forceRepairAsync", // -> repairAsync
                                                       "forceRepairRangeAsync", // -> repairAsync
                                                       "beginLocalSampling", // -> beginLocalSampling(p1: java.lang.String, p2: int, p3: int): void
                                                       "finishLocalSampling" // -> finishLocalSampling(p1: java.lang.String, p2: int): java.util.List
        );

        diff(excludeObjects, excludeAttributes, excludeOperations, "test/data/jmxdump/cassandra-3.0-jmx.yaml");
    }

    @Test
    public void diff311() throws Throwable
    {
        List<String> excludeObjects = Arrays.asList("org.apache.cassandra.metrics:type=ThreadPools.*", //lazy initialization in 4.0
                                                    "org.apache.cassandra.internal:.*",
                                                    "org.apache.cassandra.metrics:type=DroppedMessage,scope=PAGED_RANGE.*", //it was deprecated in the previous major version
                                                    "org.apache.cassandra.metrics:type=Client,name=connectedThriftClients", // removed in CASSANDRA-11115
                                                    "org.apache.cassandra.request:type=ReadRepairStage", // removed in CASSANDRA-13910
                                                    "org.apache.cassandra.db:type=HintedHandoffManager", // removed in CASSANDRA-15939

                                                    // dropped tables
                                                    "org.apache.cassandra.metrics:type=Table,keyspace=system,scope=(schema_aggregates|schema_columnfamilies|schema_columns|schema_functions|schema_keyspaces|schema_triggers|schema_usertypes),name=.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=views_builds_in_progress.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=range_xfers.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=hints.*",
                                                    ".*keyspace=system,(scope|table|columnfamily)=batchlog.*"
        );
        List<String> excludeAttributes = Arrays.asList("RPCServerRunning", // removed in CASSANDRA-11115
                                                       "MaxNativeProtocolVersion",
                                                       "StreamingSocketTimeout");
        List<String> excludeOperations = Arrays.asList("startRPCServer", "stopRPCServer", // removed in CASSANDRA-11115
                                                       // nodetool apis that were changed,
                                                       "decommission", // -> decommission(boolean)
                                                       "forceRepairAsync", // -> repairAsync
                                                       "forceRepairRangeAsync", // -> repairAsync
                                                       "beginLocalSampling", // -> beginLocalSampling(p1: java.lang.String, p2: int, p3: int): void
                                                       "finishLocalSampling" // -> finishLocalSampling(p1: java.lang.String, p2: int): java.util.List
        );

        diff(excludeObjects, excludeAttributes, excludeOperations, "test/data/jmxdump/cassandra-3.11-jmx.yaml");
    }

    @Test
    public void ciediff40() throws Throwable
    {
        List<String> excludeObjects = Arrays.asList();
        List<String> excludeAttributes = Arrays.asList();
        List<String> excludeOperations = Arrays.asList();

        diff(excludeObjects, excludeAttributes, excludeOperations, "test/data/jmxdump/cie-4.0-jmx.yaml");
    }

    @Test
    public void diff40() throws Throwable
    {
        List<String> excludeObjects = Arrays.asList();
        List<String> excludeAttributes = Arrays.asList();
        List<String> excludeOperations = Arrays.asList();

        diff(excludeObjects, excludeAttributes, excludeOperations, "test/data/jmxdump/cassandra-4.0-jmx.yaml");
    }

    private void diff(List<String> excludeObjects, List<String> excludeAttributes, List<String> excludeOperations, String original) throws Throwable
    {
        setupStandardTables();

        List<String> args = Lists.newArrayList("tools/bin/jmxtool", "diff",
                                               "-f", "yaml",
                                               "--ignore-missing-on-left",
                                               original, TMP.getRoot().getAbsolutePath() + "/out.yaml");
        excludeObjects.forEach(a -> {
            args.add("--exclude-object");
            args.add(a);
        });
        excludeAttributes.forEach(a -> {
            args.add("--exclude-attribute");
            args.add(a);
        });
        excludeOperations.forEach(a -> {
            args.add("--exclude-operation");
            args.add(a);
        });
        ToolResult result = ToolRunner.invoke(args);
        result.assertOnCleanExit();
        Assertions.assertThat(result.getStdout()).isEmpty();
    }
}
