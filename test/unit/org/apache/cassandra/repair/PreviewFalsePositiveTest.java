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

package org.apache.cassandra.repair;

import java.util.Collections;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.repair.consistent.SyncStatSummary;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ActiveRepairService.RepairSuccess;
import org.apache.cassandra.service.ActiveRepairService.RepairSuccessVerbHandler;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;

public class PreviewFalsePositiveTest extends AbstractRepairTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static final String COUNTER_TABLE = "counter_tbl";
    private static ColumnFamilyStore cfs;
    private static ColumnFamilyStore counter_cfs;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();
        CFMetaData cfm = CFMetaData.compile(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", KEYSPACE, TABLE), KEYSPACE);
        CFMetaData counter_cfm = CFMetaData.compile(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v COUNTER)", KEYSPACE, COUNTER_TABLE), KEYSPACE);
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm, counter_cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.cfId);
        counter_cfs = Schema.instance.getColumnFamilyStoreInstance(counter_cfm.cfId);
    }

    @Before
    public void setUp() throws Exception
    {
        cfs.truncateBlocking();
        counter_cfs.truncateBlocking();
    }

    private CompactionInfo.Holder createCompactionInfoHolder()
    {
        return new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return null;
            }

            public boolean isGlobal()
            {
                return false;
            }
        };
    }

    /**
     * ParentRepairSession should be marked suspect if a RepairSuccess message is received which
     * conflicts with an active validation compaction
     */
    @Test
    public void ongoingPreviewValidation() throws Exception
    {
        UUID sessionID = registerSession(cfs, false, true);

        CompactionManager.SessionData sessionData = new CompactionManager.SessionData(sessionID, Collections.singleton(RANGE1), PreviewKind.REPAIRED, createCompactionInfoHolder());
        CompactionManager.instance.markValidationActive(cfs.metadata.cfId, sessionData);
        Assert.assertFalse(sessionData.isStopRequested());

        RepairSuccess repairSuccess = new RepairSuccess(KEYSPACE, TABLE, Collections.singleton(RANGE1), 1);
        MessageIn<RepairSuccess> msgIn = MessageIn.create(COORDINATOR, repairSuccess, Collections.emptyMap(),
                                                          MessagingService.Verb.APPLE_REPAIR_SUCCESS,
                                                          MessagingService.current_version);
        RepairSuccessVerbHandler verbHandler = new RepairSuccessVerbHandler();
        verbHandler.doVerb(msgIn, 1);

        Assert.assertTrue(sessionData.isStopRequested());
    }

    /**
     * ParentRepairSession should be marked suspect if a RepairSuccess message is received which
     * conflicts with an active validation compaction
     */
    @Test
    public void ongoingNormalValidation() throws Exception
    {
        UUID sessionID = registerSession(cfs, false, true);

        CompactionManager.SessionData sessionData = new CompactionManager.SessionData(sessionID, Collections.singleton(RANGE1), PreviewKind.NONE, createCompactionInfoHolder());
        CompactionManager.instance.markValidationActive(cfs.metadata.cfId, sessionData);
        Assert.assertFalse(sessionData.isStopRequested());

        RepairSuccess repairSuccess = new RepairSuccess(KEYSPACE, TABLE, Collections.singleton(RANGE1), 1);
        MessageIn<RepairSuccess> msgIn = MessageIn.create(COORDINATOR, repairSuccess, Collections.emptyMap(),
                                                          MessagingService.Verb.APPLE_REPAIR_SUCCESS,
                                                          MessagingService.current_version);
        RepairSuccessVerbHandler verbHandler = new RepairSuccessVerbHandler();
        verbHandler.doVerb(msgIn, 1);

        Assert.assertFalse(sessionData.isStopRequested());
    }

    /**
     * no exception should be thrown for normal repairs
     */
    @Test
    public void pendingRepairForNormalRepair() throws Exception
    {
        UUID sessionID = UUIDGen.getTimeUUID();
        Range<Token> fullRange = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                             DatabaseDescriptor.getPartitioner().getMinimumToken());

        long repairedAt = System.currentTimeMillis();
        ActiveRepairService.instance.registerParentRepairSession(sessionID,
                                                                 COORDINATOR,
                                                                 Lists.newArrayList(cfs),
                                                                 Sets.newHashSet(fullRange),
                                                                 true,
                                                                 repairedAt,
                                                                 true,
                                                                 PreviewKind.NONE);

        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", KEYSPACE, TABLE), 1, 1);
        cfs.forceBlockingFlush();

        Assert.assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, ActiveRepairService.UNREPAIRED_SSTABLE, UUID.randomUUID());
        sstable.reloadSSTableMetadata();

        RepairJobDesc desc = new RepairJobDesc(sessionID, UUID.randomUUID(), KEYSPACE, TABLE, Collections.singleton(fullRange));
        Validator validator = new Validator(desc, COORDINATOR, FBUtilities.nowInSeconds(), PreviewKind.REPAIRED);
        try (Refs<SSTableReader> sstables = CompactionManager.instance.getSSTablesToValidate(cfs, validator))
        {

        }
    }

    @Test
    public void counterWarning() throws Exception
    {
        StringBuilder sb = new StringBuilder();

        SyncStatSummary.maybeWarnOfCounter(KEYSPACE, TABLE, 1, sb);
        Assert.assertFalse(sb.toString().contains("COUNTER"));

        SyncStatSummary.maybeWarnOfCounter(KEYSPACE, COUNTER_TABLE, 0, sb);
        Assert.assertFalse(sb.toString().contains("COUNTER"));

        SyncStatSummary.maybeWarnOfCounter(KEYSPACE, COUNTER_TABLE, 1, sb);
        Assert.assertTrue(sb.toString().contains("COUNTER"));
    }

    @Test
    public void disabledChristmasPatchWarning()
    {
        DatabaseDescriptor.setChristmasPatchEnabled();
        for (boolean disabled : new boolean[]{ false, true })
        {
            ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).setChristmasPatchDisabled(disabled);
            SyncStatSummary summary = new SyncStatSummary(false);
            summary.consumeRepairResult(
                new RepairResult(new RepairJobDesc(UUID.randomUUID(),
                                                   UUID.randomUUID(),
                                                   KEYSPACE,
                                                   TABLE,
                                                   Collections.singleton(RANGE1)),
                                 Collections.singletonList(new SyncStat(new NodePair(PARTICIPANT1, PARTICIPANT2), 1L))));
            Assert.assertEquals(disabled, summary.toString(true).contains("CHRISTMAS PATCH DISABLED"));
            Assert.assertFalse(summary.toString(false).contains("CHRISTMAS PATCH DISABLED"));
        }
    }
}
