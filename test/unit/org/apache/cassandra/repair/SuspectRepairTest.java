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
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairSession;
import org.apache.cassandra.service.ActiveRepairService.RepairSuccess;
import org.apache.cassandra.service.ActiveRepairService.RepairSuccessVerbHandler;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;

public class SuspectRepairTest extends AbstractRepairTest
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
    }

    /**
     * ParentRepairSession should be marked suspect if a RepairSuccess message is received which
     * conflicts with an active validation compaction
     */
    @Test
    public void ongoingValidation() throws Exception
    {
        UUID sessionID = registerSession(cfs, false, true);
        ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionID);
        Assert.assertFalse(prs.isSuspect());

        CompactionManager.instance.markValidationActive(cfs.metadata.cfId, sessionID, Collections.singleton(RANGE1));
        Assert.assertFalse(prs.isSuspect());

        RepairSuccess repairSuccess = new RepairSuccess(KEYSPACE, TABLE, Collections.singleton(RANGE1), 1);
        MessageIn<RepairSuccess> msgIn = MessageIn.create(COORDINATOR, repairSuccess, Collections.emptyMap(),
                                                          MessagingService.Verb.APPLE_REPAIR_SUCCESS,
                                                          MessagingService.current_version);
        RepairSuccessVerbHandler verbHandler = new RepairSuccessVerbHandler();
        verbHandler.doVerb(msgIn, 1);

        Assert.assertTrue(prs.isSuspect());
    }

    /**
     * preview ParentRepairSession should be marked suspect if there are sstables pending repair
     * in the range being previewed
     */
    @Test
    public void pendingRepair() throws Exception
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
                                                                 PreviewKind.REPAIRED);

        ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionID);

        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", KEYSPACE, TABLE), 1, 1);
        cfs.forceBlockingFlush();

        Assert.assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, ActiveRepairService.UNREPAIRED_SSTABLE, UUID.randomUUID());
        sstable.reloadSSTableMetadata();

        Assert.assertFalse(prs.isSuspect());

        RepairJobDesc desc = new RepairJobDesc(sessionID, UUID.randomUUID(), KEYSPACE, TABLE, Collections.singleton(fullRange));
        Validator validator = new Validator(desc, COORDINATOR, FBUtilities.nowInSeconds(), PreviewKind.REPAIRED);
        try (Refs<SSTableReader> sstables = CompactionManager.instance.getSSTablesToValidate(cfs, validator))
        {
            Assert.assertTrue(sstables.isEmpty());
        }

        Assert.assertTrue(prs.isSuspect());
    }

    @Test
    public void counterTable() throws Exception
    {
        UUID sessionID = registerSession(counter_cfs, false, true);
        ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionID);
        Assert.assertFalse(prs.isSuspect());

        prs.markCounterTablesSuspect();
        Assert.assertTrue(prs.isSuspect());
    }

    @Test
    public void nonCounterTable() throws Exception
    {
        UUID sessionID = registerSession(cfs, false, true);
        ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionID);

        prs.markCounterTablesSuspect();
        Assert.assertFalse(prs.isSuspect());
    }
}
