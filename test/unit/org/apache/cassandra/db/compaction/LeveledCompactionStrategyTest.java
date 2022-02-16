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
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspaceMigrator40;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.ValidationManager;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.util.Collections.singleton;
import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.bestBucket;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LeveledCompactionStrategyTest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategyTest.class);

    private static final String KEYSPACE1 = "LeveledCompactionStrategyTest";
    private static final String CF_STANDARDDLEVELED = "StandardLeveled";
    private static final String CF_STANDARDDLEVELED_GCGS0 = "StandardLeveledGCGS0";
    private static final String CF_STANDARDDLEVELED_SCHEDULED = "StandardLeveledScheduled";

    private Keyspace keyspace;
    private ColumnFamilyStore cfs;
    private ColumnFamilyStore cfsScheduled;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");

        SchemaLoader.prepareServer();
        Map<String, String> scheduledOpts = new HashMap<>();
        scheduledOpts.put("sstable_size_in_mb", "1");
        scheduledOpts.put("scheduled_compactions", "true");
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDDLEVELED)
                                                .compaction(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1"))),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDDLEVELED_SCHEDULED)
                                                .compaction(CompactionParams.lcs(scheduledOpts)),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDDLEVELED_GCGS0)
                                                .compaction(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1")))
                                                .gcGraceSeconds(0));
        }

    @Before
    public void enableCompaction()
    {
        keyspace = Keyspace.open(KEYSPACE1);
        cfs = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED);
        cfs.enableAutoCompaction();
        cfsScheduled = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED_SCHEDULED);
        cfsScheduled.enableAutoCompaction();
    }

    /**
     * clean up after the test.
     */
    @After
    public void truncateSTandardLeveled()
    {
        cfs.truncateBlocking();
        cfsScheduled.truncateBlocking();
    }

    /**
     * Ensure that the grouping operation preserves the levels of grouped tables
     */
    @Test
    public void testGrouperLevels() throws Exception{
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        //Need entropy to prevent compression so size is predictable with compression enabled/disabled
        new Random().nextBytes(value.array());

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        CompactionStrategyManager strategyManager = cfs.getCompactionStrategyManager();
        // Checking we're not completely bad at math

        int l1Count = strategyManager.getSSTableCountPerLevel()[1];
        int l2Count = strategyManager.getSSTableCountPerLevel()[2];
        if (l1Count == 0 || l2Count == 0)
        {
            logger.error("L1 or L2 has 0 sstables. Expected > 0 on both.");
            logger.error("L1: " + l1Count);
            logger.error("L2: " + l2Count);
            Assert.fail();
        }

        Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(cfs.getLiveSSTables());
        for (Collection<SSTableReader> sstableGroup : groupedSSTables)
        {
            int groupLevel = -1;
            Iterator<SSTableReader> it = sstableGroup.iterator();
            while (it.hasNext())
            {

                SSTableReader sstable = it.next();
                int tableLevel = sstable.getSSTableLevel();
                if (groupLevel == -1)
                    groupLevel = tableLevel;
                assert groupLevel == tableLevel;
            }
        }
    }

    /*
     * This exercises in particular the code of #4142
     */
    @Test
    public void testValidationMultipleSSTablePerLevel() throws Exception
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        CompactionStrategyManager strategyManager = cfs.getCompactionStrategyManager();
        // Checking we're not completely bad at math
        assertTrue(strategyManager.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategyManager.getSSTableCountPerLevel()[2] > 0);

        Range<Token> range = new Range<>(Util.token(""), Util.token(""));
        int gcBefore = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED).gcBefore(FBUtilities.nowInSeconds());
        UUID parentRepSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepSession,
                                                                 FBUtilities.getBroadcastAddressAndPort(),
                                                                 Arrays.asList(cfs),
                                                                 Arrays.asList(range),
                                                                 false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                 true,
                                                                 PreviewKind.NONE);
        RepairJobDesc desc = new RepairJobDesc(parentRepSession, UUID.randomUUID(), KEYSPACE1, CF_STANDARDDLEVELED, Arrays.asList(range));
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddressAndPort(), gcBefore, PreviewKind.NONE);

        ValidationManager.instance.submitValidation(cfs, validator).get();
    }

    /**
     * wait for leveled compaction to quiesce on the given columnfamily
     */
    public static void waitForLeveling(ColumnFamilyStore cfs) throws InterruptedException
    {
        CompactionStrategyManager strategyManager = cfs.getCompactionStrategyManager();
        while (true)
        {
            // since we run several compaction strategies we wait until L0 in all strategies is empty and
            // atleast one L1+ is non-empty. In these tests we always run a single data directory with only unrepaired data
            // so it should be good enough
            boolean allL0Empty = true;
            boolean anyL1NonEmpty = false;
            for (List<AbstractCompactionStrategy> strategies : strategyManager.getStrategies())
            {
                for (AbstractCompactionStrategy strategy : strategies)
                {
                    if (!(strategy instanceof LeveledCompactionStrategy))
                        return;
                    // note that we check > 1 here, if there is too little data in L0, we don't compact it up to L1
                    if (((LeveledCompactionStrategy)strategy).getLevelSize(0) > 1)
                        allL0Empty = false;
                    for (int i = 1; i < 5; i++)
                        if (((LeveledCompactionStrategy)strategy).getLevelSize(i) > 0)
                            anyL1NonEmpty = true;
                }
            }
            if (allL0Empty && anyL1NonEmpty)
                return;
            Thread.sleep(100);
        }
    }

    @Test
    public void testCompactionProgress() throws Exception
    {
        // make sure we have SSTables in L1
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        int rows = 2;
        int columns = 10;
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);
        assert strategy.getLevelSize(1) > 0;

        // get LeveledScanner for level 1 sstables
        Collection<SSTableReader> sstables = strategy.manifest.getLevel(1);
        List<ISSTableScanner> scanners = strategy.getScanners(sstables).scanners;
        assertEquals(1, scanners.size()); // should be one per level
        ISSTableScanner scanner = scanners.get(0);
        // scan through to the end
        while (scanner.hasNext())
            scanner.next();

        // scanner.getCurrentPosition should be equal to total bytes of L1 sstables
        assertEquals(scanner.getCurrentPosition(), SSTableReader.getTotalUncompressedBytes(sstables));
    }

    @Test
    public void testMutateLevel() throws Exception
    {
        cfs.disableAutoCompaction();
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);
        cfs.forceMajorCompaction();

        for (SSTableReader s : cfs.getLiveSSTables())
        {
            assertTrue(s.getSSTableLevel() != 6 && s.getSSTableLevel() > 0);
            strategy.manifest.remove(s);
            s.descriptor.getMetadataSerializer().mutateLevel(s.descriptor, 6);
            s.reloadSSTableMetadata();
            strategy.manifest.addSSTables(Collections.singleton(s));
        }
        // verify that all sstables in the changed set is level 6
        for (SSTableReader s : cfs.getLiveSSTables())
            assertEquals(6, s.getSSTableLevel());

        int[] levels = strategy.manifest.getAllLevelSize();
        // verify that the manifest has correct amount of sstables
        assertEquals(cfs.getLiveSSTables().size(), levels[6]);
    }

    @Test
    public void testNewRepairedSSTable() throws Exception
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        waitForLeveling(cfs);
        cfs.disableAutoCompaction();

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs), (sstable) -> true))
            Thread.sleep(100);

        CompactionStrategyManager manager = cfs.getCompactionStrategyManager();
        List<List<AbstractCompactionStrategy>> strategies = manager.getStrategies();
        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) strategies.get(0).get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) strategies.get(1).get(0);
        assertEquals(0, repaired.manifest.getLevelCount() );
        assertEquals(2, unrepaired.manifest.getLevelCount());
        assertTrue(manager.getSSTableCountPerLevel()[1] > 0);
        assertTrue(manager.getSSTableCountPerLevel()[2] > 0);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertFalse(sstable.isRepaired());

        int sstableCount = unrepaired.manifest.getSSTables().size();
        // we only have unrepaired sstables:
        assertEquals(sstableCount, cfs.getLiveSSTables().size());

        SSTableReader sstable1 = unrepaired.manifest.getLevel(2).iterator().next();
        SSTableReader sstable2 = unrepaired.manifest.getLevel(1).iterator().next();

        sstable1.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable1.descriptor, System.currentTimeMillis(), null, false);
        sstable1.reloadSSTableMetadata();
        assertTrue(sstable1.isRepaired());

        manager.handleNotification(new SSTableRepairStatusChanged(Arrays.asList(sstable1)), this);

        int repairedSSTableCount = repaired.manifest.getSSTables().size();
        assertEquals(1, repairedSSTableCount);
        // make sure the repaired sstable ends up in the same level in the repaired manifest:
        assertTrue(repaired.manifest.getLevel(2).contains(sstable1));
        // and that it is gone from unrepaired
        assertFalse(unrepaired.manifest.getLevel(2).contains(sstable1));

        unrepaired.removeSSTable(sstable2);
        manager.handleNotification(new SSTableAddedNotification(singleton(sstable2), null), this);
        assertTrue(unrepaired.manifest.getLevel(1).contains(sstable2));
        assertFalse(repaired.manifest.getLevel(1).contains(sstable2));
    }



    @Test
    public void testTokenRangeCompaction() throws Exception
    {
        // Remove any existing data so we can start out clean with predictable number of sstables
        cfs.truncateBlocking();

        // Disable auto compaction so cassandra does not compact
        CompactionManager.instance.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        DecoratedKey key1 = Util.dk(String.valueOf(1));
        DecoratedKey key2 = Util.dk(String.valueOf(2));
        List<DecoratedKey> keys = new ArrayList<>(Arrays.asList(key1, key2));
        int numIterations = 10;
        int columns = 2;

        // Add enough data to trigger multiple sstables.

        // create 10 sstables that contain data for both key1 and key2
        for (int i = 0; i < numIterations; i++) {
            for (DecoratedKey key : keys) {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }

        // create 20 more sstables with 10 containing data for key1 and other 10 containing data for key2
        for (int i = 0; i < numIterations; i++) {
            for (DecoratedKey key : keys) {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
                cfs.forceBlockingFlush();
            }
        }

        // We should have a total of 30 sstables by now
        assertEquals(30, cfs.getLiveSSTables().size());

        // Compact just the tables with key2
        // Bit hackish to use the key1.token as the prior key but works in BytesToken
        Range<Token> tokenRange = new Range<>(key2.getToken(), key2.getToken());
        Collection<Range<Token>> tokenRanges = new ArrayList<>(Arrays.asList(tokenRange));
        cfs.forceCompactionForTokenRange(tokenRanges);

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs), (sstable) -> true)) {
            Thread.sleep(100);
        }

        // 20 tables that have key2 should have been compacted in to 1 table resulting in 11 (30-20+1)
        assertEquals(11, cfs.getLiveSSTables().size());

        // Compact just the tables with key1. At this point all 11 tables should have key1
        Range<Token> tokenRange2 = new Range<>(key1.getToken(), key1.getToken());
        Collection<Range<Token>> tokenRanges2 = new ArrayList<>(Arrays.asList(tokenRange2));
        cfs.forceCompactionForTokenRange(tokenRanges2);


        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs), (sstable) -> true)) {
            Thread.sleep(100);
        }

        // the 11 tables containing key1 should all compact to 1 table
        assertEquals(1, cfs.getLiveSSTables().size());
        // Set it up again
        cfs.truncateBlocking();

        // create 10 sstables that contain data for both key1 and key2
        for (int i = 0; i < numIterations; i++)
        {
            for (DecoratedKey key : keys)
            {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }

        // create 20 more sstables with 10 containing data for key1 and other 10 containing data for key2
        for (int i = 0; i < numIterations; i++)
        {
            for (DecoratedKey key : keys)
            {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
                cfs.forceBlockingFlush();
            }
        }

        // We should have a total of 30 sstables again
        assertEquals(30, cfs.getLiveSSTables().size());

        // This time, we're going to make sure the token range wraps around, to cover the full range
        Range<Token> wrappingRange;
        if (key1.getToken().compareTo(key2.getToken()) < 0)
        {
            wrappingRange = new Range<>(key2.getToken(), key1.getToken());
        }
        else
        {
            wrappingRange = new Range<>(key1.getToken(), key2.getToken());
        }
        Collection<Range<Token>> wrappingRanges = new ArrayList<>(Arrays.asList(wrappingRange));
        cfs.forceCompactionForTokenRange(wrappingRanges);

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs), (sstable) -> true))
        {
            Thread.sleep(100);
        }

        // should all compact to 1 table
        assertEquals(1, cfs.getLiveSSTables().size());
    }

    @Test
    public void testCompactionCandidateOrdering() throws Exception
    {
        // add some data
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        int rows = 4;
        int columns = 10;
        // Just keep sstables in L0 for this test
        cfs.disableAutoCompaction();
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) (cfs.getCompactionStrategyManager()).getStrategies().get(1).get(0);
        // get readers for level 0 sstables
        Collection<SSTableReader> sstables = strategy.manifest.getLevel(0);
        Collection<SSTableReader> sortedCandidates = strategy.manifest.ageSortedSSTables(sstables);
        assertTrue(String.format("More than 1 sstable required for test, found: %d .", sortedCandidates.size()), sortedCandidates.size() > 1);
        long lastMaxTimeStamp = Long.MIN_VALUE;
        for (SSTableReader sstable : sortedCandidates)
        {
            assertTrue(String.format("SStables not sorted into oldest to newest by maxTimestamp. Current sstable: %d , last sstable: %d", sstable.getMaxTimestamp(), lastMaxTimeStamp),
                       sstable.getMaxTimestamp() > lastMaxTimeStamp);
            lastMaxTimeStamp = sstable.getMaxTimestamp();
        }
    }

    @Test
    public void testDisableSTCSInL0() throws IOException
    {
        /*
        First creates a bunch of sstables in L1, then overloads L0 with 50 sstables. Now with STCS in L0 enabled
        we should get a compaction task where the target level is 0, then we disable STCS-in-L0 and make sure that
        the compaction task we get targets L1 or higher.
         */
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "LeveledCompactionStrategy");
        localOptions.put("sstable_size_in_mb", "1");
        cfs.setCompactionParameters(localOptions);
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < 11; i++)
        {
            SSTableReader l1sstable = MockSchema.sstable(i, 1 * 1024 * 1024, cfs);
            l1sstable.descriptor.getMetadataSerializer().mutateLevel(l1sstable.descriptor, 1);
            l1sstable.reloadSSTableMetadata();
            sstables.add(l1sstable);
        }

        for (int i = 100; i < 150; i++)
            sstables.add(MockSchema.sstable(i, 1 * 1024 * 1024, cfs));

        cfs.disableAutoCompaction();
        cfs.addSSTables(sstables);
        assertEquals(0, getTaskLevel(cfs));

        try
        {
            CompactionManager.instance.setDisableSTCSInL0(true);
            assertTrue(getTaskLevel(cfs) > 0);
        }
        finally
        {
            CompactionManager.instance.setDisableSTCSInL0(false);
        }
    }

    private int getTaskLevel(ColumnFamilyStore cfs)
    {
        int level = -1;
        for (List<AbstractCompactionStrategy> strategies : cfs.getCompactionStrategyManager().getStrategies())
        {
            for (AbstractCompactionStrategy strategy : strategies)
            {
                AbstractCompactionTask task = strategy.getNextBackgroundTask(0);
                if (task != null)
                {
                    try
                    {
                        assertTrue(task instanceof LeveledCompactionTask);
                        LeveledCompactionTask lcsTask = (LeveledCompactionTask) task;
                        level = Math.max(level, lcsTask.getLevel());
                    }
                    finally
                    {
                        task.transaction.abort();
                    }
                }
            }
        }
        return level;
    }

    @Test
    public void testAddingOverlapping()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledManifest lm = new LeveledManifest(cfs, 10, 10, new SizeTieredCompactionStrategyOptions());
        List<SSTableReader> currentLevel = new ArrayList<>();
        int gen = 1;
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 10, 20, 1, cfs));
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 21, 30, 1, cfs));
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 51, 100, 1, cfs));
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 80, 120, 1, cfs));
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 90, 150, 1, cfs));

        lm.addSSTables(currentLevel);
        assertLevelsEqual(lm.getLevel(1), currentLevel.subList(0, 3));
        assertLevelsEqual(lm.getLevel(0), currentLevel.subList(3, 5));

        List<SSTableReader> newSSTables = new ArrayList<>();
        // this sstable last token is the same as the first token of L1 above, should get sent to L0:
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 5, 10, 1, cfs));
        lm.addSSTables(newSSTables);
        assertLevelsEqual(lm.getLevel(1), currentLevel.subList(0, 3));
        assertEquals(0, newSSTables.get(0).getSSTableLevel());
        assertTrue(lm.getLevel(0).containsAll(newSSTables));

        newSSTables.clear();
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 30, 40, 1, cfs));
        lm.addSSTables(newSSTables);
        assertLevelsEqual(lm.getLevel(1), currentLevel.subList(0, 3));
        assertEquals(0, newSSTables.get(0).getSSTableLevel());
        assertTrue(lm.getLevel(0).containsAll(newSSTables));

        newSSTables.clear();
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 100, 140, 1, cfs));
        lm.addSSTables(newSSTables);
        assertLevelsEqual(lm.getLevel(1), currentLevel.subList(0, 3));
        assertEquals(0, newSSTables.get(0).getSSTableLevel());
        assertTrue(lm.getLevel(0).containsAll(newSSTables));

        newSSTables.clear();
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 100, 140, 1, cfs));
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 120, 140, 1, cfs));
        lm.addSSTables(newSSTables);
        List<SSTableReader> newL1 = new ArrayList<>(currentLevel.subList(0, 3));
        newL1.add(newSSTables.get(1));
        assertLevelsEqual(lm.getLevel(1), newL1);
        newSSTables.remove(1);
        assertTrue(newSSTables.stream().allMatch(s -> s.getSSTableLevel() == 0));
        assertTrue(lm.getLevel(0).containsAll(newSSTables));
    }

    @Test
    public void singleTokenSSTableTest()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledManifest lm = new LeveledManifest(cfs, 10, 10, new SizeTieredCompactionStrategyOptions());
        List<SSTableReader> expectedL1 = new ArrayList<>();

        int gen = 1;
        // single sstable, single token (100)
        expectedL1.add(MockSchema.sstableWithLevel(gen++, 100, 100, 1, cfs));
        lm.addSSTables(expectedL1);

        List<SSTableReader> expectedL0 = new ArrayList<>();

        // should get moved to L0:
        expectedL0.add(MockSchema.sstableWithLevel(gen++, 99, 101, 1, cfs));
        expectedL0.add(MockSchema.sstableWithLevel(gen++, 100, 101, 1, cfs));
        expectedL0.add(MockSchema.sstableWithLevel(gen++, 99, 100, 1, cfs));
        expectedL0.add(MockSchema.sstableWithLevel(gen++, 100, 100, 1, cfs));
        lm.addSSTables(expectedL0);

        assertLevelsEqual(expectedL0, lm.getLevel(0));
        assertTrue(expectedL0.stream().allMatch(s -> s.getSSTableLevel() == 0));
        assertLevelsEqual(expectedL1, lm.getLevel(1));
        assertTrue(expectedL1.stream().allMatch(s -> s.getSSTableLevel() == 1));

        // should work:
        expectedL1.add(MockSchema.sstableWithLevel(gen++, 98, 99, 1, cfs));
        expectedL1.add(MockSchema.sstableWithLevel(gen++, 101, 101, 1, cfs));
        lm.addSSTables(expectedL1.subList(1, expectedL1.size()));
        assertLevelsEqual(expectedL1, lm.getLevel(1));
    }

    @Test
    public void randomMultiLevelAddTest()
    {
        int iterations = 100;
        int levelCount = 9;

        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledManifest lm = new LeveledManifest(cfs, 10, 10, new SizeTieredCompactionStrategyOptions());
        long seed = System.currentTimeMillis();
        Random r = new Random(seed);
        List<SSTableReader> newLevels = generateNewRandomLevels(cfs, 40, levelCount, 0, r);

        int sstableCount = newLevels.size();
        lm.addSSTables(newLevels);

        int [] expectedLevelSizes = lm.getAllLevelSize();

        for (int j = 0; j < iterations; j++)
        {
            newLevels = generateNewRandomLevels(cfs, 20, levelCount, sstableCount, r);
            sstableCount += newLevels.size();

            int[] canAdd = canAdd(lm, newLevels, levelCount);
            for (int i = 0; i < levelCount; i++)
                expectedLevelSizes[i] += canAdd[i];
            lm.addSSTables(newLevels);
        }

        // and verify no levels overlap
        int actualSSTableCount = 0;
        for (int i = 0; i < levelCount; i++)
        {
            actualSSTableCount += lm.getLevelSize(i);
            List<SSTableReader> level = new ArrayList<>(lm.getLevel(i));
            int lvl = i;
            assertTrue(level.stream().allMatch(s -> s.getSSTableLevel() == lvl));
            if (i > 0)
            {
                level.sort(SSTableReader.sstableComparator);
                SSTableReader prev = null;
                for (SSTableReader sstable : level)
                {
                    if (prev != null && sstable.first.compareTo(prev.last) <= 0)
                    {
                        String levelStr = level.stream().map(s -> String.format("[%s, %s]", s.first, s.last)).collect(Collectors.joining(", "));
                        String overlap = String.format("sstable [%s, %s] overlaps with [%s, %s] in level %d (%s) ", sstable.first, sstable.last, prev.first, prev.last, i, levelStr);
                        Assert.fail("[seed = "+seed+"] overlap in level "+lvl+": " + overlap);
                    }
                    prev = sstable;
                }
            }
        }
        assertEquals(sstableCount, actualSSTableCount);
        for (int i = 0; i < levelCount; i++)
            assertEquals("[seed = " + seed + "] wrong sstable count in level = " + i, expectedLevelSizes[i], lm.getLevel(i).size());
    }

    private static List<SSTableReader> generateNewRandomLevels(ColumnFamilyStore cfs, int maxSSTableCountPerLevel, int levelCount, int startGen, Random r)
    {
        List<SSTableReader> newLevels = new ArrayList<>();
        for (int level = 0; level < levelCount; level++)
        {
            int numLevelSSTables = r.nextInt(maxSSTableCountPerLevel) + 1;
            List<Integer> tokens = new ArrayList<>(numLevelSSTables * 2);

            for (int i = 0; i < numLevelSSTables * 2; i++)
                tokens.add(r.nextInt(4000));
            Collections.sort(tokens);
            for (int i = 0; i < tokens.size() - 1; i += 2)
            {
                SSTableReader sstable = MockSchema.sstableWithLevel(++startGen, tokens.get(i), tokens.get(i + 1), level, cfs);
                newLevels.add(sstable);
            }
        }
        return newLevels;
    }
    @Test
    public void testPerLevelSizeBytes() throws IOException
    {
        byte [] b = new byte[100];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        int rows = 5;
        int columns = 5;

        cfs.disableAutoCompaction();
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
        }
        cfs.forceBlockingFlush();

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        long [] levelSizes = cfs.getPerLevelSizeBytes();
        for (int i = 0; i < levelSizes.length; i++)
        {
            if (i != 0)
                assertEquals(0, levelSizes[i]);
            else
                assertEquals(sstable.onDiskLength(), levelSizes[i]);
        }

        assertEquals(sstable.onDiskLength(), cfs.getPerLevelSizeBytes()[0]);

        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) ( cfs.getCompactionStrategyManager()).getStrategies().get(1).get(0);
        strategy.manifest.remove(sstable);
        sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 2);
        sstable.reloadSSTableMetadata();
        strategy.manifest.addSSTables(Collections.singleton(sstable));

        levelSizes = cfs.getPerLevelSizeBytes();
        for (int i = 0; i < levelSizes.length; i++)
        {
            if (i != 2)
                assertEquals(0, levelSizes[i]);
            else
                assertEquals(sstable.onDiskLength(), levelSizes[i]);
        }

    }

    /**
     * brute-force checks if the new sstables can be added to the correct level in manifest
     *
     * @return count of expected sstables to add to each level
     */
    private static int[] canAdd(LeveledManifest lm, List<SSTableReader> newSSTables, int levelCount)
    {
        Map<Integer, Collection<SSTableReader>> sstableGroups = new HashMap<>();
        newSSTables.forEach(s -> sstableGroups.computeIfAbsent(s.getSSTableLevel(), k -> new ArrayList<>()).add(s));

        int[] canAdd = new int[levelCount];
        for (Map.Entry<Integer, Collection<SSTableReader>> lvlGroup : sstableGroups.entrySet())
        {
            int level = lvlGroup.getKey();
            if (level == 0)
            {
                canAdd[0] += lvlGroup.getValue().size();
                continue;
            }

            List<SSTableReader> newLevel = new ArrayList<>(lm.getLevel(level));
            for (SSTableReader sstable : lvlGroup.getValue())
            {
                newLevel.add(sstable);
                newLevel.sort(SSTableReader.sstableComparator);

                SSTableReader prev = null;
                boolean kept = true;
                for (SSTableReader sst : newLevel)
                {
                    if (prev != null && prev.last.compareTo(sst.first) >= 0)
                    {
                        newLevel.remove(sstable);
                        kept = false;
                        break;
                    }
                    prev = sst;
                }
                if (kept)
                    canAdd[level] += 1;
                else
                    canAdd[0] += 1;
            }
        }
        return canAdd;
    }

    private static void assertLevelsEqual(Collection<SSTableReader> l1, Collection<SSTableReader> l2)
    {
        assertEquals(l1.size(), l2.size());
        assertEquals(new HashSet<>(l1), new HashSet<>(l2));
    }

    @Test
    public void testHighestLevelHasMoreDataThanSupported()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        int fanoutSize = 2; // to generate less sstables
        LeveledManifest lm = new LeveledManifest(cfs, 1, fanoutSize, new SizeTieredCompactionStrategyOptions());

        // generate data for L7 to trigger compaction
        int l7 = 7;
        int maxBytesForL7 = (int) (Math.pow(fanoutSize, l7) * 1024 * 1024);
        int sstablesSizeForL7 = (int) (maxBytesForL7 * 1.001) + 1;
        List<SSTableReader> sstablesOnL7 = Collections.singletonList(MockSchema.sstableWithLevel( 1, sstablesSizeForL7, l7, cfs));
        lm.addSSTables(sstablesOnL7);

        // generate data for L8 to trigger compaction
        int l8 = 8;
        int maxBytesForL8 = (int) (Math.pow(fanoutSize, l8) * 1024 * 1024);
        int sstablesSizeForL8 = (int) (maxBytesForL8 * 1.001) + 1;
        List<SSTableReader> sstablesOnL8 = Collections.singletonList(MockSchema.sstableWithLevel( 2, sstablesSizeForL8, l8, cfs));
        lm.addSSTables(sstablesOnL8);

        // compaction for L8 sstables is not supposed to be run because there is no upper level to promote sstables
        // that's why we expect compaction candidates for L7 only
        Collection<SSTableReader> compactionCandidates = lm.getCompactionCandidates().sstables;
        assertThat(compactionCandidates).containsAll(sstablesOnL7);
        assertThat(compactionCandidates).doesNotContainAnyElementsOf(sstablesOnL8);
    }

    @Test
    public void testAggressiveGCCompaction() throws Exception
    {
        // The idea is to insert data in one SSTable and then delete most if it in another, with gc_grace 0.
        // Then mutate the levels so they are in levels 1 and 2. There's then no natural compaction to do
        // but aggressive GC compaction should notice one SSTable has mostly GCable data and compact the two together.
        // The resulting SSTable should be in level 1.

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED_GCGS0);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        final ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        final int numIterations = 100;

        // Insert the data
        for (int i = 0; i < numIterations; i++) {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata.get(), String.valueOf(i));
            update.newRow("column1").add("val", value).withTimestamp(0);
            update.applyUnsafe();
        }
        cfs.forceBlockingFlush();

        // Delete all but one
        for (int i = 0; i < numIterations - 1; i++) {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata.get(), 1, String.valueOf(i));
            builder.clustering("column1").delete("val");
            builder.build().apply();
        }
        cfs.forceBlockingFlush();

        assertEquals(2, cfs.getLiveSSTables().size());

        final CompactionStrategyManager compactionStrategyManager = cfs.getCompactionStrategyManager();

        // Put the 2 sstables in level 1 and level 2, respectively
        int level = 1;
        for (SSTableReader s : cfs.getLiveSSTables())
        {
            s.descriptor.getMetadataSerializer().mutateLevel(s.descriptor, level);
            s.reloadSSTableMetadata();
            level++;
        }
        // reload the compaction strategy
        compactionStrategyManager.setNewLocalCompactionStrategy(compactionStrategyManager.getCompactionParams());

        assertEquals(0, compactionStrategyManager.getSSTableCountPerLevel()[0]);
        assertEquals(1, compactionStrategyManager.getSSTableCountPerLevel()[1]);
        assertEquals(1, compactionStrategyManager.getSSTableCountPerLevel()[2]);

        // Since local deletion time is in seconds we need to wait for it to expire to make the tombstones gcable
        Thread.sleep(1000);

        // Compact without aggressive option, should do nothing
        // (this submits and waits for the pending compactions to complete)
        cfs.enableAutoCompaction(true);

        assertEquals(2, cfs.getLiveSSTables().size());

        cfs.enableAutoCompaction(false);

        // Now enable aggressive mode, we should compact
        CompactionManager.instance.setEnableAggressiveGCCompaction(true);
        try
        {
            assertTrue(CompactionManager.instance.getEnableAggressiveGCCompaction());

            // We have tombstoneCompactionInterval high so it considers the sstables as too young to compact
            compactionStrategyManager.setNewLocalCompactionStrategy(CompactionParams.lcs(Collections.singletonMap(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION, "3600")));
            // waits for pending compactions to complete
            cfs.enableAutoCompaction(true);
            assertEquals(2, cfs.getLiveSSTables().size());

            // Now set tombstoneCompactionInterval=0 so we should compact
            compactionStrategyManager.setNewLocalCompactionStrategy(CompactionParams.lcs(Collections.singletonMap(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION, "0")));

            // Set one to be repaired so check it doesn't compact them together
            Iterator<SSTableReader> it = cfs.getLiveSSTables().iterator();
            SSTableReader sstable1 = it.next();
            SSTableReader sstable2 = it.next();

            UUID repairUUID = UUID.randomUUID();
            sstable1.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable1.descriptor, System.currentTimeMillis(), null, false);
            sstable1.reloadSSTableMetadata();
            compactionStrategyManager.setNewLocalCompactionStrategy(compactionStrategyManager.getCompactionParams());

            cfs.enableAutoCompaction(true);
            assertEquals(2, cfs.getLiveSSTables().size());

            // Now set the other to be repaired and it should finally compact them
            sstable2.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable2.descriptor, System.currentTimeMillis(), null, false);
            sstable2.reloadSSTableMetadata();
            compactionStrategyManager.setNewLocalCompactionStrategy(compactionStrategyManager.getCompactionParams());

            cfs.enableAutoCompaction(true);
            assertEquals(1, cfs.getLiveSSTables().size());

            // Check the resulting table is in level 1
            assertEquals(0, compactionStrategyManager.getSSTableCountPerLevel()[0]);
            assertEquals(1, compactionStrategyManager.getSSTableCountPerLevel()[1]);
            assertEquals(0, compactionStrategyManager.getSSTableCountPerLevel()[2]);
        }
        finally
        {
            // disable so we don't infect other tests
            CompactionManager.instance.setEnableAggressiveGCCompaction(false);
        }
        assertFalse(CompactionManager.instance.getEnableAggressiveGCCompaction());
    }

    @Test
    public void splitRangeTest()
    {
        List<Range<Token>> toSplit = new ArrayList<>();
        Range<Token> r = new Range<>(new LongToken(0), new LongToken(1000000));
        toSplit.add(r);
        List<Range<Token>> splitRanges = LeveledCompactionStrategy.splitRanges(new Murmur3Partitioner(), toSplit, 100);
        List<Range<Token>> normalizedSplit = Range.normalize(splitRanges);
        assertEquals(1 ,normalizedSplit.size());
        assertEquals(normalizedSplit.iterator().next(), r);
    }

    @Test
    public void splitManyRangesTest()
    {
        List<Range<Token>> toSplit = new ArrayList<>();
        Random rand = new Random(0);
        List<Token> tokens = new ArrayList<>();
        for (int i = 0; i < 200; i++)
            tokens.add(new LongToken(rand.nextLong()));

        tokens.sort(new Comparator<Token>()
        {
            public int compare(Token o1, Token o2)
            {
                return o1.compareTo(o2);
            }
        });

        for (int i = 0; i < tokens.size() - 1; i+=2)
        {
            toSplit.add(new Range<>(tokens.get(i), tokens.get(i+1)));
        }
        List<Range<Token>> splitRanges = LeveledCompactionStrategy.splitRanges(new Murmur3Partitioner(), toSplit, 200);
        assertEquals(200, splitRanges.size());
    }

    @Test
    public void splitTooNarrow()
    {
        Range<Token> tooNarrow = new Range<>(new LongToken(0), new LongToken(10));
        assertEquals(0, LeveledCompactionStrategy.splitRanges(new Murmur3Partitioner(), Collections.singletonList(tooNarrow), 20).size());
    }

    @Test
    public void unSplittableRange()
    {
        List<Range<Token>> toSplit = new ArrayList<>();
        Range<Token> r = new Range<>(new LongToken(0), new LongToken(1)); // we can't split this range - it should be kept as-is
        toSplit.add(r);
        toSplit.add(new Range<>(new LongToken(10), new LongToken(2000)));
        List<Range<Token>> split = LeveledCompactionStrategy.splitRanges(new Murmur3Partitioner(), toSplit, 20);
        assertTrue(split.contains(r));
    }
    @Test
    public void testGetScheduledCompactionFewSplits() throws Exception
    {
        testGetScheduledCompactionOP(2);
    }
    @Test
    public void testGetScheduledCompactionNormalSplits() throws Exception
    {
        testGetScheduledCompactionOP(100);
    }

    @Test
    public void testGetScheduledCompactionManySplits() throws Exception
    {
        testGetScheduledCompactionOP(1000);
    }

    private void testGetScheduledCompactionOP(int splits) throws Exception
    {
        Token t = cfsScheduled.getPartitioner().getRandomToken();
        StorageService.instance.getTokenMetadata().updateNormalTokens(Collections.singleton(t), FBUtilities.getBroadcastAddressAndPort());
        testGetScheduledCompaction(splits, cfsScheduled);
    }

    public static void testGetScheduledCompaction(int splits, ColumnFamilyStore cfsScheduled) throws Exception
    {
        DatabaseDescriptor.setEnableScheduledCompactions(true);
        // hard to create leveling where there are non-single-sstable compactions to do after the first round
        DatabaseDescriptor.setSkipSingleSSTableScheduledCompactions(false);
        DatabaseDescriptor.setScheduledCompactionRangeSplits(splits);

        populateCfsScheduled(cfsScheduled);

        Token sstableMaxToken = null;
        Token sstableMinToken = null;
        for (SSTableReader sstable : cfsScheduled.getSSTables(SSTableSet.LIVE))
        {
            if (sstableMinToken == null || sstableMinToken.compareTo(sstable.first.getToken()) > 0)
                sstableMinToken = sstable.first.getToken();
            if (sstableMaxToken == null || sstableMaxToken.compareTo(sstable.last.getToken()) < 0)
                sstableMaxToken = sstable.last.getToken();
        }

        CompactionStrategyManager strategy = cfsScheduled.getCompactionStrategyManager();
        // Checking we're not completely bad at math
        assertTrue(strategy.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategy.getSSTableCountPerLevel()[2] > 0);
        final List<List<AbstractCompactionStrategy>> strategies = strategy.getStrategies();
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) strategies.get(1).get(0);
        int gcBefore = cfsScheduled.gcBefore((int) (System.currentTimeMillis() / 1000));
        List<Range<Token>> compactedRanges = new ArrayList<>();
        Iterable<SSTableReader> originalSSTables = cfsScheduled.getSSTables(SSTableSet.LIVE);

        Set<SSTableReader> compactedSSTables = new HashSet<>();
        while (true)
        {
            LeveledCompactionStrategy.ScheduledLeveledCompactionTask task = lcs.getNextScheduledCompactionTask(gcBefore, false, "/tmp/something");
            if (task != null)
            {
                task.execute(new ActiveCompactionsTracker()
                {
                    public void beginCompaction(CompactionInfo.Holder ci) {}
                    public void finishCompaction(CompactionInfo.Holder ci) {}
                });

                // this means we have wrapped around and can stop compacting
                if (compactedRanges.contains(task.compactedRange))
                    break;
                compactedSSTables.addAll(task.transaction.originals());
                compactedRanges.add(task.compactedRange);
            }
            Thread.sleep(10);
        }
        // all original sstables should have been compacted:
        assertTrue(compactedSSTables.containsAll(Sets.newHashSet(originalSSTables)));
        // make sure all ssstables are intersecting one of the compacted ranges
        for (SSTableReader sstable : cfsScheduled.getSSTables(SSTableSet.LIVE))
        {
            Bounds<Token> sstableRange = new Bounds<>(sstable.first.getToken(), sstable.last.getToken());
            boolean intersected = false;
            for (Range<Token> compactedRange : compactedRanges)
            {
                if (compactedRange.intersects(sstableRange))
                    intersected = true;
            }
            assertTrue(intersected);
        }

        // everything is dropped in L1 - other levels should be empty since we have covered the whole range above
        assertTrue(lcs.manifest.getLevel(0).size() == 0);
        assertTrue(lcs.manifest.getLevel(1).size() > 0);

        StorageService.instance.getTokenMetadata().clearUnsafe();
    }

    @Test
    public void testScheduledCompactionTimeWrap() throws Exception
    {
        CompactionStrategyManager strategy = cfsScheduled.getCompactionStrategyManager();
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) strategy.getStrategies().get(1).get(0);
        lcs.resetSubrangeCompactionInfo();
        // disable to not get any scheduled compactions before we actually want them
        DatabaseDescriptor.setEnableScheduledCompactions(false);
        // hard to create leveling where there are non-single-sstable compactions to do after the first round
        DatabaseDescriptor.setSkipSingleSSTableScheduledCompactions(false);
        DatabaseDescriptor.setScheduledCompactionRangeSplits(4);
        DatabaseDescriptor.setScheduledCompactionCycleTime("4s");

        Token t = cfsScheduled.getPartitioner().getRandomToken();
        StorageService.instance.getTokenMetadata().updateNormalTokens(Collections.singleton(t), FBUtilities.getBroadcastAddressAndPort());
        populateCfsScheduled(cfsScheduled);
        DatabaseDescriptor.setEnableScheduledCompactions(true);

        int scheduledCount = 0;
        List<Range<Token>> compactedRanges = new ArrayList<>();
        while (scheduledCount < 4)
        {
            long start = System.currentTimeMillis();
            AbstractCompactionTask task = lcs.getNextBackgroundTask(0);
            boolean isScheduled = task != null && task instanceof LeveledCompactionStrategy.ScheduledLeveledCompactionTask;
            // before we execute the task it is always time for a scheduled compaction:
            if (isScheduled)
                assertTrue(lcs.timeForScheduledCompaction(false, "/tmp/something"));
            if (task != null)
            {
                task.execute(new ActiveCompactionsTracker()
                {
                    public void beginCompaction(CompactionInfo.Holder ci) {}
                    public void finishCompaction(CompactionInfo.Holder ci) {}
                });
                // after executing the scheduled compaction it should not be time for a new one (in this setup where we are not backlogged on compactions)
                if (isScheduled)
                {
                    scheduledCount++;
                    // if the compaction is slow it might take > 1s (4 splits, 4s cycle time) to compact, don't assert if so (+a bit of margin)
                    if(System.currentTimeMillis() - start < 800)
                        assertFalse(lcs.timeForScheduledCompaction(false, "/tmp/something"));
                    compactedRanges.add(((LeveledCompactionStrategy.ScheduledLeveledCompactionTask) task).compactedRange);
                }
            }
            Thread.sleep(1);
        }
        // make sure we are repeating ourselves:
        assertTrue(compactedRanges.size() > new HashSet<>(compactedRanges).size());
        lcs.resetSubrangeCompactionInfo();
    }

    @Test
    public void testStoreSuccessfulScheduledCompaction() throws Exception
    {
        long beforeFirst = System.currentTimeMillis();
        testGetScheduledCompactionOP(100);
        Pair<Token, Long> lastSuccessPair = SystemKeyspace.getLastSuccessfulScheduledCompaction(KEYSPACE1, CF_STANDARDDLEVELED_SCHEDULED, false, "/tmp/something");

        long lastSuccessTime = lastSuccessPair.right;
        assertTrue(lastSuccessTime > beforeFirst);
        assertTrue(lastSuccessPair.left != null);
    }

    @Test
    public void testConfigValue()
    {
        DatabaseDescriptor.setScheduledCompactionCycleTime("1d");
        assertEquals(TimeUnit.DAYS.toSeconds(1), DatabaseDescriptor.getScheduledCompactionCycleTimeSeconds());

        DatabaseDescriptor.setScheduledCompactionCycleTime("1h");
        assertEquals(TimeUnit.HOURS.toSeconds(1), DatabaseDescriptor.getScheduledCompactionCycleTimeSeconds());

        DatabaseDescriptor.setScheduledCompactionCycleTime("55s");
        assertEquals(55, DatabaseDescriptor.getScheduledCompactionCycleTimeSeconds());

        boolean gotException = false;
        try
        {
            DatabaseDescriptor.setScheduledCompactionCycleTime("99");
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
        // and value should stay the same:
        assertEquals(55, DatabaseDescriptor.getScheduledCompactionCycleTimeSeconds());

        gotException = false;
        try
        {
            DatabaseDescriptor.setScheduledCompactionCycleTime("99x");
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
        // and value should stay the same:
        assertEquals(55, DatabaseDescriptor.getScheduledCompactionCycleTimeSeconds());

        DatabaseDescriptor.setScheduledCompactionCycleTime("  102H  ");
        assertEquals(TimeUnit.HOURS.toSeconds(102), DatabaseDescriptor.getScheduledCompactionCycleTimeSeconds());
    }

    @Test
    public void testOverlapWithMin()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Set<SSTableReader> sstables = new HashSet<>();
        sstables.add(sstable(cfs, 1, 10L, 100L));
        sstables.add(sstable(cfs, 2, 90L, 150L));
        sstables.add(sstable(cfs, 3, 150L, 200L));
        sstables.add(sstable(cfs, 4, 199L, 300L));
        sstables.add(sstable(cfs, 5, 310L, 400L));

        // from 0 -> 10 includes a single sstable, generation 1:
        assertEquals(1, Iterables.getOnlyElement(LeveledManifest.overlappingWithMin(cfs.getPartitioner(), t(0), t(10), sstables)).descriptor.generation);

        // 90 -> 100 includes 2 sstables, gen 1 and 2
        Set<SSTableReader> overlapping = LeveledManifest.overlappingWithMin(cfs.getPartitioner(), t(90), t(100), sstables);
        assertEquals(2, overlapping.size());
        assertEquals(Sets.newHashSet(1, 2), overlapping.stream().map(s -> s.descriptor.generation).collect(Collectors.toSet()));

        // 50 -> 90 includes 2 sstables, gen 1 and 2
        overlapping = LeveledManifest.overlappingWithMin(cfs.getPartitioner(), t(50), t(90), sstables);
        assertEquals(2, overlapping.size());
        assertEquals(Sets.newHashSet(1, 2), overlapping.stream().map(s -> s.descriptor.generation).collect(Collectors.toSet()));

        // 290 -> partitioner min token -> 2 sstables, gen 4 and 5
        overlapping = LeveledManifest.overlappingWithMin(cfs.getPartitioner(), t(290), cfs.getPartitioner().getMinimumToken(), sstables);
        assertEquals(2, overlapping.size());
        assertEquals(Sets.newHashSet(4, 5), overlapping.stream().map(s -> s.descriptor.generation).collect(Collectors.toSet()));

        // testing a wrapping range (1000, 100] -> normalized = [(-9223372036854775808,100], (1000,-9223372036854775808]]
        // this means that the first range should contain 2 sstables (gen 1 and 2), and the second should include none
        Range<Token> wrapping = new Range<>(t(1000), t(100));
        overlapping.clear();
        for (Range<Token> r : Range.normalize(Collections.singleton(wrapping)))
        {
            overlapping.addAll(LeveledManifest.overlappingWithMin(cfs.getPartitioner(), r.left, r.right, sstables));
        }
        assertEquals(2, overlapping.size());
        assertEquals(Sets.newHashSet(1, 2), overlapping.stream().map(s -> s.descriptor.generation).collect(Collectors.toSet()));

        // (350, 0] -> normalized = [(-9223372036854775808,0], (350,-9223372036854775808]]
        // => first range should give 0 sstables, second a single one, generation 5
        wrapping = new Range<>(t(350), t(0));
        overlapping.clear();
        for (Range<Token> r : Range.normalize(Collections.singleton(wrapping)))
            overlapping.addAll(LeveledManifest.overlappingWithMin(cfs.getPartitioner(), r.left, r.right, sstables));
        assertEquals(1, overlapping.size());
        assertEquals(Sets.newHashSet(5), overlapping.stream().map(s -> s.descriptor.generation).collect(Collectors.toSet()));
    }

    @Test
    public void testMigrateScheduledCompactions() throws IOException
    {
        Directories.DataDirectories fakeDatadirs = new Directories.DataDirectories(new String[] { "../tmp/a", "../tmp/b", "/tmp/c", "/tmp/d", "/tmp/e"},
                                                                                   new String[] {});
        Set<String> expectedAbsoluteDatadirs = fakeDatadirs.getAllDirectories().stream().map(dd -> dd.location.absolutePath()).collect(Collectors.toSet());
        IPartitioner part = DatabaseDescriptor.getPartitioner();
        try
        {
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            for (int i = 0; i < 10; i++)
            {
                ByteBuffer bbToken = ByteBuffer.wrap(SystemKeyspace.tokenToBytes(new LongToken(i)));
                for (boolean b : new boolean[] { true, false })
                {
                    QueryProcessor.executeInternal("insert into system.scheduled_compactions (keyspace_name, columnfamily_name, repaired, end_token, start_time) VALUES (?, ?, ?, ?, ?)",
                                                   "ks" + i, "tbl" + i, b, bbToken, (long)i);
                }
            }
            SystemKeyspaceMigrator40.migrateScheduledCompactions(fakeDatadirs);

            for (int i = 0; i < 10; i++)
            {
                UntypedResultSet res = QueryProcessor.executeInternal("select * from system.scheduled_compactions_v2 where keyspace_name = ? and table_name = ?", "ks" + i, "tbl" + i);
                assertEquals(2 * fakeDatadirs.getAllDirectories().size(), res.size()); // repaired + unrepaired in 5 datadirs
                Set<String> seenUnrepairedDatadirs = new HashSet<>();
                Set<String> seenRepairedDatadirs = new HashSet<>();
                for (UntypedResultSet.Row row : res)
                {
                    Token token = Token.serializer.deserialize(ByteStreams.newDataInput(ByteBufferUtil.getArray(row.getBytes("end_token"))), DatabaseDescriptor.getPartitioner(), MessagingService.current_version);
                    assertEquals(new LongToken(i), token);
                    assertEquals(i, row.getLong("start_time"));
                    String ddir = row.getString("data_directory");
                    if (row.getBoolean("repaired"))
                        seenRepairedDatadirs.add(ddir);
                    else
                        seenUnrepairedDatadirs.add(ddir);
                }
                assertEquals(seenRepairedDatadirs, expectedAbsoluteDatadirs);
                assertEquals(seenUnrepairedDatadirs, expectedAbsoluteDatadirs);
            }
            assertEquals(0, QueryProcessor.executeInternal("select * from system.scheduled_compactions").size());
        }
        finally
        {
            DatabaseDescriptor.setPartitionerUnsafe(part);
        }
    }


    @Test
    public void testRPSplit()
    {
        // RandomPartitioner has a weird behaviour where midpoint for (min token, 0) gives the midpoint for the
        // full token range
        Range<Token> wrapRP = new Range<>(new RandomPartitioner.BigIntegerToken(new BigInteger("10000")),
                                          new RandomPartitioner.BigIntegerToken(BigInteger.ONE));
        List<Range<Token>> normalized = Range.normalize(Collections.singleton(wrapRP));
        List<Range<Token>> splitRanges = LeveledCompactionStrategy.splitRanges(RandomPartitioner.instance, normalized, 100);
        Token prev = null;
        for (Range<Token> r : splitRanges)
        {
            assertTrue(prev == null || r.right.compareTo(prev) > 0 || r.right.equals(RandomPartitioner.instance.getMinimumToken()));
            prev = r.right;
        }
    }

    @Test
    public void testGetBestBucket()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<SSTableReader> sstables = new ArrayList<>();
        // first 100 sstables:
        for (int i = 0; i < 100; i++)
            sstables.add(MockSchema.sstable(i, i, cfs));

        // then 40 400-byte sstables which should not be included in the compaction:
        for (int i = 0; i < 40; i++)
            sstables.add(MockSchema.sstable(i, 400, cfs));

        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("min_sstable_size","200"); // make sure the small sstables get put in the same bucket
        SizeTieredCompactionStrategyOptions options = new SizeTieredCompactionStrategyOptions(optionsMap);
        DatabaseDescriptor.setCompactBiggestSTCSBucketInL0(true);
        DatabaseDescriptor.setBiggestBucketMaxSSTableCount(33);

        List<SSTableReader> biggestBucket = LeveledManifest.getSSTablesForSTCS(cfs, options, sstables);
        assertEquals(33, biggestBucket.size()); // all 100 small sstables should be in the biggest bucket
        for (SSTableReader sstable : biggestBucket)
        {
            assertEquals(sstable.descriptor.generation, sstable.onDiskLength());
            assertTrue(sstable.onDiskLength() < 33); // did we actually get the smallest files?
        }

        // check that max compaction threshold holds:
        DatabaseDescriptor.setBiggestBucketMaxSSTableCount(50);
        biggestBucket = LeveledManifest.getSSTablesForSTCS(cfs, options, sstables);
        assertEquals(50, biggestBucket.size()); // all 100 small sstables should be in the biggest bucket
        for (SSTableReader sstable : biggestBucket)
        {
            assertEquals(sstable.descriptor.generation, sstable.onDiskLength());
            assertTrue(sstable.onDiskLength() < 50);
        }

        DatabaseDescriptor.setBiggestBucketMaxSSTableCount(1024);
        cfs.setMaximumCompactionThreshold(105);
        biggestBucket = LeveledManifest.getSSTablesForSTCS(cfs, options, sstables);
        assertEquals(100, biggestBucket.size()); // make sure we get the full smallest (by size) bucket
        DatabaseDescriptor.setCompactBiggestSTCSBucketInL0(false);
        cfs.setMaximumCompactionThreshold(32);
    }

    @Test
    public void testBestBucket2()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<List<SSTableReader>> buckets = new ArrayList<>();
        SSTableReader sstable1 = MockSchema.sstable(1, 1, cfs);
        SSTableReader sstable2 = MockSchema.sstable(2, 2, cfs);
        SSTableReader sstable3 = MockSchema.sstable(3, 3, cfs);
        buckets.add(new ArrayList<>());
        buckets.add(new ArrayList<>());
        buckets.add(new ArrayList<>());

        for (int i = 0; i < 50; i++)
        {
            buckets.get(0).add(sstable1);
            buckets.get(1).add(sstable2);
            buckets.get(2).add(sstable3);
        }

        buckets.get(1).add(sstable2);
        buckets.get(2).add(sstable3);
        buckets.get(2).add(sstable3);

        // now we have:
        // bucket[0] with 50 sstables with size = 1,
        // bucket[1] with 51 sstables with size = 2,
        // bucket[2] with 52 sstables with size = 3,
        // best bucket should be bucket[0] if we have max_threshold < 50, but bucket[2] if max_threshold > 52:
        List<SSTableReader> best = bestBucket(buckets, 4, 32, Long.MAX_VALUE, 40, false);
        assertEquals(40, best.size());
        assertTrue(best.stream().allMatch(sstable -> sstable.onDiskLength() == 1));

        // make sure bucket size limiting works:
        best = bestBucket(buckets, 4, 32, 30, 1024, false);
        assertEquals(30, best.size());

        // this considers all buckets with size >= 51, the one with size=1-sstables is still the best one due to having a
        // smaller on-disk size
        best = bestBucket(buckets, 4,51, Long.MAX_VALUE, 40, false);
        assertEquals(40, best.size());
        assertTrue(best.stream().allMatch(sstable -> sstable.onDiskLength() == 1));

        // this considers all buckets with size >= 51 and buckets are pruned to 51 sstables - this means that the bucket with
        // 2-sized sstables is the best
        best = bestBucket(buckets, 4,51, Long.MAX_VALUE, 51, false);
        assertEquals(51, best.size());
        assertTrue(best.stream().allMatch(sstable -> sstable.onDiskLength() == 2));

        best = bestBucket(buckets, 4, 64, 5, 25, false);
        // only 5 sstables remain after pruning by size - these are each size 1
        assertEquals(5, best.size());
        assertTrue(best.stream().allMatch(sstable -> sstable.onDiskLength() == 1));

        // if min threshold is large
        best = bestBucket(buckets, 55, 64, Long.MAX_VALUE, 1024, false);
        assertEquals(0, best.size());
    }

    @Test
    public void testReduceScopeL0L1() throws IOException
    {
        ColumnFamilyStore mockcfs = MockSchema.newCFS();
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "LeveledCompactionStrategy");
        localOptions.put("sstable_size_in_mb", "1");
        mockcfs.setCompactionParameters(localOptions);
        List<SSTableReader> l1sstables = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            SSTableReader l1sstable = MockSchema.sstable(i, 1 * 1024 * 1024, mockcfs);
            l1sstable.descriptor.getMetadataSerializer().mutateLevel(l1sstable.descriptor, 1);
            l1sstable.reloadSSTableMetadata();
            l1sstables.add(l1sstable);
        }
        List<SSTableReader> l0sstables = new ArrayList<>();
        for (int i = 10; i < 20; i++)
            l0sstables.add(MockSchema.sstable(i, (i + 1) * 1024 * 1024, mockcfs));
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, Iterables.concat(l0sstables, l1sstables)))
        {
            Set<SSTableReader> nonExpired = Sets.difference(txn.originals(), Collections.emptySet());
            CompactionTask task = new LeveledCompactionTask(mockcfs, txn, 1, 0, 1024*1024, false);
            SSTableReader lastRemoved = null;
            boolean removed = true;
            for (int i = 0; i < l0sstables.size(); i++)
            {
                Set<SSTableReader> before = new HashSet<>(txn.originals());
                removed = task.reduceScopeForLimitedSpace(nonExpired, 0);
                SSTableReader removedSSTable = Iterables.getOnlyElement(Sets.difference(before, txn.originals()), null);
                if (removed)
                {
                    assertNotNull(removedSSTable);
                    assertTrue(lastRemoved == null || removedSSTable.onDiskLength() < lastRemoved.onDiskLength());
                    assertEquals(0, removedSSTable.getSSTableLevel());
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l1after = sstables.right;

                    assertEquals(l1after, new HashSet<>(l1sstables)); // we don't touch L1
                    assertEquals(before.size() - 1, txn.originals().size());
                    lastRemoved = removedSSTable;
                }
                else
                {
                    assertNull(removedSSTable);
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l0after = sstables.left;
                    Set<SSTableReader> l1after = sstables.right;
                    assertEquals(l1after, new HashSet<>(l1sstables)); // we don't touch L1
                    assertEquals(1, l0after.size()); // and we stop reducing once there is a single sstable left
                }
            }
            assertNotNull(lastRemoved);
            assertFalse(removed);
        }
    }

    @Test
    public void testReduceScopeL0()
    {

        List<SSTableReader> l0sstables = new ArrayList<>();
        for (int i = 1; i < 5; i++)
            l0sstables.add(MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs));

        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, l0sstables))
        {
            CompactionTask task = new LeveledCompactionTask(cfs, txn, 0, 0, 1024*1024, false);

            SSTableReader lastRemoved = null;
            boolean removed = true;
            for (int i = 0; i < l0sstables.size(); i++)
            {
                Set<SSTableReader> before = new HashSet<>(txn.originals());
                removed = task.reduceScopeForLimitedSpace(before, 0);
                SSTableReader removedSSTable = Sets.difference(before, txn.originals()).stream().findFirst().orElse(null);
                if (removed)
                {
                    assertNotNull(removedSSTable);
                    assertTrue(lastRemoved == null || removedSSTable.onDiskLength() < lastRemoved.onDiskLength());
                    assertEquals(0, removedSSTable.getSSTableLevel());
                    assertEquals(before.size() - 1, txn.originals().size());
                    lastRemoved = removedSSTable;
                }
                else
                {
                    assertNull(removedSSTable);
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l0after = sstables.left;
                    assertEquals(1, l0after.size()); // and we stop reducing once there is a single sstable left
                }
            }
            assertFalse(removed);
        }
    }

    @Test
    public void testNoHighLevelReduction() throws IOException
    {
        List<SSTableReader> sstables = new ArrayList<>();
        int i = 1;
        for (; i < 5; i++)
        {
            SSTableReader sstable = MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs);
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1);
            sstable.reloadSSTableMetadata();
            sstables.add(sstable);
        }
        for (; i < 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs);
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 2);
            sstable.reloadSSTableMetadata();
            sstables.add(sstable);
        }
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, sstables))
        {
            CompactionTask task = new LeveledCompactionTask(cfs, txn, 0, 0, 1024 * 1024, false);
            assertFalse(task.reduceScopeForLimitedSpace(Sets.newHashSet(sstables), 0));
            assertEquals(Sets.newHashSet(sstables), txn.originals());
        }
    }

    private Pair<Set<SSTableReader>, Set<SSTableReader>> groupByLevel(Iterable<SSTableReader> sstables)
    {
        Set<SSTableReader> l1after = new HashSet<>();
        Set<SSTableReader> l0after = new HashSet<>();
        for (SSTableReader sstable : sstables)
        {
            switch (sstable.getSSTableLevel())
            {
                case 0:
                    l0after.add(sstable);
                    break;
                case 1:
                    l1after.add(sstable);
                    break;
                default:
                    throw new RuntimeException("only l0 & l1 sstables");
            }
        }
        return Pair.create(l0after, l1after);
    }

    private static SSTableReader sstable(ColumnFamilyStore cfs, int generation, long startToken, long endToken)
    {
        return MockSchema.sstable(generation, 0, false, startToken, endToken, cfs);
    }
    private static Token t(long t)
    {
        return new LongToken(t);
    }

    static void populateCfsScheduled(ColumnFamilyStore cfs) throws InterruptedException
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        waitForLeveling(cfs);
        cfs.disableAutoCompaction();

        while (!cfs.getTracker().getCompacting().isEmpty())
        {
            System.out.println("waiting for compacting: "+cfs.getTracker().getCompacting());
            Thread.sleep(100);
        }
        System.out.println("SIZE="+cfs.getLiveSSTables().size());
    }
}
