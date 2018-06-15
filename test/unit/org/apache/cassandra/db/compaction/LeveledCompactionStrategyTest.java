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

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.hsqldb.Database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;

@RunWith(OrderedJUnit4ClassRunner.class)
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
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        CompactionStrategyManager strategy =  cfs.getCompactionStrategyManager();
        // Checking we're not completely bad at math
        int l1Count = strategy.getSSTableCountPerLevel()[1];
        int l2Count = strategy.getSSTableCountPerLevel()[2];
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
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        CompactionStrategyManager strategy =  cfs.getCompactionStrategyManager();
        // Checking we're not completely bad at math
        assertTrue(strategy.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategy.getSSTableCountPerLevel()[2] > 0);

        Range<Token> range = new Range<>(Util.token(""), Util.token(""));
        int gcBefore = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED).gcBefore(FBUtilities.nowInSeconds());
        UUID parentRepSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepSession,
                                                                 FBUtilities.getBroadcastAddress(),
                                                                 Arrays.asList(cfs),
                                                                 Arrays.asList(range),
                                                                 false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                 true,
                                                                 PreviewKind.NONE);
        RepairJobDesc desc = new RepairJobDesc(parentRepSession, UUID.randomUUID(), KEYSPACE1, CF_STANDARDDLEVELED, Arrays.asList(range));
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddress(), gcBefore, PreviewKind.NONE);
        CompactionManager.instance.submitValidation(cfs, validator).get();
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
            for (AbstractCompactionStrategy strategy : strategyManager.getStrategies())
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
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) (cfs.getCompactionStrategyManager()).getStrategies().get(1);
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
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) ( cfs.getCompactionStrategyManager()).getStrategies().get(1);
        cfs.forceMajorCompaction();

        for (SSTableReader s : cfs.getLiveSSTables())
        {
            assertTrue(s.getSSTableLevel() != 6 && s.getSSTableLevel() > 0);
            strategy.manifest.remove(s);
            s.descriptor.getMetadataSerializer().mutateLevel(s.descriptor, 6);
            s.reloadSSTableMetadata();
            strategy.manifest.add(s);
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
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        waitForLeveling(cfs);
        cfs.disableAutoCompaction();

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs)))
            Thread.sleep(100);

        CompactionStrategyManager strategy =  cfs.getCompactionStrategyManager();
        List<AbstractCompactionStrategy> strategies = strategy.getStrategies();
        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) strategies.get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) strategies.get(1);
        assertEquals(0, repaired.manifest.getLevelCount() );
        assertEquals(2, unrepaired.manifest.getLevelCount());
        assertTrue(strategy.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategy.getSSTableCountPerLevel()[2] > 0);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertFalse(sstable.isRepaired());

        int sstableCount = 0;
        for (List<SSTableReader> level : unrepaired.manifest.generations)
            sstableCount += level.size();
        // we only have unrepaired sstables:
        assertEquals(sstableCount, cfs.getLiveSSTables().size());

        SSTableReader sstable1 = unrepaired.manifest.generations[2].get(0);
        SSTableReader sstable2 = unrepaired.manifest.generations[1].get(0);

        sstable1.descriptor.getMetadataSerializer().mutateRepaired(sstable1.descriptor, System.currentTimeMillis(), null);
        sstable1.reloadSSTableMetadata();
        assertTrue(sstable1.isRepaired());

        strategy.handleNotification(new SSTableRepairStatusChanged(Arrays.asList(sstable1)), this);

        int repairedSSTableCount = 0;
        for (List<SSTableReader> level : repaired.manifest.generations)
            repairedSSTableCount += level.size();
        assertEquals(1, repairedSSTableCount);
        // make sure the repaired sstable ends up in the same level in the repaired manifest:
        assertTrue(repaired.manifest.generations[2].contains(sstable1));
        // and that it is gone from unrepaired
        assertFalse(unrepaired.manifest.generations[2].contains(sstable1));

        unrepaired.removeSSTable(sstable2);
        strategy.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable2)), this);
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
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata, key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }

        // create 20 more sstables with 10 containing data for key1 and other 10 containing data for key2
        for (int i = 0; i < numIterations; i++) {
            for (DecoratedKey key : keys) {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata, key);
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

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs))) {
            Thread.sleep(100);
        }

        // 20 tables that have key2 should have been compacted in to 1 table resulting in 11 (30-20+1)
        assertEquals(11, cfs.getLiveSSTables().size());

        // Compact just the tables with key1. At this point all 11 tables should have key1
        Range<Token> tokenRange2 = new Range<>(key1.getToken(), key1.getToken());
        Collection<Range<Token>> tokenRanges2 = new ArrayList<>(Arrays.asList(tokenRange2));
        cfs.forceCompactionForTokenRange(tokenRanges2);


        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs))) {
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
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata, key);
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
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata, key);
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

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs)))
        {
            Thread.sleep(100);
        }

        // should all compact to 1 table
        assertEquals(1, cfs.getLiveSSTables().size());
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
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(i));
            update.newRow("column1").add("val", value).withTimestamp(0);
            update.applyUnsafe();
        }
        cfs.forceBlockingFlush();

        // Delete all but one
        for (int i = 0; i < numIterations - 1; i++) {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 1, String.valueOf(i));
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

            sstable1.descriptor.getMetadataSerializer().mutateRepaired(sstable1.descriptor, System.currentTimeMillis(), NO_PENDING_REPAIR);
            sstable1.reloadSSTableMetadata();
            compactionStrategyManager.setNewLocalCompactionStrategy(compactionStrategyManager.getCompactionParams());

            cfs.enableAutoCompaction(true);
            assertEquals(2, cfs.getLiveSSTables().size());

            // Now set the other to be repaired and it should finally compact them
            sstable2.descriptor.getMetadataSerializer().mutateRepaired(sstable2.descriptor, System.currentTimeMillis(), NO_PENDING_REPAIR);
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
        testGetScheduledCompaction(2);
    }
    @Test
    public void testGetScheduledCompactionNormalSplits() throws Exception
    {
        testGetScheduledCompaction(100);
    }

    @Test
    public void testGetScheduledCompactionManySplits() throws Exception
    {
        testGetScheduledCompaction(1000);
    }

    public void testGetScheduledCompaction(int splits) throws Exception
    {
        DatabaseDescriptor.setEnableScheduledCompactions(true);
        // hard to create leveling where there are non-single-sstable compactions to do after the first round
        DatabaseDescriptor.setSkipSingleSSTableScheduledCompactions(false);
        DatabaseDescriptor.setScheduledCompactionRangeSplits(splits);
        Token t = cfsScheduled.getPartitioner().getRandomToken();
        StorageService.instance.getTokenMetadata().updateNormalTokens(Collections.singleton(t), FBUtilities.getBroadcastAddress());

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
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) strategy.getStrategies().get(1);
        int gcBefore = cfsScheduled.gcBefore((int) (System.currentTimeMillis() / 1000));
        List<Range<Token>> compactedRanges = new ArrayList<>();
        Iterable<SSTableReader> originalSSTables = cfsScheduled.getSSTables(SSTableSet.LIVE);

        Set<SSTableReader> compactedSSTables = new HashSet<>();
        while (true)
        {
            LeveledCompactionStrategy.ScheduledLeveledCompactionTask task = lcs.getNextScheduledCompactionTask(gcBefore, false);
            if (task != null)
            {
                task.execute(new CompactionManager.CompactionExecutorStatsCollector()
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
        // NOTE; If aggressive compaction only compacts a single sstable (ie, the the boundaries only contain a single sstable)
        // the sstable will stay in its current level (because it can do so without causing overlap).
        // In this case we create the sstables so that we never get a single sstable in L2, that is why we can make this assertion:
        assertTrue(lcs.manifest.getLevel(2).size() == 0);
        StorageService.instance.getTokenMetadata().clearUnsafe();
    }

    @Test
    public void testScheduledCompactionTimeWrap() throws Exception
    {
        CompactionStrategyManager strategy = cfsScheduled.getCompactionStrategyManager();
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) strategy.getStrategies().get(1);
        lcs.resetSubrangeCompactionInfo();
        // disable to not get any scheduled compactions before we actually want them
        DatabaseDescriptor.setEnableScheduledCompactions(false);
        // hard to create leveling where there are non-single-sstable compactions to do after the first round
        DatabaseDescriptor.setSkipSingleSSTableScheduledCompactions(false);
        DatabaseDescriptor.setScheduledCompactionRangeSplits(4);
        DatabaseDescriptor.setScheduledCompactionCycleTime("4s");

        Token t = cfsScheduled.getPartitioner().getRandomToken();
        StorageService.instance.getTokenMetadata().updateNormalTokens(Collections.singleton(t), FBUtilities.getBroadcastAddress());
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
                assertTrue(lcs.timeForScheduledCompaction(false));
            if (task != null)
            {
                task.execute(new CompactionManager.CompactionExecutorStatsCollector()
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
                        assertFalse(lcs.timeForScheduledCompaction(false));
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
        testGetScheduledCompaction(100);
        Pair<Token, Long> lastSuccessPair = SystemKeyspace.getLastSuccessfulScheduledCompaction(KEYSPACE1, CF_STANDARDDLEVELED_SCHEDULED, false);

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

        List<SSTableReader> biggestBucket = LeveledManifest.getSSTablesForSTCS(cfs, options, sstables);
        assertEquals(32, biggestBucket.size()); // all 100 small sstables should be in the biggest bucket
        for (SSTableReader sstable : biggestBucket)
        {
            assertEquals(sstable.descriptor.generation, sstable.onDiskLength());
            assertTrue(sstable.onDiskLength() < 32); // did we actually get the smallest files?
        }

        // check that max compaction threshold holds:
        cfs.setMaximumCompactionThreshold(50);
        biggestBucket = LeveledManifest.getSSTablesForSTCS(cfs, options, sstables);
        assertEquals(50, biggestBucket.size()); // all 100 small sstables should be in the biggest bucket
        for (SSTableReader sstable : biggestBucket)
        {
            assertEquals(sstable.descriptor.generation, sstable.onDiskLength());
            assertTrue(sstable.onDiskLength() < 50);
        }

        DatabaseDescriptor.setCompactBiggestSTCSBucketInL0(false);
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
        List<SSTableReader> best = LeveledManifest.bestBucket(buckets, 4, 32);
        assertEquals(32, best.size());
        assertTrue(best.stream().allMatch(sstable -> sstable.onDiskLength() == 1));

        // this considers all buckets with size >= 51, the one with size=2-sstables is the best one
        best = LeveledManifest.bestBucket(buckets, 4,51);
        assertEquals(51, best.size());
        assertTrue(best.stream().allMatch(sstable -> sstable.onDiskLength() == 2));

        // this gets the largest bucket by size
        best = LeveledManifest.bestBucket(buckets, 4, 64);
        assertEquals(52, best.size());
        assertTrue(best.stream().allMatch(sstable -> sstable.onDiskLength() == 3));

        // if min threshold is large
        best = LeveledManifest.bestBucket(buckets, 55, 64);
        assertEquals(0, best.size());
    }

    private static SSTableReader sstable(ColumnFamilyStore cfs, int generation, long startToken, long endToken)
    {
        DecoratedKey first = new BufferDecoratedKey(new LongToken(startToken), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        DecoratedKey last = new BufferDecoratedKey(new LongToken(endToken), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        return MockSchema.sstable(generation, 0, false, cfs, first, last);
    }
    private static Token t(long t)
    {
        return new LongToken(t);
    }

    private void populateCfsScheduled(ColumnFamilyStore cfs) throws InterruptedException
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
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
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
