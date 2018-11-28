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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.SinglePartitionSliceCommandTest;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.SinglePartitionSliceCommandTest;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";

    public static File LEGACY_SSTABLE_ROOT;

    /**
     * When adding a new sstable version, add that one here.
     * See {@link #testGenerateSstables()} to generate sstables.
     * Take care on commit as you need to add the sstable files using {@code git add -f}
     */
    public static final String[] legacyVersions = {"me", "md", "mc", "mb", "ma", "la", "kb", "ka", "jb"};

    // 1200 chars
    static final String longString = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        Keyspace.setInitialized();
        createKeyspace();
        for (String legacyVersion : legacyVersions)
        {
            createTables(legacyVersion);
        }
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        assert scp != null;
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        assert LEGACY_SSTABLE_ROOT.isDirectory();
    }

    @After
    public void tearDown()
    {
        for (String legacyVersion : legacyVersions)
        {
            truncateTables(legacyVersion);
        }
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    private Descriptor getDescriptor(String legacyVersion, String table)
    {
        return new Descriptor(legacyVersion,
                              getTableDir(legacyVersion, table),
                              "legacy_tables",
                              table,
                              1,
                              legacyVersion.compareTo("la") >= 0 ? SSTableFormat.Type.BIG : SSTableFormat.Type.LEGACY);
    }

    @Test
    public void testLoadLegacyCqlTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();
            long startCount = CacheService.instance.keyCache.size();
            verifyReads(legacyVersion);
            verifyCache(legacyVersion, startCount);
        }
    }

    @Test
    public void testMutateMetadata() throws Exception
    {
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
        {
            // Skip 2.0.1 sstables as it doesn't have repaired information
            if (legacyVersion.equals("jb"))
                continue;
            truncateTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();

            for (ColumnFamilyStore cfs : Keyspace.open("legacy_tables").getColumnFamilyStores())
            {
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    UUID random = UUID.randomUUID();
                    sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, 1234, random);
                    sstable.reloadSSTableMetadata();

                    assertEquals(1234, sstable.getRepairedAt());
                    if (sstable.descriptor.version.hasPendingRepair())
                        assertEquals(random, sstable.getPendingRepair());
                }
            }
        }
    }

    @Test
    public void testMutateLevel() throws Exception
    {
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();

            for (ColumnFamilyStore cfs : Keyspace.open("legacy_tables").getColumnFamilyStores())
            {
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1234);
                    sstable.reloadSSTableMetadata();
                    assertEquals(1234, sstable.getSSTableLevel());
                }
            }
        }
    }

    @Test
    public void testSetAncestors() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();

            for (ColumnFamilyStore cfs : Keyspace.open("legacy_tables").getColumnFamilyStores())
            {
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    // set level and repaired info to make sure they are kept when setting ancestors:
                    sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 4);
                    sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, 44, UUID.randomUUID());
                    ((MetadataSerializer)sstable.descriptor.getMetadataSerializer()).setAncestors(sstable.descriptor, Sets.newHashSet(1,2,3));
                    // compaction metadata is deserialized only when used:
                    Map<MetadataType, MetadataComponent> metadataComponents = sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, EnumSet.allOf(MetadataType.class));

                    if (sstable.descriptor.version.hasCompactionAncestors())
                        assertEquals(Sets.newHashSet(1, 2, 3), ((CompactionMetadata) metadataComponents.get(MetadataType.COMPACTION)).ancestors);

                    sstable.descriptor.getMetadataSerializer().wipeAncestors(sstable.descriptor);
                    sstable.reloadSSTableMetadata();
                    metadataComponents = sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, EnumSet.allOf(MetadataType.class));
                    assertEquals(Collections.emptySet(), ((CompactionMetadata) metadataComponents.get(MetadataType.COMPACTION)).ancestors);
                    // make sure other metadata is kept:
                    assertEquals(4, sstable.getSSTableLevel());
                    if (sstable.descriptor.version.hasPendingRepair())
                        assertTrue(sstable.isPendingRepair());
                    if (sstable.descriptor.version.hasRepairedAt())
                        assertEquals(44, sstable.getRepairedAt());
                }
            }
        }
    }

    @Test
    public void testRefreshWipesAncestors() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateTables(legacyVersion);
            loadLegacyTables(legacyVersion);

            for (ColumnFamilyStore cfs : Keyspace.open("legacy_tables").getColumnFamilyStores())
            {
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    ((MetadataSerializer)sstable.descriptor.getMetadataSerializer()).setAncestors(sstable.descriptor, Sets.newHashSet(1,2,3));
                }
                Set<SSTableReader> toRelease = cfs.getLiveSSTables();
                cfs.clearUnsafe();
                // avoid leaking refs
                toRelease.forEach(s -> s.selfRef().release());
                cfs.loadNewSSTables();
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    CompactionMetadata cm = (CompactionMetadata) sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, MetadataType.COMPACTION);
                    assertTrue(cm.ancestors.isEmpty());
                }
            }
        }
    }

    @Test
    public void testStreamLegacyCqlTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            streamLegacyTables(legacyVersion);
            verifyReads(legacyVersion);
        }
    }
    @Test
    public void testReverseIterationOfLegacyIndexedSSTable() throws Exception
    {
        // During upgrades from 2.1 to 3.0, reverse queries can drop rows before upgradesstables is completed
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_indexed (" +
                                       "  p int," +
                                       "  c int," +
                                       "  v1 int," +
                                       "  v2 int," +
                                       "  PRIMARY KEY(p, c)" +
                                       ")");
        loadLegacyTable("legacy_%s_indexed%s", "ka", "");
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * " +
                                                             "FROM legacy_tables.legacy_ka_indexed " +
                                                             "WHERE p=1 " +
                                                             "ORDER BY c DESC");
        Assert.assertEquals(5000, rs.size());
    }

    @Test
    public void testReadingLegacyIndexedSSTableWithStaticColumns() throws Exception
    {
        // During upgrades from 2.1 to 3.0, reading from tables with static columns errors before upgradesstables
        // is completed
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_indexed_static (" +
                                       "  p int," +
                                       "  c int," +
                                       "  v1 int," +
                                       "  v2 int," +
                                       "  s1 int static," +
                                       "  s2 int static," +
                                       "  PRIMARY KEY(p, c)" +
                                       ")");
        loadLegacyTable("legacy_%s_indexed_static%s", "ka", "");
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * " +
                                                             "FROM legacy_tables.legacy_ka_indexed_static " +
                                                             "WHERE p=1 ");
        Assert.assertEquals(5000, rs.size());
    }

    @Test
    public void test14766() throws Exception
    {
        /*
         * During upgrades from 2.1 to 3.0, reading from old sstables in reverse order could omit the very last row if the
         * last indexed block had only two Unfiltered-s. See CASSANDRA-14766 for details.
         *
         * The sstable used here has two indexed blocks, with 2 cells/rows of ~500 bytes each, with column index interval of 1kb.
         * Without the fix SELECT * returns 4 rows in ASC order, but only 3 rows in DESC order, omitting the last one.
         */

        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_14766 (pk int, ck int, value text, PRIMARY KEY (pk, ck));");
        loadLegacyTable("legacy_%s_14766%s", "ka", "");

        UntypedResultSet rs;

        // read all rows in ASC order, expect all 4 to be returned
        rs = QueryProcessor.executeInternal("SELECT * FROM legacy_tables.legacy_ka_14766 WHERE pk = 0 ORDER BY ck ASC;");
        Assert.assertEquals(4, rs.size());

        // read all rows in DESC order, expect all 4 to be returned
        rs = QueryProcessor.executeInternal("SELECT * FROM legacy_tables.legacy_ka_14766 WHERE pk = 0 ORDER BY ck DESC;");
        Assert.assertEquals(4, rs.size());
    }

    @Test
    public void test14803() throws Exception
    {
        /*
         * During upgrades from 2.1 to 3.0, reading from old sstables in reverse order could return early if the sstable
         * reverse iterator encounters an indexed block that only covers a single row, and that row starts in the next
         * indexed block.
         */

        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_14803 (k int, c int, v1 blob, v2 blob, PRIMARY KEY (k, c));");
        loadLegacyTable("legacy_%s_14803%s", "ka", "");

        UntypedResultSet forward = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM legacy_tables.legacy_ka_14803 WHERE k=100"));
        UntypedResultSet reverse = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM legacy_tables.legacy_ka_14803 WHERE k=100 ORDER BY c DESC"));

        logger.info("{} - {}", forward.size(), reverse.size());
        Assert.assertFalse(forward.isEmpty());
        Assert.assertEquals(forward.size(), reverse.size());
    }

    @Test
    public void test14873() throws Exception
    {
        /*
         * When reading 2.1 sstables in 3.0 in reverse order it's possible to wrongly return an empty result set if the
         * partition being read has a static row, and the read is performed backwards.
         */

        /*
         * Contents of the SSTable (column_index_size_in_kb: 1) below:
         *
         * insert into legacy_tables.legacy_ka_14873 (pkc, sc)     values (0, 0);
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 5, '5555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555');
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 4, '4444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444');
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 3, '3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333');
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 2, '2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222');
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 1, '1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111');
         */

        String ddl =
            "CREATE TABLE legacy_tables.legacy_ka_14873 ("
            + "pkc int, cc int, sc int static, rc text, PRIMARY KEY (pkc, cc)"
            + ") WITH CLUSTERING ORDER BY (cc DESC) AND compaction = {'enabled' : 'false', 'class' : 'LeveledCompactionStrategy'};";
        QueryProcessor.executeInternal(ddl);
        loadLegacyTable("legacy_%s_14873%s", "ka", "");

        UntypedResultSet forward =
            QueryProcessor.executeOnceInternal(
                String.format("SELECT * FROM legacy_tables.legacy_ka_14873 WHERE pkc = 0 AND cc > 0 ORDER BY cc DESC;"));

        UntypedResultSet reverse =
            QueryProcessor.executeOnceInternal(
                String.format("SELECT * FROM legacy_tables.legacy_ka_14873 WHERE pkc = 0 AND cc > 0 ORDER BY cc ASC;"));

        Assert.assertEquals(5, forward.size());
        Assert.assertEquals(5, reverse.size());
    }

    @Test
    public void testMultiBlockRangeTombstones() throws Exception
    {
        /**
         * During upgrades from 2.1 to 3.0, reading old sstables in reverse order would generate invalid sequences of
         * range tombstone bounds if their range tombstones spanned multiple column index blocks. The read would fail
         * in different ways depending on whether the 2.1 tables were produced by a flush or a compaction.
         */

        String version = "ka";
        for (String tableFmt : new String[]{"legacy_%s_compacted_multi_block_rt%s", "legacy_%s_flushed_multi_block_rt%s"})
        {
            String table = String.format(tableFmt, version, "");
            QueryProcessor.executeOnceInternal(String.format("CREATE TABLE legacy_tables.%s " +
                                                             "(k int, c1 int, c2 int, v1 blob, v2 blob, " +
                                                             "PRIMARY KEY (k, c1, c2))", table));
            loadLegacyTable(tableFmt, version, "");

            UntypedResultSet forward = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM legacy_tables.%s WHERE k=100", table));
            UntypedResultSet reverse = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM legacy_tables.%s WHERE k=100 ORDER BY c1 DESC, c2 DESC", table));

            Assert.assertFalse(forward.isEmpty());
            Assert.assertEquals(table, forward.size(), reverse.size());
        }
    }


    @Test
    public void testInaccurateSSTableMinMax() throws Exception
    {
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_mc_inaccurate_min_max (k int, c1 int, c2 int, c3 int, v int, primary key (k, c1, c2, c3))");
        loadLegacyTable("legacy_%s_inaccurate_min_max%s", "mc", "");

        /*
         sstable has the following mutations:
            INSERT INTO legacy_tables.legacy_mc_inaccurate_min_max (k, c1, c2, c3, v) VALUES (100, 4, 4, 4, 4)
            DELETE FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1<3
         */

        String query = "SELECT * FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1";
        List<Unfiltered> unfiltereds = SinglePartitionSliceCommandTest.getUnfilteredsFromSinglePartition(query);
        Assert.assertEquals(2, unfiltereds.size());
        Assert.assertTrue(unfiltereds.get(0).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(0)).isOpen(false));
        Assert.assertTrue(unfiltereds.get(1).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(1)).isClose(false));
    }

    @Test
    public void testVerifyOldSSTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            loadLegacyTable("legacy_%s_simple", legacyVersion, "");

            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().checkVersion(true).build()))
                {
                    verifier.verify();
                    if (!sstable.descriptor.version.isLatestVersion())
                        fail("Verify should throw RuntimeException for old sstables "+sstable);
                }
                catch (RuntimeException e)
                {}
            }
            // make sure we don't throw any exception if not checking version:
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try (Verifier verifier = new Verifier(cfs, sstable, false, Verifier.options().checkVersion(false).build()))
                {
                    verifier.verify();
                }
                catch (Throwable e)
                {
                    fail("Verify should not throw RuntimeException for old sstables when checkVersion is false"+sstable);
                }
            }
        }
    }

    @Test
    public void testAutomaticUpgrade() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            AbstractCompactionTask act = cfs.getCompactionStrategyManager().getNextBackgroundTask(0);
            // there should be no compactions to run with auto upgrades disabled:
            assertEquals(null, act);
        }

        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            if (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
                assertTrue(cfs.metric.oldVersionSSTableCount.getValue() > 0);
            while (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
            {
                CompactionManager.instance.submitBackground(cfs);
                Thread.sleep(100);
            }
            assertTrue(cfs.metric.oldVersionSSTableCount.getValue() == 0);
        }
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
    }

    @Test
    public void test14912() throws Exception
    {
        /*
         * When reading 2.1 sstables in 3.0, collection tombstones need to be checked against
         * the dropped columns stored in table metadata. Failure to do so can result in unreadable
         * rows if a column with the same name but incompatible type has subsequently been added.
         *
         * The original (i.e. pre-any ALTER statements) table definition for this test is:
         * CREATE TABLE legacy_tables.legacy_ka_14912 (k int PRIMARY KEY, v1 set<text>, v2 text);
         *
         * The SSTable loaded emulates data being written before the table is ALTERed and contains:
         *
         * insert into legacy_tables.legacy_ka_14912 (k, v1, v2) values (0, {}, 'abc') USING TIMESTAMP 1543244999672280;
         * insert into legacy_tables.legacy_ka_14912 (k, v1, v2) values (1, {'abc'}, 'abc') USING TIMESTAMP 1543244999672280;
         *
         * The timestamps of the (generated) collection tombstones are 1543244999672279, e.g. the <TIMESTAMP of the mutation> - 1
         */

        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_14912 (k int PRIMARY KEY, v1 text, v2 text)");
        loadLegacyTable("legacy_%s_14912%s", "ka", "");
        CFMetaData cfm = Keyspace.open("legacy_tables").getColumnFamilyStore("legacy_ka_14912").metadata;
        ColumnDefinition columnToDrop;

        /*
         * This first variant simulates the original v1 set<text> column being dropped
         * then re-added with the text type:
         * CREATE TABLE legacy_tables.legacy_ka_14912 (k int PRIMARY KEY, v1 set<text>, v2 text);
         * INSERT INTO legacy_tables.legacy)ka_14912 (k, v1, v2)...
         * ALTER TABLE legacy_tables.legacy_ka_14912 DROP v1;
         * ALTER TABLE legacy_tables.legacy_ka_14912 ADD v1 text;
         */
        columnToDrop = ColumnDefinition.regularDef(cfm,
                                                   UTF8Type.instance.fromString("v1"),
                                                   SetType.getInstance(UTF8Type.instance, true));
        cfm.recordColumnDrop(columnToDrop, 1543244999700000L);
        assertExpectedRowsWithDroppedCollection(true);
        // repeat the query, but simulate clock drift by shifting the recorded
        // drop time forward so that it occurs before the collection timestamp
        cfm.recordColumnDrop(columnToDrop, 1543244999600000L);
        assertExpectedRowsWithDroppedCollection(false);

        /*
         * This second test simulates the original v1 set<text> column being dropped
         * then re-added with some other, non-collection type (overwriting the dropped
         * columns record), then dropping and re-adding again as text type:
         * CREATE TABLE legacy_tables.legacy_ka_14912 (k int PRIMARY KEY, v1 set<text>, v2 text);
         * INSERT INTO legacy_tables.legacy_ka_14912 (k, v1, v2)...
         * ALTER TABLE legacy_tables.legacy_ka_14912 DROP v1;
         * ALTER TABLE legacy_tables.legacy_ka_14912 ADD v1 blob;
         * ALTER TABLE legacy_tables.legacy_ka_14912 DROP v1;
         * ALTER TABLE legacy_tables.legacy_ka_14912 ADD v1 text;
         */
        columnToDrop = ColumnDefinition.regularDef(cfm,
                                                   UTF8Type.instance.fromString("v1"),
                                                   BytesType.instance);
        cfm.recordColumnDrop(columnToDrop, 1543244999700000L);
        assertExpectedRowsWithDroppedCollection(true);
        // repeat the query, but simulate clock drift by shifting the recorded
        // drop time forward so that it occurs before the collection timestamp
        cfm.recordColumnDrop(columnToDrop, 1543244999600000L);
        assertExpectedRowsWithDroppedCollection(false);
    }

    private void assertExpectedRowsWithDroppedCollection(boolean droppedCheckSuccessful)
    {
        for (int i=0; i<=1; i++)
        {
            UntypedResultSet rows =
                QueryProcessor.executeOnceInternal(
                    String.format("SELECT * FROM legacy_tables.legacy_ka_14912 WHERE k = %s;", i));
            Assert.assertEquals(1, rows.size());
            UntypedResultSet.Row row = rows.one();

            // If the best-effort attempt to filter dropped columns was successful, then the row
            // should not contain the v1 column at all. Likewise, if no column data was written,
            // only a tombstone, then no v1 column should be present.
            // However, if collection data was written (i.e. where k=1), then if the dropped column
            // check didn't filter the legacy cells, we should expect an empty column value as the
            // legacy collection tombstone won't cover it and the dropped column check doesn't filter
            // it.
            if (droppedCheckSuccessful || i == 0)
                Assert.assertFalse(row.has("v1"));
            else
                Assert.assertEquals("", row.getString("v1"));

            Assert.assertEquals("abc", row.getString("v2"));
        }
    }

    private void streamLegacyTables(String legacyVersion) throws Exception
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            logger.info("Streaming legacy version {}{}", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact));
        }
    }

    private void streamLegacyTable(String tablePattern, String legacyVersion, String compactNameSuffix) throws Exception
    {
        String table = String.format(tablePattern, legacyVersion, compactNameSuffix);
        SSTableReader sstable = SSTableReader.open(getDescriptor(legacyVersion, table));
        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));
        ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        details.add(new StreamSession.SSTableStreamingSections(sstable.ref(),
                                                               sstable.getPositionsForRanges(ranges),
                                                               sstable.estimatedKeysForRanges(ranges)));
        new StreamPlan("LegacyStreamingTest").transferFiles(FBUtilities.getBroadcastAddress(), details)
                                             .execute().get();
    }

    private static void loadLegacyTables(String legacyVersion) throws Exception
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            logger.info("Preparing legacy version {}{}", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact));
        }
    }

    private static void verifyCache(String legacyVersion, long startCount) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        //For https://issues.apache.org/jira/browse/CASSANDRA-10778
        //Validate whether the key cache successfully saves in the presence of old keys as
        //well as loads the correct number of keys
        long endCount = CacheService.instance.keyCache.size();
        Assert.assertTrue(endCount > startCount);
        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.invalidateKeyCache();
        Assert.assertEquals(startCount, CacheService.instance.keyCache.size());
        CacheService.instance.keyCache.loadSaved();
        if (BigFormat.instance.getVersion(legacyVersion).storeRows())
            Assert.assertEquals(endCount, CacheService.instance.keyCache.size());
        else
            Assert.assertEquals(startCount, CacheService.instance.keyCache.size());
    }

    private static void verifyReads(String legacyVersion)
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            for (int ck = 0; ck < 50; ck++)
            {
                String ckValue = Integer.toString(ck) + longString;
                for (int pk = 0; pk < 5; pk++)
                {
                    logger.debug("for pk={} ck={}", pk, ck);

                    String pkValue = Integer.toString(pk);
                    UntypedResultSet rs;
                    if (ck == 0)
                    {
                        readSimpleTable(legacyVersion, getCompactNameSuffix(compact),  pkValue);
                        readSimpleCounterTable(legacyVersion, getCompactNameSuffix(compact), pkValue);
                    }

                    readClusteringTable(legacyVersion, getCompactNameSuffix(compact), ck, ckValue, pkValue);
                    readClusteringCounterTable(legacyVersion, getCompactNameSuffix(compact), ckValue, pkValue);
                }
            }
        }
    }

    private static void readClusteringCounterTable(String legacyVersion, String compactSuffix, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust_counter{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust_counter%s WHERE pk=? AND ck=?", legacyVersion, compactSuffix), pkValue, ckValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readClusteringTable(String legacyVersion, String compactSuffix, int ck, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust%s WHERE pk=? AND ck=?", legacyVersion, compactSuffix), pkValue, ckValue);
        assertLegacyClustRows(1, rs);

        String ckValue2 = Integer.toString(ck < 10 ? 40 : ck - 1) + longString;
        String ckValue3 = Integer.toString(ck > 39 ? 10 : ck + 1) + longString;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust%s WHERE pk=? AND ck IN (?, ?, ?)", legacyVersion, compactSuffix), pkValue, ckValue, ckValue2, ckValue3);
        assertLegacyClustRows(3, rs);
    }

    private static void readSimpleCounterTable(String legacyVersion, String compactSuffix, String pkValue)
    {
        logger.debug("Read legacy_{}_simple_counter{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple_counter%s WHERE pk=?", legacyVersion, compactSuffix), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readSimpleTable(String legacyVersion, String compactSuffix, String pkValue)
    {
        logger.debug("Read simple: legacy_{}_simple{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple%s WHERE pk=?", legacyVersion, compactSuffix), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("foo bar baz", rs.one().getString("val"));
    }

    private static void createKeyspace()
    {
        QueryProcessor.executeInternal("CREATE KEYSPACE legacy_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    }

    private static void createTables(String legacyVersion)
    {
        for (int i=0; i<=1; i++)
        {
            String compactSuffix = getCompactNameSuffix(i);
            String tableSuffix = i == 0? "" : " WITH COMPACT STORAGE";
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple%s (pk text PRIMARY KEY, val text)%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter%s (pk text PRIMARY KEY, val counter)%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust%s (pk text, ck text, val text, PRIMARY KEY (pk, ck))%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter%s (pk text, ck text, val counter, PRIMARY KEY (pk, ck))%s", legacyVersion, compactSuffix, tableSuffix));
        }
    }

    private static String getCompactNameSuffix(int i)
    {
        return i == 0? "" : "_compact";
    }

    private static void truncateTables(String legacyVersion)
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact)));
        }
        CacheService.instance.invalidateCounterCache();
        CacheService.instance.invalidateKeyCache();
    }

    private static void assertLegacyClustRows(int count, UntypedResultSet rs)
    {
        Assert.assertNotNull(rs);
        Assert.assertEquals(count, rs.size());
        for (int i = 0; i < count; i++)
        {
            for (UntypedResultSet.Row r : rs)
            {
                Assert.assertEquals(128, r.getString("val").length());
            }
        }
    }

    private static void loadLegacyTable(String tablePattern, String legacyVersion, String compactSuffix) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion, compactSuffix);

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            copySstablesToTestData(legacyVersion, table, cfDir);
        }

        cfs.loadNewSSTables();
    }

    /**
     * Generates sstables for 8 CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#getVersion() BigFormat.latestVersion.getVersion()}.
     * <p>
     * Run this test alone (e.g. from your IDE) when a new version is introduced or format changed
     * during development. I.e. remove the {@code @Ignore} annotation temporarily.
     * </p>
     */
    @Ignore
    @Test
    public void testGenerateSstables() throws Throwable
    {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 128; i++)
        {
            sb.append((char)('a' + rand.nextInt(26)));
        }
        String randomString = sb.toString();

        for (int compact = 0; compact <= 1; compact++)
        {
            for (int pk = 0; pk < 5; pk++)
            {
                String valPk = Integer.toString(pk);
                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_simple%s (pk, val) VALUES ('%s', '%s')",
                                                             BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, "foo bar baz"));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter%s SET val = val + 1 WHERE pk = '%s'",
                                                             BigFormat.latestVersion, getCompactNameSuffix(compact), valPk));

                for (int ck = 0; ck < 50; ck++)
                {
                    String valCk = Integer.toString(ck);

                    QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust%s (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                                 BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, valCk + longString, randomString));

                    QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter%s SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                                 BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, valCk + longString));

                }
            }
        }

        StorageService.instance.forceKeyspaceFlush("legacy_tables");

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", BigFormat.latestVersion));
        ksDir.mkdirs();
        for (int compact = 0; compact <= 1; compact++)
        {
            copySstablesFromTestData(String.format("legacy_%s_simple%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_simple_counter%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_clust%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_clust_counter%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
        }
    }

    public static void copySstablesFromTestData(String table, File ksDir) throws IOException
    {
        File cfDir = new File(ksDir, table);
        cfDir.mkdir();

        for (File srcDir : Keyspace.open("legacy_tables").getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File file : srcDir.listFiles())
            {
                copyFile(cfDir, file);
            }
        }
    }

    private static void copySstablesToTestData(String legacyVersion, String table, File cfDir) throws IOException
    {
        for (File file : getTableDir(legacyVersion, table).listFiles())
        {
            copyFile(cfDir, file);
        }
    }

    private static File getTableDir(String legacyVersion, String table)
    {
        return new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables/%s", legacyVersion, table));
    }

    private static void copyFile(File cfDir, File file) throws IOException
    {
        byte[] buf = new byte[65536];
        if (file.isFile())
        {
            File target = new File(cfDir, file.getName());
            int rd;
            FileInputStream is = new FileInputStream(file);
            FileOutputStream os = new FileOutputStream(target);
            while ((rd = is.read(buf)) >= 0)
                os.write(buf, 0, rd);
        }
    }
}
