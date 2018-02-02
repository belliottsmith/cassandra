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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionsCQLTest extends CQLTester
{

    public static final int SLEEP_TIME = 5000;

    @Test
    public void testTriggerMinorCompactionSTCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerMinorCompactionLCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':1};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }


    @Test
    public void testTriggerMinorCompactionDTCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'DateTieredCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1') using timestamp 1000"); // same timestamp = same window = minor compaction triggered
        flush();
        execute("insert into %s (id) values ('1') using timestamp 1000");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerMinorCompactionTWCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }


    @Test
    public void testTriggerNoMinorCompactionSTCSDisabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerMinorCompactionSTCSNodetoolEnabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        getCurrentColumnFamilyStore().enableAutoCompaction();
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerNoMinorCompactionSTCSNodetoolDisabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':true};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        getCurrentColumnFamilyStore().disableAutoCompaction();
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerNoMinorCompactionSTCSAlterTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':true};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("ALTER TABLE %s WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerMinorCompactionSTCSAlterTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("ALTER TABLE %s WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'enabled': true}");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testSetLocalCompactionStrategy() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), DateTieredCompactionStrategy.class));
        // altering something non-compaction related
        execute("ALTER TABLE %s WITH gc_grace_seconds = 1000");
        // should keep the local compaction strat
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), DateTieredCompactionStrategy.class));
        // altering a compaction option
        execute("ALTER TABLE %s WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':3}");
        // will use the new option
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), SizeTieredCompactionStrategy.class));
    }


    @Test
    public void testSetLocalCompactionStrategyDisable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");
        localOptions.put("enabled", "false");
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        localOptions.clear();
        localOptions.put("class", "DateTieredCompactionStrategy");
        // localOptions.put("enabled", "true"); - this is default!
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
    }


    @Test
    public void testSetLocalCompactionStrategyEnable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");

        getCurrentColumnFamilyStore().disableAutoCompaction();
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());

        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
    }



    @Test(expected = IllegalArgumentException.class)
    public void testBadLocalCompactionStrategyOptions()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class","SizeTieredCompactionStrategy");
        localOptions.put("sstable_size_in_mb","1234"); // not for STCS
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
    }

    /**
     * Christmaspatch test
     *
     * Makes sure we purge tombstones which got 'repaired' before the compaction started
     */
    @Test
    public void dropTombstoneEarlyRepairXmas() throws Throwable
    {
        delayCompactionStartHelper(true);
    }

    /**
     * Christmaspatch test
     *
     * Makes sure we don't purge tombstones which got 'repaired' after the compaction started
     */
    @Test
    public void dontDropTombstoneLateRepairXmas() throws Throwable
    {
        delayCompactionStartHelper(false);
    }

    public void delayCompactionStartHelper(boolean earlyRepair) throws Throwable
    {
        DatabaseDescriptor.setChristmasPatchEnabled(true);
        execute("alter keyspace "+keyspace()+" with replication =  {'class': 'NetworkTopologyStrategy', 'datacenter1': '2'}");
        createTable("CREATE TABLE %s (id int PRIMARY KEY, b text) with gc_grace_seconds=0");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Range<Token> fullRange = new Range<>(cfs.getPartitioner().getMinimumToken(), cfs.getPartitioner().getMinimumToken());

        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (id, b) VALUES (1, 'abc')");
        cfs.forceBlockingFlush();
        execute("DELETE b FROM %s WHERE id=1");
        cfs.forceBlockingFlush();
        Thread.sleep(2000);
        if (earlyRepair)
            cfs.updateLastSuccessfulRepair(fullRange, System.currentTimeMillis());

        ExecutorService es = Executors.newFixedThreadPool(1);
        assertEquals(2, cfs.getLiveSSTables().size());
        CompactionTask t = (CompactionTask)cfs.getCompactionStrategyManager().getUserDefinedTask(cfs.getLiveSSTables(), cfs.gcBefore(FBUtilities.nowInSeconds()));
        MockCompactionTask mock = new MockCompactionTask(t);
        Future<?> f = es.submit(mock);
        Thread.sleep(1000); // sleep to give the compaction task time to snapshot the repair history
        if (!earlyRepair) // repair completion after compaction started - it should not drop any tombstones
            cfs.updateLastSuccessfulRepair(fullRange, System.currentTimeMillis());
        mock.latch.countDown();
        FBUtilities.waitOnFuture(f);
        assertFalse(execute("SELECT * FROM %s WHERE id = 1").one().has("b"));
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        boolean foundDeletedCell = false;
        try(ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    while (iter.hasNext())
                    {
                        Unfiltered unfiltered = iter.next();
                        assertTrue(unfiltered instanceof Row);
                        for (Cell c : ((Row)unfiltered).cells())
                        {
                            foundDeletedCell = c.isTombstone();
                        }

                    }
                }
            }
        }
        assertEquals(!earlyRepair, foundDeletedCell);
        DatabaseDescriptor.setChristmasPatchEnabled(false);
    }

    @Test
    public void testPerCFSNeverPurgeTombstonesCell() throws Throwable
    {
        testPerCFSNeverPurgeTombstonesHelper(true);
    }

    @Test
    public void testPerCFSNeverPurgeTombstones() throws Throwable
    {
        testPerCFSNeverPurgeTombstonesHelper(false);
    }


    public void testPerCFSNeverPurgeTombstonesHelper(boolean deletedCell) throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, b text) with gc_grace_seconds = 0");
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (id, b) VALUES (?, ?)", i, String.valueOf(i));
        }
        getCurrentColumnFamilyStore().forceBlockingFlush();

        assertTombstones(getCurrentColumnFamilyStore().getLiveSSTables().iterator().next(), false);
        if (deletedCell)
            execute("UPDATE %s SET b=null WHERE id = ?", 50);
        else
            execute("DELETE FROM %s WHERE id = ?", 50);
        getCurrentColumnFamilyStore().setNeverPurgeTombstones(false);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Thread.sleep(2000); // wait for gcgs to pass
        getCurrentColumnFamilyStore().forceMajorCompaction();
        assertTombstones(getCurrentColumnFamilyStore().getLiveSSTables().iterator().next(), false);
        if (deletedCell)
            execute("UPDATE %s SET b=null WHERE id = ?", 44);
        else
            execute("DELETE FROM %s WHERE id = ?", 44);
        getCurrentColumnFamilyStore().setNeverPurgeTombstones(true);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Thread.sleep(1100);
        getCurrentColumnFamilyStore().forceMajorCompaction();
        assertTombstones(getCurrentColumnFamilyStore().getLiveSSTables().iterator().next(), true);
        // disable it again and make sure the tombstone is gone:
        getCurrentColumnFamilyStore().setNeverPurgeTombstones(false);
        getCurrentColumnFamilyStore().forceMajorCompaction();
        assertTombstones(getCurrentColumnFamilyStore().getLiveSSTables().iterator().next(), false);
        getCurrentColumnFamilyStore().truncateBlocking();
    }

    private void assertTombstones(SSTableReader sstable, boolean expectTS)
    {
        boolean foundTombstone = false;
        try(ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    if (!iter.partitionLevelDeletion().isLive())
                        foundTombstone = true;
                    while (iter.hasNext())
                    {
                        Unfiltered unfiltered = iter.next();
                        assertTrue(unfiltered instanceof Row);
                        for (Cell c : ((Row)unfiltered).cells())
                        {
                            if (c.isTombstone())
                                foundTombstone = true;
                        }

                    }
                }
            }
        }
        assertEquals(expectTS, foundTombstone);
    }


    private static class MockCompactionTask extends CompactionTask
    {
        private final CompactionTask task;
        public final CountDownLatch latch = new CountDownLatch(1);
        public MockCompactionTask(CompactionTask t)
        {
            super(t.cfs, t.transaction, t.gcBefore);
            this.task = t;
        }

        @Override
        protected CompactionController getCompactionController(Set<SSTableReader> sstables)
        {
            CompactionController controller = task.getCompactionController(sstables);
            try
            {
                latch.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return controller;
        }
    }

    public boolean verifyStrategies(CompactionStrategyManager manager, Class<? extends AbstractCompactionStrategy> expected)
    {
        boolean found = false;
        for (AbstractCompactionStrategy actualStrategy : manager.getStrategies())
        {
            if (!actualStrategy.getClass().equals(expected))
                return false;
            found = true;
        }
        return found;
    }

    private void waitForMinor(String keyspace, String cf, long maxWaitTime, boolean shouldFind) throws Throwable
    {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < maxWaitTime)
        {
            UntypedResultSet res = execute("SELECT * FROM system.compaction_history");
            for (UntypedResultSet.Row r : res)
            {
                if (r.getString("keyspace_name").equals(keyspace) && r.getString("columnfamily_name").equals(cf))
                    if (shouldFind)
                        return;
                    else
                        fail("Found minor compaction");
            }
            Thread.sleep(100);
        }
        if (shouldFind)
            fail("No minor compaction triggered in "+maxWaitTime+"ms");
    }
}
