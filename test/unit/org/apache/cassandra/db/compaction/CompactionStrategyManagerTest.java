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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CompactionStrategyManagerTest
{
    private static final String KS_PREFIX = "Keyspace1";
    private static final String TABLE_PREFIX = "CF_STANDARD";

    private static IPartitioner originalPartitioner;
    private static boolean backups;

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        backups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        /**
         * We use byte ordered partitioner in this test to be able to easily infer an SSTable
         * disk assignment based on its generation - See {@link this#getSSTableIndex(Integer[], SSTableReader)}
         */
        originalPartitioner = StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        SchemaLoader.createKeyspace(KS_PREFIX,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS_PREFIX, TABLE_PREFIX)
                                                .compaction(CompactionParams.scts(Collections.emptyMap())));
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setPartitionerUnsafe(originalPartitioner);
        DatabaseDescriptor.setIncrementalBackupsEnabled(backups);
    }

    @Test
    public void testAutomaticUpgradeConcurrency() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(1);

        // latch to block CompactionManager.BackgroundCompactionCandidate#maybeRunUpgradeTask
        // inside the currentlyBackgroundUpgrading check - with max_concurrent_auto_upgrade_tasks = 1 this will make
        // sure that BackgroundCompactionCandidate#maybeRunUpgradeTask returns false until the latch has been counted down
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger upgradeTaskCount = new AtomicInteger(0);
        MockCFSForCSM mock = new MockCFSForCSM(cfs, latch, upgradeTaskCount);

        CompactionManager.BackgroundCompactionCandidate r = CompactionManager.instance.getBackgroundCompactionCandidate(mock);
        CompactionStrategyManager mgr = mock.getCompactionStrategyManager();
        // basic idea is that we start a thread which will be able to get in to the currentlyBackgroundUpgrading-guarded
        // code in CompactionManager, then we try to run a bunch more of the upgrade tasks which should return false
        // due to the currentlyBackgroundUpgrading count being >= max_concurrent_auto_upgrade_tasks
        Thread t = new Thread(() -> r.maybeRunUpgradeTask(mgr));
        t.start();
        Thread.sleep(100); // let the thread start and grab the task
        assertEquals(1, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        assertFalse(r.maybeRunUpgradeTask(mgr));
        assertFalse(r.maybeRunUpgradeTask(mgr));
        latch.countDown();
        t.join();
        assertEquals(1, upgradeTaskCount.get()); // we should only call findUpgradeSSTableTask once when concurrency = 1
        assertEquals(0, CompactionManager.instance.currentlyBackgroundUpgrading.get());

        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
    }

    @Test
    public void testAutomaticUpgradeConcurrency2() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(2);
        // latch to block CompactionManager.BackgroundCompactionCandidate#maybeRunUpgradeTask
        // inside the currentlyBackgroundUpgrading check - with max_concurrent_auto_upgrade_tasks = 1 this will make
        // sure that BackgroundCompactionCandidate#maybeRunUpgradeTask returns false until the latch has been counted down
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger upgradeTaskCount = new AtomicInteger();
        MockCFSForCSM mock = new MockCFSForCSM(cfs, latch, upgradeTaskCount);

        CompactionManager.BackgroundCompactionCandidate r = CompactionManager.instance.getBackgroundCompactionCandidate(mock);
        CompactionStrategyManager mgr = mock.getCompactionStrategyManager();

        // basic idea is that we start 2 threads who will be able to get in to the currentlyBackgroundUpgrading-guarded
        // code in CompactionManager, then we try to run a bunch more of the upgrade task which should return false
        // due to the currentlyBackgroundUpgrading count being >= max_concurrent_auto_upgrade_tasks
        Thread t = new Thread(() -> r.maybeRunUpgradeTask(mgr));
        t.start();
        Thread t2 = new Thread(() -> r.maybeRunUpgradeTask(mgr));
        t2.start();
        Thread.sleep(100); // let the threads start and grab the task
        assertEquals(2, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        assertFalse(r.maybeRunUpgradeTask(mgr));
        assertFalse(r.maybeRunUpgradeTask(mgr));
        assertFalse(r.maybeRunUpgradeTask(mgr));
        assertEquals(2, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        latch.countDown();
        t.join();
        t2.join();
        assertEquals(2, upgradeTaskCount.get());
        assertEquals(0, CompactionManager.instance.currentlyBackgroundUpgrading.get());

        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(1);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
    }

    @Test
    public void testCountsByBuckets()
    {
        Assert.assertArrayEquals(
            new int[] {2, 2, 4},
            CompactionStrategyManager.sumCountsByBucket(
                ImmutableMap.of(60000L, 1, 0L, 2, 180000L, 1),
                ImmutableMap.of(60000L, 1, 0L, 2, 180000L, 1), 30));
        Assert.assertArrayEquals(
            new int[] {1, 1, 3},
            CompactionStrategyManager.sumCountsByBucket(
                ImmutableMap.of(60000L, 1, 0L, 1),
                ImmutableMap.of(0L, 2, 180000L, 1), 30));
        Assert.assertArrayEquals(
            new int[] {1, 1},
            CompactionStrategyManager.sumCountsByBucket(
                ImmutableMap.of(60000L, 1, 0L, 1),
                ImmutableMap.of(), 30));
        Assert.assertArrayEquals(
            new int[] {8, 4},
            CompactionStrategyManager.sumCountsByBucket(
                ImmutableMap.of(60000L, 2, 0L, 1, 180000L, 4),
                ImmutableMap.of(60000L, 2, 0L, 1, 180000L, 4), 2));
        Assert.assertArrayEquals(
            new int[] {1, 1, 2},
            CompactionStrategyManager.sumCountsByBucket(
                Collections.emptyMap(),
                ImmutableMap.of(60000L, 1, 0L, 2, 180000L, 1), 30));
        Assert.assertArrayEquals(
            new int[] {},
            CompactionStrategyManager.sumCountsByBucket(
                Collections.emptyMap(),
                Collections.emptyMap(), 30));
    }

    private static class MockCFSForCSM extends ColumnFamilyStore
    {
        private final CountDownLatch latch;
        private final AtomicInteger upgradeTaskCount;

        private MockCFSForCSM(ColumnFamilyStore cfs, CountDownLatch latch, AtomicInteger upgradeTaskCount)
        {
            super(cfs.keyspace, cfs.name, 10, cfs.metadata, cfs.getDirectories(), true, false);
            this.latch = latch;
            this.upgradeTaskCount = upgradeTaskCount;
        }
        @Override
        public CompactionStrategyManager getCompactionStrategyManager()
        {
            return new MockCSM(this, latch, upgradeTaskCount);
        }
    }

    private static class MockCSM extends CompactionStrategyManager
    {
        private final CountDownLatch latch;
        private final AtomicInteger upgradeTaskCount;

        private MockCSM(ColumnFamilyStore cfs, CountDownLatch latch, AtomicInteger upgradeTaskCount)
        {
            super(cfs);
            this.latch = latch;
            this.upgradeTaskCount = upgradeTaskCount;
        }

        @Override
        public AbstractCompactionTask findUpgradeSSTableTask()
        {
            try
            {
                latch.await();
                upgradeTaskCount.incrementAndGet();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return null;
        }
    }
}
