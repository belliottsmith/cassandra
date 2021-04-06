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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.getWindowBoundsInMillis;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.newestBucket;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.validateOptions;

public class TimeWindowCompactionStrategyTest extends SchemaLoader
{
    public static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");
        DatabaseDescriptor.setAllowUnsafeAggressiveSSTableExpiration(true);

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        Map<String, String> unvalidated = validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "0");
            validateOptions(options);
            fail(String.format("%s == 0 should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY));
        }
        catch (ConfigurationException e) {}

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "-1337");
            validateOptions(options);
            fail(String.format("Negative %s should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MONTHS");
            validateOptions(options);
            fail(String.format("Invalid %s should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, "not-a-boolean");
            validateOptions(options);
            fail(String.format("Invalid %s should be rejected", TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, "true");
        }

        options.put("bad_option", "1.0");
        unvalidated = validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }


    @Test
    public void testTimeWindows()
    {
        Long tstamp1 = 1451001601000L; // 2015-12-25 @ 00:00:01, in milliseconds
        Long tstamp2 = 1451088001000L; // 2015-12-26 @ 00:00:01, in milliseconds
        Long lowHour = 1451001600000L; // 2015-12-25 @ 00:00:00, in milliseconds

        // A 1 hour window should round down to the beginning of the hour
        assertTrue(getWindowBoundsInMillis(TimeUnit.HOURS, 1, tstamp1).left.compareTo(lowHour) == 0);

        // A 1 minute window should round down to the beginning of the hour
        assertTrue(getWindowBoundsInMillis(TimeUnit.MINUTES, 1, tstamp1).left.compareTo(lowHour) == 0);

        // A 1 day window should round down to the beginning of the hour
        assertTrue(getWindowBoundsInMillis(TimeUnit.DAYS, 1, tstamp1).left.compareTo(lowHour) == 0 );

        // The 2 day window of 2015-12-25 + 2015-12-26 should round down to the beginning of 2015-12-25
        assertTrue(getWindowBoundsInMillis(TimeUnit.DAYS, 2, tstamp2).left.compareTo(lowHour) == 0);


        return;
    }

    @Test
    public void testPrepBucket()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        Long tstamp = System.currentTimeMillis();
        Long tstamp2 =  tstamp - (2L * 3600L * 1000L);

        // create 5 sstables
        for (int r = 0; r < 3; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            new RowUpdateBuilder(cfs.metadata, r, key.getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();

            cfs.forceBlockingFlush();
        }
        // Decrement the timestamp to simulate a timestamp in the past hour
        for (int r = 3; r < 5; r++)
        {
            // And add progressively more cells into each sstable
            DecoratedKey key = Util.dk(String.valueOf(r));
            new RowUpdateBuilder(cfs.metadata, r, key.getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();
            cfs.forceBlockingFlush();
        }

        cfs.forceBlockingFlush();

        HashMultimap<Long, SSTableReader> buckets = HashMultimap.create();
        List<SSTableReader> sstrs = new ArrayList<>(cfs.getLiveSSTables());

        // We'll put 3 sstables into the newest bucket
        for (int i = 0 ; i < 3; i++)
        {
            Pair<Long,Long> bounds = getWindowBoundsInMillis(TimeUnit.HOURS, 1, tstamp );
            buckets.put(bounds.left, sstrs.get(i));
        }
        List<SSTableReader> newBucket = newestBucket(buckets, 4, 32, TimeUnit.HOURS, 1, new SizeTieredCompactionStrategyOptions(), getWindowBoundsInMillis(TimeUnit.HOURS, 1, System.currentTimeMillis()).left );
        assertTrue("incoming bucket should not be accepted when it has below the min threshold SSTables", newBucket.isEmpty());

        newBucket = newestBucket(buckets, 2, 32, TimeUnit.HOURS, 1, new SizeTieredCompactionStrategyOptions(), getWindowBoundsInMillis(TimeUnit.HOURS, 1, System.currentTimeMillis()).left);
        assertTrue("incoming bucket should be accepted when it is larger than the min threshold SSTables", !newBucket.isEmpty());

        // And 2 into the second bucket (1 hour back)
        for (int i = 3 ; i < 5; i++)
        {
            Pair<Long,Long> bounds = getWindowBoundsInMillis(TimeUnit.HOURS, 1, tstamp2 );
            buckets.put(bounds.left, sstrs.get(i));
        }

        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(0).getMinTimestamp(), sstrs.get(0).getMaxTimestamp());
        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(1).getMinTimestamp(), sstrs.get(1).getMaxTimestamp());
        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(2).getMinTimestamp(), sstrs.get(2).getMaxTimestamp());

        // Test trim
        int numSSTables = 40;
        for (int r = 5; r < numSSTables; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            for(int i = 0 ; i < r ; i++)
            {
                new RowUpdateBuilder(cfs.metadata, tstamp + r, key.getKey())
                    .clustering("column")
                    .add("val", value).build().applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }

        // Reset the buckets, overfill it now
        sstrs = new ArrayList<>(cfs.getLiveSSTables());
        for (int i = 0 ; i < 40; i++)
        {
            Pair<Long,Long> bounds = getWindowBoundsInMillis(TimeUnit.HOURS, 1, sstrs.get(i).getMaxTimestamp());
            buckets.put(bounds.left, sstrs.get(i));
        }
        DatabaseDescriptor.setBiggestBucketMaxSSTableCount(34);
        newBucket = newestBucket(buckets, 4, 32, TimeUnit.DAYS, 1, new SizeTieredCompactionStrategyOptions(), getWindowBoundsInMillis(TimeUnit.HOURS, 1, System.currentTimeMillis()).left);
        assertEquals("new bucket should be trimmed to max sstable count of 34", newBucket.size(),  34);
    }


    @Test
    public void testDropExpiredSSTables() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 2 sstables
        DecoratedKey key = Util.dk("expired");
        new RowUpdateBuilder(cfs.metadata, 0L, 1, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush();
        SSTableReader expiredSSTable = cfs.getLiveSSTables().iterator().next();

        key = Util.dk("nonexpired");
        new RowUpdateBuilder(cfs.metadata, 10, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush();
        assertEquals(cfs.getLiveSSTables().size(), 2);

        Map<String, String> options = new HashMap<>();

        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "SECONDS");
        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0");
        TimeWindowCompactionStrategy twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);
        twcs.startup();
        assertNull(twcs.getNextBackgroundTask(0));
        Thread.sleep(100); // we need to make sure we pass EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY
        AbstractCompactionTask t = twcs.getNextBackgroundTask(Integer.MAX_VALUE);
        assertNotNull(t);
        assertEquals(1, Iterables.size(t.transaction.originals()));
        SSTableReader sstable = t.transaction.originals().iterator().next();
        assertEquals(sstable, expiredSSTable);
        t.transaction.abort();
    }

    @Test
    public void testDropOverlappingExpiredSSTables() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 2 sstables
        DecoratedKey key = Util.dk(String.valueOf("expired"));
        new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis(), 1, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush();
        SSTableReader expiredSSTable = cfs.getLiveSSTables().iterator().next();
        Thread.sleep(10);

        new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis() - 1000, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();
        key = Util.dk(String.valueOf("nonexpired"));
        new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis(), key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush();
        assertEquals(cfs.getLiveSSTables().size(), 2);

        Map<String, String> options = new HashMap<>();

        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "SECONDS");
        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0");
        TimeWindowCompactionStrategy twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);

        twcs.startup();
        assertNull(twcs.getNextBackgroundTask((int) (System.currentTimeMillis() / 1000)));
        Thread.sleep(2000);
        assertNull(twcs.getNextBackgroundTask((int) (System.currentTimeMillis()/1000)));

        options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, "true");
        twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);

        twcs.startup();
        AbstractCompactionTask t = twcs.getNextBackgroundTask((int) (System.currentTimeMillis()/1000));
        assertNotNull(t);
        assertEquals(1, Iterables.size(t.transaction.originals()));
        SSTableReader sstable = t.transaction.originals().iterator().next();
        assertEquals(sstable, expiredSSTable);
        twcs.shutdown();
        t.transaction.abort();
    }

    @Test
    public void testBucketCount() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create some sstables
        DecoratedKey key = Util.dk("nonexpired");
        new RowUpdateBuilder(cfs.metadata, 1000 * 1, key.getKey())
        .clustering("column").add("val", value).build().applyUnsafe();
        cfs.forceBlockingFlush();

        key = Util.dk("nonexpired");
        new RowUpdateBuilder(cfs.metadata, 1000 * 10, key.getKey())
        .clustering("column").add("val", value).build().applyUnsafe();
        cfs.forceBlockingFlush();

        key = Util.dk("nonexpired");
        new RowUpdateBuilder(cfs.metadata, 1000 * 90, key.getKey())
        .clustering("column").add("val", value).build().applyUnsafe();
        cfs.forceBlockingFlush();

        key = Util.dk("nonexpired");
        new RowUpdateBuilder(cfs.metadata, 1000 * 180, key.getKey())
        .clustering("column").add("val", value).build().applyUnsafe();
        cfs.forceBlockingFlush();

        assertEquals(cfs.getLiveSSTables().size(), 4);

        Map<String, String> options = new HashMap<>();

        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0");
        TimeWindowCompactionStrategy twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);
        twcs.startup();
        Thread.sleep(100);
        assertNotNull(twcs.getSSTableCountByBuckets());
        assertTrue(twcs.getSSTableCountByBuckets().isEmpty());
        AbstractCompactionTask t = twcs.getNextBackgroundTask(0);
        assertEquals(ImmutableMap.of(60000L, 1, 0L, 2, 180000L, 1), twcs.getSSTableCountByBuckets());
        twcs.shutdown();
        t.transaction.abort();
    }

    @Test
    public void testBiggestBucket()
    {
        DatabaseDescriptor.setCompactBiggestSTCSBucketInL0(false);
        ColumnFamilyStore cfs = MockSchema.newCFS();
        long now = System.currentTimeMillis();
        List<SSTableReader> sstables = new ArrayList<>();
        int generation = 0;
        for (int i = 0; i < 100; i++)
            sstables.add(MockSchema.sstableWithTimestamp(generation++, 10, now * 1000, cfs));

        RestorableMeter meter = new RestorableMeter(1000, 1000);
        for (int i = 0; i < 40; i++)
        {
            SSTableReader sstable = MockSchema.sstableWithTimestamp(generation++, 400, now * 1000, cfs);
            sstables.add(sstable);
            sstable.overrideReadMeter(meter); // make sure these larger sstables are "hot"
        }

        Pair<HashMultimap<Long, SSTableReader>, Long> buckets = TimeWindowCompactionStrategy.getBuckets(sstables, TimeUnit.HOURS, 1, TimeUnit.MICROSECONDS);
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("min_sstable_size","200"); // make sure the small sstables get put in the same bucket
        SizeTieredCompactionStrategyOptions options = new SizeTieredCompactionStrategyOptions(optionsMap);
        List<SSTableReader> newestBucket = TimeWindowCompactionStrategy.newestBucket(buckets.left, 4, 32, TimeUnit.HOURS, 1, options, 0);
        assertEquals(cfs.getMaximumCompactionThreshold(), newestBucket.size());
        for (SSTableReader sstable : newestBucket)
            assertEquals(400, sstable.onDiskLength());

        DatabaseDescriptor.setCompactBiggestSTCSBucketInL0(true);
        int oldVal = DatabaseDescriptor.getBiggestBucketMaxSSTableCount();
        DatabaseDescriptor.setBiggestBucketMaxSSTableCount(35);
        newestBucket = TimeWindowCompactionStrategy.newestBucket(buckets.left, 4, 32, TimeUnit.HOURS, 1, options, 0);
        assertEquals(35, newestBucket.size());
        for (SSTableReader sstable : newestBucket)
            assertEquals(10, sstable.onDiskLength());

        DatabaseDescriptor.setBiggestBucketMaxSSTableCount(oldVal);

    }
}
