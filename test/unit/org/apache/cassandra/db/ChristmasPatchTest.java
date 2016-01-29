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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.xmas.BoundsAndOldestTombstone;
import org.apache.cassandra.db.xmas.InvalidatedRepairedRange;
import org.apache.cassandra.db.xmas.SuccessfulRepairTimeHolder;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.ColumnFamilyStore.updateLastSuccessfulRepair;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ChristmasPatchTest extends CQLTester
{
    SuccessfulRepairTimeHolder holder = new SuccessfulRepairTimeHolder(ImmutableList.of(), ImmutableList.of());

    private static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }


    private static DecoratedKey dk(long t)
    {
        return new BufferDecoratedKey(tk(t), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }
    private static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
    }
    private static Bounds<Token> bounds(long left, long right)
    {
        return new Bounds<>(tk(left), tk(right));
    }

    void update(long left, long right, int time)
    {
        holder = updateLastSuccessfulRepair(range(left, right), time, holder);
        Assert.assertNotNull(holder);
    }

    /**
     * When inserting values for multiple ranges, old times for ranges should not be left in the list
     */
    @Test
    public void dedupeTest() throws Exception
    {
        update(50, 100, 99);
        update(0, 100, 100);
        update(50, 100, 101);
        Assert.assertEquals(2, holder.successfulRepairs.size());
    }

    /**
     * Most recent times should be at the head of the list
     */
    @Test
    public void orderTest() throws Exception
    {
        update(50, 100, 99);
        update(0, 100, 100);
        update(150, 250, 101);

        List<Pair<Range<Token>, Integer>> expected = Lists.newArrayList(Pair.create(range(150, 250), 101),
                                                                        Pair.create(range(0, 100), 100),
                                                                        Pair.create(range(50, 100), 99));
        Assert.assertEquals(expected, holder.successfulRepairs);
    }

    @Test
    public void rejectPastRepairs()
    {
        update(0, 100, 100);
        SuccessfulRepairTimeHolder newHolder = updateLastSuccessfulRepair(range(0, 100), 99, holder);
        assertNull(newHolder);
    }

    @Test
    public void testGCGS()
    {
        createTable("CREATE TABLE %s (id int primary key, t text) WITH gc_grace_seconds=100");
        DatabaseDescriptor.setChristmasPatchEnabled();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.skipRFCheckForXmasPatch();
        int gcBefore = cfs.gcBefore(FBUtilities.nowInSeconds());
        DecoratedKey dk = new BufferDecoratedKey(cfs.getPartitioner().getRandomToken(), ByteBufferUtil.EMPTY_BYTE_BUFFER);

        Assert.assertEquals(Integer.MIN_VALUE, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk, gcBefore));
        Token min = cfs.getPartitioner().getMinimumToken();
        cfs.updateLastSuccessfulRepair(new Range<>(min, min), System.currentTimeMillis());
        Assert.assertEquals(gcBefore, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk, gcBefore));
    }

    @Test
    public void testClearSystemTable() throws InterruptedException
    {
        createTable("CREATE TABLE %s (id int primary key, t text)");
        DatabaseDescriptor.setChristmasPatchEnabled();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.skipRFCheckForXmasPatch();
        cfs.updateLastSuccessfulRepair(range(10, 100), 10 * 1000);
        cfs.updateLastSuccessfulRepair(range(200, 300), 20 * 1000);
        cfs.updateLastSuccessfulRepair(range(400, 500), 30 * 1000);
        cfs.updateLastSuccessfulRepair(range(90, 120), 40 * 1000);
        List<Pair<Range<Token>, Integer>> successBefore = new ArrayList<>(cfs.getRepairTimeSnapshot().successfulRepairs);
        int gen = 0;
        List<SSTableReader> toImport = Lists.newArrayList(MockSchema.sstable(++gen, 5, 10, 7, cfs), // shouldn't affect - range is start-exclusive
                                                          MockSchema.sstable(++gen, 300, 300, 4, cfs),
                                                          MockSchema.sstable(++gen, 400, 1500, 2, cfs));

        cfs.clearLastRepairTimesFor(toImport, 50);
        List<InvalidatedRepairedRange> afterFirstImport = cfs.getRepairTimeSnapshot().invalidatedRepairs;
        assertEquals(successBefore, cfs.getRepairTimeSnapshot().successfulRepairs);
        List<InvalidatedRepairedRange> cur = new ArrayList<>(SystemKeyspace.getInvalidatedSuccessfulRepairRanges(keyspace(), currentTable()));
        assertEquals(2, cur.size());
        for (InvalidatedRepairedRange irr : cur)
        {
            assertTrue(irr.minLDTSeconds == 2 || irr.minLDTSeconds == 4);
            if (irr.minLDTSeconds == 2)
                assertEquals(range(400, 500), irr.range);
            else
                assertEquals(range(200, 300), irr.range);
        }

        cur.sort((a, b) -> Ints.compare(a.minLDTSeconds, b.minLDTSeconds));
        assertEquals(cfs.getRepairTimeSnapshot().invalidatedRepairs, cur);

        assertEquals(Integer.MIN_VALUE, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(7), 100));
        assertEquals(10, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(15), 100));
        assertEquals(2, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(450), 100));
        assertEquals(2, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(500), 100));
        assertEquals(Integer.MIN_VALUE, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(400), 100));
        assertEquals(4, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(250), 100));

        // import an sstable where min ldt is after the last repairs - should not affect anything:
        cfs.clearLastRepairTimesFor(Collections.singletonList(MockSchema.sstable(++gen, 5, 10000, 50, cfs)), 50);
        assertEquals(afterFirstImport, cfs.getRepairTimeSnapshot().invalidatedRepairs);

        /*
now we have something like this:
[InvalidatedRepairedRange{invalidatedAtSeconds=1528851329, minLDTSeconds=2, range=(400,500]},
 InvalidatedRepairedRange{invalidatedAtSeconds=1528851329, minLDTSeconds=4, range=(200,300]}]
and we invalidate (5, 10000] with minLDT = 25 -> this means that we will find the (400, 500] and (90, 120] ranges above
since those repairs are newer than the min ldt we are importing. It will insert a new invalidation for (90, 120] with minLDT = 25
and it will update the (400, 500] one with a new invalidatedAtSeconds
         */
        InvalidatedRepairedRange irr400500 = cfs.getRepairTimeSnapshot().invalidatedRepairs.stream().filter(i -> i.range.equals(range(400, 500))).findFirst().get();
        InvalidatedRepairedRange irr200300 = cfs.getRepairTimeSnapshot().invalidatedRepairs.stream().filter(i -> i.range.equals(range(200, 300))).findFirst().get();
        cfs.clearLastRepairTimesFor(Collections.singletonList(MockSchema.sstable(++gen, 5, 10000, 25, cfs)), 55);
        InvalidatedRepairedRange irr400500After = cfs.getRepairTimeSnapshot().invalidatedRepairs.stream().filter(i -> i.range.equals(range(400, 500))).findFirst().get();
        InvalidatedRepairedRange irr200300After = cfs.getRepairTimeSnapshot().invalidatedRepairs.stream().filter(i -> i.range.equals(range(200, 300))).findFirst().get();
        InvalidatedRepairedRange irr90120After = cfs.getRepairTimeSnapshot().invalidatedRepairs.stream().filter(i -> i.range.equals(range(90, 120))).findFirst().get();
        assertEquals(irr200300, irr200300After); // untouched
        assertEquals(irr400500.minLDTSeconds, irr400500After.minLDTSeconds); // min ldt should stay the same since 2 < 25
        assertEquals(50, irr400500.invalidatedAtSeconds);
        assertEquals(55, irr400500After.invalidatedAtSeconds);
        assertEquals(25, irr90120After.minLDTSeconds);
        assertEquals(55, irr90120After.invalidatedAtSeconds);

        assertEquals(successBefore, cfs.getRepairTimeSnapshot().successfulRepairs);
        SystemKeyspace.clearRepairedRanges(keyspace(), currentTable());
        cfs.clearLastSucessfulRepairUnsafe();
    }

    @Test
    public void testClearSystemTableOverlappingTimes()
    {
        createTable("CREATE TABLE %s (id int primary key, t text)");
        DatabaseDescriptor.setChristmasPatchEnabled();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.skipRFCheckForXmasPatch();
        cfs.updateLastSuccessfulRepair(range(10, 100), 10 * 1000);
        cfs.updateLastSuccessfulRepair(range(20, 110), 20 * 1000);
        cfs.updateLastSuccessfulRepair(range(30, 120), 30 * 1000);
        cfs.updateLastSuccessfulRepair(range(40, 130), 40 * 1000);
        // intersects all repaired ranges, but oldest tombstone is newer than the two first repaired ranges
        List<SSTableReader> toImport = Lists.newArrayList(MockSchema.sstable(1, 15, 102, 25, cfs));
        cfs.clearLastRepairTimesFor(toImport, 45);

        // check a few tokens that they have the correct gcBefore
        // on the successful boundary, not repaired since range is (x, y]
        assertEquals(Integer.MIN_VALUE, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(10), 100));
        // repaired, not invalidated:
        assertEquals(10, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(11), 100));
        // not invalidated since (20, 110] was repaired before the oldest tombstone (25) in the new sstable:
        assertEquals(20, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(30), 100));
        // invalidated repaired range
        assertEquals(25, cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, dk(130), 100));

        List<InvalidatedRepairedRange> invalidatedRanges = SystemKeyspace.getInvalidatedSuccessfulRepairRanges(keyspace(), currentTable());
        assertEquals(2, invalidatedRanges.size());
        Set<Range<Token>> expectedRanges = Sets.newHashSet(range(30, 120), range(40, 130));
        for (InvalidatedRepairedRange irr : invalidatedRanges)
        {
            assertEquals(25, irr.minLDTSeconds);
            assertTrue(expectedRanges.remove(irr.range));
            assertEquals(45, irr.invalidatedAtSeconds);
        }

        // make sure a new repair bumps bumps the gcBefore for a key
        assertKeyGCBefore(cfs, dk(125), range(121, 140), 25, 50);
        assertKeyGCBefore(cfs, dk(55), range(53, 57), 25, 55);
        assertKeyGCBefore(cfs, dk(11), range(0, 12), 10, 77);

        cfs.clearLastSucessfulRepairUnsafe();
        SystemKeyspace.clearRepairedRanges(keyspace(), currentTable());
    }

    private void assertKeyGCBefore(ColumnFamilyStore cfs, DecoratedKey key, Range<Token> repairedRange, int gcBeforeRepair, int succeedAt)
    {
        int keyGCBefore = cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, key, Integer.MAX_VALUE);
        assertEquals(gcBeforeRepair, keyGCBefore);
        cfs.updateLastSuccessfulRepair(repairedRange, succeedAt * 1000);
        keyGCBefore = cfs.getRepairTimeSnapshot().gcBeforeForKey(cfs, key, Integer.MAX_VALUE);
        assertEquals(succeedAt, keyGCBefore);
    }

    @Test
    public void testBoundsAndOldestTombstone()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        int gen = 0;
        List<SSTableReader> sstables = Lists.newArrayList(MockSchema.sstable(++gen, 5, 10, 10, cfs),
                                                          MockSchema.sstable(++gen, 300, 300, 13, cfs),
                                                          MockSchema.sstable(++gen, 400, 1500, 5, cfs),
                                                          MockSchema.sstable(++gen, 401, 1500, 6, cfs),
                                                          MockSchema.sstable(++gen, 402, 1500, 7, cfs));
        BoundsAndOldestTombstone bot = new BoundsAndOldestTombstone(sstables);
        // non-intersecting
        assertEquals(Integer.MAX_VALUE, bot.getOldestIntersectingTombstoneLDT(range(0,1), 0));
        // intersecting but repaired before the tombstones LDT:
        assertEquals(Integer.MAX_VALUE, bot.getOldestIntersectingTombstoneLDT(range(0,2000), 0));
        // intersecting, repaired newer
        assertEquals(10, bot.getOldestIntersectingTombstoneLDT(range(0,350), 20));
        // make sure we get the oldest when querying overlapping ranges:
        assertEquals(5, bot.getOldestIntersectingTombstoneLDT(range(350,2000), 20));

    }

    @Test
    public void testInvalidatedRepairRange()
    {
        Range<Token> r = range(10,100);
        InvalidatedRepairedRange irr = new InvalidatedRepairedRange(10, 10, r);
        InvalidatedRepairedRange irr2 = new InvalidatedRepairedRange(20, 4, r);
        InvalidatedRepairedRange irrMerged = irr.merge(5, irr2);
        InvalidatedRepairedRange irrMerged2 = irr2.merge(5, irr);
        assertEquals(irrMerged, irrMerged2);
        // we should keep the smallest LDT
        assertEquals(4, irrMerged.minLDTSeconds);
        // and the largest invalidatedAtSeconds
        assertEquals(20, irrMerged.invalidatedAtSeconds);
        assertEquals(r, irrMerged.range);
        assertNull(irr.merge(40, irr2));

        assertEquals(irr2, irr.merge(15, irr2)); // irr is not active - repair has been run after the invalidatedAtSeconds
        assertEquals(irr2, irr2.merge(15, irr));
    }

    @Test
    public void testLastRepairForSSTable()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Range<Token> oldRepairedRange = range(0, 100);
        Range<Token> newRepairedRange = range(10, 20);
        ImmutableList<Pair<Range<Token>, Integer>> l = ImmutableList.<Pair<Range<Token>, Integer>>builder()
                                                       .add(Pair.create(newRepairedRange, 2000))
                                                       .add(Pair.create(oldRepairedRange, 1000)).build();

        int gen = 0;
        SuccessfulRepairTimeHolder srt = new SuccessfulRepairTimeHolder(l, ImmutableList.of());
        assertEquals(1000, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 15, 50, 10, cfs), Integer.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, -10, 50, 10, cfs), Integer.MAX_VALUE));
        // range is start-exclusive:
        assertEquals(Integer.MIN_VALUE, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 0, 50, 10, cfs), Integer.MAX_VALUE));
        assertEquals(1000, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 1, 100, 10, cfs), Integer.MAX_VALUE));
        assertEquals(2000, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 15, 16, 10, cfs), Integer.MAX_VALUE));
    }

    @Test
    public void testNewerTombstone()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        ImmutableList<Pair<Range<Token>, Integer>> l = ImmutableList.<Pair<Range<Token>, Integer>>builder()
                                                                    .add(Pair.create(range(0, 10), 3000))
                                                                    .add(Pair.create(range(10, 20), 2000)).build();

        SuccessfulRepairTimeHolder srt = new SuccessfulRepairTimeHolder(l, ImmutableList.of());
        int i = 0;
        assertEquals(Integer.MIN_VALUE, srt.getFullyRepairedTimeFor(MockSchema.sstable(++i, 5, 15, 2500, cfs), Integer.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, srt.getFullyRepairedTimeFor(MockSchema.sstable(++i, 5, 15, 10, cfs), 5));
        assertEquals(2000, srt.getFullyRepairedTimeFor(MockSchema.sstable(++i, 5, 15, 10, cfs), 100));
    }


    @Test
    public void testLastRepairWithInvalidationsForSSTable()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Range<Token> oldRepairedRange = range(0, 100);
        Range<Token> newRepairedRange = range(10, 20);
        ImmutableList<Pair<Range<Token>, Integer>> l = ImmutableList.<Pair<Range<Token>, Integer>>builder()
                                                       .add(Pair.create(newRepairedRange, 2000))
                                                       .add(Pair.create(oldRepairedRange, 1000)).build();
        InvalidatedRepairedRange irr = new InvalidatedRepairedRange(2300, 800, range(18, 25));

        SuccessfulRepairTimeHolder srt = new SuccessfulRepairTimeHolder(l, ImmutableList.of(irr));

        int gen = 0;
        
        // does not intersect the invalidated range, should give the newly repaired time:
        assertEquals(2000, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 11, 15, 10, cfs), Integer.MAX_VALUE));

        // not fully contained in a repaired range
        assertEquals(Integer.MIN_VALUE, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, -10, 50, 10, cfs), Integer.MAX_VALUE));

        // fully contained in the invalidated range
        assertEquals(800, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 19, 21, 10, cfs), Integer.MAX_VALUE));

        // the sstable was fully repaired, but the invalidation intersects the sstable
        assertEquals(800, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 1, 100, 10, cfs), Integer.MAX_VALUE));

        // intersects the invalidation
        assertEquals(800, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 1, 19, 10, cfs), Integer.MAX_VALUE));
    }

    @Test
    public void testManyRepairsSSTable()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<Pair<Range<Token>, Integer>> repairs = new ArrayList<>();
        for (int i = 0; i < 100; i+= 10)
            repairs.add(Pair.create(range(i, i + 10), i + 100));
        // and add an older repair
        repairs.add(Pair.create(range(0, 200), 50));
        // and a non-intersecting one
        repairs.add(Pair.create(range(200, 300), 50));

        repairs.sort(ColumnFamilyStore.lastRepairTimeComparator);
        SuccessfulRepairTimeHolder srt = new SuccessfulRepairTimeHolder(ImmutableList.copyOf(repairs), ImmutableList.of());
        int gen = 0;
        assertEquals(100, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 5, 95, 10, cfs), Integer.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, -1, 95, 10, cfs), Integer.MAX_VALUE));
        assertEquals(190, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 93, 95, 10, cfs), Integer.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 93, 400, 10, cfs), Integer.MAX_VALUE));

        InvalidatedRepairedRange irr = new InvalidatedRepairedRange(300, 5, range(0, 40));
        srt = new SuccessfulRepairTimeHolder(srt.successfulRepairs, ImmutableList.of(irr));
        assertEquals(5, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 5, 95, 10, cfs), Integer.MAX_VALUE));
        assertEquals(140, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 45, 95, 10, cfs), Integer.MAX_VALUE));
    }

    @Test
    public void testWrapAroundRepairsSSTable()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<Pair<Range<Token>, Integer>> repairs = new ArrayList<>();
        repairs.add(Pair.create(range(500, 300), 150));

        repairs.sort(ColumnFamilyStore.lastRepairTimeComparator);
        SuccessfulRepairTimeHolder srt = new SuccessfulRepairTimeHolder(ImmutableList.copyOf(repairs), ImmutableList.of());
        int gen = 0;
        assertEquals(150, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 5, 95, 10, cfs), Integer.MAX_VALUE));
        // we have a gap between 300 and 500, this sstable is not fully repaired:
        assertEquals(Integer.MIN_VALUE, srt.getFullyRepairedTimeFor(MockSchema.sstable(++gen, 200, 700, 10, cfs), Integer.MAX_VALUE));
    }


    @Test
    public void boundsSubtractTest()
    {
        //   [-------]
        // -   (--]
        // = [-]  (--]
        assertEquals(Sets.newHashSet(bounds(10, 15), range(20, 100)),
                     new HashSet<>(SuccessfulRepairTimeHolder.subtract(bounds(10, 100), range(15, 20))));
        assertEquals(Collections.emptyList(),
                     SuccessfulRepairTimeHolder.subtract(range(10, 100), range(10, 100)));

        //     [-----]
        // - (---]
        // =     (---]
       assertEquals(Collections.singletonList(range(20, 100)),
                    SuccessfulRepairTimeHolder.subtract(bounds(10, 100), range(5, 20)));
       // first token overlapping - should become start-non-inclusive (a Range)
       assertEquals(Collections.singletonList(range(10, 100)),
                    SuccessfulRepairTimeHolder.subtract(bounds(10, 100), range(5, 10)));

        //    [-----]
        // -     (-----]
        // =  [--]
       assertEquals(Collections.singletonList(bounds(10, 30)),
                    SuccessfulRepairTimeHolder.subtract(bounds(10, 100), range(30, 120)));
        assertEquals(Collections.singletonList(bounds(10, 99)),
                     SuccessfulRepairTimeHolder.subtract(bounds(10, 100), range(99, 110)));
        // non-intersecting:
        assertEquals(Collections.singletonList(bounds(10, 100)),
                     SuccessfulRepairTimeHolder.subtract(bounds(10, 100), range(100, 120)));
    }

    @Test
    public void rangeSubtractTest()
    {
        //   (-------]
        // -   (--]
        // = (-]  (--]
        assertEquals(Sets.newHashSet(range(10, 15), range(20, 100)),
                     new HashSet<>(SuccessfulRepairTimeHolder.subtract(range(10, 100), range(15, 20))));
        assertEquals(Collections.emptyList(),
                     SuccessfulRepairTimeHolder.subtract(range(10, 100), range(10, 100)));

        //     (-----]
        // - (---]
        // =     (---]
        assertEquals(Collections.singletonList(range(20, 100)),
                     SuccessfulRepairTimeHolder.subtract(range(10, 100), range(5, 20)));
        // first token overlapping - should become start-non-inclusive (a Range)
        assertEquals(Collections.singletonList(range(10, 100)),
                     SuccessfulRepairTimeHolder.subtract(range(10, 100), range(5, 10)));

        //    (-----]
        // -     (-----]
        // =  (--]
        assertEquals(Collections.singletonList(range(10, 30)),
                     SuccessfulRepairTimeHolder.subtract(range(10, 100), range(30, 120)));
        assertEquals(Collections.singletonList(range(10, 30)),
                     SuccessfulRepairTimeHolder.subtract(range(10, 100), range(30, 100)));
        assertEquals(Collections.singletonList(range(10, 99)),
                     SuccessfulRepairTimeHolder.subtract(range(10, 100), range(99, 110)));

        // non-intersecting:
        assertEquals(Collections.singletonList(range(10, 100)),
                     SuccessfulRepairTimeHolder.subtract(range(10, 100), range(100, 120)));
    }

    @Test
    public void testWrapBounds()
    {
        Bounds<Token> sstableBound = bounds(10, 100);
        List<Range<Token>> repairedRanges = new ArrayList<>();
        for (int i = 0; i < 50; i++)
            repairedRanges.add(range(100 - i, i));

        List<AbstractBounds<Token>> sstableBounds = new ArrayList<>();
        sstableBounds.add(sstableBound);
        int i = 0;
        for (Range<Token> repairedRange : repairedRanges)
        {
            for (Range<Token> unwrappedRange : repairedRange.unwrap()) // avoid handling wrapping ranges in subtract below
                sstableBounds = SuccessfulRepairTimeHolder.subtract(sstableBounds, unwrappedRange);

            assertEquals(1, sstableBounds.size());
            assertEquals(Math.max(10, i), (long)sstableBounds.get(0).left.getTokenValue());
            assertEquals(100 - i, (long)sstableBounds.get(0).right.getTokenValue());
            i++;
        }
    }

    @Test
    public void randomSubtractTest()
    {
        Bounds<Token> sstableBounds = bounds(1, 2000);
        List<Range<Token>> repairedRanges = new ArrayList<>(500);
        for (int i = 0; i < 2000; i+=4)
            repairedRanges.add(range(i, i+4));

        for (int i = 0; i < 1000; i++)
        {
            long seed = System.currentTimeMillis();
            Random r = new Random(seed);
            List<Range<Token>> shuffledRepairs = new ArrayList<>(repairedRanges);
            Collections.shuffle(shuffledRepairs, r);
            List<AbstractBounds<Token>> unrepairedSSTableBounds = new ArrayList<>();
            unrepairedSSTableBounds.add(sstableBounds);
            for (Range<Token> repairedRange : shuffledRepairs)
            {
                assertFalse("SEED="+seed, unrepairedSSTableBounds.isEmpty());
                unrepairedSSTableBounds = SuccessfulRepairTimeHolder.subtract(unrepairedSSTableBounds, repairedRange);
            }

            assertTrue(unrepairedSSTableBounds.toString(), unrepairedSSTableBounds.isEmpty());
        }

        for (int i = 0; i < 1000; i++)
        {
            long seed = System.currentTimeMillis();
            Random r = new Random(seed);
            List<Range<Token>> shuffledRepairs = new ArrayList<>(repairedRanges);
            Collections.shuffle(shuffledRepairs, r);
            shuffledRepairs.remove(shuffledRepairs.size() - 1);
            List<AbstractBounds<Token>> unrepairedSSTableBounds = new ArrayList<>();
            unrepairedSSTableBounds.add(sstableBounds);
            for (Range<Token> repairedRange : shuffledRepairs)
                unrepairedSSTableBounds = SuccessfulRepairTimeHolder.subtract(unrepairedSSTableBounds, repairedRange);
            assertFalse("SEED="+seed, unrepairedSSTableBounds.isEmpty());
        }
    }

    @Test
    public void testGarbageCollectionCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, t text) with gc_grace_seconds=1");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.skipRFCheckForXmasPatch();
        DatabaseDescriptor.setChristmasPatchEnabled();
        cfs.disableAutoCompaction();
        // create 2 sstables, one with data, one with tombstones
        for (int i =  0; i < 100; i++)
        {
            execute("insert into %s (id, t) values (?, 'hello')", i);
        }
        cfs.forceBlockingFlush();
        for (int i = 0; i < 10; i++)
        {
            execute("delete from %s where id = ?", i);
        }
        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for gcgs
        cfs.garbageCollect(CompactionParams.TombstoneOption.CELL, 1);
        boolean foundTombstones = false;
        boolean foundWithoutTombstones = false;
        assertEquals(2, cfs.getLiveSSTables().size());

        // make sure that one sstable still has the tombstones;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.mayHaveTombstones())
                foundTombstones = true;
            else
                foundWithoutTombstones = true;
        }
        assertTrue(foundTombstones);
        assertTrue(foundWithoutTombstones);
        IPartitioner partitioner = cfs.getPartitioner();
        cfs.updateLastSuccessfulRepair(new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()), System.currentTimeMillis());
        cfs.garbageCollect(CompactionParams.TombstoneOption.CELL, 1);
        foundTombstones = false;
        foundWithoutTombstones = false;
        assertEquals(1, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable.mayHaveTombstones())
                foundTombstones = true;
            else
                foundWithoutTombstones = true;
        }
        assertTrue(foundWithoutTombstones);
        assertFalse(foundTombstones);
    }
}

