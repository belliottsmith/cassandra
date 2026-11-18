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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.BTreeReducingRangeMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Checks against a model that {@link MaxUncachedTxnId} bounds the latest TxnId of every key in a range it knows about,
 * and that {@link MaxUncachedTxnId#forEachMaybeRelevant} reports every key whose bound may be relevant, whatever the
 * sequence of evictions, establishments, forgetting and coarsening.
 */
public class MaxUncachedTxnIdTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final TableId TABLE = TableId.fromLong(1);
    private static final int KEYS = 200;

    private static TokenKey key(int i)
    {
        return new TokenKey(TABLE, new LongToken(i * 10L));
    }

    private static Range range(int from, int to)
    {
        return TokenRange.create(key(from), key(to));
    }

    private static TxnId txnId(int hlc)
    {
        return hlc == 0 ? TxnId.NONE : new TxnId(1, hlc, 0, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
    }

    @Test
    public void testEvictedAndEstablished()
    {
        MaxUncachedTxnId bounds = MaxUncachedTxnId.EMPTY;
        // nothing is known, so an eviction tells us nothing
        assertEquals(bounds, bounds.evicted(key(5), txnId(10)));

        bounds = bounds.establish(range(0, 100), txnId(10), new TreeMap<>());
        assertEquals(txnId(10), bounds.get(key(5)).max);
        // a lower eviction changes nothing, a higher one is bounded alone
        assertEquals(bounds, bounds.evicted(key(5), txnId(5)));
        bounds = bounds.evicted(key(5), txnId(20));
        assertEquals(txnId(20), bounds.get(key(5)).max);
        assertTrue(bounds.get(key(5)).isKey());
        assertEquals(txnId(10), bounds.get(key(4)).max);
        assertEquals(txnId(10), bounds.get(key(6)).max);
        // an eviction with an unknown TxnId forgets the key alone
        bounds = bounds.evicted(key(6), null);
        assertNull(bounds.get(key(6)));
        assertEquals(txnId(10), bounds.get(key(7)).max);

        // re-establishing replaces everything in its range
        bounds = bounds.establish(range(0, 50), txnId(3), new TreeMap<>());
        assertEquals(txnId(3), bounds.get(key(5)).max);
        assertEquals(txnId(3), bounds.get(key(6)).max);
        assertEquals(txnId(10), bounds.get(key(60)).max);
    }

    @Test
    public void testForEachMaybeRelevant()
    {
        TreeMap<RoutingKey, TxnId> keys = new TreeMap<>();
        keys.put(key(30), txnId(50));
        MaxUncachedTxnId bounds = MaxUncachedTxnId.EMPTY.establish(range(10, 40), txnId(5), keys)
                                                        .establish(range(60, 80), txnId(50), new TreeMap<>());
        List<Range> scans = new ArrayList<>();
        List<RoutingKey> loads = new ArrayList<>();
        // relevant if later than 10
        bounds.forEachMaybeRelevant(Ranges.of(range(0, 100)), e -> e.max.compareTo(txnId(10)) > 0, scans::add, loads::add);
        // (0,10] is unknown, (10,40] is irrelevant but for key 30, (40,60] is unknown, (60,80] is relevant, (80,100] is unknown
        assertEquals(List.of(range(0, 10), range(40, 100)), scans);
        assertEquals(List.of(key(30)), loads);
    }

    /**
     * Forgetting many ranges at once is the same as forgetting each in turn
     */
    @Test
    public void testWithoutRanges()
    {
        for (long seed = 0 ; seed < 200 ; ++seed)
        {
            Random random = new Random(seed);
            MaxUncachedTxnId bounds = randomBounds(random);
            List<Range> ranges = new ArrayList<>();
            int from = random.nextInt(10);
            while (from < KEYS - 1)
            {
                int to = Math.min(KEYS - 1, from + 1 + random.nextInt(random.nextBoolean() ? 2 : 30));
                ranges.add(range(from, to));
                from = to + random.nextInt(30);
            }

            MaxUncachedTxnId expect = bounds;
            for (Range range : ranges)
                expect = expect.without(range);
            MaxUncachedTxnId actual = bounds.without(Ranges.of(ranges.toArray(new Range[0])));
            assertSameBounds("seed " + seed, expect, actual);
        }
    }

    /**
     * Applying many evictions at once is the same as applying each in turn
     */
    @Test
    public void testBatchedEvictions()
    {
        for (long seed = 0 ; seed < 200 ; ++seed)
        {
            Random random = new Random(seed);
            MaxUncachedTxnId bounds = randomBounds(random);
            TreeMap<RoutingKey, TxnId> evicted = new TreeMap<>();
            MaxUncachedTxnId expect = bounds;
            for (int i = random.nextInt(40) ; i >= 0 ; --i)
            {
                int k = random.nextInt(KEYS);
                if (evicted.containsKey(key(k)))
                    continue;
                TxnId last = random.nextInt(5) == 0 ? null : txnId(random.nextInt(100));
                evicted.put(key(k), last);
                expect = expect.evicted(key(k), last);
            }
            assertSameBounds("seed " + seed, expect, bounds.evicted(evicted));
        }
    }

    /**
     * Buffered evictions are applied before each read or update, coalescing evictions of the same key
     */
    @Test
    public void testHolder()
    {
        MaxUncachedTxnId.Holder holder = new MaxUncachedTxnId.Holder(100, 3);
        holder.update(ignore -> MaxUncachedTxnId.EMPTY.establish(range(0, 100), txnId(10), new TreeMap<>()));

        // ignored as no higher than the current bound
        holder.evicted(key(1), txnId(5));
        assertEquals(0, holder.pendingCount());

        // coalesced
        holder.evicted(key(1), txnId(20));
        holder.evicted(key(1), txnId(15));
        holder.evicted(key(2), txnId(20));
        holder.evicted(key(2), null);
        assertEquals(2, holder.pendingCount());

        // applied when full
        holder.evicted(key(3), txnId(30));
        assertEquals(0, holder.pendingCount());
        MaxUncachedTxnId bounds = holder.get();
        assertEquals(txnId(20), bounds.get(key(1)).max);
        assertNull(bounds.get(key(2)));
        assertEquals(txnId(30), bounds.get(key(3)).max);

        // applied on read
        holder.evicted(key(4), txnId(40));
        assertEquals(1, holder.pendingCount());
        assertEquals(txnId(40), holder.get().get(key(4)).max);
        assertEquals(0, holder.pendingCount());

        // applied before an update
        holder.evicted(key(5), txnId(50));
        holder.update(bounds5 -> {
            assertEquals(txnId(50), bounds5.get(key(5)).max);
            return bounds5.establish(range(0, 100), txnId(10), new TreeMap<>());
        });
        assertEquals(0, holder.pendingCount());
        assertEquals(txnId(10), holder.get().get(key(5)).max);
    }

    /**
     * Retaining some ranges is the same as forgetting the gaps between them
     */
    @Test
    public void testSlice()
    {
        for (long seed = 0 ; seed < 200 ; ++seed)
        {
            Random random = new Random(seed);
            MaxUncachedTxnId bounds = randomBounds(random);
            List<Range> retain = new ArrayList<>();
            List<Range> forget = new ArrayList<>();
            int from = 0;
            while (from < KEYS - 1)
            {
                int to = Math.min(KEYS - 1, from + 1 + random.nextInt(random.nextBoolean() ? 2 : 30));
                (random.nextBoolean() ? retain : forget).add(range(from, to));
                from = to;
            }

            Ranges retained = Ranges.of(retain.toArray(new Range[0]));
            MaxUncachedTxnId expect = bounds.without(Ranges.of(forget.toArray(new Range[0])));
            MaxUncachedTxnId actual = bounds.slice(retained);
            assertSameBounds("seed " + seed, expect, actual);
            for (MaxUncachedTxnId.Entry entry : actual)
            {
                int i = retained.indexOf(entry.end());
                assertTrue("seed " + seed + ": " + entry + " not in " + retained, i >= 0 && retained.get(i).start().compareTo(entry.start()) <= 0);
            }
        }
    }

    @Test
    public void testSerialization() throws IOException
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            Serializers.testSerde(buffer, CommandStoreSerializers.maxUncachedTxnId, MaxUncachedTxnId.EMPTY);
            Range full = TokenRange.fullRange(TABLE, Murmur3Partitioner.instance);
            Serializers.testSerde(buffer, CommandStoreSerializers.maxUncachedTxnId,
                                  MaxUncachedTxnId.EMPTY.establish(full, txnId(5), new TreeMap<>()).evicted(key(3), txnId(7)));
            for (long seed = 0 ; seed < 200 ; ++seed)
                Serializers.testSerde(buffer, CommandStoreSerializers.maxUncachedTxnId, randomBounds(new Random(seed)));
        }
    }

    private static MaxUncachedTxnId randomBounds(Random random)
    {
        MaxUncachedTxnId bounds = MaxUncachedTxnId.EMPTY;
        for (int i = random.nextInt(10) ; i >= 0 ; --i)
        {
            int from = random.nextInt(KEYS - 1), to = from + 1 + random.nextInt(KEYS - from - 1);
            TreeMap<RoutingKey, TxnId> keys = new TreeMap<>();
            for (int j = random.nextInt(5) ; j > 0 ; --j)
                keys.put(key(from + 1 + random.nextInt(to - from)), txnId(random.nextInt(100)));
            bounds = bounds.establish(range(from, to), txnId(random.nextInt(100)), keys);
        }
        return bounds;
    }

    private static void assertSameBounds(String desc, MaxUncachedTxnId expect, MaxUncachedTxnId actual)
    {
        assertTrue(desc + ": malformed " + actual, BTreeReducingRangeMap.isWellFormed(actual));
        for (int k = 0 ; k < KEYS ; ++k)
        {
            MaxUncachedTxnId.Entry e = expect.get(key(k)), a = actual.get(key(k));
            assertEquals(desc + ": key " + k + " expected " + expect + " but was " + actual,
                         e == null ? null : e.max, a == null ? null : a.max);
        }
    }

    @Test
    public void testRandom()
    {
        for (long seed = 0 ; seed < 200 ; ++seed)
        {
            testRandom(seed, false);
            testRandom(seed, true);
        }
    }

    private static MaxUncachedTxnId update(Random random, int kind, MaxUncachedTxnId bounds, int[] last, int hlc, int maxSize)
    {
        switch (kind)
        {
            default: throw new AssertionError();
            case 2:
            {
                // a range is scanned: bound the keys we judge irrelevant together, and the rest alone
                int from = random.nextInt(KEYS - 1), to = from + 1 + random.nextInt(KEYS - from - 1);
                int threshold = random.nextInt(hlc + 1);
                int max = 0;
                TreeMap<RoutingKey, TxnId> keys = new TreeMap<>();
                for (int k = from + 1 ; k <= to ; ++k)
                {
                    if (last[k] < threshold) max = Math.max(max, last[k]);
                    else keys.put(key(k), txnId(last[k]));
                }
                return bounds.establish(range(from, to), txnId(max), keys);
            }
            case 3:
            {
                int from = random.nextInt(KEYS - 1), to = from + 1 + random.nextInt(Math.min(10, KEYS - from - 1));
                return bounds.without(range(from, to));
            }
            case 4:
            {
                bounds = bounds.bound(maxSize);
                assertTrue(String.valueOf(bounds.size()), bounds.size() <= maxSize);
                return bounds;
            }
        }
    }

    /**
     * @param buffered whether to maintain the bounds via a {@link MaxUncachedTxnId.Holder}, reading them only sometimes
     */
    private static void testRandom(long seed, boolean buffered)
    {
        Random random = new Random(seed);
        // the latest TxnId of each key, as hlc; every key is treated as uncached
        int[] last = new int[KEYS];
        MaxUncachedTxnId bounds = MaxUncachedTxnId.EMPTY;
        int maxSize = 1 + random.nextInt(32);
        MaxUncachedTxnId.Holder holder = buffered ? new MaxUncachedTxnId.Holder(maxSize, 1 + random.nextInt(8)) : null;
        int hlc = 1;
        for (int op = 0 ; op < 500 ; ++op)
        {
            String desc = "seed " + seed + " op " + op + (buffered ? " (buffered)" : "");
            int kind = random.nextInt(5);
            if (kind <= 1)
            {
                // a key is written and evicted
                int k = random.nextInt(KEYS);
                last[k] = Math.max(last[k], hlc += 1 + random.nextInt(3));
                TxnId evicted = random.nextInt(10) == 0 ? null : txnId(last[k]);
                if (holder == null) bounds = bounds.evicted(key(k), evicted);
                else holder.evicted(key(k), evicted);
                // with a holder, only sometimes read our bounds, so that evictions accumulate
                if (holder != null && random.nextInt(4) > 0)
                    continue;
            }
            else if (holder == null)
            {
                bounds = update(random, kind, bounds, last, hlc, maxSize);
            }
            else
            {
                int now = hlc;
                holder.update(b -> update(random, kind, b, last, now, maxSize));
            }

            if (holder != null)
            {
                bounds = holder.get();
                assertTrue(desc + ": " + bounds.size(), bounds.size() <= maxSize);
            }

            assertTrue(desc + ": malformed " + bounds, BTreeReducingRangeMap.isWellFormed(bounds));
            for (int k = 0 ; k < KEYS ; ++k)
            {
                MaxUncachedTxnId.Entry entry = bounds.get(key(k));
                assertTrue(desc + ": key " + k + " with " + last[k] + " has bound " + entry,
                           entry == null || entry.max.compareTo(txnId(last[k])) >= 0);
            }

            int threshold = random.nextInt(hlc + 1);
            int from = random.nextInt(KEYS - 1), to = from + 1 + random.nextInt(KEYS - from - 1);
            TreeSet<RoutingKey> reported = new TreeSet<>();
            List<Range> scans = new ArrayList<>();
            bounds.forEachMaybeRelevant(Ranges.of(range(from, to)), e -> e.max.compareTo(txnId(threshold)) >= 0, scans::add, reported::add);
            Ranges scanned = Ranges.of(scans.toArray(new Range[0]));
            for (int k = from + 1 ; k <= to ; ++k)
            {
                if (last[k] >= threshold)
                    assertTrue(desc + ": key " + k + " with " + last[k] + " not reported for threshold " + threshold + " from " + bounds,
                               reported.contains(key(k)) || scanned.contains(key(k)));
            }
        }
    }
}
