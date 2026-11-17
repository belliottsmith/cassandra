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
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;

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

    @Test
    public void testRandom()
    {
        for (long seed = 0 ; seed < 200 ; ++seed)
            testRandom(seed);
    }

    private static void testRandom(long seed)
    {
        Random random = new Random(seed);
        // the latest TxnId of each key, as hlc; every key is treated as uncached
        int[] last = new int[KEYS];
        MaxUncachedTxnId bounds = MaxUncachedTxnId.EMPTY;
        int maxSize = 1 + random.nextInt(32);
        int hlc = 1;
        for (int op = 0 ; op < 500 ; ++op)
        {
            String desc = "seed " + seed + " op " + op;
            switch (random.nextInt(5))
            {
                case 0:
                case 1:
                {
                    // a key is written and evicted
                    int k = random.nextInt(KEYS);
                    last[k] = Math.max(last[k], hlc += 1 + random.nextInt(3));
                    bounds = bounds.evicted(key(k), random.nextInt(10) == 0 ? null : txnId(last[k]));
                    break;
                }
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
                    bounds = bounds.establish(range(from, to), txnId(max), keys);
                    break;
                }
                case 3:
                {
                    int from = random.nextInt(KEYS - 1), to = from + 1 + random.nextInt(Math.min(10, KEYS - from - 1));
                    bounds = bounds.without(range(from, to));
                    break;
                }
                case 4:
                    bounds = bounds.bound(maxSize);
                    assertTrue(desc + ": " + bounds.size(), bounds.size() <= maxSize);
                    break;
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
