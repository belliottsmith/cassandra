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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.utils.BTreeReducingRangeMap;
import accord.utils.Invariants;
import accord.utils.btree.ReducingBTree;

import org.apache.cassandra.service.accord.api.TokenKey;

/**
 * For each range we know about, an upper bound on the latest {@link TxnId} of every {@code CommandsForKey} in the range
 * that is not in the cache. A range we know nothing about is absent, and should be treated as unbounded.
 * <p>
 * A sync point scanning a range for the keys it must load need not consult disk for any part of the range whose bound
 * it finds irrelevant (see {@code SummaryLoader.isRelevant(Range, TxnId)}), and may load directly the key of any relevant
 * bound that covers only that key.
 * <p>
 * A bound is established for a range by a scan of the range on disk, and maintained on eviction: every write of a
 * {@code CommandsForKey} goes through the cache, so it may leave memory only by eviction. Besides (re-)establishing a
 * bound, we only ever raise a bound or forget it, so it remains an upper bound whatever it is compared with.
 * <p>
 * Keys with recent transactions are given their own bound, so that they do not make the rest of their range relevant;
 * to constrain our size we may coarsen bounds, taking their maximum, or forget them. Either only makes us over-report.
 * <p>
 * Accessed only with the command store's executor lock held.
 */
public class MaxUncachedTxnId extends BTreeReducingRangeMap<MaxUncachedTxnId.Entry>
{
    public static class Entry extends ReducingBTree.Entry<Entry>
    {
        public final TxnId max;

        public Entry(RoutingKey start, RoutingKey end, TxnId max)
        {
            super(start, end);
            this.max = max;
        }

        /**
         * Whether this bound covers a single key
         */
        public boolean isKey()
        {
            // only a real key (not a sentinel) has a range of its own
            return !((TokenKey) end()).isSentinel() && start().equals(end().asRange().start());
        }

        @Override
        public boolean equalsIgnoreRange(Entry that)
        {
            return max.equals(that.max);
        }

        @Override
        public Entry with(RoutingKey start, RoutingKey end)
        {
            return new Entry(start, end, max);
        }

        @Override
        public String toString()
        {
            return super.toString() + '=' + max;
        }

        static Entry max(RoutingKey start, RoutingKey end, Entry a, Entry b)
        {
            Entry max = a.max.compareTo(b.max) >= 0 ? a : b;
            return max.equalsRange(start, end) ? max : max.with(start, end);
        }

        static Entry replace(RoutingKey start, RoutingKey end, Entry existing, Entry replacement)
        {
            return replacement.equalsRange(start, end) ? replacement : replacement.with(start, end);
        }
    }

    public static final MaxUncachedTxnId EMPTY = new MaxUncachedTxnId();

    private MaxUncachedTxnId()
    {
        super();
    }

    private MaxUncachedTxnId(Object[] tree)
    {
        super(tree);
    }

    /**
     * {@code key} has left the cache, with latest TxnId {@code last} (which is {@link TxnId#NONE} if it has no
     * transactions), or with unknown latest TxnId if {@code last} is null.
     */
    public MaxUncachedTxnId evicted(RoutingKey key, @Nullable TxnId last)
    {
        Entry entry = get(key);
        if (entry == null)
            return this;

        if (last == null)
            return without(key.asRange());

        if (last.compareTo(entry.max) <= 0)
            return this;

        Range range = key.asRange();
        return merge(this, create(Ranges.of(range), new Entry(range.start(), range.end(), plain(last)), MaxUncachedTxnId::new), Entry::max, MaxUncachedTxnId::new);
    }

    /**
     * Replace our bounds over {@code range} with {@code max}, except for each key in {@code keys}, which has its own bound
     */
    public MaxUncachedTxnId establish(Range range, TxnId max, SortedMap<RoutingKey, TxnId> keys)
    {
        MaxUncachedTxnId result = merge(this, create(Ranges.of(range), new Entry(range.start(), range.end(), plain(max)), MaxUncachedTxnId::new), Entry::replace, MaxUncachedTxnId::new);
        if (keys.isEmpty())
            return result;

        try (Builder builder = new Builder())
        {
            for (Map.Entry<RoutingKey, TxnId> e : keys.entrySet())
            {
                Invariants.require(range.contains(e.getKey()));
                Range keyRange = e.getKey().asRange();
                builder.append(new Entry(keyRange.start(), keyRange.end(), plain(e.getValue())));
            }
            return merge(result, builder.build(), Entry::max, MaxUncachedTxnId::new);
        }
    }

    /**
     * Forget our bounds over {@code range}
     */
    public MaxUncachedTxnId without(Range range)
    {
        boolean intersects = false;
        for (Entry entry : this)
        {
            if (intersects(entry, range))
            {
                intersects = true;
                break;
            }
        }
        if (!intersects)
            return this;

        try (Builder builder = new Builder())
        {
            for (Entry entry : this)
            {
                if (!intersects(entry, range))
                {
                    builder.append(entry);
                    continue;
                }

                if (entry.start().compareTo(range.start()) < 0)
                    builder.append(entry.with(entry.start(), range.start()));
                if (entry.end().compareTo(range.end()) > 0)
                    builder.append(entry.with(range.end(), entry.end()));
            }
            return builder.build();
        }
    }

    private static boolean intersects(Entry entry, Range range)
    {
        return entry.end().compareTo(range.start()) > 0 && entry.start().compareTo(range.end()) < 0;
    }

    /**
     * For each range in {@code ranges}, report the parts that must be scanned on disk for keys that may be relevant:
     * those we know nothing about, and those with a relevant bound; except that we report directly the key of any
     * relevant bound that covers only that key.
     */
    public void forEachMaybeRelevant(Ranges ranges, Predicate<Entry> isRelevant, Consumer<Range> scan, Consumer<RoutingKey> load)
    {
        ScanBuilder scans = new ScanBuilder(scan);
        for (Range range : ranges)
        {
            RoutingKey cursor = range.start();
            for (Entry entry : this)
            {
                if (entry.end().compareTo(cursor) <= 0)
                    continue;
                if (entry.start().compareTo(range.end()) >= 0)
                    break;

                if (entry.start().compareTo(cursor) > 0)
                    scans.add(range.newRange(cursor, entry.start()));

                RoutingKey start = entry.start().compareTo(cursor) > 0 ? entry.start() : cursor;
                RoutingKey end = entry.end().compareTo(range.end()) < 0 ? entry.end() : range.end();
                if (isRelevant.test(entry))
                {
                    if (entry.isKey() && start.equals(entry.start()) && end.equals(entry.end())) load.accept(entry.end());
                    else scans.add(range.newRange(start, end));
                }
                cursor = end;
            }
            if (cursor.compareTo(range.end()) < 0)
                scans.add(range.newRange(cursor, range.end()));
        }
        scans.flush();
    }

    /**
     * Bound our size to {@code maxSize}: first merge contiguous runs of our lowest bounds, which are the most likely to
     * be irrelevant; then, if necessary, forget bounds that cover a single key; then any.
     */
    public MaxUncachedTxnId bound(int maxSize)
    {
        if (size() <= maxSize)
            return this;

        int target = Math.max(1, (maxSize * 3) / 4);
        TxnId[] maxes = new TxnId[size()];
        int count = 0;
        for (Entry entry : this)
            maxes[count++] = entry.max;
        Arrays.sort(maxes);
        TxnId cutoff = maxes[Math.max(0, size() - target)];

        List<Entry> entries = new ArrayList<>(size());
        Entry run = null;
        for (Entry entry : this)
        {
            boolean low = entry.max.compareTo(cutoff) <= 0;
            if (run != null && low && run.end().equals(entry.start()))
            {
                run = new Entry(run.start(), entry.end(), TxnId.max(run.max, entry.max));
                continue;
            }
            if (run != null)
                entries.add(run);
            run = low ? entry : null;
            if (!low)
                entries.add(entry);
        }
        if (run != null)
            entries.add(run);

        for (int pass = 0 ; pass < 2 && entries.size() > target ; ++pass)
        {
            boolean keysOnly = pass == 0;
            int excess = entries.size() - target;
            List<Entry> retain = new ArrayList<>(target);
            for (Entry entry : entries)
            {
                if (excess > 0 && (!keysOnly || entry.isKey())) --excess;
                else retain.add(entry);
            }
            entries = retain;
        }

        try (Builder builder = new Builder())
        {
            for (Entry entry : entries)
                builder.append(entry);
            return builder.build();
        }
    }

    private static TxnId plain(TxnId txnId)
    {
        return txnId.hasNonIdentityFlags() ? txnId.withoutNonIdentityFlags() : txnId;
    }

    private static class ScanBuilder
    {
        final Consumer<Range> scan;
        Range pending;

        ScanBuilder(Consumer<Range> scan)
        {
            this.scan = scan;
        }

        void add(Range range)
        {
            if (pending != null && pending.end().equals(range.start()))
            {
                pending = pending.newRange(pending.start(), range.end());
                return;
            }
            flush();
            pending = range;
        }

        void flush()
        {
            if (pending != null)
                scan.accept(pending);
            pending = null;
        }
    }

    public static class Builder extends BTreeReducingRangeMap.Builder<Entry, MaxUncachedTxnId>
    {
        @Override
        public MaxUncachedTxnId build()
        {
            return build(MaxUncachedTxnId::new);
        }
    }
}
