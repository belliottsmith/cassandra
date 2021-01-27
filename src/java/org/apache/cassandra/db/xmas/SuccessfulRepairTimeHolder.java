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

package org.apache.cassandra.db.xmas;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

/**
 * Holds an immutable list of successfully repaired ranges, sorted by newest repair
 *
 * We 'snapshot' the list when we start compaction to make sure we don't drop any tombstones
 * that were marked repaired after the compaction started (because we might not include the newly streamed-in
 * sstables in the overlap calculation)
 */
public class SuccessfulRepairTimeHolder
{
    @VisibleForTesting
    public final ImmutableList<Pair<Range<Token>, Integer>> successfulRepairs;
    public final ImmutableList<InvalidatedRepairedRange> invalidatedRepairs;

    public static final SuccessfulRepairTimeHolder EMPTY = new SuccessfulRepairTimeHolder(ImmutableList.of(), ImmutableList.of());

    public SuccessfulRepairTimeHolder(ImmutableList<Pair<Range<Token>, Integer>> successfulRepairs,
                                      ImmutableList<InvalidatedRepairedRange> invalidatedRepairs)
    {
        // we don't need to copy here - the list is never updated, it is swapped out
        this.successfulRepairs = successfulRepairs;
        this.invalidatedRepairs = invalidatedRepairs;
    }

    // todo: there is a possible compaction performance improvment here - we could sort the successfulRepairs by
    // range instead and binary search or use the fact that the tokens sent in to this method are increasing - we would
    // have to make sure the successfulRepairs are non-overlapping for this
    public int getLastSuccessfulRepairTimeFor(Token t)
    {
        assert t != null;
        int lastRepairTime = Integer.MIN_VALUE;
        // successfulRepairs are sorted by last repair time - this means that if we find a range that contains the
        // token, it is guaranteed to be the newest repair for that token
        for (int i = 0, isize = successfulRepairs.size(); i < isize; i++)
        {
            Pair<Range<Token>, Integer> lastRepairSuccess = successfulRepairs.get(i);
            if (lastRepairSuccess.left.contains(t))
            {
                lastRepairTime = lastRepairSuccess.right;
                break;
            }
        }
        if (lastRepairTime == Integer.MIN_VALUE)
            return lastRepairTime;

        // now we need to check if a range that contains this token has been invalidated by nodetool import:
        // invalidatedRepairs is sorted by smallest local deletion time first (ie, we keep the oldest tombstone ldt
        // first in the list since this is what we are interested in here)
        for (int i = 0, isize = invalidatedRepairs.size(); i < isize; i++)
        {
            InvalidatedRepairedRange irr = invalidatedRepairs.get(i);
            // if the nodetool import was run *after* the last repair time and the range repaired contains
            // the token, it is invalid and we must use the min ldt as a last repair time.
            if (irr.invalidatedAtSeconds > lastRepairTime && irr.range.contains(t))
            {
                return irr.minLDTSeconds;
            }
        }
        return lastRepairTime;
    }

    /**
     * Get the time of the oldest repair that made this sstable fully repaired
     *
     * @return the last successful repair time for the sstable or Integer.MIN_VALUE if the sstable has not been completely covered by repair
     */
    public int getFullyRepairedTimeFor(SSTableReader sstable)
    {
        /*
        Idea is that we subtract the time-sorted repaired ranges from the sstable bound until it is gone

        Result being that the last range we subtract before the sstable bounds are gone is the oldest repair that made the
        sstable fully repaired.
         */
        Bounds<Token> sstableBound = new Bounds<>(sstable.first.getToken(), sstable.last.getToken());
        List<AbstractBounds<Token>> sstableBounds = new ArrayList<>();
        sstableBounds.add(sstableBound);
        for (int i = 0, isize = successfulRepairs.size(); i < isize; i++)
        {
            Pair<Range<Token>, Integer> intersectingRepair = successfulRepairs.get(i);
            Range<Token> repairedRange = intersectingRepair.left;
            int repairTime = intersectingRepair.right;

            for (Range<Token> unwrappedRange : repairedRange.unwrap()) // avoid handling wrapping ranges in subtract below
                sstableBounds = subtract(sstableBounds, unwrappedRange);

            if (sstableBounds.isEmpty())
            {
                for (int j = 0, jsize = invalidatedRepairs.size(); j < jsize; j++)
                {
                    InvalidatedRepairedRange irr = invalidatedRepairs.get(j);
                    if (irr.invalidatedAtSeconds > repairTime && irr.range.intersects(sstableBound))
                    {
                        // The sstable range was (partly) invalidated after the last repair.
                        // If several invalidations intersect the sstable range, keep the oldest one
                        repairTime = Math.min(repairTime, irr.minLDTSeconds);
                    }
                }
                return repairTime;
            }
        }

        return Integer.MIN_VALUE;
    }

    @VisibleForTesting
    public static List<AbstractBounds<Token>> subtract(List<AbstractBounds<Token>> sstableBounds, Range<Token> repairedRange)
    {
        List<AbstractBounds<Token>> result = new ArrayList<>();
        for (AbstractBounds<Token> sstableBound : sstableBounds)
            result.addAll(subtract(sstableBound, repairedRange));
        return result;
    }

    @VisibleForTesting
    public static List<AbstractBounds<Token>> subtract(AbstractBounds<Token> bounds, Range<Token> range)
    {
        assert bounds instanceof Range || bounds instanceof Bounds : "When subtracting a Range from a Bounds we can only ever create a new Range or a new Bounds";
        /*
        bounds:  |--------|
         range:                |----|
         */
        if (!range.intersects(bounds))
            return Collections.singletonList(bounds);

        int leftComparison = range.left.compareTo(bounds.left);
        int rightComparison = range.right.compareTo(bounds.right);

        if (leftComparison < 0 && rightComparison >= 0)
        {
            /*
            bounds:   |-------|
             range: |------------|
             - range is end-inclusive so if bounds.right == range.right it is always removed
             - if range.left == bounds.left we might need to keep that token: [10, 15] - (10, 17] = [10, 10]
             -                                                           but: (10, 15] - (10, 17] = empty
             */
            return Collections.emptyList();
        }
        else if (leftComparison < 0)
        {
            /*
             bounds:    |-------|
              range: |-----|
            */
            AbstractBounds<Token> res = bounds(range.right,
                                               bounds.right,
                                               false, // Range.isEndInclusive is always true -> the result should be left-exclusive
                                               bounds.isEndInclusive());
            if (res != null)
                return Collections.singletonList(res);
            return Collections.emptyList();
        }
        else if (rightComparison < 0)
        {
            /*
            bounds:    |------|
             range:      |--|
             */
            List<AbstractBounds<Token>> results = new ArrayList<>(2);
            AbstractBounds<Token> res = bounds(bounds.left,
                                               range.left,
                                               bounds.isStartInclusive(),
                                               true); // Range.isStartInclusive is always false -> the first result should be right-inclusive
            if (res != null) results.add(res);
            res = bounds(range.right,
                         bounds.right,
                         false, // Range.isEndInclusive is always true -> the second result should be left-exclusive
                         bounds.isEndInclusive());
            if (res != null) results.add(res);
            return results;
        }
        else
        {
            /*
            bounds:    |------|
             range:       |------|
             */
            AbstractBounds<Token> res = bounds(bounds.left,
                                               range.left,
                                               bounds.isStartInclusive(),
                                               true); // Range.isStartInclusive is alwayse false -> result should be right-inclusive
            if (res != null)
                return Collections.singletonList(res);
            return Collections.emptyList();
        }
    }

    private static AbstractBounds<Token> bounds(Token left, Token right, boolean leftInclusive, boolean rightInclusive)
    {
        if (leftInclusive && rightInclusive)
            return new Bounds<>(left, right);

        // only case we generate bounds with start == end is for Bounds:
        if (left.equals(right))
            return null;

        if (rightInclusive)
            return new Range<>(left, right);

        throw new UnsupportedOperationException("We currently only support Bounds and Range");
    }

    public int gcBeforeForKey(ColumnFamilyStore cfs, DecoratedKey key, int fallbackGCBefore)
    {
        if (!cfs.useRepairHistory() || cfs.isChristmasPatchDisabled())
            return fallbackGCBefore;

        return Math.min(getLastSuccessfulRepairTimeFor(key.getToken()), fallbackGCBefore);
    }

    public SuccessfulRepairTimeHolder withoutInvalidationRepairs()
    {
        return new SuccessfulRepairTimeHolder(successfulRepairs, ImmutableList.of());
    }
}
