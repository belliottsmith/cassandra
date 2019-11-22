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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
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
        for (Pair<Range<Token>, Integer> lastRepairSuccess : successfulRepairs)
        {
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
        for (InvalidatedRepairedRange irr : invalidatedRepairs)
        {
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
     * Get the time of the last successful repair that the sstable was involved in
     *
     * Both first and last token need to have been involved in the same repair
     *
     * @return the last successful repair time for the sstable or Integer.MIN_VALUE if no repairs were found
     */
    public int getLastSuccessfulRepairTimeFor(SSTableReader sstable)
    {
        for (Pair<Range<Token>, Integer> repair : successfulRepairs)
        {
            Range<Token> range = repair.left;
            int repairTime = repair.right;
            if (range.contains(sstable.first.getToken()) && range.contains(sstable.last.getToken()))
            {
                if (!invalidatedRepairs.isEmpty())
                {
                    Bounds<Token> sstableBounds = new Bounds<>(sstable.first.getToken(), sstable.last.getToken());
                    for (InvalidatedRepairedRange irr : invalidatedRepairs)
                    {
                        if (irr.invalidatedAtSeconds > repairTime && irr.range.intersects(sstableBounds))
                        {
                            // The sstable range was (partly) invalidated after the last repair.
                            // If several invalidations intersect the sstable range, keep the oldest one
                            repairTime = Math.min(repairTime, irr.minLDTSeconds);
                        }
                    }
                }
                return repairTime;
            }
        }
        return Integer.MIN_VALUE;
    }

    public int gcBeforeForKey(ColumnFamilyStore cfs, DecoratedKey key, int fallbackGCBefore)
    {
        if (!DatabaseDescriptor.enableChristmasPatch() || !cfs.useRepairHistory())
            return fallbackGCBefore;

        return Math.min(getLastSuccessfulRepairTimeFor(key.getToken()), fallbackGCBefore);
    }

    public SuccessfulRepairTimeHolder withoutInvalidationRepairs()
    {
        return new SuccessfulRepairTimeHolder(successfulRepairs, ImmutableList.of());
    }
}
