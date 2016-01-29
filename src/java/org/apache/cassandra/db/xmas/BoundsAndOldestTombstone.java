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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class BoundsAndOldestTombstone
{
    private final List<Pair<Bounds<Token>, Integer>> boundsAndOldestTombstones;

    public BoundsAndOldestTombstone(Collection<SSTableReader> sstables)
    {
        boundsAndOldestTombstones = sstables.stream()
                                            .map(sstable ->
                                                 Pair.create(new Bounds<>(sstable.first.getToken(),
                                                                          sstable.last.getToken()),
                                                             sstable.getMinLocalDeletionTime()))
                                            .collect(Collectors.toList());
    }

    /**
     * Finds the local deletion time of the oldest tombstone among the sstables which intersects repairedRange
     * and the repairTime is newer than the tombstone, Integer.MAX_VALUE if none found.
     */
    public int getOldestIntersectingTombstoneLDT(Range<Token> repairedRange, int repairTime)
    {
        int minLDT = Integer.MAX_VALUE;
        for (Pair<Bounds<Token>, Integer> boundsAndOldestTombstone : boundsAndOldestTombstones)
        {
            Bounds<Token> sstableBounds = boundsAndOldestTombstone.left;
            int sstableMinLDT = boundsAndOldestTombstone.right;
            if (repairedRange.intersects(sstableBounds) && sstableMinLDT < repairTime)
                minLDT = Math.min(minLDT, sstableMinLDT);
        }
        return minLDT;
    }
}
