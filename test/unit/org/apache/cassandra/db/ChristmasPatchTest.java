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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore.SuccessfulRepairTimeHolder;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.ColumnFamilyStore.updateLastSuccessfulRepair;

public class ChristmasPatchTest
{
    SuccessfulRepairTimeHolder holder = new SuccessfulRepairTimeHolder(ImmutableList.of());

    private static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
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
        Assert.assertNull(newHolder);
    }
}

