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

package org.apache.cassandra.db.rows;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;

public class EncodingStatsTest
{
    private UnfilteredRowIterator emptyWithStats(final EncodingStats stats) {
        return new UnfilteredRowIterator()
        {

            public boolean hasNext()
            {
                return false;
            }

            public Unfiltered next()
            {
                return null;
            }

            public void close()
            {

            }

            public TableMetadata metadata()
            {
                return null;
            }

            public boolean isReverseOrder()
            {
                return false;
            }

            public RegularAndStaticColumns columns()
            {
                return null;
            }

            public DecoratedKey partitionKey()
            {
                return null;
            }

            public Row staticRow()
            {
                return null;
            }

            public DeletionTime partitionLevelDeletion()
            {
                return null;
            }

            public EncodingStats stats()
            {
                return stats;
            }
        };
    }

    @Test
    public void testCollectWithNoStats()
    {
        EncodingStats none = EncodingStats.collect(Lists.newArrayList(
            emptyWithStats(EncodingStats.NO_STATS),
            emptyWithStats(EncodingStats.NO_STATS),
            emptyWithStats(EncodingStats.NO_STATS)
        ));
        Assert.assertEquals(none, EncodingStats.NO_STATS);
    }

    @Test
    public void testCollectWithNoStatsWithEmpty()
    {
        EncodingStats none = EncodingStats.collect(Lists.newArrayList(
            emptyWithStats(EncodingStats.NO_STATS),
            emptyWithStats(EncodingStats.NO_STATS),
            emptyWithStats(new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME, 0))
        ));
        Assert.assertEquals(none, EncodingStats.NO_STATS);
    }

    @Test
    public void testCollectWithNoStatsWithTimestamp()
    {
        EncodingStats single = new EncodingStats(1, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats result = EncodingStats.collect(Lists.newArrayList(
            emptyWithStats(EncodingStats.NO_STATS),
            emptyWithStats(EncodingStats.NO_STATS),
            emptyWithStats(single),
            emptyWithStats(EncodingStats.NO_STATS)
        ));
        Assert.assertEquals(single, result);
    }

    @Test
    public void testCollectWithNoStatsWithExpires()
    {
        EncodingStats single = new EncodingStats(LivenessInfo.NO_TIMESTAMP, 1, 0);
        EncodingStats result = EncodingStats.collect(Lists.newArrayList(
        emptyWithStats(EncodingStats.NO_STATS),
        emptyWithStats(single),
        emptyWithStats(EncodingStats.NO_STATS)
        ));
        Assert.assertEquals(single, result);
    }

    @Test
    public void testCollectWithNoStatsWithTTL()
    {
        EncodingStats single = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME, 1);
        EncodingStats result = EncodingStats.collect(Lists.newArrayList(
            emptyWithStats(EncodingStats.NO_STATS),
            emptyWithStats(single),
            emptyWithStats(EncodingStats.NO_STATS)
        ));
        Assert.assertEquals(single, result);
    }

    @Test
    public void testCollectOneEach()
    {
        EncodingStats tsp = new EncodingStats(1, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats exp = new EncodingStats(LivenessInfo.NO_TIMESTAMP, 1, 0);
        EncodingStats ttl = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME, 1);
        EncodingStats result = EncodingStats.collect(Lists.newArrayList(
        emptyWithStats(tsp),
        emptyWithStats(exp),
        emptyWithStats(ttl)
        ));
        Assert.assertEquals(new EncodingStats(1, 1, 1), result);
    }

    @Test
    public void testTimestamp()
    {
        EncodingStats one = new EncodingStats(1, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats two = new EncodingStats(2, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats thr = new EncodingStats(3, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats result = EncodingStats.collect(Lists.newArrayList(
            emptyWithStats(one),
            emptyWithStats(two),
            emptyWithStats(thr)
        ));
        Assert.assertEquals(one, result);
    }

    @Test
    public void testExpires()
    {
        EncodingStats one = new EncodingStats(LivenessInfo.NO_TIMESTAMP,1, 0);
        EncodingStats two = new EncodingStats(LivenessInfo.NO_TIMESTAMP,2, 0);
        EncodingStats thr = new EncodingStats(LivenessInfo.NO_TIMESTAMP,3, 0);
        EncodingStats result = EncodingStats.collect(Lists.newArrayList(
            emptyWithStats(one),
            emptyWithStats(two),
            emptyWithStats(thr)
        ));
        Assert.assertEquals(one, result);
    }

    @Test
    public void testTTL()
    {
        EncodingStats one = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME,1);
        EncodingStats two = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME,2);
        EncodingStats thr = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME,3);
        EncodingStats result = EncodingStats.collect(Lists.newArrayList(
            emptyWithStats(thr),
            emptyWithStats(one),
            emptyWithStats(two)
        ));
        Assert.assertEquals(one, result);
    }

    @Test
    public void testEncodingStatsCollectWithNone()
    {
        qt().forAll(longs().between(Long.MIN_VALUE+1, Long.MAX_VALUE),
                    integers().between(0, Integer.MAX_VALUE-1),
                    integers().allPositive())
            .asWithPrecursor(EncodingStats::new)
            .check((timestamp, expires, ttl, stats) ->
                   {
                       EncodingStats result = EncodingStats.collect(Lists.newArrayList(
                           emptyWithStats(EncodingStats.NO_STATS),
                           emptyWithStats(stats),
                           emptyWithStats(EncodingStats.NO_STATS)
                       ));
                       return result.minTTL == ttl
                              && result.minLocalDeletionTime == expires
                              && result.minTimestamp == timestamp;
                   });
    }

}
