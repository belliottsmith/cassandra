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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.cassandra.dht.Range.normalize;
import static org.apache.cassandra.utils.DiagnosticSnapshotService.checkIntersection;
import static org.apache.cassandra.utils.DiagnosticSnapshotService.isNormalized;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiagnosticSnapshotServiceTest
{
    @Test
    public void testWrapIsNotNormalized()
    {
        Range<Token> wrap = r(10, 1);
        assertFalse(isNormalized(Collections.singletonList(wrap)));
    }

    @Test
    public void testWrapIsNormalized()
    {
        Range<Token> wrap = r(10, 1);
        assertTrue(isNormalized(normalize(singleton(wrap))));
    }

    @Test
    public void testSortedIsNormalized()
    {
        Range<Token> r1 = r(10, 100);
        Range<Token> r2 = r(110, 200);
        assertTrue(isNormalized(asList(r1, r2)));
        assertFalse(isNormalized(asList(r2, r1)));
    }

    @Test
    public void testOverlapIsNormalized()
    {
        Range<Token> r1 = r(10, 100);
        Range<Token> r2 = r(50, 150);
        Range<Token> r3 = r(100, 200);

        assertTrue(isNormalized(asList(r1, r3)));
        assertFalse(isNormalized(asList(r1, r2)));
        assertFalse(isNormalized(asList(r1, r2, r3)));
    }

    @Test
    public void basicIntersectionTest()
    {
        List<Range<Token>> ranges = asList(r(1, 10),
                                           r(10, 20));
        assertTrue( checkIntersection(ranges, t(10), t(10)));
        assertFalse(checkIntersection(ranges, t(1),  t(1)));
        assertTrue( checkIntersection(ranges, t(20), t(20)));
        assertTrue( checkIntersection(ranges, t(1),  t(21)));

        ranges = asList(r(1, 10),
                        r(11, 20),
                        r(21, 30));

        assertFalse(checkIntersection(ranges, t(11), t(11)));
        assertTrue( checkIntersection(ranges, t(11), t(20)));

        ranges = asList(r(1, 10),
                        r(20, 30));
        assertFalse(checkIntersection(ranges, t(11), t(15)));
    }

    @Test
    public void randomIntersectionTest()
    {
        int iters = 200000;
        int trueCount = 0;

        for (int x = 0; x < iters; x++)
        {
            long seed = System.currentTimeMillis();
            Random random = new Random(seed);
            int tokenSpace = 1000;
            int rangeCount = 20;

            List<Token> rts = new ArrayList<>(rangeCount * 2);
            for (int i = 0; i < rangeCount * 2; i++)
                rts.add(t(random.nextInt(tokenSpace)));

            rts.sort(Comparator.naturalOrder());
            List<Range<Token>> rrs = new ArrayList<>(rangeCount + 1);
            for (int i = 0; i < rangeCount; i++)
                rrs.add(r(rts.get(i), rts.get(++i)));

            // maybe add wraparound range
            if (random.nextBoolean())
                rrs.add(r(rts.get(rts.size() - 1), rts.get(0)));

            long a = random.nextInt(tokenSpace);
            long b = random.nextInt(tokenSpace);
            Token first = t(Math.min(a, b));
            Token last = t(Math.max(a, b));
            List<Range<Token>> norm = Range.normalize(rrs);
            boolean slowCheck = slowCheck(norm, first, last);
            boolean newCheck = checkIntersection(norm, first, last);
            if (newCheck)
                trueCount++;
            assertEquals(Range.normalize(rrs) +" first="+first + " last="+last+ " seed = "+seed,
                         slowCheck, newCheck);
        }
        System.out.println("iters = "+iters + " trueCount = "+trueCount);

    }

    boolean slowCheck(List<Range<Token>> randomRanges, Token first, Token last)
    {
        Bounds<Token> sstableBounds = new Bounds<>(first, last);
        return sstableBounds.intersects(randomRanges);
    }

    Token t(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    Range<Token> r(long start, long end)
    {
        return r(t(start), t(end));
    }
    Range<Token> r(Token start, Token end)
    {
        return new Range<>(start, end);
    }
}
