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

package org.apache.cassandra.service.paxos;

import java.util.Collections;
import java.util.UUID;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.service.paxos.Ballot.none;
import static org.apache.cassandra.service.paxos.PaxosRepairHistory.trim;

public class PaxosRepairHistoryTest
{
    static
    {
        System.setProperty("cassandra.partitioner", Murmur3Partitioner.class.getName());
        assert DatabaseDescriptor.getPartitioner() instanceof Murmur3Partitioner;
    }

    private static final Token MIN_TOKEN = Murmur3Partitioner.instance.getMinimumToken();

    private static Token t(long t)
    {
        return new LongToken(t);
    }

    private static UUID b(int b)
    {
        return UUIDGen.getTimeUUID(b, 0, 0);
    }

    private static Range<Token> r(Token l, Token r)
    {
        return new Range<>(l, r);
    }

    private static Range<Token> r(long l, long r)
    {
        return r(t(l), t(r));
    }

    private static Pair<Token, UUID> pt(long t, int b)
    {
        return Pair.create(t(t), b(b));
    }

    private static Pair<Token, UUID> pt(long t, UUID b)
    {
        return Pair.create(t(t), b);
    }

    private static Pair<Token, UUID> pt(Token t, int b)
    {
        return Pair.create(t, b(b));
    }

    private static PaxosRepairHistory h(Pair<Token, UUID>... points)
    {
        int length = points.length + (points[points.length - 1].left == null ? 0 : 1);
        Token[] tokens = new Token[length - 1];
        UUID[] ballots = new UUID[length];
        for (int i = 0 ; i < length - 1 ; ++i)
        {
            tokens[i] = points[i].left;
            ballots[i] = points[i].right;
        }
        ballots[length - 1] = length == points.length ? points[length - 1].right : Ballot.none();
        return new PaxosRepairHistory(tokens, ballots);
    }

    static
    {
        assert t(100).equals(t(100));
        assert b(111).equals(b(111));
    }

    private static class Builder
    {
        PaxosRepairHistory history = PaxosRepairHistory.EMPTY;

        Builder add(UUID ballot, Range<Token>... ranges)
        {
            history = PaxosRepairHistory.add(history, Lists.newArrayList(ranges), ballot);
            return this;
        }

        Builder clear()
        {
            history = PaxosRepairHistory.EMPTY;
            return this;
        }
    }

    static Builder builder()
    {
        return new Builder();
    }

    @Test
    public void testAdd()
    {
        Builder builder = builder();
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 5)),
                            builder.add(b(5), r(10, 20), r(30, 40)).history);

        Assert.assertEquals(none(), builder.history.ballotForToken(t(0)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(10)));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(11)));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(20)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(21)));

        builder.clear();
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 6)),
                            builder.add(b(5), r(10, 20)).add(b(6), r(30, 40)).history);
        builder.clear();
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, 6), pt(40, 5)),
                            builder.add(b(5), r(10, 40)).add(b(6), r(20, 30)).history);

        builder.clear();
        Assert.assertEquals(h(pt(10, none()), pt(20, 6), pt(30, 5)),
                            builder.add(b(6), r(10, 20)).add(b(5), r(15, 30)).history);

        builder.clear();
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, 6)),
                            builder.add(b(5), r(10, 25)).add(b(6), r(20, 30)).history);
    }

    @Test
    public void testTrim()
    {
        Assert.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 5), pt(50, none()), pt(60, 5)),
                            trim(h(pt(0, none()), pt(70, 5)), Lists.newArrayList(r(10, 20), r(30, 40), r(50, 60))));

        Assert.assertEquals(h(pt(10, none()), pt(20, 5)),
                            trim(h(pt(0, none()), pt(20, 5)), Lists.newArrayList(r(10, 30))));

        Assert.assertEquals(h(pt(10, none()), pt(20, 5)),
                            trim(h(pt(10, none()), pt(30, 5)), Lists.newArrayList(r(0, 20))));
    }

    @Test
    public void testFullRange()
    {
        // test full range is collapsed
        Builder builder = builder();
        Assert.assertEquals(h(pt(null, 5)),
                            builder.add(b(5), r(MIN_TOKEN, MIN_TOKEN)).history);

        Assert.assertEquals(b(5), builder.history.ballotForToken(MIN_TOKEN));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(0)));
    }

    @Test
    public void testWrapAroundRange()
    {
        Builder builder = builder();
        Assert.assertEquals(h(pt(-100, 5), pt(100, none()), pt(null, 5)),
                            builder.add(b(5), r(100, -100)).history);

        Assert.assertEquals(b(5), builder.history.ballotForToken(MIN_TOKEN));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(-101)));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(-100)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(-99)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(0)));
        Assert.assertEquals(none(), builder.history.ballotForToken(t(100)));
        Assert.assertEquals(b(5), builder.history.ballotForToken(t(101)));
    }

    private static Token[] tks(long ... tks)
    {
        return LongStream.of(tks).mapToObj(LongToken::new).toArray(Token[]::new);
    }

    private static UUID[] uuids(String ... uuids)
    {
        return Stream.of(uuids).map(UUID::fromString).toArray(UUID[]::new);
    }

    @Test
    public void testRegression()
    {
        Assert.assertEquals(Ballot.none(), trim(
                new PaxosRepairHistory(
                        tks(-9223372036854775807L, -3952873730080618203L, -1317624576693539401L, 1317624576693539401L, 6588122883467697005L),
                        uuids("1382954c-1dd2-11b2-8fb2-f45d70d6d6d8", "138260a4-1dd2-11b2-abb2-c13c36b179e1", "1382951a-1dd2-11b2-1dd8-b7e242b38dbe", "138294fc-1dd2-11b2-83c4-43fb3a552386", "13829510-1dd2-11b2-f353-381f2ed963fa", "1382954c-1dd2-11b2-8fb2-f45d70d6d6d8")),
                Collections.singleton(new Range<>(new LongToken(-1317624576693539401L), new LongToken(1317624576693539401L))))
            .ballotForToken(new LongToken(-4208619967696141037L)));
    }
}
