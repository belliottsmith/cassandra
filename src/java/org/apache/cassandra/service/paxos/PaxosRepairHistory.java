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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import static java.lang.Math.min;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.service.paxos.Commit.latest;

public class PaxosRepairHistory
{
    public static final PaxosRepairHistory EMPTY = new PaxosRepairHistory(new Token[0], new UUID[] { Ballot.none() });
    private static final Token.TokenFactory TOKEN_FACTORY = DatabaseDescriptor.getPartitioner().getTokenFactory();
    private static final Token MIN_TOKEN = DatabaseDescriptor.getPartitioner().getMinimumToken();

    private final Token[] tokenInclusiveUpperBound;
    private final UUID[] ballotLowBound; // always one longer to capture values up to "MAX_VALUE" (which in some cases doesn't exist, as is infinite)

    PaxosRepairHistory(Token[] tokenInclusiveUpperBound, UUID[] ballotLowBound)
    {
        assert ballotLowBound.length == tokenInclusiveUpperBound.length + 1;
        this.tokenInclusiveUpperBound = tokenInclusiveUpperBound;
        this.ballotLowBound = ballotLowBound;
    }

    public String toString()
    {
        return "PaxosRepairHistory{" +
                IntStream.range(0, ballotLowBound.length)
                        .filter(i -> !Ballot.none().equals(ballotLowBound[i]))
                        .mapToObj(i -> range(i) + "=" + ballotLowBound[i])
                        .collect(Collectors.joining(", ")) + '}';
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaxosRepairHistory that = (PaxosRepairHistory) o;
        return Arrays.equals(ballotLowBound, that.ballotLowBound) && Arrays.equals(tokenInclusiveUpperBound, tokenInclusiveUpperBound);
    }

    public int hashCode()
    {
        return Arrays.hashCode(ballotLowBound);
    }

    UUID ballotForToken(Token token)
    {
        int idx = Arrays.binarySearch(tokenInclusiveUpperBound, token);
        if (idx < 0) idx = -1 - idx;
        return ballotLowBound[idx];
    }

    private RangeIterator rangeIterator()
    {
        return new RangeIterator();
    }

    private Range<Token> range(int i)
    {
        return new Range<>(tokenExclusiveLowerBound(i), tokenInclusiveUpperBound(i));
    }

    private Token tokenExclusiveLowerBound(int i)
    {
        return i == 0 ? MIN_TOKEN : tokenInclusiveUpperBound[i - 1];
    }

    private Token tokenInclusiveUpperBound(int i)
    {
        return i == tokenInclusiveUpperBound.length ? MIN_TOKEN : tokenInclusiveUpperBound[i];
    }


    private int size()
    {
        return tokenInclusiveUpperBound.length;
    }

    public List<ByteBuffer> toTupleBufferList()
    {
        List<ByteBuffer> tuples = new ArrayList<>(size() + 1);
        for (int i = 0 ; i < 1 + size() ; ++i)
            tuples.add(TupleType.buildValue(new ByteBuffer[] { TOKEN_FACTORY.toByteArray(tokenInclusiveUpperBound(i)), UUIDType.instance.decompose(ballotLowBound[i])}));
        return tuples;
    }

    public static PaxosRepairHistory fromTupleBufferList(List<ByteBuffer> tuples)
    {
        Token[] tokenInclusiveUpperBounds = new Token[tuples.size() - 1];
        UUID[] ballotLowBounds = new UUID[tuples.size()];
        for (int i = 0 ; i < tuples.size() ; ++i)
        {
            ByteBuffer[] split = TupleType.split(tuples.get(i), 2);
            if (i < tokenInclusiveUpperBounds.length)
                tokenInclusiveUpperBounds[i] = TOKEN_FACTORY.fromByteArray(split[0]);
            ballotLowBounds[i] = UUIDType.instance.compose(split[1]);
        }

        return new PaxosRepairHistory(tokenInclusiveUpperBounds, ballotLowBounds);
    }

    // append the item to the given list, modifying the underlying list
    // if the item makes previoud entries redundant

    public static PaxosRepairHistory merge(PaxosRepairHistory historyLeft, PaxosRepairHistory historyRight)
    {
        if (historyLeft == null)
            return historyRight;

        if (historyRight == null)
            return historyLeft;

        Builder builder = new Builder(historyLeft.size() + historyRight.size());

        RangeIterator left = historyRight.rangeIterator();
        RangeIterator right = historyLeft.rangeIterator();
        while (left.hasUpperBound() && right.hasUpperBound())
        {
            int cmp = left.tokenInclusiveUpperBound().compareTo(right.tokenInclusiveUpperBound());

            UUID ballot = latest(left.ballotLowBound(), right.ballotLowBound());
            if (cmp == 0)
            {
                builder.append(left.tokenInclusiveUpperBound(), ballot);
                left.next();
                right.next();
            }
            else
            {
                RangeIterator firstIter = cmp < 0 ? left : right;
                builder.append(firstIter.tokenInclusiveUpperBound(), ballot);
                firstIter.next();
            }
        }

        while (left.hasUpperBound())
        {
            builder.append(left.tokenInclusiveUpperBound(), left.ballotLowBound());
            left.next();
        }

        while (right.hasUpperBound())
        {
            builder.append(right.tokenInclusiveUpperBound(), right.ballotLowBound());
            right.next();
        }

        builder.appendLast(latest(left.ballotLowBound(), right.ballotLowBound()));
        return builder.build();
    }

    @VisibleForTesting
    static PaxosRepairHistory add(PaxosRepairHistory existing, Collection<Range<Token>> ranges, UUID ballot)
    {
        ranges = Range.normalize(ranges);
        Builder builder = new Builder(ranges.size() * 2);
        for (Range<Token> range : ranges)
        {
            // don't add a point for an opening min token, since it
            // effectively leaves the bottom of the range unbounded
            builder.appendMaybeMin(range.left, Ballot.none());
            builder.appendMaybeMax(range.right, ballot);
        }

        return merge(existing, builder.build());
    }

    /**
     * returns a copy of this PaxosRepairHistory limited to the ranges supplied, with all other ranges reporting Ballot.none()
     */
    @VisibleForTesting
    static PaxosRepairHistory trim(PaxosRepairHistory existing, Collection<Range<Token>> ranges)
    {
        Builder builder = new Builder(existing.size());

        ranges = Range.normalize(ranges);
        for (Range<Token> select : ranges)
        {
            RangeIterator intersects = existing.intersects(select);
            while (intersects.hasNext())
            {
                if (Ballot.none().equals(intersects.ballotLowBound()))
                {
                    intersects.next();
                    continue;
                }

                Token exclusiveLowerBound = maxExclusiveLowerBound(select.left, intersects.tokenExclusiveLowerBound());
                Token inclusiveUpperBound = minInclusiveUpperBound(select.right, intersects.tokenInclusiveUpperBound());
                assert exclusiveLowerBound.compareTo(inclusiveUpperBound) < 0 || inclusiveUpperBound.isMinimum();

                builder.appendMaybeMin(exclusiveLowerBound, Ballot.none());
                builder.appendMaybeMax(inclusiveUpperBound, intersects.ballotLowBound());
                intersects.next();
            }
        }

        return builder.build();
    }

    RangeIterator intersects(Range<Token> unwrapped)
    {
        int from = Arrays.binarySearch(tokenInclusiveUpperBound, unwrapped.left);
        if (from < 0) from = -1 - from; else ++from;
        int to = unwrapped.right.isMinimum() ? ballotLowBound.length - 1 : Arrays.binarySearch(tokenInclusiveUpperBound, unwrapped.right);
        if (to < 0) to = -1 - to;
        return new RangeIterator(from, min(1 + to, ballotLowBound.length));
    }

    private static Token maxExclusiveLowerBound(Token a, Token b)
    {
        return a.compareTo(b) < 0 ? b : a;
    }

    private static Token minInclusiveUpperBound(Token a, Token b)
    {
        if (!a.isMinimum() && !b.isMinimum()) return a.compareTo(b) <= 0 ? a : b;
        else if (!a.isMinimum()) return a;
        else if (!b.isMinimum()) return b;
        else return a;
    }

    public static final IVersionedSerializer<PaxosRepairHistory> serializer = new IVersionedSerializer<PaxosRepairHistory>()
    {
        public void serialize(PaxosRepairHistory history, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(history.size());
            for (int i = 0; i < history.size() ; ++i)
            {
                Token.serializer.serialize(history.tokenInclusiveUpperBound[i], out, version);
                UUIDSerializer.serializer.serialize(history.ballotLowBound[i], out, version);
            }
            UUIDSerializer.serializer.serialize(history.ballotLowBound[history.size()], out, version);
        }

        public PaxosRepairHistory deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = (int) in.readUnsignedVInt();
            Token[] tokenInclusiveUpperBounds = new Token[size];
            UUID[] ballotLowBounds = new UUID[size + 1];
            for (int i = 0; i < size; i++)
            {
                tokenInclusiveUpperBounds[i] = Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version);
                ballotLowBounds[i] = UUIDSerializer.serializer.deserialize(in, version);
            }
            ballotLowBounds[size] = UUIDSerializer.serializer.deserialize(in, version);
            return new PaxosRepairHistory(tokenInclusiveUpperBounds, ballotLowBounds);
        }

        public long serializedSize(PaxosRepairHistory history, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(history.size());
            for (int i = 0; i < history.size() ; ++i)
            {
                size += Token.serializer.serializedSize(history.tokenInclusiveUpperBound[i], version);
                size += UUIDSerializer.serializer.serializedSize(history.ballotLowBound[i], version);
            }
            size += UUIDSerializer.serializer.serializedSize(history.ballotLowBound[history.size()], version);
            return size;
        }
    };

    class RangeIterator
    {
        final int end;
        int i;

        RangeIterator()
        {
            this.end = ballotLowBound.length;
        }

        RangeIterator(int from, int to)
        {
            this.i = from;
            this.end = to;
        }

        boolean hasNext()
        {
            return i < end;
        }

        boolean hasUpperBound()
        {
            return i < tokenInclusiveUpperBound.length;
        }

        void next()
        {
            ++i;
        }

        Token tokenExclusiveLowerBound()
        {
            return PaxosRepairHistory.this.tokenExclusiveLowerBound(i);
        }

        Token tokenInclusiveUpperBound()
        {
            return PaxosRepairHistory.this.tokenInclusiveUpperBound(i);
        }

        UUID ballotLowBound()
        {
            return ballotLowBound[i];
        }
    }

    static class Builder
    {
        final List<Token> tokenInclusiveUpperBounds;
        final List<UUID> ballotLowBounds;

        Builder(int capacity)
        {
            this.tokenInclusiveUpperBounds = new ArrayList<>(capacity);
            this.ballotLowBounds = new ArrayList<>(capacity + 1);
        }

        void appendMaybeMin(Token inclusiveLowBound, UUID ballotLowBound)
        {
            if (inclusiveLowBound.isMinimum())
                assert ballotLowBound.equals(Ballot.none()) && ballotLowBounds.isEmpty();
            else
                append(inclusiveLowBound, ballotLowBound);
        }

        void appendMaybeMax(Token inclusiveLowBound, UUID ballotLowBound)
        {
            if (inclusiveLowBound.isMinimum())
                appendLast(ballotLowBound);
            else
                append(inclusiveLowBound, ballotLowBound);
        }

        void append(Token inclusiveLowBound, UUID ballotLowBound)
        {
            assert tokenInclusiveUpperBounds.size() == ballotLowBounds.size();
            int tailIdx = tokenInclusiveUpperBounds.size() - 1;
            boolean sameAsTailToken = tailIdx >= 0 && inclusiveLowBound.equals(tokenInclusiveUpperBounds.get(tailIdx));
            boolean sameAsTailBallot = tailIdx >= 0 && ballotLowBound.equals(ballotLowBounds.get(tailIdx));
            if (sameAsTailToken || sameAsTailBallot)
            {
                if (sameAsTailBallot)
                    tokenInclusiveUpperBounds.set(tailIdx, inclusiveLowBound);
                else if (isAfter(ballotLowBound, ballotLowBounds.get(tailIdx)))
                    ballotLowBounds.set(tailIdx, ballotLowBound);
            }
            else
            {
                tokenInclusiveUpperBounds.add(inclusiveLowBound);
                ballotLowBounds.add(ballotLowBound);
            }
        }

        void appendLast(UUID ballotLowBound)
        {
            ballotLowBounds.add(ballotLowBound);
        }

        PaxosRepairHistory build()
        {
            if (tokenInclusiveUpperBounds.size() == ballotLowBounds.size())
                ballotLowBounds.add(Ballot.none());
            return new PaxosRepairHistory(tokenInclusiveUpperBounds.toArray(new Token[0]), ballotLowBounds.toArray(new UUID[0]));
        }
    }
}
