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

package org.apache.cassandra.locator;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A Replica represents an owning node for a copy of a portion of the token ring.
 *
 * It consists of:
 *  - the logical token range that is being replicated (i.e. for the first logical replica only, this will be equal
 *      to one of its owned portions of the token ring; all other replicas will have this token range also)
 *  - an endpoint (IP and port)
 *  - whether the range is replicated in full, or transiently (CASSANDRA-14404)
 *
 * In general, it is preferred to use a Replica to a Range&lt;Token&gt;, particularly when users of the concept depend on
 * knowledge of the full/transient status of the copy.
 *
 * That means you should avoid unwrapping and rewrapping these things and think hard about subtraction
 * and such and what the result is WRT to transientness. Definitely avoid creating fake Replicas with misinformation
 * about endpoints, ranges, or transientness.
 */
public final class Replica implements Comparable<Replica>
{
    private final Range<Token> range;
    private final InetAddressAndPort endpoint;
    private final boolean full;

    public Replica(InetAddressAndPort endpoint, Range<Token> range, boolean full)
    {
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(range);
        this.endpoint = endpoint;
        this.range = range;
        this.full = full;
    }

    public Replica(InetAddressAndPort endpoint, Token start, Token end, boolean full)
    {
        this(endpoint, new Range<>(start, end), full);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Replica replica = (Replica) o;
        return full == replica.full &&
               Objects.equals(endpoint, replica.endpoint) &&
               Objects.equals(range, replica.range);
    }

    @Override
    public int compareTo(Replica o)
    {
        int c = range.compareTo(o.range);
        if (c == 0)
            c = endpoint.compareTo(o.endpoint);
        if (c == 0)
            c =  Boolean.compare(full, o.full);
        return c;
    }

    public int hashCode()
    {
        return Objects.hash(endpoint, range, full);
    }

    @Override
    public String toString()
    {
        return (full ? "Full" : "Transient") +
                '(' + getEndpoint() + ',' + range + ')';
    }

    public final InetAddressAndPort getEndpoint()
    {
        return endpoint;
    }

    public boolean isLocal()
    {
        return endpoint.equals(FBUtilities.getBroadcastAddressAndPort());
    }

    public Range<Token> getRange()
    {
        return range;
    }

    public boolean isFull()
    {
        return full;
    }

    public final boolean isTransient()
    {
        return !isFull();
    }

    public ReplicaSet subtract(Replica that)
    {
        assert isFull() && that.isFull();  // FIXME: this
        Set<Range<Token>> ranges = range.subtract(that.range);
        ReplicaSet replicatedRanges = new ReplicaSet(ranges.size());
        for (Range<Token> range : ranges)
            replicatedRanges.add(new Replica(getEndpoint(), range, isFull()));
        return replicatedRanges;
    }

    /**
     * Subtract the ranges of the given replicas from the range of this replica,
     * returning a set of replicas with the endpoint and transient information of
     * this replica, and the ranges resulting from the subtraction.
     */
    public ReplicaSet subtractByRange(ReplicaCollection toSubtract)
    {
        if (isFull() && Iterables.all(toSubtract, Replica::isFull))
        {
            Set<Range<Token>> subtractedRanges = getRange().subtractAll(toSubtract.asRangeSet());
            ReplicaSet replicaSet = new ReplicaSet(subtractedRanges.size());
            for (Range<Token> range : subtractedRanges)
                replicaSet.add(new Replica(getEndpoint(), range, isFull()));
            return replicaSet;
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public boolean contains(Range<Token> that)
    {
        return getRange().contains(that);
    }

    public boolean intersectsOnRange(Replica replica)
    {
        return getRange().intersects(replica.getRange());
    }

    public Replica decorateSubrange(Range<Token> subrange)
    {
        Preconditions.checkArgument(range.contains(subrange));
        return new Replica(getEndpoint(), subrange, isFull());
    }

    public static Replica full(InetAddressAndPort endpoint, Range<Token> range)
    {
        return new Replica(endpoint, range, true);
    }

    public static Replica full(InetAddressAndPort endpoint, Token start, Token end)
    {
        return full(endpoint, new Range<>(start, end));
    }

    public static Replica trans(InetAddressAndPort endpoint, Range<Token> range)
    {
        return new Replica(endpoint, range, false);
    }

    public static Replica trans(InetAddressAndPort endpoint, Token start, Token end)
    {
        return trans(endpoint, new Range<>(start, end));
    }

}

