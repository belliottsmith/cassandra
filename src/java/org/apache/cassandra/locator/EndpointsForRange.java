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

import com.google.common.base.Preconditions;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.all;

public class EndpointsForRange extends Endpoints<EndpointsForRange>
{
    private static final EndpointsForRange EMPTY = new EndpointsForRange(null, Collections.emptyList());

    private final Range<Token> range;

    private EndpointsForRange(Range<Token> range, List<Replica> list)
    {
        super(list);
        this.range = range;
    }

    private EndpointsForRange(Range<Token> range, List<Replica> list, Map<InetAddressAndPort, Replica> byEndpoint)
    {
        super(list, byEndpoint);
        this.range = range;
    }

    @Override
    public Mutable newMutable(int initialCapacity)
    {
        return new Mutable(range, initialCapacity);
    }

    @Override
    public EndpointsForRange asImmutableView()
    {
        return this;
    }

    public EndpointsForToken forToken(Token token)
    {
        if (range != null && !range.contains(token))
            throw new IllegalArgumentException(token + " is not contained within " + range);
        return new EndpointsForToken(token, list, byEndpoint);
    }

    @Override
    protected EndpointsForRange subClone(List<Replica> subList)
    {
        return subList == null ? this.asImmutableView() : new EndpointsForRange(range, subList);
    }

    public static class Mutable extends EndpointsForRange implements ReplicaCollection.Mutable<EndpointsForRange>
    {
        public Mutable(Range<Token> range) { this(range, 0); }
        public Mutable(Range<Token> range, int capacity) { super(range, new ArrayList<>(capacity), new LinkedHashMap<>()); }

        public boolean add(Replica replica)
        {
            Preconditions.checkNotNull(replica);
            if (!replica.range().contains(super.range))
                throw new IllegalArgumentException("Mismatching ranges added to EndpointsForRange.Builder: " + replica + " does not contain " + super.range);

            Replica prev = super.byEndpoint.put(replica.endpoint(), replica);
            if (prev != null)
            {
                super.byEndpoint.put(replica.endpoint(), prev); // restore prev
                return false;
            }

            list.add(replica);
            return true;
        }

        public EndpointsForRange asImmutableView()
        {
            return new EndpointsForRange(super.range, super.list, Collections.unmodifiableMap(super.byEndpoint));
        }
    }

    public static class Builder extends ReplicaCollection.Builder<EndpointsForRange, Mutable, EndpointsForRange.Builder>
    {
        public Builder(Range<Token> range) { this(range, 0); }
        public Builder(Range<Token> range, int capacity) { super (new Mutable(range, capacity)); }
        public boolean containsEndpoint(InetAddressAndPort endpoint)
        {
            return mutable.asImmutableView().byEndpoint.containsKey(endpoint);
        }
    }

    public static Builder builder(Range<Token> range)
    {
        return new Builder(range);
    }
    public static Builder builder(Range<Token> range, int capacity)
    {
        return new Builder(range, capacity);
    }

    public static EndpointsForRange empty()
    {
        return EMPTY;
    }

    public static EndpointsForRange empty(Range<Token> range)
    {
        return new EndpointsForRange(range, Collections.emptyList(), Collections.emptyMap());
    }

    public static EndpointsForRange of(Replica replica)
    {
        return new EndpointsForRange(replica.range(), Collections.singletonList(replica), Collections.singletonMap(replica.endpoint(), replica));
    }

    public static EndpointsForRange of(Replica ... replicas)
    {
        return copyOf(Arrays.asList(replicas));
    }

    public static EndpointsForRange copyOf(Collection<Replica> replicas)
    {
        if (replicas.isEmpty()) return empty();
        Range<Token> range = replicas.iterator().next().range();
        assert all(replicas, r -> range.equals(r.range()));
        return builder(range, replicas.size()).addAll(replicas).build();
    }
}
