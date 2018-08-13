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
import org.apache.cassandra.dht.Token;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EndpointsForToken extends Endpoints<EndpointsForToken>
{
    private static final EndpointsForToken EMPTY = new EndpointsForToken(null, Collections.emptyList());

    private final Token token;
    private EndpointsForToken(Token token, List<Replica> list)
    {
        super(list);
        this.token = token;
    }

    EndpointsForToken(Token token, List<Replica> list, Map<InetAddressAndPort, Replica> byEndpoint)
    {
        super(list, byEndpoint);
        this.token = token;
    }

    @Override
    public Mutable newMutable(int initialCapacity)
    {
        return new Mutable(token, initialCapacity);
    }

    public Token token()
    {
        return token;
    }

    @Override
    public EndpointsForToken asImmutableView()
    {
        return this;
    }

    @Override
    protected EndpointsForToken subClone(List<Replica> subList)
    {
        return subList == null ? this.asImmutableView() : new EndpointsForToken(token, subList);
    }

    public static class Mutable extends EndpointsForToken implements ReplicaCollection.Mutable<EndpointsForToken>
    {
        public Mutable(Token token) { this(token, 0); }
        public Mutable(Token token, int capacity) { super(token, new ArrayList<>(capacity), new LinkedHashMap<>()); }

        public boolean add(Replica replica)
        {
            Preconditions.checkNotNull(replica);
            if (!replica.range().contains(super.token))
                throw new IllegalArgumentException("Mismatching ranges added to EndpointsForRange.Builder: " + replica + " does not contain " + super.token);

            Replica prev = super.byEndpoint.put(replica.endpoint(), replica);
            if (prev != null)
            {
                super.byEndpoint.put(replica.endpoint(), prev); // restore prev
                return false;
            }

            list.add(replica);
            return true;
        }

        public EndpointsForToken asImmutableView()
        {
            return new EndpointsForToken(super.token, super.list, Collections.unmodifiableMap(super.byEndpoint));
        }
    }

    public static class Builder extends ReplicaCollection.Builder<EndpointsForToken, Mutable, EndpointsForToken.Builder>
    {
        public Builder(Token token) { this(token, 0); }
        public Builder(Token token, int capacity) { super (new Mutable(token, capacity)); }
        public boolean containsEndpoint(InetAddressAndPort endpoint)
        {
            return mutable.asImmutableView().byEndpoint.containsKey(endpoint);
        }
    }

    public static Builder builder(Token token)
    {
        return new Builder(token);
    }
    public static Builder builder(Token token, int capacity)
    {
        return new Builder(token, capacity);
    }

    public static EndpointsForToken empty()
    {
        return EMPTY;
    }

    public static EndpointsForToken of(Token token, Replica replica)
    {
        return new EndpointsForToken(token, Collections.singletonList(replica), Collections.singletonMap(replica.endpoint(), replica));
    }

    public static EndpointsForToken of(Token token, Replica ... replicas)
    {
        return copyOf(token, Arrays.asList(replicas));
    }

    public static EndpointsForToken copyOf(Token token, Collection<Replica> replicas)
    {
        if (replicas.isEmpty()) return empty();
        return builder(token, replicas.size()).addAll(replicas).build();
    }
}
