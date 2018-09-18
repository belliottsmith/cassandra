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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * A ReplicaCollection where all Replica are required to cover a range that fully contains the token() defined in the builder().
 * Endpoints are guaranteed to be unique; on construction, this is enforced unless optionally silenced (in which case
 * only the first occurrence makes the cut).
 */
public class EndpointsForToken extends Endpoints<EndpointsForToken>
{
    private final Token token;
    private EndpointsForToken(Token token, ReplicaList list, boolean isSnapshot)
    {
        this(token, list, isSnapshot, null);
    }

    EndpointsForToken(Token token, ReplicaList list, boolean isSnapshot, ReplicaMap<InetAddressAndPort> byEndpoint)
    {
        super(list, isSnapshot, byEndpoint);
        this.token = token;
        assert token != null;
    }

    public Token token()
    {
        return token;
    }

    @Override
    public Mutable newMutable(int initialCapacity)
    {
        return new Mutable(token, initialCapacity);
    }

    @Override
    public EndpointsForToken self()
    {
        return this;
    }

    @Override
    protected EndpointsForToken snapshot(ReplicaList newList)
    {
        if (newList.isEmpty()) return empty(token);
        ReplicaMap<InetAddressAndPort> byEndpoint = null;
        if (this.byEndpoint != null && list.isSubList(newList))
            byEndpoint = this.byEndpoint.isSubList(newList);
        return new EndpointsForToken(token, newList, true, byEndpoint);
    }

    public static class Mutable extends EndpointsForToken implements ReplicaCollection.Mutable<EndpointsForToken>
    {
        boolean hasSnapshot;
        public Mutable(Token token) { this(token, 0); }
        public Mutable(Token token, int capacity) { this(token, new ReplicaList(capacity)); }
        private Mutable(Token token, ReplicaList list) { super(token, list, false, endpointMap(list)); }

        public void add(Replica replica, Conflict ignoreConflict)
        {
            if (hasSnapshot) throw new IllegalStateException();
            Preconditions.checkNotNull(replica);
            if (!replica.range().contains(super.token))
                throw new IllegalArgumentException("Replica " + replica + " does not contain " + super.token);

            if (!super.byEndpoint.internalPutIfAbsent(replica, list.size()))
            {
                switch (ignoreConflict)
                {
                    case DUPLICATE:
                        if (byEndpoint().get(replica.endpoint()).equals(replica))
                            break;
                    case NONE:
                        throw new IllegalArgumentException("Conflicting replica added (expected unique endpoints): "
                                + replica + "; existing: " + byEndpoint().get(replica.endpoint()));
                    case ALL:
                }
                return;
            }

            list.add(replica);
        }

        @Override
        public Map<InetAddressAndPort, Replica> byEndpoint()
        {
            // our internal map is modifiable, but it is unsafe to modify the map externally
            // it would be possible to implement a safe modifiable map, but it is probably not valuable
            return Collections.unmodifiableMap(super.byEndpoint());
        }

        private EndpointsForToken get(boolean isSnapshot)
        {
            return new EndpointsForToken(super.token, super.list, isSnapshot, super.byEndpoint);
        }

        public EndpointsForToken asImmutableView()
        {
            return get(false);
        }

        public EndpointsForToken asSnapshot()
        {
            hasSnapshot = true;
            return get(true);
        }
    }

    public static class Builder extends ReplicaCollection.Builder<EndpointsForToken, Mutable, EndpointsForToken.Builder>
    {
        public Builder(Token token) { this(token, 0); }
        public Builder(Token token, int capacity) { super (new Mutable(token, capacity)); }
    }

    public static Builder builder(Token token)
    {
        return new Builder(token);
    }
    public static Builder builder(Token token, int capacity)
    {
        return new Builder(token, capacity);
    }

    public static EndpointsForToken empty(Token token)
    {
        return new EndpointsForToken(token, EMPTY_LIST, true, EMPTY_MAP);
    }

    public static EndpointsForToken of(Token token, Replica replica)
    {
        // we only use ArrayList or ArrayList.SubList, to ensure callsites are bimorphic
        ReplicaList one = new ReplicaList(1);
        one.add(replica);
        // we can safely use singletonMap, as we only otherwise use LinkedHashMap
        return new EndpointsForToken(token, one, true, endpointMap(one));
    }

    public static EndpointsForToken of(Token token, Replica ... replicas)
    {
        return copyOf(token, Arrays.asList(replicas));
    }

    public static EndpointsForToken copyOf(Token token, Collection<Replica> replicas)
    {
        if (replicas.isEmpty()) return empty(token);
        return builder(token, replicas.size()).addAll(replicas).build();
    }
}
