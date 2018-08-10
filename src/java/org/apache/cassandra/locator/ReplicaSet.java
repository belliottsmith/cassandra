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

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collector;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

// warning: equals() depends on class identity; if extending, be sure to assess this
public final class ReplicaSet extends ReplicaCollection
{
    static final ReplicaSet EMPTY = new ReplicaSet(ImmutableSet.of());

    public static ReplicaSet empty()
    {
        return EMPTY;
    }

    private static final Set<Collector.Characteristics> SET_COLLECTOR_CHARACTERISTICS = ImmutableSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH);
    public static final Collector<Replica, ReplicaSet, ReplicaSet> COLLECTOR = ReplicaCollection.collector(SET_COLLECTOR_CHARACTERISTICS, ReplicaSet::new);

    private final Set<Replica> replicaSet;

    public ReplicaSet()
    {
        this(new HashSet<>());
    }

    public ReplicaSet(int expectedSize)
    {
        this(new HashSet<>(expectedSize));
    }

    public ReplicaSet(ReplicaCollection replicas)
    {
        this(new HashSet<>(replicas.asCollection()));
    }

    private ReplicaSet(Set<Replica> replicaSet)
    {
        super(replicaSet);
        this.replicaSet = replicaSet;
    }

    public ReplicaSet differenceOnEndpoint(ReplicaCollection differenceOn)
    {
        if (Iterables.all(this, Replica::isFull) && Iterables.all(differenceOn, Replica::isFull))
        {
            Set<InetAddressAndPort> diffEndpoints = differenceOn.toEndpointCollection(HashSet::new);
            return new ReplicaSet(Replicas.filterOnEndpoints(this, e -> !diffEndpoints.contains(e)));
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public ReplicaSet filter(Predicate<Replica> predicate)
    {
        return new ReplicaSet(this.<Set<Replica>>filterToCollection(predicate, HashSet::new));
    }

    public boolean contains(Object replica)
    {
        return replicaSet.contains(replica);
    }

    @Override
    public Spliterator<Replica> spliterator()
    {
        return replicaSet.spliterator();
    }

    public static ReplicaSet immutableCopyOf(ReplicaSet from)
    {
        return new ReplicaSet(ImmutableSet.copyOf(from.replicaSet));
    }

    public static ReplicaSet immutableCopyOf(ReplicaCollection from)
    {
        return new ReplicaSet(ImmutableSet.copyOf(from.asCollection()));
    }

    public static ReplicaSet of(Replica replica)
    {
        HashSet<Replica> set = Sets.newHashSetWithExpectedSize(1);
        set.add(replica);
        return new ReplicaSet(set);
    }

    public static ReplicaSet of(Replica ... replicas)
    {
        ReplicaSet set = new ReplicaSet(Sets.newHashSetWithExpectedSize(replicas.length));
        for (Replica replica : replicas)
        {
            set.add(replica);
        }
        return set;
    }

    /**
     * Returns a ReplicaSet wrapping a LinkedHashSet that preserves order the same way LinkedHashSet does.
     * @return
     */
    public static ReplicaSet orderPreserving()
    {
        return new ReplicaSet(new LinkedHashSet<>());
    }
}
