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
import com.google.common.collect.Iterators;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ReplicaList extends AbstractReplicaCollection<ReplicaList>
{
    private static final ReplicaList EMPTY = new ReplicaList(Collections.emptyList());

    private ReplicaList(List<Replica> list)
    {
        super(list);
    }

    private <T> Set<T> toSet(com.google.common.base.Function<Replica, T> transform)
    {
        List<Replica> unmodifiable = Collections.unmodifiableList(list);
        return new AbstractSet<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return Iterators.transform(unmodifiable.iterator(), transform);
            }
            @Override
            public int size()
            {
                return list.size();
            }
        };
    }

    @Override
    public Set<InetAddressAndPort> endpoints()
    {
        return toSet(Replica::endpoint);
    }

    @Override
    protected ReplicaList subClone(List<Replica> subList)
    {
        return subList == null ? this.asImmutableView() : new ReplicaList(subList);
    }

    @Override
    public ReplicaList asImmutableView()
    {
        return this;
    }

    @Override
    public boolean contains(Replica replica)
    {
        return list.contains(replica);
    }

    public static class Mutable extends ReplicaList implements ReplicaCollection.Mutable<ReplicaList>
    {
        public Mutable() { this(0); }
        public Mutable(int capacity) { super(new ArrayList<>(capacity)); }

        public void add(Replica replica, boolean ignoreConflict)
        {
            Preconditions.checkNotNull(replica);
            list.add(replica);
        }

        public ReplicaList asImmutableView()
        {
            return new ReplicaList(super.list);
        }
    }

    public static class Builder extends ReplicaCollection.Builder<ReplicaList, Mutable, ReplicaList.Builder>
    {
        public Builder() { this(0); }
        public Builder(int capacity) { super (new Mutable(capacity)); }
    }

    public static Builder builder()
    {
        return new Builder();
    }
    public static Builder builder(int capacity)
    {
        return new Builder(capacity);
    }

    public static ReplicaList empty()
    {
        return EMPTY;
    }

    public static ReplicaList of(Replica replica)
    {
        return new ReplicaList(Collections.singletonList(replica));
    }

    public static ReplicaList of(Replica ... replicas)
    {
        return copyOf(Arrays.asList(replicas));
    }

    public static ReplicaList copyOf(Collection<Replica> replicas)
    {
        return builder(replicas.size()).addAll(replicas).build();
    }
}
