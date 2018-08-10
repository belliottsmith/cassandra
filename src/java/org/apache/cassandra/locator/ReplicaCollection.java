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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * A collection like class for Replica objects. Since the Replica class contains inetaddress, range, and
 * transient replication status, basic contains and remove methods can be ambiguous. Replicas forces you
 * to be explicit about what you're checking the container for, or removing from it.
 */
public abstract class ReplicaCollection extends AbstractCollection<Replica>
{

    protected static <C extends ReplicaCollection> Collector<Replica, C, C> collector(Set<Collector.Characteristics> characteristics, Supplier<C> supplier)
    {
        return new Collector<Replica, C, C>()
        {
            private final BiConsumer<C, Replica> accumulator = ReplicaCollection::add;
            private final BinaryOperator<C> combiner = (a, b) ->
            {
                if (a.size() > b.size())
                {
                    a.addAll(b);
                    return a;
                }
                b.addAll(a);
                return b;
            };
            private final Function<C, C> finisher = collection -> collection;

            public Supplier<C> supplier() { return supplier; }
            public BiConsumer<C, Replica> accumulator() { return accumulator; }
            public BinaryOperator<C> combiner() { return combiner; }
            public Function<C, C> finisher() { return finisher; }
            public Set<Characteristics> characteristics() { return characteristics; }
        };
    }

    private Collection<Replica> asUnmodifiableCollection() {
        return Collections.unmodifiableCollection(this);
    }

    public Collection<InetAddressAndPort> asUnmodifiableEndpointCollection()
    {
        return Collections2.transform(asUnmodifiableCollection(), Replica::getEndpoint);
    }

    public Collection<Range<Token>> asUnmodifiableRangeCollection()
    {
        return Collections2.transform(asUnmodifiableCollection(), Replica::getRange);
    }

    public Collection<InetAddressAndPort> asEndpoints()
    {
        return Collections2.transform(this, Replica::getEndpoint);
    }

    public Collection<Range<Token>> asRanges()
    {
        return Collections2.transform(this, Replica::getRange);
    }

    public <T, C extends Collection<T>> C toCollection(Function<Replica, T> transform, IntFunction<C> constructor)
    {
        C result = constructor.apply(size());
        for (Replica replica : this)
            result.add(transform.apply(replica));
        return result;
    }

    public <C extends Collection<Replica>> C filterToCollection(Predicate<Replica> predicate, IntFunction<C> constructor)
    {
        C result = constructor.apply(size());
        for (Replica replica : this)
            if (predicate.apply(replica))
                result.add(replica);
        return result;
    }

    public <C extends Collection<InetAddressAndPort>> C toEndpointCollection(IntFunction<C> constructor)
    {
        return toCollection(Replica::getEndpoint, constructor);
    }

    public Set<Range<Token>> toRangeSet()
    {
        return toCollection(Replica::getRange, HashSet::new);
    }

    public Iterable<Range<Token>> fullRanges()
    {
        return Iterables.transform(Iterables.filter(this, Replica::isFull), Replica::getRange);
    }

    public Iterable<Range<Token>> transientRanges()
    {
        return Iterables.transform(Iterables.filter(this, Replica::isTransient), Replica::getRange);
    }

    public void removeReplicas(ReplicaCollection toRemove)
    {
        Preconditions.checkNotNull(toRemove);
        if (Iterables.all(this, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            removeAll(toRemove);
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public void removeEndpoints(ReplicaCollection toRemove)
    {
        Preconditions.checkNotNull(toRemove);
        if (Iterables.all(this, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            asEndpoints().removeAll(toRemove.asEndpoints());
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    @Override
    public String toString()
    {
        Iterator<Replica> i = iterator();
        if (!i.hasNext())
            return "[]";

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        while (true)
        {
            Replica replica = i.next();
            sb.append(replica);
            if (!i.hasNext())
                return sb.append(']').toString();
            sb.append(", ");
        }
    }

}
