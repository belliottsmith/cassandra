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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * A collection like class for Replica objects. Since the Replica class contains inetaddress, range, and
 * transient replication status, basic contains and remove methods can be ambiguous. Replicas forces you
 * to be explicit about what you're checking the container for, or removing from it.
 */
public class ReplicaCollection implements Iterable<Replica>
{

    protected static <C extends ReplicaCollection> Collector<Replica, C, C> collector(Set<Collector.Characteristics> characteristics, Supplier<C> supplier)
    {
        return new Collector<Replica, C, C>()
        {
            private final BiConsumer<C, Replica> accumulator = ReplicaCollection::add;
            private final BinaryOperator<C> combiner = characteristics.contains(Characteristics.UNORDERED)
                    ? (a, b) ->
                    {
                        if (a.size() > b.size())
                        {
                            a.asCollection().addAll(b.asCollection());
                            return a;
                        }
                        b.asCollection().addAll(a.asCollection());
                        return b;
                    }
                    : (a, b) -> { a.asCollection().addAll(b.asCollection()); return a; };
            private final Function<C, C> finisher = collection -> collection;

            public Supplier<C> supplier() { return supplier; }
            public BiConsumer<C, Replica> accumulator() { return accumulator; }
            public BinaryOperator<C> combiner() { return combiner; }
            public Function<C, C> finisher() { return finisher; }
            public Set<Characteristics> characteristics() { return characteristics; }
        };
    }

    protected final Collection<Replica> collection;
    protected final Collection<Replica> asCollection() { return collection; }
    protected ReplicaCollection(Collection<Replica> collection)
    {
        this.collection = collection;
    }

    public final Iterator<Replica> iterator()
    {
        return asCollection().iterator();
    }

    public Stream<Replica> stream() { return asCollection().stream(); }

    private <T> Collection<T> asUnmodifiableCollection(Function<Replica, T> map)
    {
        return new AbstractCollection<T>()
        {
            final Iterator<Replica> iterator = asCollection().iterator();
            @Override
            public Iterator<T> iterator()
            {
                return new Iterator<T>()
                {
                    @Override
                    public boolean hasNext() { return iterator.hasNext(); }

                    @Override
                    public T next() { return map.apply(iterator.next()); }
                };
            }

            @Override
            public int size()
            {
                return ReplicaCollection.this.size();
            }
        };
    }

    public Collection<InetAddressAndPort> asEndpoints()
    {
        return asUnmodifiableCollection(Replica::getEndpoint);
    }

    public Collection<Range<Token>> asRanges()
    {
        return asUnmodifiableCollection(Replica::getRange);
    }

    <T, C extends Collection<T>> C toCollection(Function<Replica, T> transform, IntFunction<C> constructor)
    {
        C result = constructor.apply(size());
        for (Replica replica : this)
            result.add(transform.apply(replica));
        return result;
    }

    <C extends Collection<Replica>> C filterToCollection(Predicate<Replica> predicate, IntFunction<C> constructor)
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

    public boolean add(Replica replica)
    {
        Preconditions.checkNotNull(replica);
        return asCollection().add(replica);
    }

    public boolean addAll(ReplicaCollection replicas)
    {
        Preconditions.checkNotNull(replicas);
        return asCollection().addAll(replicas.asCollection());
    }

    public int size()
    {
        return asCollection().size();
    }

    public boolean isEmpty()
    {
        return asCollection().isEmpty();
    }

    public void removeReplicas(ReplicaCollection toRemove)
    {
        Preconditions.checkNotNull(toRemove);
        if (Iterables.all(this, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            asCollection().removeAll(toRemove.asCollection());
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public void removeEndpoint(InetAddressAndPort endpoint)
    {
        Iterables.removeIf(asCollection(), test -> endpoint.equals(test.getEndpoint()));
    }

    public void removeEndpoints(ReplicaCollection toRemove)
    {
        Preconditions.checkNotNull(toRemove);
        if (Iterables.all(this, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            for (Replica remove: toRemove)
            {
                removeEndpoint(remove.getEndpoint());
            }
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaCollection that = (ReplicaCollection) o;
        return Objects.equals(asCollection(), that.asCollection());
    }

    public int hashCode()
    {
        return asCollection().hashCode();
    }

    @Override
    public String toString()
    {
        return asCollection().toString();
    }

}
