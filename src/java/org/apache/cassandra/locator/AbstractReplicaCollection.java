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

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A collection like class for Replica objects. Since the Replica class contains inetaddress, range, and
 * transient replication status, basic contains and remove methods can be ambiguous. Replicas forces you
 * to be explicit about what you're checking the container for, or removing from it.
 */
public abstract class AbstractReplicaCollection<C extends AbstractReplicaCollection<? extends C>> implements ReplicaCollection<C>
{
    protected static final List<Replica> EMPTY_LIST = new ArrayList<>(); // since immutable, can safely return this to avoid megamorphic callsites

    public static <C extends ReplicaCollection<? extends C>, B extends Builder<C, ?, B>> Collector<Replica, B, C> collector(Set<Collector.Characteristics> characteristics, Supplier<B> supplier)
    {
        return new Collector<Replica, B, C>()
        {
            private final BiConsumer<B, Replica> accumulator = Builder::add;
            private final BinaryOperator<B> combiner = (a, b) -> { a.addAll(b.mutable); return a; };
            private final Function<B, C> finisher = Builder::build;
            public Supplier<B> supplier() { return supplier; }
            public BiConsumer<B, Replica> accumulator() { return accumulator; }
            public BinaryOperator<B> combiner() { return combiner; }
            public Function<B, C> finisher() { return finisher; }
            public Set<Characteristics> characteristics() { return characteristics; }
        };
    }

    protected final List<Replica> list;
    protected AbstractReplicaCollection(List<Replica> list)
    {
        this.list = list;
    }

    // if subList == null, should return self (or a clone thereof)
    protected abstract C subClone(List<Replica> subList);
    public abstract C asImmutableView();

    public final C subList(int start, int end)
    {
        if (start == 0 && end == size()) return asImmutableView();
        return subClone(list.subList(start, end));
    }

    public final C filter(Predicate<Replica> predicate)
    {
        if (isEmpty())
            return asImmutableView();

        List<Replica> copy = null;
        int beginRun = -1, endRun = -1;
        for (int i = 0 ; i < list.size() ; ++i)
        {
            Replica replica = list.get(i);
            if (predicate.test(replica))
            {
                if (copy != null)
                    copy.add(replica);
                else if (beginRun < 0)
                    beginRun = i;
                else if (endRun > 0)
                {
                    copy = new ArrayList<>((list.size() - i) + (endRun - beginRun));
                    for (int j = beginRun ; j < endRun ; ++j)
                        copy.add(list.get(j));
                    copy.add(list.get(i));
                }
            }
            else if (beginRun >= 0 && endRun < 0)
                endRun = i;
        }

        if (beginRun < 0)
            beginRun = endRun = 0;
        if (endRun < 0)
            endRun = list.size();
        if (copy == null)
            return subList(beginRun, endRun);
        return subClone(copy);
    }

    public final class Select
    {
        private final ArrayList<Replica> result;
        private final int targetSize;
        public Select(int targetSize)
        {
            this.result = new ArrayList<>(targetSize);
            this.targetSize = targetSize;
        }
        public Select add(Predicate<Replica> predicate)
        {
            for (int i = 0 ; result.size() < targetSize && i < list.size() ; ++i)
                if (predicate.test(list.get(i)))
                    result.add(list.get(i));
            return this;
        }
        public C get()
        {
            return subClone(result);
        }
    }

    /**
     * An efficient method for selecting a subset of replica via a sequence of filters.
     * Once the target number of replica are met, no more filters are applied, or replicas added.
     *
     * Example: select(3).add(filter1).add(filter2).get();
     *
     * @param targetSize the number of Replica you want to select into a new ReplicaCollection of type C
     * @return a Select object
     */
    public final Select select(int targetSize)
    {
        return new Select(targetSize);
    }

    public final C sorted(Comparator<Replica> comparator)
    {
        List<Replica> copy = new ArrayList<>(list);
        copy.sort(comparator);
        return subClone(copy);
    }

    public final Replica get(int i)
    {
        return list.get(i);
    }

    public final int size()
    {
        return list.size();
    }

    public final boolean isEmpty()
    {
        return list.isEmpty();
    }

    public final Iterator<Replica> iterator()
    {
        return list.iterator();
    }

    public final Stream<Replica> stream() { return list.stream(); }

    public final boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof AbstractReplicaCollection<?>))
        {
            if (!(o instanceof ReplicaCollection<?>))
                return false;

            ReplicaCollection<?> that = (ReplicaCollection<?>) o;
            return Iterables.elementsEqual(this, that);
        }
        AbstractReplicaCollection<?> that = (AbstractReplicaCollection<?>) o;
        return Objects.equals(list, that.list);
    }

    public final int hashCode()
    {
        return list.hashCode();
    }

    @Override
    public final String toString()
    {
        return list.toString();
    }
}
