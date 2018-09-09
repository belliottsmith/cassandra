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

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
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
public abstract class AbstractReplicaCollection<C extends AbstractReplicaCollection<C>> implements ReplicaCollection<C>
{
    protected static final ReplicaList EMPTY_LIST = new ReplicaList(); // since immutable, can safely return this to avoid megamorphic callsites

    public static <C extends ReplicaCollection<C>, B extends Builder<C, ?, B>> Collector<Replica, B, C> collector(Set<Collector.Characteristics> characteristics, Supplier<B> supplier)
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

    /**
     * A simple list with no comodification checks and immutability by default (only append permitted, and only one initial copy)
     * this permits us to reduce the amount of garbage generated, by not wrapping iterators or unnecessarily copying
     * and reduces the amount of indirection necessary, as well as ensuring monomorphic callsites
     */
    protected static class ReplicaList implements Iterable<Replica>
    {
        private static final Replica[] EMPTY = new Replica[0];
        Replica[] contents;
        int begin, size;

        public ReplicaList() { this(0); }
        public ReplicaList(int capacity) { contents = capacity == 0 ? EMPTY : new Replica[capacity]; }
        public ReplicaList(Replica[] contents, int begin, int size) { this.contents = contents; this.begin = begin; this.size = size; }

        public boolean isSubList(ReplicaList subList)
        {
            return subList.contents == contents;
        }

        public Replica get(int index)
        {
            if (index > size)
                throw new IndexOutOfBoundsException();
            return contents[begin + index];
        }

        public void add(Replica replica)
        {
            // can only add to full array - if we have sliced it, we must be a snapshot
            assert begin == 0;
            if (size == contents.length)
            {
                int newSize;
                if (size < 3) newSize = 3;
                else if (size < 9) newSize = 9;
                else newSize = size * 2;
                contents = Arrays.copyOf(contents, newSize);
            }
            contents[size++] = replica;
        }

        public int size()
        {
            return size;
        }

        public boolean isEmpty()
        {
            return size == 0;
        }

        public ReplicaList subList(int begin, int end)
        {
            if (end > size || begin > end) throw new IndexOutOfBoundsException();
            return new ReplicaList(contents, this.begin + begin, end - begin);
        }

        public ReplicaList sorted(Comparator<Replica> comparator)
        {
            Replica[] copy = Arrays.copyOfRange(contents, begin, begin + size);
            Arrays.sort(copy, comparator);
            return new ReplicaList(copy, 0, copy.length);
        }

        public Stream<Replica> stream()
        {
            return Arrays.stream(contents, begin, begin + size);
        }

        @Override
        public Iterator<Replica> iterator()
        {
            return new Iterator<Replica>()
            {
                final int end = begin + size;
                int i = begin;
                @Override
                public boolean hasNext()
                {
                    return i < end;
                }

                @Override
                public Replica next()
                {
                    return contents[i++];
                }
            };
        }

        public <K> Iterator<K> transformIterator(Function<Replica, K> function)
        {
            return new Iterator<K>()
            {
                final int end = begin + size;
                int i = begin;
                @Override
                public boolean hasNext()
                {
                    return i < end;
                }

                @Override
                public K next()
                {
                    return function.apply(contents[i++]);
                }
            };
        }

        private Iterator<Replica> filterIterator(Predicate<Replica> predicate, int limit)
        {
            return new Iterator<Replica>()
            {
                final int end = begin + size;
                int next = begin;
                int count = 0;
                { updateNext(); }
                void updateNext()
                {
                    if (count == limit) next = end;
                    while (next < end && !predicate.test(contents[next]))
                        ++next;
                    ++count;
                }
                @Override
                public boolean hasNext()
                {
                    return next < end;
                }

                @Override
                public Replica next()
                {
                    if (!hasNext()) throw new IllegalStateException();
                    Replica result = contents[next++];
                    updateNext();
                    return result;
                }
            };
        }

        @VisibleForTesting
        public boolean equals(Object to)
        {
            if (to == null || to.getClass() != ReplicaList.class)
                return false;
            ReplicaList that = (ReplicaList) to;
            if (this.size != that.size) return false;
            for (int i = 0 ; i < size ; ++i)
                if (!this.get(i).equals(that.get(i)))
                    return false;
            return true;
        }
    }

    /**
     * A simple list with no comodification checks and immutability by default (only append permitted, and only one initial copy)
     * this permits us to reduce the amount of garbage generated, by not wrapping iterators or unnecessarily copying
     * and reduces the amount of indirection necessary, as well as ensuring monomorphic callsites
     */
    protected static class ReplicaMap<K> extends AbstractMap<K, Replica>
    {
        private final Function<Replica, K> toKey;
        private final ReplicaList list;
        private final ObjectIntOpenHashMap<K> map;
        private Set<K> keySet;
        private Set<Entry<K, Replica>> entrySet;

        abstract class AbstractImmutableSet<K> extends AbstractSet<K>
        {
            @Override
            public boolean remove(Object o)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public int size()
            {
                return list.size();
            }
        }

        class KeySet extends AbstractImmutableSet<K>
        {
            @Override
            public boolean contains(Object o)
            {
                return containsKey(o);
            }

            @Override
            public Iterator<K> iterator()
            {
                return list.transformIterator(toKey);
            }
        }

        class EntrySet extends AbstractImmutableSet<Entry<K, Replica>>
        {
            @Override
            public boolean contains(Object o)
            {
                if (!(o instanceof Entry<?, ?>)) return false;
                return Objects.equals(get(((Entry) o).getKey()), ((Entry) o).getValue());
            }

            @Override
            public Iterator<Entry<K, Replica>> iterator()
            {
                return list.transformIterator(r -> new SimpleImmutableEntry<>(toKey.apply(r), r));
            }
        }

        private static <K> boolean putIfAbsent(ObjectIntOpenHashMap<K> map, Function<Replica, K> toKey, Replica replica, int index)
        {
            K key = toKey.apply(replica);
            int otherIndex = map.put(key, index + 1);
            if (otherIndex == 0)
                return true;
            map.put(key, otherIndex);
            return false;
        }

        public ReplicaMap(ReplicaList list, Function<Replica, K> toKey)
        {
            // 8*0.65 => RF=5; 16*0.65 ==> RF=10
            // use list capacity if empty, otherwise use actual list size
            ObjectIntOpenHashMap<K> map = new ObjectIntOpenHashMap<>(list.size == 0 ? list.contents.length : list.size, 0.65f);
            for (int i = list.begin ; i < list.begin + list.size ; ++i)
            {
                boolean inserted = putIfAbsent(map, toKey, list.contents[i], i);
                assert inserted;
            }
            this.toKey = toKey;
            this.list = list;
            this.map = map;
        }
        public ReplicaMap(ReplicaList list, Function<Replica, K> toKey, ObjectIntOpenHashMap<K> map)
        {
            this.toKey = toKey;
            this.list = list;
            this.map = map;
        }

        boolean putIfAbsent(Replica replica, int index)
        {
            return putIfAbsent(map, toKey, replica, index);
        }

        @Override
        public boolean containsKey(Object key)
        {
            return get((K)key) != null;
        }

        public Replica get(Object key)
        {
            int index = map.get((K)key) - 1;
            if (index < list.begin || index >= list.begin + list.size)
                return null;
            return list.contents[index];
        }

        @Override
        public Replica remove(Object key)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<K> keySet()
        {
            Set<K> ks = keySet;
            if (ks == null)
                keySet = ks = new KeySet();
            return ks;
        }

        @Override
        public Set<Entry<K, Replica>> entrySet()
        {
            Set<Entry<K, Replica>> es = entrySet;
            if (es == null)
                entrySet = es = new EntrySet();
            return es;
        }

        public int size()
        {
            return list.size();
        }

        public ReplicaMap<K> subList(ReplicaList subList)
        {
            assert subList.contents == list.contents;
            return new ReplicaMap<>(subList, toKey, map);
        }
    }

    protected final ReplicaList list;
    protected final boolean isSnapshot;
    protected AbstractReplicaCollection(ReplicaList list, boolean isSnapshot)
    {
        this.list = list;
        this.isSnapshot = isSnapshot;
    }

    // if subList == null, should return self (or a clone thereof)
    protected abstract C snapshot(ReplicaList subList);
    protected abstract C self();
    /**
     * construct a new Mutable of our own type, so that we can concatenate
     * TODO: this isn't terribly pretty, but we need sometimes to select / merge two Endpoints of unknown type;
     */
    public abstract Mutable<C> newMutable(int initialCapacity);

    public C snapshot()
    {
        return isSnapshot ? self()
                          : snapshot(list.isEmpty() ? EMPTY_LIST
                                                    : list);
    }

    public final C subList(int start, int end)
    {
        if (isSnapshot && start == 0 && end == size()) return self();

        ReplicaList subList;
        if (start == end) subList = EMPTY_LIST;
        else subList = list.subList(start, end);

        return snapshot(subList);
    }

    public int count(Predicate<Replica> predicate)
    {
        int count = 0;
        for (int i = 0 ; i < list.size() ; ++i)
            if (predicate.test(list.get(i)))
                ++count;
        return count;
    }

    public final C filter(Predicate<Replica> predicate)
    {
        return filter(predicate, Integer.MAX_VALUE);
    }

    public final C filter(Predicate<Replica> predicate, int limit)
    {
        if (isEmpty())
            return snapshot();

        ReplicaList copy = null;
        int beginRun = -1, endRun = -1;
        int i = 0;
        for (; i < list.size() ; ++i)
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
                    copy = new ReplicaList(Math.min(limit, (list.size() - i) + (endRun - beginRun)));
                    for (int j = beginRun ; j < endRun ; ++j)
                        copy.add(list.get(j));
                    copy.add(list.get(i));
                }
                if (--limit == 0)
                {
                    ++i;
                    break;
                }
            }
            else if (beginRun >= 0 && endRun < 0)
                endRun = i;
        }

        if (beginRun < 0)
            beginRun = endRun = 0;
        if (endRun < 0)
            endRun = i;
        if (copy == null)
            return subList(beginRun, endRun);
        return snapshot(copy);
    }

    public final Iterable<Replica> filterLazily(Predicate<Replica> predicate)
    {
        return filterLazily(predicate, Integer.MAX_VALUE);
    }

    public final Iterable<Replica> filterLazily(Predicate<Replica> predicate, int limit)
    {
        return () -> list.filterIterator(predicate, limit);
    }

    public final C sorted(Comparator<Replica> comparator)
    {
        return snapshot(list.sorted(comparator));
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

    static <C extends AbstractReplicaCollection<C>> C concat(C replicas, C extraReplicas, Mutable.Conflict ignoreConflicts)
    {
        if (extraReplicas.isEmpty())
            return replicas;
        if (replicas.isEmpty())
            return extraReplicas;
        Mutable<C> mutable = replicas.newMutable(replicas.size() + extraReplicas.size());
        mutable.addAll(replicas, Mutable.Conflict.NONE);
        mutable.addAll(extraReplicas, ignoreConflicts);
        return mutable.asSnapshot();
    }

}
