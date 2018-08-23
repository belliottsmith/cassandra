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
package org.apache.cassandra.utils.btree;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import org.apache.cassandra.utils.btree.BTree.Dir;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.cassandra.utils.btree.BTree.findIndex;


public class BTreeMap<K, V> implements NavigableMap<K, V>
{
    protected final Comparator<? super K> keyComparator;
    protected final Comparator<? super Entry<K, V>> entryComparator;
    protected final Comparator<Object> entryToKeyComparator; // accepts (entry, key)
    protected final Object[] tree;

    public BTreeMap(Object[] tree, Comparator<? super K> comparator)
    {
        this(tree, comparator, (a, b) -> comparator.compare(a.getKey(), b.getKey()));
    }

    private BTreeMap(Object[] tree, Comparator<? super K> keyComparator, Comparator<? super Entry<K, V>> entryComparator)
    {
        this(tree, keyComparator, entryComparator, (e, k) -> keyComparator.compare(((Entry<K, V>)e).getKey(), (K)k));
    }
    private BTreeMap(Object[] tree, Comparator<? super K> keyComparator, Comparator<? super Entry<K, V>> entryComparator, Comparator<Object> entryToKeyComparator)
    {
        this.tree = tree;
        this.keyComparator = keyComparator;
        this.entryComparator = entryComparator;
        this.entryToKeyComparator = entryToKeyComparator;
    }

    private BTreeMap<K, V> update(Object[] tree)
    {
        return new BTreeMap<>(tree, keyComparator, entryComparator, entryToKeyComparator);
    }

    public BTreeMap<K, V> update(Collection<Entry<K, V>> updateWith)
    {
        return update(BTree.update(tree, entryComparator, updateWith, UpdateFunction.noOp()));
    }

    @Override
    public Comparator<? super K> comparator()
    {
        return keyComparator;
    }

    protected BTreeSearchIterator<Entry<K, V>, Entry<K, V>> slice(Dir dir)
    {
        return BTree.slice(tree, entryComparator, dir);
    }

    public Object[] tree()
    {
        return tree;
    }

    private static <K, V> K maybeKey(Entry<K, V> e)
    {
        return e == null ? null : e.getKey();
    }

    private static <K, V> V maybeValue(Entry<K, V> e)
    {
        return e == null ? null : e.getValue();
    }

    /**
     * The index of the item within the list, or its insertion point otherwise. i.e. binarySearch semantics
     */
    public int indexOf(Object key)
    {
        return findIndex(tree, entryToKeyComparator, key);
    }

    /**
     * The converse of indexOf: provided an index between 0 and size, returns the i'th item, in set order.
     */
    public Entry<K, V> get(int index)
    {
        return BTree.findByIndex(tree, index);
    }

    public BTreeMap<K, V> subList(int fromIndex, int toIndex)
    {
        return new BTreeMapRange<>(this, fromIndex, toIndex - 1);
    }

    @Override
    public int size()
    {
        return BTree.size(tree);
    }

    @Override
    public boolean isEmpty()
    {
        return BTree.isEmpty(tree);
    }

    @Override
    public boolean containsKey(Object key)
    {
        return indexOf(key) >= 0;
    }

    @Override
    public boolean containsValue(Object value)
    {
        return values().contains(value);
    }

    @Override
    public V get(Object key)
    {
        return maybeValue((Entry<K, V>) BTree.find(tree, entryToKeyComparator, key));
    }

    @Override
    public Entry<K, V> higherEntry(K key)
    {
        return (Entry<K, V>) BTree.higher(tree, entryToKeyComparator, key);
    }

    @Override
    public Entry<K, V> lowerEntry(K key)
    {
        return (Entry<K, V>) BTree.lower(tree, entryToKeyComparator, key);
    }

    @Override
    public Entry<K, V> floorEntry(K key)
    {
        return (Entry<K, V>) BTree.floor(tree, entryToKeyComparator, key);
    }

    @Override
    public Entry<K, V> ceilingEntry(K key)
    {
        return (Entry<K, V>) BTree.ceil(tree, entryToKeyComparator, key);
    }

    @Override
    public Entry<K, V> firstEntry()
    {
        return isEmpty() ? null : get(0);
    }

    @Override
    public Entry<K, V> lastEntry()
    {
        return isEmpty() ? null : get(size() - 1);
    }

    @Override
    public K firstKey()
    {
        return maybeKey(firstEntry());
    }

    @Override
    public K lastKey()
    {
        return maybeKey(lastEntry());
    }

    @Override
    public K higherKey(K key)
    {
        return maybeKey(higherEntry(key));
    }

    @Override
    public K lowerKey(K key)
    {
        return maybeKey(lowerEntry(key));
    }

    @Override
    public K floorKey(K key)
    {
        return maybeKey(floorEntry(key));
    }

    @Override
    public K ceilingKey(K key)
    {
        return maybeKey(ceilingEntry(key));
    }

    @Override
    public NavigableSet<K> descendingKeySet()
    {
        return navigableKeySet().descendingSet();
    }
    @Override
    public NavigableSet<K> keySet()
    {
        return navigableKeySet();
    }

    @Override
    public NavigableSet<Entry<K, V>> entrySet()
    {
        return new BTreeSet<>(tree, entryComparator);
    }

    @Override
    public Collection<V> values()
    {
        return new AbstractCollection<V>()
        {
            @Override
            public Iterator<V> iterator()
            {
                return Iterators.transform(entrySet().iterator(), Entry::getValue);
            }
            @Override
            public int size()
            {
                return BTreeMap.this.size();
            }
        };
    }

    @Override
    public BTreeMap<K, V> subMap(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive)
    {
        return new BTreeMapRange<>(this, fromElement, fromInclusive, toElement, toInclusive);
    }

    @Override
    public BTreeMap<K, V> headMap(K toElement, boolean inclusive)
    {
        return new BTreeMapRange<>(this, null, true, toElement, inclusive);
    }

    @Override
    public BTreeMap<K, V> tailMap(K fromElement, boolean inclusive)
    {
        return new BTreeMapRange<>(this, fromElement, inclusive, null, true);
    }

    @Override
    public BTreeMap<K, V> subMap(K fromElement, K toElement)
    {
        return subMap(fromElement, true, toElement, false);
    }

    @Override
    public BTreeMap<K, V> headMap(K toElement)
    {
        return headMap(toElement, false);
    }

    @Override
    public BTreeMap<K, V> tailMap(K fromElement)
    {
        return tailMap(fromElement, true);
    }

    @Override
    public BTreeMap<K, V> descendingMap()
    {
        return new BTreeMapRange<>(this, 0, size()).descendingMap();
    }

    public int hashCode()
    {
        return BTree.hashCode(tree, o -> {
            Entry<K, V> e = (Entry<K, V>)o;
            return (Objects.hashCode(e.getKey()) * 31) + Objects.hashCode(e.getValue());
        });
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> pollFirstEntry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> pollLastEntry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V put(K key, V value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(K key, V value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(K key, V value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        throw new UnsupportedOperationException();
    }

    public static class BTreeMapRange<K, V> extends BTreeMap<K, V>
    {
        // both inclusive
        protected final int lowerBound, upperBound;
        BTreeMapRange(BTreeMapRange<K, V> from)
        {
            this(from, from.lowerBound, from.upperBound);
        }

        BTreeMapRange(BTreeMap<K, V> from, int lowerBound, int upperBound)
        {
            super(from.tree, from.keyComparator, from.entryComparator, from.entryToKeyComparator);
            if (upperBound < lowerBound - 1)
                upperBound = lowerBound - 1;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        BTreeMapRange(BTreeMap<K, V> from, K lowerBound, boolean inclusiveLowerBound, K upperBound, boolean inclusiveUpperBound)
        {
            super(from.tree, from.keyComparator, from.entryComparator, from.entryToKeyComparator);
            this.lowerBound = lowerBound == null
                    ? 0
                    : inclusiveLowerBound
                        ? BTree.ceilIndex(tree, entryToKeyComparator, lowerBound)
                        : BTree.higherIndex(tree, entryToKeyComparator, lowerBound);
            this.upperBound = upperBound == null
                    ? BTree.size(tree) - 1
                    : inclusiveUpperBound
                        ? BTree.floorIndex(tree, entryToKeyComparator, upperBound)
                        : BTree.lowerIndex(tree, entryToKeyComparator, upperBound);
        }

        // narrowing range constructor - makes this the intersection of the two ranges over the same tree b
        BTreeMapRange(BTreeMapRange<K, V> a, BTreeMapRange<K, V> b)
        {
            this(a, Math.max(a.lowerBound, b.lowerBound), Math.min(a.upperBound, b.upperBound));
            assert a.tree == b.tree;
        }

        @Override
        protected BTreeSearchIterator<Entry<K, V>, Entry<K, V>> slice(Dir dir)
        {
            return BTree.slice(tree, entryToKeyComparator, lowerBound, upperBound, dir);
        }

        @Override
        public boolean isEmpty()
        {
            return upperBound < lowerBound;
        }

        public int size()
        {
            return (upperBound - lowerBound) + 1;
        }

        boolean outOfBounds(int i)
        {
            return (i < lowerBound) | (i > upperBound);
        }

        public Entry<K, V> get(int index)
        {
            index += lowerBound;
            if (outOfBounds(index))
                throw new NoSuchElementException();
            return super.get(index);
        }

        private Entry<K, V> maybe(int i)
        {
            if (outOfBounds(i))
                return null;
            return super.get(i);
        }

        public int indexOf(Object key)
        {
            int i = super.indexOf(key);
            boolean negate = i < 0;
            if (negate)
                i = -1 - i;
            if (outOfBounds(i))
                return i < lowerBound ? -1 : -1 - size();
            i = i - lowerBound;
            if (negate)
                i = -1 -i;
            return i;
        }

        public Entry<K, V> lowerEntry(K k)
        {
            return maybe(Math.min(upperBound, BTree.lowerIndex(tree, entryToKeyComparator, k)));
        }

        public Entry<K, V> floorEntry(K k)
        {
            return maybe(Math.min(upperBound, BTree.floorIndex(tree, entryToKeyComparator, k)));
        }

        public Entry<K, V> ceilingEntry(K k)
        {
            return maybe(Math.max(lowerBound, BTree.ceilIndex(tree, entryToKeyComparator, k)));
        }

        public Entry<K, V> higherEntry(K k)
        {
            return maybe(Math.max(lowerBound, BTree.higherIndex(tree, entryToKeyComparator, k)));
        }

        @Override
        public Entry<K, V> firstEntry()
        {
            return maybe(lowerBound);
        }

        @Override
        public Entry<K, V> lastEntry()
        {
            return maybe(upperBound);
        }

        @Override
        public NavigableSet<Entry<K, V>> entrySet()
        {
            return new BTreeSet.BTreeSetRange<>(tree, entryComparator, lowerBound, upperBound);
        }

        @Override
        public BTreeMap<K, V> subMap(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive)
        {
            return new BTreeMapRange<>(this, new BTreeMapRange<>(this, fromElement, fromInclusive, toElement, toInclusive));
        }

        @Override
        public BTreeMap<K, V> headMap(K toElement, boolean inclusive)
        {
            return new BTreeMapRange<>(this, new BTreeMapRange<>(this, null, true, toElement, inclusive));
        }

        @Override
        public BTreeMap<K, V> tailMap(K fromElement, boolean inclusive)
        {
            return new BTreeMapRange<>(this, new BTreeMapRange<>(this, fromElement, inclusive, null, true));
        }

        @Override
        public BTreeMap<K, V> descendingMap()
        {
            return new BTreeDescMapRange<>(this);
        }

        public BTreeMap<K, V> subList(int fromIndex, int toIndex)
        {
            if (fromIndex < 0 || toIndex > size())
                throw new IndexOutOfBoundsException();
            return new BTreeMapRange<>(this, lowerBound + fromIndex, lowerBound + toIndex - 1);
        }

    }

    public static class BTreeDescMapRange<K, V> extends BTreeMapRange<K, V>
    {
        BTreeDescMapRange(BTreeMapRange<K, V> from)
        {
            super(from);
        }

        @Override
        protected BTreeSearchIterator<Entry<K, V>, Entry<K, V>> slice(Dir dir)
        {
            return super.slice(dir.invert());
        }

        /* Flip the methods we call for inequality searches */

        public Entry<K, V> higherEntry(K k)
        {
            return super.lowerEntry(k);
        }

        public Entry<K, V> ceilingEntry(K k)
        {
            return super.floorEntry(k);
        }

        public Entry<K, V> floorEntry(K k)
        {
            return super.ceilingEntry(k);
        }

        public Entry<K, V> lowerEntry(K k)
        {
            return super.higherEntry(k);
        }

        public Entry<K, V> get(int index)
        {
            index = upperBound - index;
            if (outOfBounds(index))
                throw new NoSuchElementException();
            return BTree.findByIndex(tree, index);
        }

        public int indexOf(Object key)
        {
            int i = super.indexOf(key);
            // i is in range [-1 - size()..size())
            // so we just need to invert by adding/subtracting from size
            return i < 0 ? -2 - size() - i  : size() - (i + 1);
        }

        public BTreeMap<K, V> subList(int fromIndex, int toIndex)
        {
            if (fromIndex < 0 || toIndex > size())
                throw new IndexOutOfBoundsException();
            return new BTreeMapRange<>(this, upperBound - (toIndex - 1), upperBound - fromIndex).descendingMap();
        }

        @Override
        public BTreeMap<K, V> subMap(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive)
        {
            return super.subMap(toElement, toInclusive, fromElement, fromInclusive).descendingMap();
        }

        @Override
        public BTreeMap<K, V> headMap(K toElement, boolean inclusive)
        {
            return super.tailMap(toElement, inclusive).descendingMap();
        }

        @Override
        public BTreeMap<K, V> tailMap(K fromElement, boolean inclusive)
        {
            return super.headMap(fromElement, inclusive).descendingMap();
        }

        @Override
        public BTreeMap<K, V> descendingMap()
        {
            return new BTreeMapRange<>(this);
        }

        public Comparator<? super K> comparator()
        {
            return keyComparator;
        }
    }

    @Override
    public NavigableSet<K> navigableKeySet()
    {
        return new NavigableSet<K>()
        {
            @Override
            public K lower(K k)
            {
                return lowerKey(k);
            }

            @Override
            public K floor(K k)
            {
                return floorKey(k);
            }

            @Override
            public K ceiling(K k)
            {
                return ceilingKey(k);
            }

            @Override
            public K higher(K k)
            {
                return higherKey(k);
            }

            @Override
            public K pollFirst()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public K pollLast()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<K> iterator()
            {
                return Iterators.transform(entrySet().iterator(), Entry::getKey);
            }

            @Override
            public NavigableSet<K> descendingSet()
            {
                return descendingMap().navigableKeySet();
            }

            @Override
            public Iterator<K> descendingIterator()
            {
                return descendingMap().navigableKeySet().iterator();
            }

            @Override
            public NavigableSet<K> subSet(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive)
            {
                return subMap(fromElement, fromInclusive, toElement, toInclusive).navigableKeySet();
            }

            @Override
            public NavigableSet<K> headSet(K toElement, boolean inclusive)
            {
                return headMap(toElement, inclusive).navigableKeySet();
            }

            @Override
            public NavigableSet<K> tailSet(K fromElement, boolean inclusive)
            {
                return tailMap(fromElement, inclusive).navigableKeySet();
            }

            @Override
            public SortedSet<K> subSet(K fromElement, K toElement)
            {
                return subMap(fromElement, toElement).navigableKeySet();
            }

            @Override
            public SortedSet<K> headSet(K toElement)
            {
                return headMap(toElement).navigableKeySet();
            }

            @Override
            public SortedSet<K> tailSet(K fromElement)
            {
                return tailMap(fromElement).navigableKeySet();
            }

            @Override
            public Comparator<? super K> comparator()
            {
                return keyComparator;
            }

            @Override
            public K first()
            {
                return firstKey();
            }

            @Override
            public K last()
            {
                return lastKey();
            }

            @Override
            public int size()
            {
                return BTreeMap.this.size();
            }

            @Override
            public boolean isEmpty()
            {
                return BTreeMap.this.isEmpty();
            }

            @Override
            public boolean contains(Object o)
            {
                return containsKey(o);
            }

            @Override
            public Object[] toArray()
            {
                return toArray(new Object[0]);
            }

            @Override
            public <T> T[] toArray(T[] a)
            {
                int size = size();
                if (a.length < size)
                    a = Arrays.copyOf(a, size);
                int i = 0;
                for (K key : this)
                    a[i++] = (T) key;
                return a;
            }

            @Override
            public boolean add(K k)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(Collection<?> c)
            {
                for (Object o : c)
                    if (!contains(o))
                        return false;
                return true;
            }

            @Override
            public boolean addAll(Collection<? extends K> c)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static class Builder<K, V>
    {
        final BTree.Builder<Entry<K, V>> builder;
        final Comparator<? super K> keyComparator;
        protected Builder(Comparator<? super K> comparator)
        {
            keyComparator = comparator;
            builder = BTree.builder((a, b) -> keyComparator.compare(a.getKey(), b.getKey()));
        }

        public Builder<K, V> add(K key, V value)
        {
            builder.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            return this;
        }

        public Builder<K, V> addAll(Collection<Entry<K, V>> iter)
        {
            builder.addAll(Collections2.transform(iter,
                    e -> e instanceof AbstractMap.SimpleImmutableEntry
                            ? e : new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue())));
            return this;
        }

        public boolean isEmpty()
        {
            return builder.isEmpty();
        }

        public BTreeMap<K, V> build()
        {
            return new BTreeMap<>(builder.build(), keyComparator, builder.comparator);
        }
    }

    public static <K, V> Builder<K, V> builder(Comparator<? super K> comparator)
    {
        return new Builder<>(comparator);
    }

    public static <K, V> BTreeMap<K, V> wrap(Object[] btree, Comparator<? super K> comparator)
    {
        return new BTreeMap<>(btree, comparator);
    }

    public static <K extends Comparable<K>, V> BTreeMap<K, V> of(Collection<Entry<K, V>> sortedValues)
    {
        return new BTreeMap<>(BTree.build(sortedValues, UpdateFunction.noOp()), Ordering.natural());
    }

    public static <K extends Comparable<K>, V> BTreeMap<K, V> of(K key, V value)
    {
        return of(Ordering.natural(), key, value);
    }

    public static <K, V> BTreeMap<K, V> of(Comparator<? super K> comparator, K key, V value)
    {
        return new BTreeMap<>(BTree.build(Collections.singletonList(new AbstractMap.SimpleImmutableEntry<>(key, value)), UpdateFunction.noOp()), comparator);
    }

    public static <K, V> BTreeMap<K, V> empty(Comparator<? super K> comparator)
    {
        return new BTreeMap<>(BTree.empty(), comparator);
    }

}
