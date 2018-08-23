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

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SortedSetMultimap;
import org.apache.cassandra.utils.btree.BTree.Dir;
import org.apache.cassandra.utils.btree.BTreeSet.BTreeSetRange;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

import javax.annotation.Nullable;

import static org.apache.cassandra.utils.btree.BTree.ceilIndex;
import static org.apache.cassandra.utils.btree.BTree.findIndex;
import static org.apache.cassandra.utils.btree.BTree.floorIndex;


public class BTreeMultimap<K, V> implements SortedSetMultimap<K, V>
{
    protected final Comparator<? super K> keyComparator;
    protected final Comparator<? super V> valueComparator;
    // TODO: it might be possible to avoid allocating any of these comparators if the points of use are inlined
    //       might be easier to update ceilIndex/higherIndex and floorIndex/lowerIndex to account for duplicates,
    protected final Comparator<Object> entryKeyCeilComparator;
    protected final Comparator<Object> entryKeyFloorComparator;
    protected final Comparator<Object> entryValueComparator;
    protected final Comparator<Entry<K, V>> entryComparator;
    protected final Object[] tree;

    public BTreeMultimap(Object[] tree, Comparator<? super K> keyComparator, Comparator<? super V> valueComparator)
    {
        this(tree, keyComparator, valueComparator,
             (Object entry, Object key) -> { int c = keyComparator.compare(((Entry<K, V>)entry).getKey(), (K)key); return c == 0 ? -1 : c; },
             (Object entry, Object key) -> { int c = keyComparator.compare(((Entry<K, V>)entry).getKey(), (K)key); return c == 0 ? 1 : c; },
             (Object entry, Object value) -> valueComparator.compare(((Entry<K, V>)entry).getValue(), (V)value),
             (left, right) -> {
                 int c = keyComparator.compare(left.getKey(), right.getKey());
                 if (c == 0) c = valueComparator.compare(left.getValue(), right.getValue());
                 return c;
             });
    }

    private BTreeMultimap(Object[] tree, Comparator<? super K> keyComparator, Comparator<? super V> valueComparator, Comparator<Object> entryKeyCeilComparator, Comparator<Object> entryKeyFloorComparator, Comparator<Object> entryValueComparator, Comparator<Entry<K, V>> entryComparator)
    {
        this.tree = tree;
        this.keyComparator = keyComparator;
        this.valueComparator = valueComparator;
        this.entryKeyCeilComparator = entryKeyCeilComparator;
        this.entryKeyFloorComparator = entryKeyFloorComparator;
        this.entryValueComparator = entryValueComparator;
        this.entryComparator = entryComparator;
    }

    private BTreeMultimap<K, V> update(Object[] tree)
    {
        return new BTreeMultimap<>(tree, keyComparator, valueComparator, entryKeyCeilComparator, entryKeyFloorComparator, entryValueComparator, entryComparator);
    }

    public BTreeMultimap<K, V> update(Collection<Entry<K, V>> updateWith)
    {
        return update(BTree.update(tree, entryComparator, updateWith, UpdateFunction.noOp()));
    }

    public Object[] tree()
    {
        return tree;
    }

    @Override
    public Comparator<? super V> valueComparator()
    {
        return valueComparator;
    }

    @Override
    public SortedSet<V> get(@Nullable K key)
    {
        int lowerBound = ceilIndex(tree, entryKeyCeilComparator, key);
        int upperBound = floorIndex(tree, entryKeyFloorComparator, key);
        return new BTreeSetRange<>(tree, valueComparator, lowerBound, upperBound);
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
        int lowerBound = ceilIndex(tree, entryKeyCeilComparator, (K) key);
        int upperBound = floorIndex(tree, entryKeyFloorComparator, (K) key);
        return upperBound >= lowerBound;
    }

    @Override
    public boolean containsValue(Object value)
    {
        return values().contains(value);
    }

    @Override
    public boolean containsEntry(@Nullable Object o, @Nullable Object o1)
    {
        return findIndex(tree, entryComparator, new SimpleEntry<>((K) o, (V) o1)) >= 0;
    }

    @Override
    public Set<Entry<K, V>> entries()
    {
        return new BTreeSet<>(tree, entryComparator);
    }

    @Override
    public Set<K> keySet()
    {
        return new KeysAsSet();
    }

    public Multiset<K> keys()
    {
        return new KeysAsMultiset();
    }

    @Override
    public Collection<V> values()
    {
        return new AbstractCollection<V>()
        {
            @Override
            public Iterator<V> iterator()
            {
                return Iterators.transform(entries().iterator(), Entry::getValue);
            }
            @Override
            public int size()
            {
                return BTreeMultimap.this.size();
            }
        };
    }

    @Override
    public Map<K, Collection<V>> asMap()
    {
        return new AsMap();
    }

    public class AsMap extends AbstractMap<K, Collection<V>>
    {
        @Override
        public int size()
        {
            return entrySet().size();
        }

        @Override
        public boolean isEmpty()
        {
            return BTreeMultimap.this.isEmpty();
        }

        @Override
        public boolean containsKey(Object key)
        {
            return BTreeMultimap.this.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value)
        {
            return values().contains(value);
        }

        @Override
        public Collection<V> get(Object key)
        {
            return BTreeMultimap.this.get((K) key);
        }

        @Override
        public Set<K> keySet()
        {
            return BTreeMultimap.this.keySet();
        }

        public Set<Entry<K, Collection<V>>> entrySet()
        {
            return new AbstractSet<Entry<K, Collection<V>>>()
            {
                @Override
                public Iterator<Entry<K, Collection<V>>> iterator()
                {
                    class Iter extends KeyIterator implements Iterator<Entry<K, Collection<V>>>
                    {
                        public Entry<K, Collection<V>> next()
                        {
                            moveNext();
                            Collection<V> value = new BTreeSetRange<>(tree, entryValueComparator, lowerBound, upperBound);
                            return new SimpleEntry<>(key, value);
                        }
                    }
                    return new Iter();
                }

                @Override
                public int size()
                {
                    return Iterables.size(this);
                }

                @Override
                public boolean isEmpty()
                {
                    return BTreeMultimap.this.isEmpty();
                }
            };
        }
    }

    private class KeyIterator
    {
        K key = null;
        int lowerBound = 0;
        int upperBound = -1;
        int limit = size();

        public boolean hasNext()
        {
            return upperBound + 1 < limit;
        }

        void moveNext()
        {
            lowerBound = upperBound + 1;
            key = ((Entry<K, V>)BTree.findByIndex(tree, lowerBound)).getKey();
            upperBound = BTree.floorIndex(tree, entryKeyFloorComparator, key);
        }
    }

    private class KeysAsSet extends AbstractSet<K>
    {
        @Override
        public int size()
        {
            return Iterables.size(this);
        }

        @Override
        public boolean isEmpty()
        {
            return BTreeMultimap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o)
        {
            return BTreeMultimap.this.containsKey(o);
        }

        @Override
        public Iterator<K> iterator()
        {
            class Iter extends KeyIterator implements Iterator<K>
            { public K next() { return key; } }

            return new Iter();
        }
    }

    class KeysAsMultiEntrySet extends AbstractSet<Multiset.Entry<K>>
    {
        @Override
        public Iterator<Multiset.Entry<K>> iterator()
        {
            class Iter extends KeyIterator implements Iterator<Multiset.Entry<K>>
            {
                public Multiset.Entry<K> next()
                {
                    moveNext();
                    return new Multiset.Entry<K>()
                    {
                        K key = Iter.this.key;
                        int count = 1 + upperBound - lowerBound;
                        public K getElement() { return key; }
                        public int getCount() { return count; }
                    };
                }
            }

            return new Iter();
        }

        @Override
        public int size()
        {
            return Iterables.size(this);
        }
    }

    class KeysAsMultiset implements Multiset<K>
    {
        @Override
        public boolean isEmpty()
        {
            return BTreeMultimap.this.isEmpty();
        }

        @Override
        public int size()
        {
            return BTreeMultimap.this.size();
        }

        @Override
        public int count(@Nullable Object key)
        {
            int lowerBound = ceilIndex(tree, entryKeyCeilComparator, key);
            int upperBound = floorIndex(tree, entryKeyFloorComparator, key);
            return Math.max(0, 1 + upperBound - lowerBound);
        }

        @Override
        public Set<K> elementSet()
        {
            return keySet();
        }

        @Override
        public Set<Multiset.Entry<K>> entrySet()
        {
            return new KeysAsMultiEntrySet();
        }

        public boolean contains(@Nullable Object o)
        {
            return containsKey(o);
        }

        @Override
        public boolean containsAll(Collection<?> collection)
        {
            for (Object o : collection)
            {
                if (!contains(o))
                    return false;
            }
            return true;
        }

        public Iterator<K> iterator()
        {
            return Iterators.transform(BTree.iterator(tree), entry -> ((Map.Entry<K, V>)entry).getKey());
        }

        @Override
        public Object[] toArray()
        {
            return stream().toArray();
        }

        @Override
        public <T> T[] toArray(T[] a)
        {
            if (a.length < size())
                a = Arrays.copyOf(a, size());

            int i = 0;
            for (K key : this)
                a[i++] = (T) key;
            return a;
        }

        @Override
        public boolean addAll(Collection<? extends K> c)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int add(@Nullable K k, int i)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int remove(@Nullable Object o, int i)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int setCount(K k, int i)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean setCount(K k, int i, int i1)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(K k)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(@Nullable Object o)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> collection)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> collection)
        {
            throw new UnsupportedOperationException();
        }
    };

    @Override
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
    public boolean remove(Object key, Object value)
    {
        throw new UnsupportedOperationException();
    }

    public SortedSet<V> removeAll(@Nullable Object o)
    {
        throw new UnsupportedOperationException();
    }

    public SortedSet<V> replaceValues(K k, Iterable<? extends V> iterable)
    {
        throw new UnsupportedOperationException();
    }

    public boolean put(@Nullable K k, @Nullable V v)
    {
        throw new UnsupportedOperationException();
    }

    public boolean putAll(@Nullable K k, Iterable<? extends V> iterable)
    {
        throw new UnsupportedOperationException();
    }

    public boolean putAll(Multimap<? extends K, ? extends V> multimap)
    {
        throw new UnsupportedOperationException();
    }

}
