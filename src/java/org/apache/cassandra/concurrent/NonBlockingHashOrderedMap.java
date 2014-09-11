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
package org.apache.cassandra.concurrent;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.AtomicReferenceArrayUpdater;

/**
 * <p>An insert-only hash map that also supports range slicing (as defined by normal signed integer comparison).
 * The only condition is that for two keys k1, k2: k1 < k2 => k1.hashCode() <= k2.hashCode()
 *
 * This data structure essentially only works for keys that are first sorted by some hash value (and may then be sorted
 * within those hashes arbitrarily), where a 32-bit (signed) prefix of the hash we sort by is returned by hashCode()
 *
 * <p>This is essentially a variant of a shalev/shavit "split ordered list" hashmap, except for simplicity we treat the hash
 * table as only an index into our hash-ordered linked-list, and we update the index lazily on reads
 *
 * <p>We are composed of two structures:
 * 1) a simple linked-list, in sorted order
 * 2) an index (hash table) that permits quick lookups inside the linked-list
 *
 * This works by, for any bucket i, keying into the position in the linked-list that directly precedes the region containing
 * values > reverse(i); by reversing the index/hash, we have ordered the linked-list in the same order we would split
 * our buckets on a resize, so that populating the new index buckets simply involves looking up the prior bucket
 * and walking forwards O(1) nodes. Since we do this lazily, we may have to look back multiple buckets, filling forwards
 * as we go, but the same principle applies
 */
public class NonBlockingHashOrderedMap<K extends Comparable<? super K>, V> implements InsertOnlyOrderedMap<K, V>
{
    public static final int ITEM_HEAP_OVERHEAD = (int) (ObjectSizes.measure(new Node(0, null, null)) + ObjectSizes.sizeOfReferenceArray(2) -  + ObjectSizes.sizeOfReferenceArray(0));
    private static int INDEX_SHIFT = 18;
    private static int INDEX_BUCKET_MASK = (1 << INDEX_SHIFT) - 1;

    private volatile int size;

    // the predecessor to the whole list - we don't really need to track it independently, but do so for neatness
    private final Node<K, V> head = new Node<>(Integer.MIN_VALUE, null, null);

    /**
     * our index into the linked list; each entry defines the entry-point to a specific slice of the hash range,
     * and maintains a link to the last node _preceding_ that range. this is updated lazily (and without synchronization)
     * because it tends towards stability, and is easily and automatically repaired
     *
     * since our hashCode() is sorted by signed integer comparison, we have to essentially partition this index
     * into two adjacent ranges, which we do by mapping all negative integers to even addresses, and all positive integers
     * to odd addresses
     */
    private volatile Node<K, V>[][] index = new Node[1][1 << 10];
    {
        // insert the head into the first location in the index; all other index locations will be populated
        // by chained back-reference to the initial seed.
        // this particular item is the only one in the index to not honour index[i].hash < firstHashOfIndex(i),
        // however it honours the condition that it sorts before all items in the bucket, which is effectively the same
        index[0][0] = head;
    }

    private static final class Node<K extends Comparable<? super K>, V> implements Map.Entry<K, V>
    {
        final int hash;
        final K key;
        final V value;
        volatile Node<K, V> next;

        private Node(int hash, K key, V value)
        {
            this.hash = hash;
            this.key = key;
            this.value = value;
        }

        public K getKey()
        {
            return key;
        }

        public V getValue()
        {
            return value;
        }

        public V setValue(V value)
        {
            throw new UnsupportedOperationException();
        }

        int compareTo(int hash, K key)
        {
            int r = Integer.compare(this.hash, hash);
            if (r != 0)
                return r;
            if (this.key == null)
                return -1;
            return this.key.compareTo(key);
        }
    }

    public V putIfAbsent(K key, V value)
    {
        if (value == null)
            throw new IllegalArgumentException();
        int hash = key.hashCode();
        // may not be direct predecessor, but will be _a_ predecessor
        Node<K, V> pred = predecessor(hash);
        Node<K, V> newNode = new Node<>(hash, key, value);
        while (true)
        {
            Node<K, V> next = pred.next;
            int c = next == null ? 1 : next.compareTo(hash, key);
            if (c >= 0)
            {
                if (c == 0)
                    return next.value;
                // next is after the node we want to insert, so attempt to insert our new node here
                nextUpdater.lazySet(newNode, next);
                if (nextUpdater.compareAndSet(pred, next, newNode))
                {
                    // if we succeeded, update size and maybe trigger a resize
                    maybeResize(sizeUpdater.incrementAndGet(this));
                    return null;
                }
                // if we failed, we want to continue from the same predecessor, as we may still want to insert here
            }
            else
            {
                // otherwise walk forwards, as we haven't found our insertion point yet
                pred = next;
            }
        }
    }

    public V get(K key)
    {
        int hash = key.hashCode();
        // may not be direct predecessor, but will be _a_ predecessor
        Node<K, V> node = predecessor(hash).next;
        while (node != null)
        {
            int c = node.compareTo(hash, key);
            if (c >= 0)
                return c == 0 ? node.value : null;
            node = node.next;
        }
        return null;
    }

    // find the node directly preceding the provided hash; always non-null return
    private Node<K, V> predecessor(int hash)
    {
        Node<K, V>[][] index = this.index;
        int indexHash = indexHash(hash);
        int indexMask = indexLength(index) - 1;
        int i = indexHash & indexMask;
        Node<K, V> node;
        {
            Node<K, V>[] indexBucket = index[i >> INDEX_SHIFT];
            if (indexBucket == null)
            {
                // we permit a bucket to be null so that when growing we more quickly have access to
                // the increased capacity
                indexMask >>= 1;
                i &= indexMask;
                indexBucket = index[i >> INDEX_SHIFT];
            }
            node = indexBucket[i & INDEX_BUCKET_MASK];
        }
        if (node == null)
        {
            // if there's no index entry, remove the most significant bits from the index position
            // to find the nearest prior index entry
            int j = i;
            while (node == null)
            {
                j ^= Integer.highestOneBit(j);
                node = index[j >> INDEX_SHIFT][j & INDEX_BUCKET_MASK];
            }
            // then reintroduce the bits, populating the index buckets as we go
            while (j != i)
            {
                // (i ^ j) yields i with all bits in j unset, so the lowest bit is the next to reintroduce
                j |= Integer.lowestOneBit(i ^ j);
                node = scrollToBucket(j, node, null, index);
            }
        }
        else
        {
            node = scrollToBucket(i, node, node, index);
        }

        // walk forward until the next node's hash is >= the provided hash
        for (Node<K, V> next = node.next ; next != null && next.hash < hash ; next = next.next)
            node = next;
        return node;
    }

    // walk forwards until we find the true predecessor of the range we should find from the given bucket
    // and update the index if necessary
    private Node<K, V> scrollToBucket(int i, Node<K, V> node, Node<K, V> exp, Node<K, V>[][] index)
    {
        Node<K, V> result = node;
        int bucketStart = firstHashOfIndex(i);
        for (Node<K, V> next = node.next ; next != null && next.hash < bucketStart ; next = next.next)
            result = next;
        if (result != exp)
        {
            Node[] indexBucket = index[i >> INDEX_SHIFT];
            if (indexBucket != null)
                indexUpdater.compareAndSet(indexBucket, i & INDEX_BUCKET_MASK, exp, result);
        }
        return result;
    }

    private static int indexLength(Node<?, ?>[][] index)
    {
        return index.length == 1 ? index[0].length : index.length << INDEX_SHIFT;
    }

    // convert a hash into the key we use for index lookups, by reversing its bits
    // since the index is sign partitioned, we ignore the sign bit from the reverse and shift it to the bottom result bit
    private static int indexHash(int hash)
    {
        return (Integer.reverse(hash) << 1) | ((hash >>> 31) ^ 1);
    }

    // convert an index position into a lower-bound for the hashes it should index into
    // since the index is sign partitioned, we the least significant bit defines the sign of the hash we're indexing into
    private static int firstHashOfIndex(int position)
    {
        return (Integer.reverse(position) << 1) | ((position ^ 1) << 31);
    }

    // find the first node that is equal to or greater than key
    private Node<K, V> onOrAfter(K key)
    {
        int hash = key.hashCode();
        Node<K, V> node = predecessor(hash);
        while (node != null && node.compareTo(hash, key) < 0)
            node = node.next;
        return node;
    }

    public int size()
    {
        return size;
    }

    private void maybeResize(int size)
    {
        // => 1.5*size == X * index.length (where hopefully X is 1)
        // => index 66% full
        if (((size + (size / 2)) & (index.length - 1)) == 0)
            resize(size * 2);
    }

    // we perform the resize asynchronously; all we do is allocate a suitably large copy of the existing index
    // and let the readers/writers lazily populate it
    // TODO: since we have a 2d array for the index, we can easily support non-doubling growth - possibly even linear
    // this would not provide even distribution of the extra indexing capacity, but would spread the cost of filling in
    private void resize(final int targetSize)
    {
        resizer.execute(new Runnable()
        {
            public void run()
            {
                Node<K, V>[][] resize = index;
                int cur = indexLength(resize);
                int len = cur;
                while (len < targetSize)
                    len *= 2;
                if (cur != len)
                {
                    resize = Arrays.copyOf(resize, Math.max(1, len >> INDEX_SHIFT));
                    if (cur < 1 << INDEX_SHIFT)
                        resize[0] = Arrays.copyOf(resize[0], Math.min(len, 1 << INDEX_SHIFT));
                    // we write to index after every update of its internal array to ensure visibility ASAP
                    index = resize;
                    for (int i = Math.max(1, cur >> INDEX_SHIFT) ; i != resize.length ; i++)
                    {
                        resize[i] = new Node[1 << INDEX_SHIFT];
                        index = resize;
                    }
                }
            }
        });
    }

    // bounds are always inclusive
    public Iterable<Map.Entry<K, V>> range(final K lb, final K ub)
    {
        final int ubHash = ub == null ? Integer.MAX_VALUE : ub.hashCode();
        return new Iterable<Map.Entry<K, V>>()
        {
            public Iterator<Map.Entry<K, V>> iterator()
            {
                return new Iterator<Map.Entry<K, V>>()
                {
                    Node<K, V> node = lb == null ? head.next : onOrAfter(lb);
                    public boolean hasNext()
                    {
                        return node != null && (ub == null || node.compareTo(ubHash, ub) <= 0);
                    }

                    public Map.Entry<K, V> next()
                    {
                        Node<K, V> r = node;
                        node = node.next;
                        return r;
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public boolean valid()
    {
        Node prev = head;
        for (Node n = prev.next ; n != null ; n = n.next)
            if (prev.compareTo(n.hash, n.key) >= 0 || predecessor(n.hash).next.hash != n.hash)
                return false;
        return true;
    }

    private static final AtomicIntegerFieldUpdater<NonBlockingHashOrderedMap> sizeUpdater = AtomicIntegerFieldUpdater.newUpdater(NonBlockingHashOrderedMap.class, "size");
    private static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
    private static final AtomicReferenceArrayUpdater<Node> indexUpdater = new AtomicReferenceArrayUpdater<>(Node[].class);
    private static final ExecutorService resizer = JMXEnabledSharedExecutorPool.SHARED.newExecutor(1, Integer.MAX_VALUE);
}