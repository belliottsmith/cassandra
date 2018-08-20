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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * A collection like class for Replica objects. Represents both a well defined order on the contained Replica objects,
 * and efficient methods for accessing the contained Replicas, directly and as a projection onto their endpoints and ranges.
 */
public interface ReplicaCollection<C extends ReplicaCollection<? extends C>> extends Iterable<Replica>
{
    /**
     * @return a Set of the endpoints of the contained Replicas.
     * Iteration order is maintained where there is a 1:1 relationship between endpoint and Replica
     * Typically this collection offers O(1) access methods, and this is true for all but ReplicaList.
     */
    public abstract Set<InetAddressAndPort> endpoints();

    /**
     * @param i a value in the range [0..size())
     * @return the i'th Replica, in our iteration order
     */
    public abstract Replica get(int i);

    /**
     * @return the number of Replica contained
     */
    public abstract int size();

    /**
     * @return true iff size() > 1
     */
    public abstract boolean isEmpty();

    /**
     * @return true iff a Replica in this collection is equal to the provided Replica.
     * Typically this method is expected to take O(1) time, and this is true for all but ReplicaList.
     */
    public abstract boolean contains(Replica replica);

    /**
     * @return a *eagerly constructed* copy of this collection containing the Replica that match the provided predicate;
     * an effort will be made to either return ourself, or a subList.
     */
    public abstract C filter(Predicate<Replica> predicate);

    /**
     * @return an *eagerly constructed* copy of this collection containing the Replica at positions [start..end);
     * if start == 0 and end == size(), this collection may be returned (if it is already immutable)
     */
    public abstract C subList(int start, int end);

    /**
     * @return an *eagerly constructed* copy of this collection containing the Replica re-ordered according to this comparator
     */
    public abstract C sorted(Comparator<Replica> comparator);

    public abstract Iterator<Replica> iterator();
    public abstract Stream<Replica> stream();

    public abstract boolean equals(Object o);
    public abstract int hashCode();
    public abstract String toString();

    /**
     * A mutable extension of a ReplicaCollection.  This is append-only, so it is safe to select a subList,
     * or at any time take an asImmutableView() snapshot.
     * TODO: TR-Review do we think this constraint is OK?
     * TODO: TR-Review is it better to extend ReplicaCollection, or require to take an asImmutableView() for reading?
     */
    public interface Mutable<C extends ReplicaCollection<? extends C>> extends ReplicaCollection<C>
    {
        C asImmutableView();
        void add(Replica replica, boolean ignoreConflict);

        default public void add(Replica replica)
        {
            add(replica, false);
        }

        default public void addAll(Iterable<Replica> replicas, boolean ignoreConflicts)
        {
            for (Replica replica : replicas)
                add(replica, ignoreConflicts);
        }

        default public void addAll(Iterable<Replica> replicas)
        {
            addAll(replicas, false);
        }
    }

    public static class Builder<C extends ReplicaCollection<? extends C>, M extends Mutable<C>, B extends Builder<C, M, B>>
    {
        Mutable<C> mutable;
        public Builder(Mutable<C> mutable) { this.mutable = mutable; }

        public int size() { return mutable.size(); }
        public B add(Replica replica) { mutable.add(replica); return (B) this; }
        public B add(Replica replica, boolean ignoreDuplicate) { mutable.add(replica, ignoreDuplicate); return (B) this; }
        public B addAll(Iterable<Replica> replica) { mutable.addAll(replica); return (B) this; }
        public B addAll(Iterable<Replica> replica, boolean ignoreDuplicate) { mutable.addAll(replica, ignoreDuplicate); return (B) this; }

        public C build()
        {
            C result = mutable.asImmutableView();
            mutable = null;
            return result;
        }
    }

}
