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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

// warning: equals() depends on class identity; if extending, be sure to assess this
public final class ReplicaList extends ReplicaCollection
{
    static final ReplicaList EMPTY = new ReplicaList(ImmutableList.of());

    private final List<Replica> replicaList;

    private static final Set<Collector.Characteristics> LIST_COLLECTOR_CHARACTERISTICS = ImmutableSet.of(Collector.Characteristics.IDENTITY_FINISH);
    public static final Collector<Replica, ReplicaList, ReplicaList> COLLECTOR = ReplicaCollection.collector(LIST_COLLECTOR_CHARACTERISTICS, ReplicaList::new);

    public ReplicaList()
    {
        this(new ArrayList<>());
    }

    public ReplicaList(int capacity)
    {
        this(new ArrayList<>(capacity));
    }

    public ReplicaList(Collection<Replica> from)
    {
        this(new ArrayList<>(from));
    }

    private ReplicaList(List<Replica> replicaList)
    {
        this.replicaList = replicaList;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaList that = (ReplicaList) o;
        return Objects.equals(replicaList, that.replicaList);
    }

    public int hashCode()
    {
        return replicaList.hashCode();
    }

    @Override
    public String toString()
    {
        return replicaList.toString();
    }

    @Override
    public boolean add(Replica replica)
    {
        Preconditions.checkNotNull(replica);
        return replicaList.add(replica);
    }

    @Override
    public boolean remove(Object replica)
    {
        Preconditions.checkNotNull(replica);
        return replicaList.remove(replica);
    }

    public Replica get(int idx)
    {
        return replicaList.get(idx);
    }

    @Override
    public int size()
    {
        return replicaList.size();
    }

    @Override
    public Iterator<Replica> iterator()
    {
        return replicaList.iterator();
    }

    @Override
    public Spliterator<Replica> spliterator()
    {
        return replicaList.spliterator();
    }

    public ReplicaList subList(int fromIndex, int toIndex)
    {
        return new ReplicaList(replicaList.subList(fromIndex, toIndex));
    }

    public ReplicaList filter(Predicate<Replica> predicate)
    {
        Preconditions.checkNotNull(predicate);
        return filterToCollection(predicate, ReplicaList::new);
    }

    public void sort(Comparator<Replica> comparator)
    {
        replicaList.sort(comparator);
    }

    public static ReplicaList intersectEndpoints(ReplicaList l1, ReplicaList l2)
    {
        Preconditions.checkNotNull(l1);
        Preconditions.checkNotNull(l2);
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.

        return l1.filter(Predicates.compose(l2.asEndpoints()::contains, Replica::getEndpoint));
    }

    public static ReplicaList of()
    {
        return new ReplicaList(0);
    }

    public static ReplicaList of(Replica replica)
    {
        ReplicaList replicaList = new ReplicaList(1);
        replicaList.add(replica);
        return replicaList;
    }

    public static ReplicaList of(Replica... replicas)
    {
        ReplicaList replicaList = new ReplicaList(replicas.length);
        replicaList.addAll(Arrays.asList(replicas));
        return replicaList;
    }

    public static ReplicaList immutableCopyOf(ReplicaCollection replicas)
    {
        return new ReplicaList(ImmutableList.<Replica>builder().addAll(replicas).build());
    }

    public static ReplicaList immutableCopyOf(List<Replica> replicas)
    {
        return new ReplicaList(ImmutableList.copyOf(replicas));
    }

    public static ReplicaList immutableCopyOf(ReplicaList replicas)
    {
        return new ReplicaList(ImmutableList.copyOf(replicas.replicaList));
    }

    public static ReplicaList immutableCopyOf(Replica... replicas)
    {
        return new ReplicaList(ImmutableList.copyOf(replicas));
    }

    public static ReplicaList empty()
    {
        return EMPTY;
    }

    /**
     * Use of this method to synthesize Replicas is almost always wrong. In repair it turns out the concerns of transient
     * vs non-transient are handled at a higher level, but eventually repair needs to ask streaming to actually move
     * the data and at that point it doesn't have a great handle on what the replicas are and it doesn't really matter.
     *
     * Streaming expects to be given Replicas with each replica indicating what type of data (transient or not transient)
     * should be sent.
     *
     * So in this one instance we can lie to streaming and pretend all the replicas are full and use a dummy address
     * and it doesn't matter because streaming doesn't rely on the address for anything other than debugging and full
     * is a valid value for transientness because streaming is selecting candidate tables from the repair/unrepaired
     * set already.
     * @param ranges
     * @return
     */
    @VisibleForTesting
    static ReplicaList toDummyList(Collection<Range<Token>> ranges)
    {
        InetAddressAndPort dummy;
        try
        {
            dummy = InetAddressAndPort.getByNameOverrideDefaults("0.0.0.0", 0);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        //For repair we are less concerned with full vs transient since repair is already dealing with those concerns.
        //Always say full and then if the repair is incremental or not will determine what is streamed.
        return new ReplicaList(ranges.stream()
                               .map(range -> new Replica(dummy, range, true))
                               .collect(Collectors.toList()));

    }
}
