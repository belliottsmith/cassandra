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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
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
public abstract class ReplicaCollection implements Iterable<Replica>
{

    public abstract boolean add(Replica replica);
    public abstract void addAll(Iterable<Replica> replicas);
    public abstract void removeEndpoint(InetAddressAndPort endpoint);
    public abstract void removeReplica(Replica replica);
    public abstract int size();
    public abstract Stream<Replica> stream();
    protected abstract Collection<Replica> getUnmodifiableCollection();

    public Iterable<InetAddressAndPort> asEndpoints()
    {
        return Iterables.transform(this, Replica::getEndpoint);
    }

    public Set<InetAddressAndPort> asEndpointSet()
    {
        Set<InetAddressAndPort> result = Sets.newHashSetWithExpectedSize(size());
        for (Replica replica: this)
        {
            result.add(replica.getEndpoint());
        }
        return result;
    }

    public List<InetAddressAndPort> asEndpointList()
    {
        List<InetAddressAndPort> result = new ArrayList<>(size());
        for (Replica replica: this)
        {
            result.add(replica.getEndpoint());
        }
        return result;
    }

    public Collection<InetAddressAndPort> asUnmodifiableEndpointCollection()
    {
        return Collections2.transform(getUnmodifiableCollection(), Replica::getEndpoint);
    }

    public Iterable<Range<Token>> asRanges()
    {
        return Iterables.transform(this, Replica::getRange);
    }

    public Set<Range<Token>> asRangeSet()
    {
        Set<Range<Token>> result = Sets.newHashSetWithExpectedSize(size());
        for (Replica replica: this)
        {
            result.add(replica.getRange());
        }
        return result;
    }

    public Collection<Range<Token>> asUnmodifiableRangeCollection()
    {
        return Collections2.transform(getUnmodifiableCollection(), Replica::getRange);
    }

    public Iterable<Range<Token>> fullRanges()
    {
        return Iterables.transform(Iterables.filter(this, Replica::isFull), Replica::getRange);
    }

    public Iterable<Range<Token>>  transientRanges()
    {
        return Iterables.transform(Iterables.filter(this, Replica::isTransient), Replica::getRange);
    }

    public boolean containsEndpoint(InetAddressAndPort endpoint)
    {
        Preconditions.checkNotNull(endpoint);
        for (Replica replica: this)
        {
            if (replica.getEndpoint().equals(endpoint))
                return true;
        }
        return false;
    }

    public boolean containsRange(Range<Token> range)
    {
        Preconditions.checkNotNull(range);
        for (Replica replica : this)
        {
            if (replica.getRange().equals(range))
                return true;
        }
        return false;
    }

    public void removeReplicas(ReplicaCollection toRemove)
    {
        Preconditions.checkNotNull(toRemove);
        if (Iterables.all(this, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            for (Replica remove: toRemove)
            {
                removeReplica(remove);
            }
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public void removeRanges(ReplicaCollection toRemove)
    {
        Preconditions.checkNotNull(toRemove);
        Iterator<Replica> replicaIterator = iterator();
        while (replicaIterator.hasNext())
        {
            Replica replica = replicaIterator.next();
            if (toRemove.containsRange(replica.getRange()))
            {
                replicaIterator.remove();
            }
        }
    }

    public boolean isEmpty()
    {
        return size() == 0;
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

    public boolean noneMatch(Predicate<Replica> predicate)
    {
        return !anyMatch(predicate);
    }

    public boolean anyMatch(Predicate<Replica> predicate)
    {
        Preconditions.checkNotNull(predicate);
        for (Replica replica : this)
        {
            if (predicate.apply(replica))
                return true;
        }
        return false;
    }

    public boolean allMatch(Predicate<Replica> predicate)
    {
        Preconditions.checkNotNull(predicate);
        for (Replica replica : this)
        {
            if (!predicate.apply(replica))
            {
                return false;
            }
        }
        return true;
    }

    public <T extends ReplicaCollection> T filter(java.util.function.Predicate<Replica> predicates[], Supplier<T> collectorSupplier)
    {
        Preconditions.checkNotNull(predicates);
        Preconditions.checkNotNull(collectorSupplier);
        T newReplicas = collectorSupplier.get();
        for (Replica replica : this)
        {
            boolean success = true;
            for (java.util.function.Predicate<Replica> predicate : predicates)
            {
                if (!predicate.test(replica))
                {
                    success = false;
                    break;
                }
            }

            if (success)
            {
                newReplicas.add(replica);
            }
        }
        return newReplicas;
    }

    public long count(Predicate<Replica> predicate)
    {
        Preconditions.checkNotNull(predicate);
        long count = 0;
        for (Replica replica : this)
        {
            if (predicate.apply(replica))
            {
                count++;
            }
        }
        return count;
    }

    public Optional<Replica> findFirst(Predicate<Replica> predicate)
    {
        Preconditions.checkNotNull(predicate);
        for (Replica replica : this)
        {
            if (predicate.apply(replica))
            {
                return Optional.of(replica);
            }
        }

        return Optional.empty();
    }

}
