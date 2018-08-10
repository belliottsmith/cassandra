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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.ToIntFunction;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import org.apache.cassandra.utils.FBUtilities;

public class Replicas
{
    private static abstract class ImmutableReplicaContainer extends ReplicaCollection
    {
        @Override
        public boolean add(Replica replica)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static ReplicaCollection from(Iterable<Replica> iterable, ToIntFunction<Iterable<Replica>> size)
    {
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return size.applyAsInt(iterable);
            }

            public Iterator<Replica> iterator()
            {
                final Iterator<Replica> iterator = iterable.iterator();
                return new Iterator<Replica>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return iterator.hasNext();
                    }

                    @Override
                    public Replica next()
                    {
                        return iterator.next();
                    }
                };
            }
        };
    }

    public static ReplicaCollection from(Iterable<Replica> iterable)
    {
        return from(iterable, Iterables::size);
    }

    public static ReplicaCollection filter(ReplicaCollection source, Predicate<Replica> predicate)
    {
        return from(Iterables.filter(source, predicate));
    }

    public static ReplicaCollection filterOnEndpoints(ReplicaCollection source, Predicate<InetAddressAndPort> predicate)
    {
        Preconditions.checkNotNull(predicate);
        return filter(source, Predicates.compose(predicate, Replica::getEndpoint));
    }

    public static ReplicaCollection filterOutLocalEndpoint(ReplicaCollection replicas)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        return filterOnEndpoints(replicas, local::equals);
    }

    public static ReplicaCollection concatNaturalAndPending(ReplicaCollection natural, ReplicaCollection pending)
    {
        Preconditions.checkNotNull(natural);
        Preconditions.checkNotNull(pending);
        return from(Iterables.concat(natural, pending));
    }

    public static ReplicaCollection concat(Iterable<ReplicaCollection> replicasIterable)
    {
        Preconditions.checkNotNull(replicasIterable);
        return from(Iterables.concat(replicasIterable));
    }

    public static ReplicaCollection of(Replica replica)
    {
        Preconditions.checkNotNull(replica);
        return from(Collections.singleton(replica), i -> 1);
    }

    public static ReplicaCollection of(Replica ... replica)
    {
        Preconditions.checkNotNull(replica);
        return from(Arrays.asList(replica), i -> replica.length);
    }

    private static ReplicaCollection EMPTY = from(Collections.emptyList(), i -> 0);
    public static ReplicaCollection empty()
    {
        return EMPTY;
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void checkFull(Replica replica)
    {
        if (!replica.isFull())
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void checkFull(Iterable<Replica> replicas)
    {
        if (!Iterables.all(replicas, Replica::isFull))
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static List<String> stringify(ReplicaCollection replicas, boolean withPort)
    {
        return replicas.toCollection(r -> r.getEndpoint().getHostAddress(withPort), ArrayList::new);
    }
}
