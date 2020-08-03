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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.CloseableIterator;

public class PaxosMockUpdateSupplier implements PaxosUncommittedTracker.UpdateSupplier
{
    private final Map<UUID, NavigableMap<PartitionPosition, PaxosKeyState>> states = new HashMap<>();

    private NavigableMap<PartitionPosition, PaxosKeyState> mapFor(UUID cfId)
    {
        return states.computeIfAbsent(cfId, key -> new TreeMap<>());
    }

    private void updateTo(UUID cfId, PaxosKeyState newState)
    {
        NavigableMap<PartitionPosition, PaxosKeyState> map = mapFor(cfId);
        PaxosKeyState current = map.get(newState.key);
        if (current != null && PaxosKeyState.BALLOT_COMPARATOR.compare(current, newState) > 0)
            return;

        map.put(newState.key, newState);
    }

    void inProgress(UUID cfId, DecoratedKey key, UUID ballot)
    {
        updateTo(cfId, new PaxosKeyState(cfId, key, ballot, false));
    }

    void committed(UUID cfId, DecoratedKey key, UUID ballot)
    {
        updateTo(cfId, new PaxosKeyState(cfId, key, ballot, true));
    }

    public CloseableIterator<PaxosKeyState> getIterator(UUID cfId, Collection<Range<Token>> ranges)
    {
        Iterator<PaxosKeyState> iterator = Iterators.filter(mapFor(cfId).values().iterator(), k -> Iterables.any(ranges, r -> r.contains(k.key.getToken())));

        return new CloseableIterator<PaxosKeyState>()
        {
            public void close() {}

            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            public PaxosKeyState next()
            {
                return iterator.next();
            }
        };
    }

    public CloseableIterator<PaxosKeyState> flushIterator()
    {
        ArrayList<PaxosKeyState> keyStates = new ArrayList<>();
        for (Map.Entry<UUID, NavigableMap<PartitionPosition, PaxosKeyState>> statesEntry : states.entrySet())
        {
            for (Map.Entry<PartitionPosition, PaxosKeyState> entry : statesEntry.getValue().entrySet())
            {
                keyStates.add(entry.getValue());
            }
        }
        states.clear();

        Iterator<PaxosKeyState> iterator = keyStates.iterator();

        return new CloseableIterator<PaxosKeyState>()
        {
            public void close() {}

            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            public PaxosKeyState next()
            {
                return iterator.next();
            }
        };
    }
}
