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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.UUIDGen;

class PaxosKeyState implements UncommittedPaxosKey
{
    static final Comparator<PaxosKeyState> KEY_COMPARATOR = Comparator.comparing(o -> o.key);
    static final Comparator<PaxosKeyState> BALLOT_COMPARATOR = (o1, o2) -> Longs.compare(o1.ballot.timestamp(), o2.ballot.timestamp());

    final UUID cfId;
    final DecoratedKey key;
    final UUID ballot;
    final boolean committed;

    PaxosKeyState(UUID cfId, DecoratedKey key, UUID ballot, boolean committed)
    {
        Preconditions.checkNotNull(cfId);
        Preconditions.checkNotNull(ballot);
        this.cfId = cfId;
        this.key = key;
        this.ballot = ballot;
        this.committed = committed;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return Paxos.decodeConsistency(ballot);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaxosKeyState that = (PaxosKeyState) o;
        return committed == that.committed &&
               Objects.equals(key, that.key) &&
               Objects.equals(ballot, that.ballot);
    }

    public int hashCode()
    {
        return Objects.hash(key, ballot, committed);
    }

    public String toString()
    {
        return "BallotState{" +
               "cfId=" + cfId +
               ", key=" + key +
               ", ballot=" + ballot +
               ", committed=" + committed +
               '}';
    }

    static PaxosKeyState merge(PaxosKeyState left, PaxosKeyState right)
    {
        if (left == null)
            return right;

        if (right == null)
            return left;

        int cmp = BALLOT_COMPARATOR.compare(left, right);

        // prefer committed operations if the ballots are the same so they can be filtered out later
        if (cmp == 0)
            return left.committed ? left : right;
        else
            return cmp > 0 ? left : right;
    }

    static class Reducer extends MergeIterator.Reducer<PaxosKeyState, PaxosKeyState>
    {
        private PaxosKeyState mostRecent = null;

        public void reduce(int idx, PaxosKeyState current)
        {
            mostRecent = merge(mostRecent, current);
        }

        protected PaxosKeyState getReduced()
        {
            return mostRecent;
        }

        protected void onKeyChange()
        {
            super.onKeyChange();
            mostRecent = null;
        }
    }

    private static class FilteringIterator extends AbstractIterator<PaxosKeyState> implements CloseableIterator<PaxosKeyState>
    {
        private final MergeIterator<PaxosKeyState, PaxosKeyState> mergeIterator;
        private final UUID before;

        public FilteringIterator(MergeIterator<PaxosKeyState, PaxosKeyState> mergeIterator, UUID before)
        {
            this.mergeIterator = mergeIterator;
            this.before = before;
        }

        protected PaxosKeyState computeNext()
        {
            while (mergeIterator.hasNext())
            {
                PaxosKeyState next = mergeIterator.next();
                if (next.committed || (before != null && Commit.isAfter(next.ballot, before)))
                    continue;
                return next;
            }
            return endOfData();
        }

        public void close()
        {
            mergeIterator.close();
        }
    }

    public static CloseableIterator<PaxosKeyState> mergeUncommitted(UUID before, CloseableIterator<PaxosKeyState>... iterators)
    {
        MergeIterator<PaxosKeyState, PaxosKeyState> mergeIterator = MergeIterator.get(Lists.newArrayList(iterators), PaxosKeyState.KEY_COMPARATOR, new Reducer());
        return new FilteringIterator(mergeIterator, before);
    }

    public static CloseableIterator<UncommittedPaxosKey> toUncommittedInfo(CloseableIterator<PaxosKeyState> iter)
    {
        Iterator<PaxosKeyState> filtered = Iterators.filter(iter, k -> !k.committed);
        return new CloseableIterator<UncommittedPaxosKey>()
        {
            public void close()
            {
                iter.close();
            }

            public boolean hasNext()
            {
                return filtered.hasNext();
            }

            public UncommittedPaxosKey next()
            {
                return filtered.next();
            }
        };
    }
}
