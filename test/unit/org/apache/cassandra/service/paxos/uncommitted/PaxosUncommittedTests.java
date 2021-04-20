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

import java.util.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

class PaxosUncommittedTests
{
    static final UUID CFID = UUID.randomUUID();
    static final IPartitioner PARTITIONER = new ByteOrderedPartitioner();
    static final Token MIN_TOKEN = PARTITIONER.getMinimumToken();
    static final Range<Token> FULL_RANGE = new Range<>(MIN_TOKEN, MIN_TOKEN);
    static final Collection<Range<Token>> ALL_RANGES = Collections.singleton(FULL_RANGE);
    static final ColumnFamilyStore PAXOS_CFS = Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.PAXOS);
    static final CFMetaData PAXOS_CFM = PAXOS_CFS.metadata;

    static UUID[] createBallots(int num)
    {
        UUID[] ballots = new UUID[num];
        for (int i=0; i<num; i++)
            ballots[i] = UUIDGen.getTimeUUID();

        return ballots;
    }

    static DecoratedKey dk(int v)
    {
        return DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(v));
    }

    static List<PaxosKeyState> kl(Iterator<PaxosKeyState> iter)
    {
        return Lists.newArrayList(iter);
    }

    static List<PaxosKeyState> kl(PaxosKeyState... states)
    {
        return Lists.newArrayList(states);
    }

    static Token tk(int v)
    {
        return dk(v).getToken();
    }

    static Range<Token> r(Token start, Token stop)
    {
        return new Range<>(start != null ? start : MIN_TOKEN, stop != null ? stop : MIN_TOKEN);
    }

    static Range<Token> r(int start, int stop)
    {
        return r(PARTITIONER.getToken(ByteBufferUtil.bytes(start)), PARTITIONER.getToken(ByteBufferUtil.bytes(stop)));
    }

    static Commit commitFor(CFMetaData cfm, UUID ballot, DecoratedKey key)
    {
        return new Commit(ballot, PartitionUpdate.emptyUpdate(cfm, key));
    }

    static Row paxosRowFor(UUID cfId, DecoratedKey key)
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(PAXOS_CFM,
                                                                               FBUtilities.nowInSeconds(),
                                                                               key,
                                                                               new Clustering(UUIDType.instance.decompose(cfId)));
        try (ReadOrderGroup opGroup = command.startOrderGroup();
             UnfilteredPartitionIterator iterator = command.executeLocally(opGroup);
             UnfilteredRowIterator partition = Iterators.getOnlyElement(iterator))
        {
            return (Row) Iterators.getOnlyElement(partition);
        }
    };
}
