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

package org.apache.cassandra.service.paxos;

import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class TablePaxosRepairHistory
{
    private final String keyspace;
    private final String table;
    private volatile PaxosRepairHistory history;

    private TablePaxosRepairHistory(String keyspace, String table, PaxosRepairHistory history)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.history = history;
    }

    public static TablePaxosRepairHistory load(String keyspace, String table)
    {
        return new TablePaxosRepairHistory(keyspace, table, SystemKeyspace.loadPaxosRepairHistory(keyspace, table));
    }

    public UUID getBallotForToken(Token token)
    {
        return history.ballotForToken(token);
    }

    private void updatePaxosRepairTable(PaxosRepairHistory update)
    {
        SystemKeyspace.savePaxosRepairHistory(keyspace, table, update);
    }

    public synchronized void add(Collection<Range<Token>> ranges, UUID ballot)
    {
        PaxosRepairHistory update = PaxosRepairHistory.add(history, ranges, ballot);
        updatePaxosRepairTable(update);
        history = update;
    }

    public synchronized void merge(PaxosRepairHistory toMerge)
    {
        PaxosRepairHistory update = PaxosRepairHistory.merge(history, toMerge);
        if (!update.equals(history))
            updatePaxosRepairTable(update);
        history = update;
    }

    public PaxosRepairHistory getHistoryForRanges(Collection<Range<Token>> ranges)
    {
        return PaxosRepairHistory.trim(history, ranges);
    }
}
