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

package org.apache.cassandra.service.paxos.cleanup;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.utils.UUIDSerializer;

public class PaxosCleanupHistory
{
    final UUID cfId;
    final UUID highBound;
    final PaxosRepairHistory history;

    public PaxosCleanupHistory(UUID cfId, UUID highBound, PaxosRepairHistory history)
    {
        this.cfId = cfId;
        this.highBound = highBound;
        this.history = history;
    }

    public static final IVersionedSerializer<PaxosCleanupHistory> serializer = new IVersionedSerializer<PaxosCleanupHistory>()
    {
        public void serialize(PaxosCleanupHistory message, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.cfId, out, version);
            UUIDSerializer.serializer.serialize(message.highBound, out, version);
            PaxosRepairHistory.serializer.serialize(message.history, out, version);
        }

        public PaxosCleanupHistory deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
            UUID lowBound = UUIDSerializer.serializer.deserialize(in, version);
            PaxosRepairHistory history = PaxosRepairHistory.serializer.deserialize(in, version);
            return new PaxosCleanupHistory(cfId, lowBound, history);
        }

        public long serializedSize(PaxosCleanupHistory message, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(message.cfId, version);
            size += UUIDSerializer.serializer.serializedSize(message.highBound, version);
            size += PaxosRepairHistory.serializer.serializedSize(message.history, version);
            return size;
        }
    };
}
