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
package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import accord.local.Node;
import accord.messages.Apply;
import accord.messages.ApplyRelay;
import accord.messages.ApplyRelay.ApplyRelayReply;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.txn.TxnDataResult;
import org.apache.cassandra.utils.CollectionSerializers;

import static accord.primitives.Txn.Kind.Write;

public class ApplyRelaySerializers
{
    private static final EncodeAsVInt32<Apply.Kind> kinds = EncodeAsVInt32.of(Apply.Kind.class);

    public static final VersionedSerializer<ApplyRelay, Version> request = new VersionedSerializer<>()
    {
        @Override
        public void serialize(ApplyRelay msg, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(msg.txnId, out);
            KeySerializers.fullRoute.serialize(msg.route, out);
            CommandSerializers.ballot.serialize(msg.ballot, out);
            kinds.serialize(msg.kind, out);
            ExecuteAtSerializer.serialize(msg.txnId, msg.executeAt, out);
            CommandSerializers.nullablePartialTxn.serialize(msg.txn, out, version);
            DepsSerializers.partialDeps.serialize(msg.deps, out);
            if (msg.txnId.is(Write))
                CommandSerializers.writes.serialize(msg.writes, out, version);
            CollectionSerializers.serializeList(msg.forwardTo, out, TopologySerializers.nodeId);
            CollectionSerializers.serializeList(msg.noDepsAt, out, TopologySerializers.nodeId);
            out.writeUnsignedVInt(msg.minEpoch);
            out.writeUnsignedVInt(msg.maxEpoch - msg.minEpoch);
        }

        @Override
        public ApplyRelay deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            FullRoute<?> route = KeySerializers.fullRoute.deserialize(in);
            Ballot ballot = CommandSerializers.ballot.deserialize(in);
            Apply.Kind kind = kinds.deserialize(in);
            Timestamp executeAt = ExecuteAtSerializer.deserialize(txnId, in);
            PartialTxn txn = CommandSerializers.nullablePartialTxn.deserialize(in, version);
            PartialDeps deps = DepsSerializers.partialDeps.deserialize(in);
            Writes writes = txnId.is(Write) ? CommandSerializers.writes.deserialize(in, version) : null;
            SortedArrayList<Node.Id> forwardTo = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            SortedArrayList<Node.Id> noDepsAt = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            long minEpoch = in.readUnsignedVInt();
            long maxEpoch = minEpoch + in.readUnsignedVInt();
            return new ApplyRelay(txnId, route, ballot, kind, executeAt, txn, deps, writes, TxnDataResult.PERSISTABLE, forwardTo, noDepsAt, minEpoch, maxEpoch);
        }

        @Override
        public long serializedSize(ApplyRelay msg, Version version)
        {
            return CommandSerializers.txnId.serializedSize(msg.txnId)
                   + KeySerializers.fullRoute.serializedSize(msg.route)
                   + CommandSerializers.ballot.serializedSize(msg.ballot)
                   + kinds.serializedSize(msg.kind)
                   + ExecuteAtSerializer.serializedSize(msg.txnId, msg.executeAt)
                   + CommandSerializers.nullablePartialTxn.serializedSize(msg.txn, version)
                   + DepsSerializers.partialDeps.serializedSize(msg.deps)
                   + (msg.txnId.is(Write) ? CommandSerializers.writes.serializedSize(msg.writes, version) : 0)
                   + CollectionSerializers.serializedListSize(msg.forwardTo, TopologySerializers.nodeId)
                   + CollectionSerializers.serializedListSize(msg.noDepsAt, TopologySerializers.nodeId)
                   + TypeSizes.sizeofUnsignedVInt(msg.minEpoch)
                   + TypeSizes.sizeofUnsignedVInt(msg.maxEpoch - msg.minEpoch);
        }
    };

    public static final VersionedSerializer<ApplyRelayReply, Version> reply = new VersionedSerializer<>()
    {
        @Override
        public void serialize(ApplyRelayReply msg, DataOutputPlus out, Version version) throws IOException
        {
            CollectionSerializers.serializeList(msg.appliedAt, out, TopologySerializers.nodeId);
            CollectionSerializers.serializeList(msg.failedAt, out, TopologySerializers.nodeId);
        }

        @Override
        public ApplyRelayReply deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new ApplyRelayReply(CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new),
                                       CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new));
        }

        @Override
        public long serializedSize(ApplyRelayReply msg, Version version)
        {
            return CollectionSerializers.serializedListSize(msg.appliedAt, TopologySerializers.nodeId)
                   + CollectionSerializers.serializedListSize(msg.failedAt, TopologySerializers.nodeId);
        }
    };
}
