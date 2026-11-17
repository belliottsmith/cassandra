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

import accord.api.Result.PersistableResult;
import accord.coordinate.ExecuteFlag;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.messages.Apply;
import accord.messages.Apply.ApplyReply;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.VIntCoding;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.txn.TxnDataResult;

import static accord.primitives.Txn.Kind.Write;

public class ApplySerializers
{
    public abstract static class ApplySerializer<A extends Apply> extends TxnRequestSerializer<A>
    {
        private static final EncodeAsVInt32<Apply.Kind> kinds = EncodeAsVInt32.of(Apply.Kind.class);

        @Override
        public void serializeBody(A apply, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.ballot.serialize(apply.ballot, out);
            out.writeVInt(apply.minEpoch - apply.waitForEpoch);
            out.writeUnsignedVInt(apply.maxEpoch - apply.minEpoch);
            kinds.serialize(apply.kind, out);
            ExecuteAtSerializer.serialize(apply.txnId, apply.executeAt, out);
            // NoDeps was introduced later, so we must not change the encoding of the other kinds
            if (apply.kind != Apply.Kind.NoDeps)
                DepsSerializers.partialDeps.serialize(apply.deps(), out);
            CommandSerializers.nullablePartialTxn.serialize(apply.txn(), out, version);
            KeySerializers.nullableFullRoute.serialize(apply.fullRoute, out);
            if (apply.txnId.is(Write))
                CommandSerializers.writes.serialize(apply.writes(), out, version);
            out.writeUnsignedVInt32(apply.flags.bits());
            // RELAYED was introduced later, and is only set when relaying (see relay_remote_apply)
            if (apply.flags.contains(ExecuteFlag.RELAYED))
                TopologySerializers.nodeId.serialize(apply.executedBy, out);
        }

        protected abstract A deserializeApply(TxnId txnId, Ballot ballot, Route<?> scope, long minEpoch, long waitForEpoch, long maxEpoch, Apply.Kind kind,
                                              Timestamp executeAt, PartialDeps deps, PartialTxn txn, FullRoute<?> fullRoute, Writes writes, PersistableResult result, ExecuteFlags flags);

        @Override
        public A deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch) throws IOException
        {
            Ballot ballot = CommandSerializers.ballot.deserialize(in);
            long minEpoch = waitForEpoch + in.readVInt();
            long maxEpoch = minEpoch + in.readUnsignedVInt();
            Apply.Kind kind = kinds.deserialize(in);
            Timestamp executeAt = ExecuteAtSerializer.deserialize(txnId, in);
            PartialDeps deps = kind == Apply.Kind.NoDeps ? null : DepsSerializers.partialDeps.deserialize(in);
            PartialTxn txn = CommandSerializers.nullablePartialTxn.deserialize(in, version);
            FullRoute<?> fullRoute = KeySerializers.nullableFullRoute.deserialize(in);
            Writes writes = txnId.is(Write) ? CommandSerializers.writes.deserialize(in, version) : null;
            ExecuteFlags flags = ExecuteFlags.get(in.readUnsignedVInt32());
            A apply = deserializeApply(txnId, ballot, scope, minEpoch, waitForEpoch, maxEpoch, kind, executeAt, deps, txn, fullRoute, writes, TxnDataResult.PERSISTABLE, flags);
            if (flags.contains(ExecuteFlag.RELAYED))
                apply.executedBy = TopologySerializers.nodeId.deserialize(in);
            return apply;
        }

        @Override
        public long serializedBodySize(A apply, Version version)
        {
            return CommandSerializers.ballot.serializedSize(apply.ballot)
                   + TypeSizes.sizeofVInt(apply.minEpoch - apply.waitForEpoch)
                   + TypeSizes.sizeofUnsignedVInt(apply.maxEpoch - apply.minEpoch)
                   + kinds.serializedSize(apply.kind)
                   + ExecuteAtSerializer.serializedSize(apply.txnId, apply.executeAt)
                   + (apply.kind == Apply.Kind.NoDeps ? 0 : DepsSerializers.partialDeps.serializedSize(apply.deps()))
                   + CommandSerializers.nullablePartialTxn.serializedSize(apply.txn(), version)
                   + KeySerializers.nullableFullRoute.serializedSize(apply.fullRoute)
                   + (apply.txnId.is(Write) ? CommandSerializers.writes.serializedSize(apply.writes(), version) : 0)
                   + VIntCoding.sizeOfUnsignedVInt(apply.flags.bits())
                   + (apply.flags.contains(ExecuteFlag.RELAYED) ? TopologySerializers.nodeId.serializedSize(apply.executedBy) : 0)
            ;
        }
    }

    public static final IVersionedSerializer<Apply> request = new ApplySerializer<>()
    {
        @Override
        protected Apply deserializeApply(TxnId txnId, Ballot ballot, Route<?> scope, long minEpoch, long waitForEpoch, long maxEpoch, Apply.Kind kind,
                                         Timestamp executeAt, PartialDeps deps, PartialTxn txn, FullRoute<?> fullRoute, Writes writes, PersistableResult result, ExecuteFlags flags)
        {
            return Apply.SerializationSupport.create(txnId, ballot, scope, minEpoch, waitForEpoch, maxEpoch, kind, executeAt, deps, txn, fullRoute, writes, result, flags);
        }
    };

    public static final IVersionedSerializer<ApplyReply> reply = new ReplySerializer();

    public static final class ReplySerializer implements IVersionedSerializer<ApplyReply>
    {
        private static final EncodeAsVInt32<ApplyReply.Kind> kinds = EncodeAsVInt32.of(ApplyReply.Kind.class);
        @Override
        public void serialize(ApplyReply t, DataOutputPlus out, Version version) throws IOException
        {
            kinds.serialize(t.kind, out);
            if (t.kind == ApplyReply.Kind.InsufficientEpochs)
                out.writeUnsignedVInt(t.minEpoch());
        }

        @Override
        public ApplyReply deserialize(DataInputPlus in, Version version) throws IOException
        {
            ApplyReply.Kind kind = kinds.deserialize(in);
            if (kind != ApplyReply.Kind.InsufficientEpochs)
                return ApplyReply.lookupByKind(kind);

            long minEpoch = in.readUnsignedVInt();
            return new ApplyReply(kind, minEpoch);
        }

        @Override
        public long serializedSize(ApplyReply t, Version version)
        {
            long size = kinds.serializedSize(t.kind);
            if (t.kind == ApplyReply.Kind.InsufficientEpochs)
                size += VIntCoding.sizeOfUnsignedVInt(t.minEpoch());
            return size;
        }
    }
}
