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

import com.google.common.annotations.VisibleForTesting;

import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialDepsView;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.VIntCoding;

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.NullableSerializer;

import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIds;
import static accord.primitives.KeyDeps.SerializerSupport.txnIdsToKeys;
import static accord.primitives.RangeDeps.SerializerSupport.ranges;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIds;
import static accord.primitives.RangeDeps.SerializerSupport.txnIdsToRanges;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.deserializePackedInts;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.serializePackedInts;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.serializedPackedIntsSize;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.serializedPackedSize;

public class DepsSerializers
{
    public static final UnversionedSerializer<Range> tokenRange;
    public static final AbstractDepsSerializer<Deps> deps;
    public static final UnversionedSerializer<Deps> nullableDeps;
    public static final AbstractDepsSerializer<PartialDeps> partialDeps;
    public static final AbstractDepsSerializer<PartialDeps> partialDepsById;
    public static final AbstractDepsSerializer<PartialDeps> partialDepsByKey;
    public static final UnversionedSerializer<PartialDeps> nullablePartialDeps;

    static
    {
        // We use a separate class for initialization to make it easier for BurnTest to plug its own serializers.
        Impl serializers = new Impl((UnversionedSerializer<Range>) (UnversionedSerializer<?>) TokenRange.serializer);
        tokenRange = serializers.tokenRange;
        deps = serializers.deps;
        nullableDeps = serializers.nullableDeps;
        partialDeps = serializers.partialDeps;
        partialDepsById = serializers.partialDepsById;
        partialDepsByKey = serializers.partialDepsByKey;
        nullablePartialDeps = serializers.nullablePartialDeps;
    }

    /**
     * Select the serializer to use for persisting the deps of {@code txnId} in the journal (or the cache's shrunk form).
     *
     * Key-domain commands are the most common, and their deps are normally accessed by key (e.g. to determine if
     * they are ready to execute), so we store their KeyDeps by key. Range-domain commands may be queried by
     * TxnId in their serialized form when scanning for recovery (see {@link SerializedPartialDeps#isStableDepCovering}),
     * so we store their KeyDeps by TxnId. The serialized form encodes its layout, so both may be read by either.
     */
    public static AbstractDepsSerializer<PartialDeps> journalSerializer(TxnId txnId)
    {
        return txnId.is(Routable.Domain.Key) ? partialDepsByKey : partialDepsById;
    }

    /**
     * Serialize deps for the journal (or the cache's shrunk form). If the deps are already in their serialized form
     * we simply copy the bytes, avoiding any materialisation or re-serialization.
     */
    public static void serializeForJournal(TxnId txnId, PartialDepsView deps, DataOutputPlus out) throws IOException
    {
        if (deps instanceof SerializedPartialDeps) ((SerializedPartialDeps) deps).serialize(out);
        else journalSerializer(txnId).serialize(deps.materialize(), out);
    }

    public static long serializedSizeForJournal(TxnId txnId, PartialDepsView deps)
    {
        if (deps instanceof SerializedPartialDeps) return ((SerializedPartialDeps) deps).serializedSize();
        else return journalSerializer(txnId).serializedSize(deps.materialize());
    }

    /**
     * Read deps written by {@link #serializeForJournal}. Where possible the deps are retained in their serialized form,
     * so that they may be (partially) deserialized only if and when they are needed. The bytes are always copied, so
     * that we do not retain any reference to the source buffer (which may, for instance, be memory mapped).
     */
    public static PartialDepsView deserializeForJournal(DataInputPlus in) throws IOException
    {
        if (in instanceof DataInputBuffer)
            return SerializedPartialDeps.read((DataInputBuffer) in);
        return partialDepsById.deserialize(in);
    }

    public static abstract class AbstractDepsSerializer<D extends Deps> implements UnversionedSerializer<D>
    {
        static final int KEYS_BY_TXNID = 0x1;
        static final int RANGES_BY_TXNID = 0x2;
        static final int FLAGS_SIZE = VIntCoding.sizeOfUnsignedVInt(KEYS_BY_TXNID | RANGES_BY_TXNID);
        final boolean keysByTxnId, rangesByTxnId;

        protected final UnversionedSerializer<Range> tokenRange;
        public AbstractDepsSerializer(boolean byTxnId, UnversionedSerializer<Range> tokenRange)
        {
            this(byTxnId, byTxnId, tokenRange);
        }

        public AbstractDepsSerializer(boolean keysByTxnId, boolean rangesByTxnId, UnversionedSerializer<Range> tokenRange)
        {
            this.keysByTxnId = keysByTxnId;
            this.rangesByTxnId = rangesByTxnId;
            this.tokenRange = tokenRange;
        }

        abstract D deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in) throws IOException;

        @Override
        public void serialize(D deps, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32((keysByTxnId ? KEYS_BY_TXNID : 0) | (rangesByTxnId ? RANGES_BY_TXNID : 0));
            {
                KeyDeps keyDeps = deps.keyDeps;
                KeySerializers.routingKeys.serialize(keyDeps.keys(), out);
                CommandSerializers.txnId.serializeArray(KeyDeps.SerializerSupport.txnIds(keyDeps), out);
                if (keysByTxnId) serializePackedXtoY(txnIdsToKeys(keyDeps), keyDeps.txnIdCount(), keyDeps.keys().size(), out);
                else serializePackedXtoY(keysToTxnIds(keyDeps), keyDeps.keys().size(), keyDeps.txnIdCount(), out);
            }
            {
                RangeDeps rangeDeps = deps.rangeDeps;
                KeySerializers.rangeArray.serialize(ranges(rangeDeps), out);
                CommandSerializers.txnId.serializeArray(RangeDeps.SerializerSupport.txnIds(rangeDeps), out);
                if (rangesByTxnId) serializePackedXtoY(txnIdsToRanges(rangeDeps), rangeDeps.txnIdCount(), rangeDeps.rangeCount(), out);
                else serializePackedXtoY(rangesToTxnIds(rangeDeps), rangeDeps.rangeCount(), rangeDeps.txnIdCount(), out);
            }
        }

        private static void serializePackedXtoY(int[] xtoy, int xCount, int yCount, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(xtoy.length);

            if (isInferredXtoY(xCount, yCount, xtoy.length))
            {
                // no point serializing as can be directly inferred
                if (Invariants.isParanoid())
                {
                    if (xCount == 1)
                    {
                        Invariants.require(xtoy[0] == xtoy.length, "%d != %d", xtoy[0], xtoy.length);
                        for (int i = 0 ; i < yCount ; ++i) Invariants.require(xtoy[1 + i] == i, "%d != %d", xtoy[1 + i], i);
                    }
                    else if (yCount == 1)
                    {
                        for (int i = 0 ; i < xCount ; ++i) Invariants.require(xtoy[i] == xCount + i + 1, "%d != %d", xtoy[i], xCount + i + 1);
                        for (int i = xCount ; i < xtoy.length ; ++i) Invariants.require(xtoy[i] == 0, "%d != %d", xtoy[i], 0);
                    }
                    else if (yCount == 0)
                    {
                        for (int i = 0 ; i < xCount ; ++i) Invariants.require(xtoy[i] == xCount, "%d != %d", xtoy[i], xCount);
                    }
                    else
                    {
                        Invariants.require(xtoy.length == 0);
                    }
                }
            }
            else
            {
                serializePackedInts(xtoy, 0, xCount, xtoy.length, out);
                serializePackedInts(xtoy, xCount, xtoy.length, yCount - 1, out);
            }
        }

        @Override
        public D deserialize(DataInputPlus in) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            KeyDeps keyDeps;
            {
                RoutingKeys keys = KeySerializers.routingKeys.deserialize(in);
                TxnId[] txnIds = CommandSerializers.txnId.deserializeArray(in);
                int[] txnIdsToKeys = null, keysToTxnIds = null;
                if (0 != (flags & KEYS_BY_TXNID)) txnIdsToKeys = deserializePackedXtoY(txnIds.length, keys.size(), in);
                else keysToTxnIds = deserializePackedXtoY(keys.size(), txnIds.length, in);
                keyDeps = KeyDeps.SerializerSupport.create(keys, txnIds, keysToTxnIds, txnIdsToKeys);
            }

            RangeDeps rangeDeps;
            {
                Range[] ranges = KeySerializers.rangeArray.deserialize(in);
                TxnId[] txnIds = CommandSerializers.txnId.deserializeArray(in);
                int[] txnIdsToRanges = null, rangesToTxnIds = null;
                if (0 != (flags & RANGES_BY_TXNID)) txnIdsToRanges = deserializePackedXtoY(txnIds.length, ranges.length, in);
                else rangesToTxnIds = deserializePackedXtoY(ranges.length, txnIds.length, in);
                rangeDeps = RangeDeps.SerializerSupport.create(ranges, txnIds, rangesToTxnIds, txnIdsToRanges);
            }
            return deserialize(keyDeps, rangeDeps, in);
        }

        /**
         * If there is only one x or y, the mapping may be inferred from its length and is not serialized
         * (see {@link SerializedPartialDeps.PackedRelation} for random access to the inferred mapping)
         */
        static boolean isInferredXtoY(int xCount, int yCount, int length)
        {
            return (xCount <= 1 || yCount <= 1) && (length == (xCount == 1 ? 1 + yCount : 2 * xCount) || xCount == 0 || yCount == 0);
        }

        static int[] deserializePackedXtoY(int xCount, int yCount, DataInputPlus in) throws IOException
        {
            int length = in.readUnsignedVInt32();
            int[] xtoy = new int[length];

            if (isInferredXtoY(xCount, yCount, length))
            {
                // no point serializing as can be directly inferred
                if (xCount == 1)
                {
                    xtoy[0] = xtoy.length;
                    for (int i = 0 ; i < yCount ; ++i)
                        xtoy[1 + i] = i;
                }
                else if (yCount == 1)
                {
                    for (int i = 0 ; i < xCount ; ++i)
                        xtoy[i] = xCount + i + 1;
                }
                else if (yCount == 0)
                {
                    for (int i = 0 ; i < xCount ; ++i)
                        xtoy[i] = xCount;
                }
                else
                {
                    Invariants.require(length == 0);
                }
            }
            else
            {
                deserializePackedInts(xtoy, 0, xCount, xtoy.length, in);
                deserializePackedInts(xtoy, xCount, xtoy.length, yCount - 1, in);
            }
            return xtoy;
        }

        // returns the length of the xtoy mapping
        static int skipPackedXtoY(int xCount, int yCount, DataInputPlus in) throws IOException
        {
            int length = in.readUnsignedVInt32();

            if (!isInferredXtoY(xCount, yCount, length))
            {
                in.skipBytesFully(serializedPackedSize(xCount, length)
                                  + serializedPackedSize(length - xCount, yCount - 1));
            }
            return length;
        }

        @Override
        public void skip(DataInputPlus in) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            {
                int keyCount = KeySerializers.routingKeys.countAndSkip(in);
                int txnIdCount = CommandSerializers.txnId.skipArray(in);
                if (0 != (flags & KEYS_BY_TXNID)) skipPackedXtoY(txnIdCount, keyCount, in);
                else skipPackedXtoY(keyCount, txnIdCount, in);
            }

            {
                int rangeCount = KeySerializers.rangeArray.countAndSkip(in);
                int txnIdCount = CommandSerializers.txnId.skipArray(in);
                if (0 != (flags & RANGES_BY_TXNID)) skipPackedXtoY(txnIdCount, rangeCount, in);
                else skipPackedXtoY(rangeCount, txnIdCount, in);
            }
        }

        @Override
        public long serializedSize(D deps)
        {
            long size = FLAGS_SIZE;
            {
                KeyDeps keyDeps = deps.keyDeps;
                size += KeySerializers.routingKeys.serializedSize(keyDeps.keys());
                size += CommandSerializers.txnId.serializedArraySize(KeyDeps.SerializerSupport.txnIds(keyDeps));
                size += keysByTxnId ? serializedPackedXtoYSize(txnIdsToKeys(keyDeps), keyDeps.txnIdCount(), keyDeps.keys().size())
                                    : serializedPackedXtoYSize(keysToTxnIds(keyDeps), keyDeps.keys().size(), keyDeps.txnIdCount());
            }
            {
                RangeDeps rangeDeps = deps.rangeDeps;
                size += KeySerializers.rangeArray.serializedSize(ranges(rangeDeps));
                size += CommandSerializers.txnId.serializedArraySize(RangeDeps.SerializerSupport.txnIds(rangeDeps));
                size += rangesByTxnId ? serializedPackedXtoYSize(txnIdsToRanges(rangeDeps), rangeDeps.txnIdCount(), rangeDeps.rangeCount())
                                      : serializedPackedXtoYSize(rangesToTxnIds(rangeDeps), rangeDeps.rangeCount(), rangeDeps.txnIdCount());
            }
            return size;
        }

        private static long serializedPackedXtoYSize(int[] xtoy, int xCount, int yCount)
        {
            long size = VIntCoding.sizeOfUnsignedVInt(xtoy.length);
            if (isInferredXtoY(xCount, yCount, xtoy.length))
            {
                // no point serializing as can be directly inferred
            }
            else
            {
                size += serializedPackedIntsSize(xtoy, 0, xCount, xtoy.length);
                size += serializedPackedIntsSize(xtoy, xCount, xtoy.length, yCount - 1);
            }
            return size;
        }
    }

    static class PartialDepsSerializer extends AbstractDepsSerializer<PartialDeps>
    {
        public PartialDepsSerializer(boolean preferByTxnId, UnversionedSerializer<Range> tokenRange)
        {
            super(preferByTxnId, tokenRange);
        }

        public PartialDepsSerializer(boolean keysByTxnId, boolean rangesByTxnId, UnversionedSerializer<Range> tokenRange)
        {
            super(keysByTxnId, rangesByTxnId, tokenRange);
        }

        @Override
        PartialDeps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in) throws IOException
        {
            Participants<?> covering = KeySerializers.participants.deserialize(in);
            return new PartialDeps(covering, keyDeps, rangeDeps);
        }

        @Override
        public void serialize(PartialDeps partialDeps, DataOutputPlus out) throws IOException
        {
            super.serialize(partialDeps, out);
            KeySerializers.participants.serialize(partialDeps.covering, out);
        }

        @Override
        public long serializedSize(PartialDeps partialDeps)
        {
            return super.serializedSize(partialDeps)
                   + KeySerializers.participants.serializedSize(partialDeps.covering);
        }

        @Override
        public void skip(DataInputPlus in) throws IOException
        {
            super.skip(in);
            KeySerializers.participants.skip(in);
        }
    }

    static class DepsSerializer extends AbstractDepsSerializer<Deps>
    {
        public DepsSerializer(boolean byTxnId, UnversionedSerializer<Range> tokenRange)
        {
            super(byTxnId, tokenRange);
        }

        @Override
        Deps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in) throws IOException
        {
            return new Deps(keyDeps, rangeDeps);
        }
    }

    @VisibleForTesting
    public static class Impl
    {
        final UnversionedSerializer<Range> tokenRange;
        final DepsSerializer deps;
        final UnversionedSerializer<Deps> nullableDeps;
        final PartialDepsSerializer partialDeps; // currently happens to be byId but doesn't rely upon it
        final PartialDepsSerializer partialDepsById;
        final PartialDepsSerializer partialDepsByKey; // keys by key, ranges by txnId
        final UnversionedSerializer<PartialDeps> nullablePartialDeps;

        public Impl(UnversionedSerializer<Range> tokenRange)
        {
            this.tokenRange = tokenRange;
            this.deps = new DepsSerializer(false, tokenRange);
            this.nullableDeps = NullableSerializer.wrap(deps);
            this.partialDeps = new PartialDepsSerializer(true, tokenRange);
            this.partialDepsById = partialDeps;
            this.partialDepsByKey = new PartialDepsSerializer(false, true, tokenRange);
            this.nullablePartialDeps = NullableSerializer.wrap(partialDeps);
        }
    }
}