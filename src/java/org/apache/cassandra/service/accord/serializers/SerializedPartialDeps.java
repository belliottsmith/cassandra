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
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.KeyDeps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialDepsView;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.BitUtils;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.local.cfk.CommandsForKey.managesExecution;
import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static org.apache.cassandra.service.accord.serializers.DepsSerializers.AbstractDepsSerializer.KEYS_BY_TXNID;
import static org.apache.cassandra.service.accord.serializers.DepsSerializers.AbstractDepsSerializer.RANGES_BY_TXNID;
import static org.apache.cassandra.service.accord.serializers.DepsSerializers.AbstractDepsSerializer.deserializePackedXtoY;
import static org.apache.cassandra.service.accord.serializers.DepsSerializers.AbstractDepsSerializer.isInferredXtoY;
import static org.apache.cassandra.service.accord.serializers.DepsSerializers.AbstractDepsSerializer.skipPackedXtoY;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.readPackedBits;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.serializedPackedBitsSize;

/**
 * A {@link PartialDepsView} that retains the deps in the serialized form produced by {@link DepsSerializers.PartialDepsSerializer},
 * deserializing (and memoising) only those portions that are required to answer a given query.
 *
 * The serialized form is (see {@link DepsSerializers.AbstractDepsSerializer}):
 * <pre>
 *   flags                    (KEYS_BY_TXNID | RANGES_BY_TXNID)
 *   keys                     (see {@link KeySerializers.AbstractSearchableSerializer})
 *   key TxnId[]              (columnar fixed-width packed; see {@link CommandSerializers.VariableWidthTimestampSerializer#serializeArray})
 *   key xtoy mapping         (fixed-width packed, or omitted if inferrable; see {@link #isInferredXtoY})
 *   ranges
 *   range TxnId[]
 *   range xtoy mapping
 *   covering                 (see {@link KeySerializers#participants})
 * </pre>
 * We scan this once on construction to record the offsets of each section, so that we may later decode any section
 * directly. Key-domain {@code TxnId} membership, stability and participants may be queried directly against the
 * serialized form without allocating any TxnId or deserializing the collection.
 *
 * Memoised state is not guarded by any synchronisation: decoded objects are immutable and may be safely recomputed,
 * so racing threads at worst duplicate work.
 *
 * {@link #unmemoize()} discards only the potentially large {@link KeyDeps} (and the fully materialised {@link PartialDeps}),
 * retaining {@link #keys()}, {@link #rangeDeps()} and {@link #covering()} as these are typically small and are
 * referenced by {@link accord.local.Command.WaitingOn}, so that discarding them would only lead to duplication.
 */
public final class SerializedPartialDeps implements PartialDepsView
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new SerializedPartialDeps(new byte[0], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false));

    private final byte[] bytes;
    private final int flags;
    // offsets are into bytes
    private final int keysOffset, keyCount;
    private final int keyTxnIdsOffset, keyTxnIdCount;
    private final int keyMapOffset, keyMapLength;
    private final int rangesOffset, rangeCount;
    private final int rangeTxnIdsOffset, rangeTxnIdCount;
    private final int rangeMapOffset, rangeMapLength;
    private final int coveringOffset, coveringCount;
    private final boolean coveringIsRanges;

    private RoutingKeys keys;
    private KeyDeps keyDeps;
    private RangeDeps rangeDeps;
    private Participants<?> covering;
    private PartialDeps materialized;

    private SerializedPartialDeps(byte[] bytes, int flags,
                                  int keysOffset, int keyCount, int keyTxnIdsOffset, int keyTxnIdCount, int keyMapOffset, int keyMapLength,
                                  int rangesOffset, int rangeCount, int rangeTxnIdsOffset, int rangeTxnIdCount, int rangeMapOffset, int rangeMapLength,
                                  int coveringOffset, int coveringCount, boolean coveringIsRanges)
    {
        this.bytes = bytes;
        this.flags = flags;
        this.keysOffset = keysOffset;
        this.keyCount = keyCount;
        this.keyTxnIdsOffset = keyTxnIdsOffset;
        this.keyTxnIdCount = keyTxnIdCount;
        this.keyMapOffset = keyMapOffset;
        this.keyMapLength = keyMapLength;
        this.rangesOffset = rangesOffset;
        this.rangeCount = rangeCount;
        this.rangeTxnIdsOffset = rangeTxnIdsOffset;
        this.rangeTxnIdCount = rangeTxnIdCount;
        this.rangeMapOffset = rangeMapOffset;
        this.rangeMapLength = rangeMapLength;
        this.coveringOffset = coveringOffset;
        this.coveringCount = coveringCount;
        this.coveringIsRanges = coveringIsRanges;
    }

    /**
     * Read a {@link PartialDeps} serialized by {@link DepsSerializers.PartialDepsSerializer} from {@code in},
     * advancing {@code in} past it. The serialized bytes are copied, so no reference to {@code in} is retained.
     */
    public static SerializedPartialDeps read(DataInputBuffer in) throws IOException
    {
        ByteBuffer buffer = in.buffer();
        int start = buffer.position();
        int flags = in.readUnsignedVInt32();

        int keysOffset = buffer.position() - start;
        int keyCount = KeySerializers.routingKeys.countAndSkip(in);
        int keyTxnIdsOffset = buffer.position() - start;
        int keyTxnIdCount = CommandSerializers.txnId.skipArray(in);
        int keyMapOffset = buffer.position() - start;
        int keyMapLength = 0 != (flags & KEYS_BY_TXNID) ? skipPackedXtoY(keyTxnIdCount, keyCount, in)
                                                        : skipPackedXtoY(keyCount, keyTxnIdCount, in);

        int rangesOffset = buffer.position() - start;
        int rangeCount = KeySerializers.rangeArray.countAndSkip(in);
        int rangeTxnIdsOffset = buffer.position() - start;
        int rangeTxnIdCount = CommandSerializers.txnId.skipArray(in);
        int rangeMapOffset = buffer.position() - start;
        int rangeMapLength = 0 != (flags & RANGES_BY_TXNID) ? skipPackedXtoY(rangeTxnIdCount, rangeCount, in)
                                                            : skipPackedXtoY(rangeCount, rangeTxnIdCount, in);

        int coveringOffset = buffer.position() - start;
        // the first byte encodes the kind of Participants: 1-3 are keys, 4-6 are ranges (see KeySerializers.AbstractRoutablesSerializer)
        boolean coveringIsRanges = buffer.get(buffer.position()) >= 4;
        int coveringCount = KeySerializers.participants.countAndSkip(in);
        int end = buffer.position();

        byte[] bytes = new byte[end - start];
        buffer.duplicate().position(start).get(bytes);
        return new SerializedPartialDeps(bytes, flags,
                                         keysOffset, keyCount, keyTxnIdsOffset, keyTxnIdCount, keyMapOffset, keyMapLength,
                                         rangesOffset, rangeCount, rangeTxnIdsOffset, rangeTxnIdCount, rangeMapOffset, rangeMapLength,
                                         coveringOffset, coveringCount, coveringIsRanges);
    }

    @VisibleForTesting
    public static SerializedPartialDeps serialize(PartialDeps deps, DepsSerializers.AbstractDepsSerializer<PartialDeps> serializer)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(deps, out);
            try (DataInputBuffer in = new DataInputBuffer(out.unsafeGetBufferAndFlip(), false))
            {
                SerializedPartialDeps result = read(in);
                Invariants.require(!in.buffer().hasRemaining());
                return result;
            }
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public void serialize(DataOutputPlus out) throws IOException
    {
        out.write(bytes);
    }

    public int serializedSize()
    {
        return bytes.length;
    }

    public boolean keysByTxnId()
    {
        return 0 != (flags & KEYS_BY_TXNID);
    }

    public boolean rangesByTxnId()
    {
        return 0 != (flags & RANGES_BY_TXNID);
    }

    @Override
    public PartialDeps materialize()
    {
        PartialDeps result = materialized;
        if (result == null)
            materialized = result = new PartialDeps(covering(), keyDeps(), rangeDeps());
        return result;
    }

    @Override
    public KeyDeps keyDeps()
    {
        KeyDeps result = keyDeps;
        if (result == null)
        {
            RoutingKeys keys = keys();
            try (DataInputBuffer in = in(keyTxnIdsOffset))
            {
                TxnId[] txnIds = CommandSerializers.txnId.deserializeArray(in);
                int[] keysToTxnIds = null, txnIdsToKeys = null;
                if (keysByTxnId()) txnIdsToKeys = deserializePackedXtoY(txnIds.length, keys.size(), in);
                else keysToTxnIds = deserializePackedXtoY(keys.size(), txnIds.length, in);
                keyDeps = result = KeyDeps.SerializerSupport.create(keys, txnIds, keysToTxnIds, txnIdsToKeys);
            }
            catch (IOException e)
            {
                throw failedToDeserialize(e);
            }
        }
        return result;
    }

    @Override
    public RangeDeps rangeDeps()
    {
        RangeDeps result = rangeDeps;
        if (result == null)
        {
            try (DataInputBuffer in = in(rangesOffset))
            {
                Range[] ranges = KeySerializers.rangeArray.deserialize(in);
                TxnId[] txnIds = CommandSerializers.txnId.deserializeArray(in);
                int[] rangesToTxnIds = null, txnIdsToRanges = null;
                if (rangesByTxnId()) txnIdsToRanges = deserializePackedXtoY(txnIds.length, ranges.length, in);
                else rangesToTxnIds = deserializePackedXtoY(ranges.length, txnIds.length, in);
                rangeDeps = result = RangeDeps.SerializerSupport.create(ranges, txnIds, rangesToTxnIds, txnIdsToRanges);
            }
            catch (IOException e)
            {
                throw failedToDeserialize(e);
            }
        }
        return result;
    }

    @Override
    public Participants<?> covering()
    {
        Participants<?> result = covering;
        if (result == null)
        {
            try (DataInputBuffer in = in(coveringOffset))
            {
                covering = result = KeySerializers.participants.deserialize(in);
            }
            catch (IOException e)
            {
                throw failedToDeserialize(e);
            }
        }
        return result;
    }

    @Override
    public RoutingKeys keys()
    {
        RoutingKeys result = keys;
        if (result == null)
        {
            try (DataInputBuffer in = in(keysOffset))
            {
                keys = result = KeySerializers.routingKeys.deserialize(in);
            }
            catch (IOException e)
            {
                throw failedToDeserialize(e);
            }
        }
        return result;
    }

    @Override
    public boolean isEmpty()
    {
        // see KeyDeps.isEmpty() and RangeDeps.isEmpty(): a mapping is empty if it contains only the per-x end offsets
        int keyMapXCount = keysByTxnId() ? keyTxnIdCount : keyCount;
        int rangeMapXCount = rangesByTxnId() ? rangeTxnIdCount : rangeCount;
        return keyMapLength == keyMapXCount && rangeMapLength == rangeMapXCount;
    }

    @Override
    public int txnIdCount()
    {
        return keyTxnIdCount + rangeTxnIdCount;
    }

    @Override
    public boolean isStableDepCovering(TxnId txnId, Unseekables<?> intersecting)
    {
        switch (txnId.domain())
        {
            default: throw new UnhandledEnum(txnId.domain());
            case Key:
            {
                Invariants.require(managesExecution(txnId));
                KeyDeps keyDeps = this.keyDeps;
                if (keyDeps != null)
                {
                    int i = keyDeps.indexOf(txnId);
                    return i >= 0 && !keyDeps.txnIdWithFlags(i).is(UNSTABLE) && keyDeps.participants(i).containsAll(intersecting);
                }

                PackedTxnIds txnIds = keyTxnIds();
                int i = txnIds.find(txnId);
                return i >= 0 && 0 == (txnIds.flags(i) & UNSTABLE.bit) && keyParticipants(i).containsAll(intersecting);
            }
            case Range:
            {
                RangeDeps rangeDeps = rangeDeps();
                int i = rangeDeps.indexOf(txnId);
                return i >= 0 && !rangeDeps.txnIdWithFlags(i).is(UNSTABLE) && rangeDeps.participants(i).containsAll(intersecting);
            }
        }
    }

    @Override
    public Participants<?> participants(TxnId txnId)
    {
        if (txnId.domain() != Routable.Domain.Key || keyDeps != null)
            return PartialDepsView.super.participants(txnId);

        Invariants.require(managesExecution(txnId));
        int i = keyTxnIds().find(txnId);
        if (i < 0)
            return RoutingKeys.EMPTY;
        return keyParticipants(i);
    }

    @Override
    public void unmemoize()
    {
        materialized = null;
        keyDeps = null;
    }

    @Override
    public boolean isSameAs(@Nullable PartialDepsView that)
    {
        return this == that || (that != null && that == materialized);
    }

    /**
     * An estimate of the heap consumed by this object and its serialized form, excluding any deserialized state.
     * The caller is responsible for estimating the deserialized state, which may be done without deserializing it
     * using the shape of the serialized form (see e.g. {@link #keyCount()}, {@link #keysSerializedSize()}).
     */
    public long unsharedHeapSizeExcludingMemoized()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(bytes);
    }

    public @Nullable RoutingKeys memoizedKeys() { return keys; }
    public @Nullable KeyDeps memoizedKeyDeps() { return keyDeps; }
    public @Nullable RangeDeps memoizedRangeDeps() { return rangeDeps; }
    public @Nullable Participants<?> memoizedCovering() { return covering; }

    // the shape of the serialized form, permitting the size of deserialized components to be estimated without deserializing them
    public int keyCount() { return keyCount; }
    public int keysSerializedSize() { return keyTxnIdsOffset - keysOffset; }
    public int rangeCount() { return rangeCount; }
    public int rangesSerializedSize() { return rangeTxnIdsOffset - rangesOffset; }
    public int rangeTxnIdCount() { return rangeTxnIdCount; }
    public int rangeMapLength() { return rangeMapLength; }
    public int coveringCount() { return coveringCount; }
    public boolean coveringIsRanges() { return coveringIsRanges; }
    public int coveringSerializedSize() { return bytes.length - coveringOffset; }

    @Override
    public String toString()
    {
        return materialize().toString();
    }

    private DataInputBuffer in(int offset)
    {
        return new DataInputBuffer(bytes, offset, bytes.length - offset);
    }

    private IllegalStateException failedToDeserialize(IOException e)
    {
        return new IllegalStateException("Failed to deserialize partially deserialized deps", e);
    }

    private PackedTxnIds keyTxnIds()
    {
        try
        {
            PackedTxnIds result = new PackedTxnIds(bytes, keyTxnIdsOffset);
            Invariants.require(result.count == keyTxnIdCount);
            return result;
        }
        catch (IOException e)
        {
            throw failedToDeserialize(e);
        }
    }

    private PackedRelation keyMap()
    {
        try
        {
            return keysByTxnId() ? new PackedRelation(bytes, keyMapOffset, keyTxnIdCount, keyCount)
                                 : new PackedRelation(bytes, keyMapOffset, keyCount, keyTxnIdCount);
        }
        catch (IOException e)
        {
            throw failedToDeserialize(e);
        }
    }

    /**
     * Equivalent to {@code keyDeps().participants(txnIdx)}, computed directly against the serialized form.
     */
    @VisibleForTesting
    RoutingKeys keyParticipants(int txnIdx)
    {
        RoutingKeys keys = keys();
        PackedRelation map = keyMap();
        Invariants.require(map.length == keyMapLength);
        if (keysByTxnId())
        {
            int start = map.start(txnIdx), end = map.end(txnIdx);
            if (start == end)
                return RoutingKeys.EMPTY;

            RoutingKey[] result = new RoutingKey[end - start];
            for (int i = start ; i < end ; ++i)
                result[i - start] = keys.get(map.get(i));
            return RoutingKeys.ofSortedUnique(result);
        }
        else
        {
            RoutingKey[] result = null;
            int count = 0;
            for (int keyIdx = 0 ; keyIdx < keyCount ; ++keyIdx)
            {
                if (!map.contains(keyIdx, txnIdx))
                    continue;

                if (result == null)
                    result = new RoutingKey[keyCount - keyIdx];
                result[count++] = keys.get(keyIdx);
            }
            if (count == 0)
                return RoutingKeys.EMPTY;
            if (count < result.length)
                result = Arrays.copyOf(result, count);
            return RoutingKeys.ofSortedUnique(result);
        }
    }

    @VisibleForTesting
    TxnId keyTxnIdWithFlags(int txnIdx)
    {
        return keyTxnIds().get(txnIdx);
    }

    @VisibleForTesting
    int findKeyTxnId(TxnId txnId)
    {
        return keyTxnIds().find(txnId);
    }

    /**
     * Random access to a {@code TxnId[]} serialized by {@link CommandSerializers.VariableWidthTimestampSerializer#serializeArray}:
     * <pre>
     *   count (vint)
     *   minEpoch (vint), minHlc (vint), minFlags (vint), minNodeId (vint)
     *   epochBits, hlcBits, flagBits, nodeBits (byte)
     *   packed epoch deltas, packed hlc deltas, packed flag deltas, packed node id deltas
     * </pre>
     * The deltas are fixed width, so any element may be decoded directly, and as the array is sorted (and the deltas
     * preserve the ordering of each component) we may binary search it without materialising any TxnId.
     */
    static final class PackedTxnIds
    {
        final byte[] bytes;
        final int count;
        final long minEpoch, minHlc;
        final int minFlags, minNodeId;
        final int epochBits, hlcBits, flagBits, nodeBits;
        final int epochOffset, hlcOffset, flagOffset, nodeOffset;
        final int epochBytes, hlcBytes, flagBytes, nodeBytes;

        PackedTxnIds(byte[] bytes, int offset) throws IOException
        {
            this.bytes = bytes;
            try (DataInputBuffer in = new DataInputBuffer(bytes, offset, bytes.length - offset))
            {
                count = in.readUnsignedVInt32();
                if (count == 0)
                {
                    minEpoch = minHlc = 0;
                    minFlags = minNodeId = 0;
                    epochBits = hlcBits = flagBits = nodeBits = 0;
                    epochOffset = hlcOffset = flagOffset = nodeOffset = 0;
                    epochBytes = hlcBytes = flagBytes = nodeBytes = 0;
                    return;
                }

                minEpoch = in.readUnsignedVInt();
                minHlc = in.readUnsignedVInt();
                minFlags = in.readUnsignedVInt32();
                minNodeId = in.readUnsignedVInt32();
                epochBits = in.readByte();
                hlcBits = in.readByte();
                flagBits = in.readByte();
                nodeBits = in.readByte();
                epochOffset = offset + in.buffer().position();
            }
            epochBytes = serializedPackedBitsSize(count, epochBits);
            hlcOffset = epochOffset + epochBytes;
            hlcBytes = serializedPackedBitsSize(count, hlcBits);
            flagOffset = hlcOffset + hlcBytes;
            flagBytes = serializedPackedBitsSize(count, flagBits);
            nodeOffset = flagOffset + flagBytes;
            nodeBytes = serializedPackedBitsSize(count, nodeBits);
        }

        long epoch(int i) { return minEpoch + readPackedBits(bytes, epochOffset, epochBytes, epochBits, i); }
        long hlc(int i) { return minHlc + readPackedBits(bytes, hlcOffset, hlcBytes, hlcBits, i); }
        int flags(int i) { return minFlags + (int) readPackedBits(bytes, flagOffset, flagBytes, flagBits, i); }
        int node(int i) { return minNodeId + (int) readPackedBits(bytes, nodeOffset, nodeBytes, nodeBits, i); }

        private static long msb(long epoch, long hlc) { return Timestamp.epochMsb(epoch) | Timestamp.hlcMsb(hlc); }
        private static long lsb(long hlc, int flags) { return Timestamp.hlcLsb(hlc) | flags; }

        TxnId get(int i)
        {
            Invariants.requireIndex(i, count);
            long hlc = hlc(i);
            return TxnId.fromBits(msb(epoch(i), hlc), lsb(hlc, flags(i)), new Node.Id(node(i)));
        }

        /**
         * Equivalent to {@code get(i).compareTo(find)}
         */
        int compare(int i, TxnId find)
        {
            long hlc = hlc(i);
            int c = Timestamp.compareMsb(msb(epoch(i), hlc), find.msb);
            if (c == 0) c = Timestamp.compareLsb(lsb(hlc, flags(i)), find.lsb);
            if (c == 0) c = Integer.compareUnsigned(node(i), find.node.id);
            return c;
        }

        /**
         * Equivalent to {@code Arrays.binarySearch(txnIds, find)}
         */
        int find(TxnId find)
        {
            int lo = 0, hi = count - 1;
            while (lo <= hi)
            {
                int mid = (lo + hi) >>> 1;
                int c = compare(mid, find);
                if (c < 0) lo = mid + 1;
                else if (c > 0) hi = mid - 1;
                else return mid;
            }
            return -1 - lo;
        }
    }

    /**
     * Random access to an xtoy mapping serialized by {@link DepsSerializers.AbstractDepsSerializer} (see {@link accord.utils.RelationMultiMap}):
     * the first xCount entries are the (exclusive) end offsets of each x's y entries, which follow.
     * Where the mapping may be inferred (see {@link #isInferredXtoY}) it is not serialized, and we infer it here.
     */
    static final class PackedRelation
    {
        final byte[] bytes;
        final int xCount, yCount, length;
        final boolean inferred;
        final int offsetsOffset, offsetsBits, offsetsBytes;
        final int valuesOffset, valuesBits, valuesBytes;

        PackedRelation(byte[] bytes, int offset, int xCount, int yCount) throws IOException
        {
            this.bytes = bytes;
            this.xCount = xCount;
            this.yCount = yCount;
            try (DataInputBuffer in = new DataInputBuffer(bytes, offset, bytes.length - offset))
            {
                this.length = in.readUnsignedVInt32();
                this.offsetsOffset = offset + in.buffer().position();
            }
            this.inferred = isInferredXtoY(xCount, yCount, length);
            if (inferred)
            {
                offsetsBits = offsetsBytes = valuesOffset = valuesBits = valuesBytes = 0;
            }
            else
            {
                // see DepsSerializers.AbstractDepsSerializer.serializePackedXtoY
                offsetsBits = BitUtils.numberOfBitsToRepresent(length);
                offsetsBytes = serializedPackedBitsSize(xCount, offsetsBits);
                valuesOffset = offsetsOffset + offsetsBytes;
                valuesBits = BitUtils.numberOfBitsToRepresent(yCount - 1);
                valuesBytes = serializedPackedBitsSize(length - xCount, valuesBits);
            }
        }

        /**
         * Equivalent to {@code xtoy[i]}
         */
        int get(int i)
        {
            Invariants.requireIndex(i, length);
            if (!inferred)
            {
                return i < xCount ? (int) readPackedBits(bytes, offsetsOffset, offsetsBytes, offsetsBits, i)
                                  : (int) readPackedBits(bytes, valuesOffset, valuesBytes, valuesBits, i - xCount);
            }

            // see DepsSerializers.AbstractDepsSerializer.deserializePackedXtoY
            if (xCount == 1) return i == 0 ? length : i - 1;
            else if (yCount == 1) return i < xCount ? xCount + i + 1 : 0;
            else if (yCount == 0) return xCount;
            else throw new IllegalStateException("Unexpected inferred mapping with length " + length);
        }

        int start(int x)
        {
            return x == 0 ? xCount : get(x - 1);
        }

        int end(int x)
        {
            return get(x);
        }

        /**
         * @return true iff x maps to y; as each x's y entries are sorted we may binary search them
         */
        boolean contains(int x, int y)
        {
            int lo = start(x), hi = end(x) - 1;
            while (lo <= hi)
            {
                int mid = (lo + hi) >>> 1;
                int c = Integer.compare(get(mid), y);
                if (c < 0) lo = mid + 1;
                else if (c > 0) hi = mid - 1;
                else return true;
            }
            return false;
        }
    }
}
