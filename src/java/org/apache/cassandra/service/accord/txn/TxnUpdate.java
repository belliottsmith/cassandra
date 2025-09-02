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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Data;
import accord.api.Key;
import accord.api.Update;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import accord.utils.SimpleBitSets;
import accord.utils.SortedArrays;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.accord.AccordObjectSizes;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.SerializePacked;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.txn.TxnCondition.SerializedTxnCondition;
import org.apache.cassandra.service.accord.txn.TxnWrite.Fragment;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SimpleBitSetSerializers;

import static accord.utils.Invariants.requireArgument;
import static accord.utils.SortedArrays.Search.CEIL;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.FALSE;
import static org.apache.cassandra.service.accord.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.utils.ArraySerializers.skipArray;
import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.skipWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class TxnUpdate extends AccordUpdate
{
    static class ConditionalBlock
    {
        public static final UnversionedSerializer<ConditionalBlock> serializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(ConditionalBlock t, DataOutputPlus out) throws IOException
            {
                out.writeUnsignedVInt32(t.id);
                writeWithVIntLength(t.condition.bytes(), out);
                SerializePacked.serializePackedSortedIntsAndLength(t.fragments, out);
            }

            @Override
            public ConditionalBlock deserialize(DataInputPlus in) throws IOException
            {
                int id = in.readUnsignedVInt32();
                ByteBuffer conditionBytes = readWithVIntLength(in);
                SerializedTxnCondition condition = new SerializedTxnCondition(conditionBytes);

                // Deserialize mutations
                int[] mutations = SerializePacked.deserializePackedSortedIntsAndLength(in);
                return new ConditionalBlock(id, condition, mutations);
            }

            @Override
            public void skip(DataInputPlus in) throws IOException
            {
                in.readUnsignedVInt32();
                skipWithVIntLength(in);
                SerializePacked.skipPackedSortedIntsAndLength(in);
            }

            @Override
            public long serializedSize(ConditionalBlock t)
            {
                long size = TypeSizes.sizeofUnsignedVInt(t.id);
                size += serializedSizeWithVIntLength(t.condition.bytes());
                size += SerializePacked.serializedSizeOfPackedSortedIntsAndLength(t.fragments);
                return size;
            }
        };

        final int id;
        @Nonnull final SerializedTxnCondition condition;
        final int[] fragments;

        ConditionalBlock(int id, @Nonnull SerializedTxnCondition condition, int[] fragments)
        {
            this.id = id;
            this.condition = Invariants.nonNull(condition);
            this.fragments = fragments;
        }

        public long estimatedSizeOnHeap()
        {
            long size = 0; //TODO (correctness): EMPTY_SIZE
            size += condition.estimatedSizeOnHeap();
            size += ObjectSizes.sizeOfArray(fragments);
            return size;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            ConditionalBlock that = (ConditionalBlock) o;
            return id == that.id && Objects.equals(condition, that.condition) && Arrays.equals(fragments, that.fragments);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, condition, Arrays.hashCode(fragments));
        }

        public void toString(StringBuilder sb, TableMetadatas tables, Keys keys, Block block)
        {
            sb.append("{condition=")
              .append(condition.deserialize(tables))
              .append(", fragments=")
              .append(deserialize(keys, tables, block, fragments))
              .append('}');
        }
    }

    static class Block
    {
        private static SimpleBitSet bitset(Keys superset, Keys subset)
        {
            SimpleBitSet bits = SimpleBitSet.allocate(superset.size());
            int i = 0, m = 0;
            while (true)
            {
                long im = superset.findNextIntersection(i, subset, m);
                if (im < 0)
                    break;
                i = (int)(im);
                m = (int)(im >>> 32);
                bits.set(i);

                i++; m++;
            }
            return bits;
        }

        public static final ParameterisedUnversionedSerializer<Block, Keys> serializer = new ParameterisedUnversionedSerializer<>()
        {
            @Override
            public void serialize(Block t, Keys superset, DataOutputPlus out) throws IOException
            {
                SimpleBitSetSerializers.any.serialize(bitset(superset, t.keys), out);
                ArraySerializers.serializeArray(t.fragments, out, ByteBufferUtil.byteBufferSerializer);
                SerializePacked.serializePackedSortedInts(t.fragmentIds, out);
                ArraySerializers.serializeArray(t.conditionalBlocks, out, ConditionalBlock.serializer);
            }

            @Override
            public Block deserialize(Keys superset, DataInputPlus in) throws IOException
            {
                SimpleBitSet knownKeys = SimpleBitSetSerializers.any.deserialize(in);
                Key[] keyArray = new Key[knownKeys.getSetBitCount()];
                int c = 0;
                for (int i = knownKeys.nextSetBit(0); i >= 0; i = knownKeys.nextSetBit(i + 1))
                    keyArray[c++] = superset.get(i);
                Keys keys = Keys.ofSortedUnique(keyArray);
                ByteBuffer[] fragments = ArraySerializers.deserializeArray(in, ByteBufferUtil.byteBufferSerializer, ByteBuffer[]::new);
                int[] fragmentIds = SerializePacked.deserializePackedSortedInts(fragments.length, in);
                ConditionalBlock[] conditionalBlocks = ArraySerializers.deserializeArray(in, ConditionalBlock.serializer, ConditionalBlock[]::new);
                return new Block(keys, fragmentIds, fragments, conditionalBlocks);
            }

            @Override
            public void skip(Keys superset, DataInputPlus in) throws IOException
            {
                SimpleBitSetSerializers.any.skip(in);
                int length = ArraySerializers.skipArray(in, ByteBufferUtil.byteBufferSerializer);
                SerializePacked.skipPackedSortedInts(length, in);
                // array / collection share the same binary format, so its safe to mix and match
                ArraySerializers.skipArray(in, ConditionalBlock.serializer);
            }

            @Override
            public long serializedSize(Block t, Keys outter)
            {
                long size = 0;
                size += SimpleBitSetSerializers.any.serializedSize(bitset(outter, t.keys));
                size += ArraySerializers.serializedArraySize(t.fragments, ByteBufferUtil.byteBufferSerializer);
                size += SerializePacked.serializedSizeOfPackedSortedInts(t.fragmentIds);
                size += ArraySerializers.serializedArraySize(t.conditionalBlocks, ConditionalBlock.serializer);
                return size;
            }

        };

        final Keys keys;
        final int[] fragmentIds;
        final ByteBuffer[] fragments;
        final ConditionalBlock[] conditionalBlocks;

        Block(Keys keys, int[] fragmentIds, ByteBuffer[] fragments, ConditionalBlock[] conditionalBlocks)
        {
            this.keys = keys;
            this.fragmentIds = fragmentIds;
            this.fragments = fragments;
            this.conditionalBlocks = conditionalBlocks;
        }

        public long estimatedSizeOnHeap()
        {
            long size = 0; //TODO (correctness): EMPTY_SIZE, keys
            size += ObjectSizes.sizeOfArray(fragmentIds);
            for (ByteBuffer bb : fragments)
                size += ByteBufferUtil.estimatedSizeOnHeap(bb);
            for (ConditionalBlock conditionalBlock : conditionalBlocks)
                size += conditionalBlock.estimatedSizeOnHeap();
            return size;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            Block block = (Block) o;
            return Objects.equals(keys, block.keys) && Arrays.equals(fragmentIds, block.fragmentIds) && Arrays.equals(fragments, block.fragments) && Arrays.equals(conditionalBlocks, block.conditionalBlocks);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keys, Arrays.hashCode(fragmentIds), Arrays.hashCode(fragments), Arrays.hashCode(conditionalBlocks));
        }

        public void toString(StringBuilder sb, TableMetadatas tables)
        {
            sb.append("{conditionalBlocks=[");
            for (int j = 0; j < conditionalBlocks.length; j++)
            {
                if (j > 0) sb.append(", ");
                conditionalBlocks[j].toString(sb, tables, keys, this);
            }
            sb.append("]}");
        }

        public Block select(Function<Keys, Keys> fn)
        {
            Keys out = fn.apply(keys);
            if (keys.equals(out)) return this;

            int[] outFragmentIds = new int[out.size()];
            ByteBuffer[] outFragments = new ByteBuffer[out.size()];
            {
                int j = 0;
                for (int i = 0 ; i < out.size() ; ++i)
                {
                    j = keys.findNext(j, out.get(i), CEIL);
                    outFragmentIds[i] = fragmentIds[j];
                    outFragments[i] = fragments[j];
                    ++j;
                }
            }

            ConditionalBlock[] outConditions;
            if (outFragmentIds.length == 0) outConditions = new ConditionalBlock[0];
            else
            {
                List<ConditionalBlock> collect = new ArrayList<>(conditionalBlocks.length);
                int[] is = outFragmentIds;
                for (ConditionalBlock conditionalBlock : conditionalBlocks)
                {
                    boolean include = conditionalBlock.fragments.length == 0;
                    if (!include)
                    {
                        int i = 0, j = 0;
                        int[] js = conditionalBlock.fragments;
                        while (true)
                        {
                            i = SortedArrays.exponentialSearch(is, i, is.length, js[j]);
                            if (i >= 0)
                            {
                                include = true;
                                break;
                            }

                            i = -1 - i;
                            if (i == is.length)
                                break;

                            j = SortedArrays.exponentialSearch(js, j, js.length, is[i]);
                            if (j >= 0)
                            {
                                include = true;
                                break;
                            }

                            j = -1 - j;
                            if (j == js.length)
                                break;
                        }
                    }
                    if (include)
                        collect.add(conditionalBlock);
                }
                if (collect.size() == conditionalBlocks.length) outConditions = conditionalBlocks;
                else outConditions = collect.toArray(ConditionalBlock[]::new);
            }

            return new Block(out, outFragmentIds, outFragments, outConditions);
        }

        public Block merge(Block that)
        {
            Keys outKeys = keys.with(that.keys);
            int[] outFragmentIds = new int[outKeys.size()];
            ByteBuffer[] outFragments = new ByteBuffer[outKeys.size()];
            {
                int i = 0, j = 0, count = 0;
                while (i < this.fragments.length || j < that.fragments.length)
                {
                    int cmp;
                    if (i == this.fragments.length) cmp = 1;
                    else if (j == that.fragments.length) cmp = -1;
                    else cmp = this.fragmentIds[i] - that.fragmentIds[j];

                    if (cmp <= 0)
                    {
                        outFragmentIds[count] = this.fragmentIds[i];
                        outFragments[count] = this.fragments[i];
                        ++i;
                        j += cmp == 0 ? 1 : 0;
                    }
                    else
                    {
                        outFragmentIds[count] = that.fragmentIds[j];
                        outFragments[count] = that.fragments[j];
                        ++j;
                    }
                    ++count;
                }
            }

            ConditionalBlock[] outConditions;
            if (this.conditionalBlocks.length == 0) outConditions = that.conditionalBlocks;
            else if (that.conditionalBlocks.length == 0) outConditions = this.conditionalBlocks;
            else
            {
                int minId = Math.min(this.conditionalBlocks[0].id, that.conditionalBlocks[0].id);
                int maxId = Math.max(this.conditionalBlocks[this.conditionalBlocks.length - 1].id, that.conditionalBlocks[that.conditionalBlocks.length - 1].id);
                outConditions = new ConditionalBlock[Math.min(this.conditionalBlocks.length + that.conditionalBlocks.length, 1 + maxId - minId)];
                int i = 0, j = 0, count = 0;
                while (i < this.conditionalBlocks.length || j < that.conditionalBlocks.length)
                {
                    int cmp;
                    if (i == this.conditionalBlocks.length) cmp = 1;
                    else if (j == that.conditionalBlocks.length) cmp = -1;
                    else cmp = this.conditionalBlocks[i].id - that.conditionalBlocks[j].id;

                    if (cmp <= 0)
                    {
                        outConditions[count] = this.conditionalBlocks[i];
                        ++i;
                        j += cmp == 0 ? 1 : 0;
                    }
                    else
                    {
                        outConditions[count] = that.conditionalBlocks[j];
                        ++j;
                    }
                    ++count;
                }
                if (count < outConditions.length)
                    outConditions = Arrays.copyOf(outConditions, count);
            }
            return new Block(outKeys, outFragmentIds, outFragments, outConditions);
        }
    }

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnUpdate(TableMetadatas.none(), Keys.EMPTY, Collections.emptyList(), null, PreserveTimestamp.no));
    private static final int FLAG_PRESERVE_TIMESTAMPS = 0x1;

    final TableMetadatas tables;
    private final Keys keys;
    final List<Block> blocks;

    @Nullable
    private final ConsistencyLevel cassandraCommitCL;

    // Hints and batchlog want to write with the lower timestamp they generated when applying their writes via Accord
    // so they don't resurrect data if they are applied at a later time. Accord should be fine with this because
    // the writes are still deterministic from the perspective of coordinators/recovery coordinators.
    private final PreserveTimestamp preserveTimestamps;

    // Memoize computation of condition
    private Boolean anyConditionResult;

    public TxnUpdate(TableMetadatas tables, List<Fragment> fragments, TxnCondition condition, @Nullable ConsistencyLevel cassandraCommitCL, PreserveTimestamp preserveTimestamps)
    {
        requireArgument(cassandraCommitCL == null || IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.contains(cassandraCommitCL));
        this.tables = tables;
        this.keys = Keys.of(fragments, fragment -> fragment.key);
        fragments.sort(Fragment::compareKeys);
        // TODO (required): this node could be on version N while the peers are on N-1, which would have issues as the peers wouldn't know about N yet.
        //  Can not eagerly serialize until we know the "correct" version, else we need a way to fallback on mismatch.
        ByteBuffer[] serializedFragments = toSerializedValuesArray(keys, fragments, tables, Version.LATEST);
        int[] fragmentIds = new int[serializedFragments.length];
        for (int i = 0; i < serializedFragments.length; i++)
            fragmentIds[i] = i;

        SerializedTxnCondition serializedCondition = new SerializedTxnCondition(condition, tables);
        serializedCondition.unmemoize();
        serializedCondition.deserialize(tables);

        this.blocks = Collections.singletonList(new Block(keys, fragmentIds, serializedFragments, new ConditionalBlock[] { new ConditionalBlock(0, serializedCondition, fragmentIds) }));
        this.cassandraCommitCL = cassandraCommitCL;
        this.preserveTimestamps = preserveTimestamps;
    }

    private TxnUpdate(TableMetadatas tables, Keys keys, List<Block> blocks, ConsistencyLevel cassandraCommitCL, PreserveTimestamp preserveTimestamps)
    {
        this.tables = tables;
        this.keys = keys;
        this.blocks = blocks;
        this.cassandraCommitCL = cassandraCommitCL;
        this.preserveTimestamps = preserveTimestamps;
    }

    public static TxnUpdate empty()
    {
        return new TxnUpdate(TableMetadatas.none(), Keys.EMPTY, Collections.emptyList(), null, PreserveTimestamp.no);
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (Block block : blocks)
            size += block.estimatedSizeOnHeap();
        size += AccordObjectSizes.keys(keys);
        return size;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("TxnUpdate{blocks=[");
        for (int i = 0; i < blocks.size(); i++)
        {
            if (i > 0) sb.append(", ");
            blocks.get(i).toString(sb, tables);
        }
        sb.append("]}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnUpdate txnUpdate = (TxnUpdate) o;
        return Objects.equals(blocks, txnUpdate.blocks);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(blocks);
    }

    @Override
    public Keys keys()
    {
        // TODO: It doesn't seem to affect correctness, but should we return the union of the fragment + condition keys?
        return keys;
    }

    // Batch log and hints want to keep their lower timestamp for the applied writes to avoid resurrecting old data
    // when they are applied later, possibly after further updates have already been acknowledged.
    public PreserveTimestamp preserveTimestamps()
    {
        return preserveTimestamps;
    }

    @Override
    public TxnUpdate slice(Ranges ranges)
    {
        return getTxnUpdate(keys -> keys.slice(ranges));
    }

    @Override
    public TxnUpdate intersecting(Participants<?> participants)
    {
        return getTxnUpdate(keys -> keys.intersecting(participants));
    }

    @VisibleForTesting
    TxnUpdate getTxnUpdate(Function<Keys, Keys> fn)
    {
        List<Block> outterUpdate = new ArrayList<>();
        for (Block block : blocks)
            outterUpdate.add(block.select(fn));
        return new TxnUpdate(tables, fn.apply(keys), outterUpdate, cassandraCommitCL, preserveTimestamps);
    }

    @Override
    public TxnUpdate merge(Update update)
    {
        TxnUpdate that = (TxnUpdate) update;
        requireArgument(that.blocks.size() == this.blocks.size(), "Blocks dont have the same sizes; expected %d but was %d", this.blocks.size(), that.blocks.size());
        Keys mergedKeys = this.keys.with(that.keys);
        
        List<Block> mergedBlocks = new ArrayList<>(this.blocks.size());
        for (int i = 0; i < this.blocks.size(); i++)
            mergedBlocks.add(this.blocks.get(i).merge(that.blocks.get(i)));
        
        return new TxnUpdate(tables, mergedKeys, mergedBlocks, cassandraCommitCL, preserveTimestamps);
    }

    @Override
    public TxnWrite apply(Timestamp executeAt, Data data)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        checkState(cm.epoch.getEpoch() >= executeAt.epoch(), "TCM epoch %d is < executeAt epoch %d", cm.epoch.getEpoch(), executeAt.epoch());

        Pair<List<TxnWrite.Update>, SimpleBitSet> pair = processCondition(executeAt, data);
        if (pair == null)
            return new TxnWrite(TableMetadatas.none(), Collections.emptyList(), SimpleBitSets.allUnset(numConditionalBlocks()));

        List<TxnWrite.Update> allUpdates = pair.left;
        SimpleBitSet conditionalBlockBitSet = pair.right;
        if (keys.isEmpty())
            return new TxnWrite(TableMetadatas.none(), Collections.emptyList(), SimpleBitSets.allSet(numConditionalBlocks()));

        return new TxnWrite(tables, allUpdates, conditionalBlockBitSet);
    }

    
    private boolean checkCondition(Data data, @Nullable SerializedTxnCondition condition)
    {
        if (condition == null)
            return true;
        TxnCondition deserializedCondition = condition.deserialize(tables);
        if (deserializedCondition == TxnCondition.none())
            return true;
        return deserializedCondition.applies((TxnData) data);
    }

    public List<TxnWrite.Update> completeUpdatesForKey(SimpleBitSet conditionalBlockBitSet, RoutableKey key)
    {
        List<TxnWrite.Update> updates = new ArrayList<>();
        
        for (Block block : blocks)
        {
            for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
            {
                if (!conditionalBlockBitSet.get(conditionalBlock.id)) continue;
                List<Fragment> fragments = deserialize(block.keys, tables, block, conditionalBlock.fragments);
                for (Fragment fragment : fragments)
                    if (fragment.isComplete() && fragment.key.equals(key))
                        updates.add(fragment.toUpdate(tables));
            }
        }

        return updates;
    }

    public static final AccordUpdateSerializer<TxnUpdate> serializer = new AccordUpdateSerializer<>()
    {
        @Override
        public void serialize(TxnUpdate update, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out, Version version) throws IOException
        {
            // Serializing it with the condition result set shouldn't be needed
            checkState(update.anyConditionResult == null, "Can't serialize if conditionResult is set without adding it to serialization");
            // Once in accord "mixedTimeSource" and "yes" are the same, so only care about the side effect: that the timestamp is preserved or not
            out.writeByte(update.preserveTimestamps.preserve ? FLAG_PRESERVE_TIMESTAMPS : 0);
            tablesAndKeys.serializeKeys(update.keys, out);
            serializeNullable(update.cassandraCommitCL, out, consistencyLevelSerializer);
            CollectionSerializers.serializeList(update.blocks, update.keys, out, Block.serializer);
        }

        @Override
        public TxnUpdate deserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            int flags = in.readByte();
            boolean preserveTimestamps = (FLAG_PRESERVE_TIMESTAMPS & flags) == 1;
            Keys keys = tablesAndKeys.deserializeKeys(in);
            ConsistencyLevel consistencyLevel = deserializeNullable(in, consistencyLevelSerializer);
            List<Block> blocks = CollectionSerializers.deserializeList(keys, in, Block.serializer);

            return new TxnUpdate(tablesAndKeys.tables, keys, blocks, consistencyLevel, preserveTimestamps ? PreserveTimestamp.yes : PreserveTimestamp.no);
        }

        @Override
        public void skip(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            in.readByte(); // flags
            Keys keys = tablesAndKeys.deserializeKeys(in);
            deserializeNullable(in, consistencyLevelSerializer); // consistency level
            skipArray(keys, in, Block.serializer);
        }

        @Override
        public long serializedSize(TxnUpdate update, TableMetadatasAndKeys tablesAndKeys, Version version)
        {
            long size = 1; // flags
            size += tablesAndKeys.serializedKeysSize(update.keys);
            size += serializedNullableSize(update.cassandraCommitCL, consistencyLevelSerializer);
            size += CollectionSerializers.serializedListSize(update.blocks, update.keys(), Block.serializer);
            return size;
        }
    };

    private static ByteBuffer[] toSerializedValuesArray(Keys keys, List<Fragment> items, TableMetadatas tables, Version version)
    {
        ByteBuffer[] result = new ByteBuffer[keys.size()];
        int i = 0, mi = items.size(), ki = 0;
        while (i < mi)
        {
            PartitionKey key = items.get(i).key;
            int j = i + 1;
            while (j < mi && items.get(j).key.equals(key))
                ++j;

            int nextki = keys.findNext(ki, key, CEIL);
            Arrays.fill(result, ki, nextki, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            ki = nextki;
            result[ki++] = toSerializedValues(items, tables, i, j, version);
            i = j;
        }
        Arrays.fill(result, ki, result.length, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        return result;
    }

    private static ByteBuffer toSerializedValues(List<Fragment> items, TableMetadatas tables, int start, int end, Version version)
    {
        long size = TypeSizes.sizeofUnsignedVInt(version.version) + TypeSizes.sizeofUnsignedVInt(end - start);
        for (int i = start ; i < end ; ++i)
            size += Fragment.serializer.serializedSize(items.get(i), tables, version);

        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeUnsignedVInt32(version.version);
            out.writeUnsignedVInt32(end - start);
            for (int i = start ; i < end ; ++i)
                Fragment.serializer.serialize(items.get(i), tables, out, version);
            return out.buffer(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static List<Fragment> deserialize(PartitionKey key, TableMetadatas tables, ByteBuffer bytes)
    {
        if (!bytes.hasRemaining())
            return Collections.emptyList();

        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            Version version = Version.fromVersion(in.readUnsignedVInt32());
            int count = in.readUnsignedVInt32();
            switch (count)
            {
                case 0: throw new IllegalStateException();
                case 1: return Collections.singletonList(Fragment.serializer.deserialize(key, tables, in, version));
                default:
                    List<Fragment> result = new ArrayList<>();
                    for (int i = 0 ; i < count ; ++i)
                        result.add(Fragment.serializer.deserialize(key, tables, in, version));
                    return result;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static List<Fragment> deserialize(Keys keys, TableMetadatas tables, Block block, int[] fragments)
    {
        Invariants.require(keys.size() == fragments.length);
        List<Fragment> result = new ArrayList<>(fragments.length);
        for (int i = 0 ; i < keys.size() ; ++i)
        {
            ByteBuffer fragment = block.fragments[fragments[i]];
            Invariants.nonNull(fragment);
            result.addAll(deserialize((PartitionKey) keys.get(i), tables, fragment));
        }
        return result;
    }

    @Override
    public void failCondition()
    {
        anyConditionResult = FALSE;
    }

    @Override
    public boolean checkAnyConditionMatch(Data data)
    {
        // Assert data that was memoized is same as data that is provided?
        if (anyConditionResult != null)
            return anyConditionResult;
            
        // Check if any block has a matching condition
        for (Block block : blocks)
        {
            for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
            {
                if (checkCondition(data, conditionalBlock.condition))
                    return anyConditionResult = true;
            }
        }
        return anyConditionResult = false;
    }

    @Nullable
    private Pair<List<TxnWrite.Update>, SimpleBitSet> processCondition(Timestamp executeAt, Data data)
    {
        int numConditionalBlocks = numConditionalBlocks();
        SimpleBitSet conditionalBlocksMatched = SimpleBitSet.allocate(numConditionalBlocks);
        List<Fragment> fragments = null;
        // Each block is executed indepdendently so a match in one block has no effect on another block,
        // this is done this way to support conditional with unconditional writes, and multiple if/end if blocks
        for (Block block : blocks)
        {
            // This loop needs to support the expected semantics of if/else if/else blocks;
            // first condition that is true is the only one that applies.
            for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
            {
                if (checkCondition(data, conditionalBlock.condition))
                {
                    conditionalBlocksMatched.set(conditionalBlock.id);
                    if (fragments == null) fragments = new ArrayList<>();
                    fragments.addAll(deserialize(block.keys, tables, block, conditionalBlock.fragments));
                    break;
                }
            }
        }
        if (fragments == null) return null;

        List<TxnWrite.Update> allUpdates = new ArrayList<>(fragments.size());
        QueryOptions options = QueryOptions.forProtocolVersion(ProtocolVersion.CURRENT);
        AccordUpdateParameters parameters = new AccordUpdateParameters((TxnData) data, options, executeAt.uniqueHlc());

        for (Fragment fragment : fragments)
            if (!fragment.isComplete())
                allUpdates.add(fragment.complete(parameters, tables));
        return Pair.create(allUpdates, conditionalBlocksMatched);
    }

    private int numConditionalBlocks()
    {
        int numConditionalBlocks = 0;
        for (Block block : blocks)
            numConditionalBlocks += block.conditionalBlocks.length;
        return numConditionalBlocks;
    }

    @Override
    public Kind kind()
    {
        return Kind.TXN;
    }

    @Override
    public ConsistencyLevel cassandraCommitCL()
    {
        return cassandraCommitCL;
    }

    @VisibleForTesting
    public void unsafeResetCondition()
    {
        anyConditionResult = null;
    }

    private static int maxSorted(int[] sortedInts)
    {
        return sortedInts.length == 0 ? 0 : sortedInts[sortedInts.length - 1];
    }
}
