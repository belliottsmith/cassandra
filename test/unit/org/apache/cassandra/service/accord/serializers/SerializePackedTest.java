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
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.utils.Property.qt;

public class SerializePackedTest
{
    private static final Gen<int[]> zeros = rs -> new int[rs.nextInt(0, 10)];
    private static final Gen<int[]> randomZeroAndPositive = array(rs -> rs.nextInt(-1, Integer.MAX_VALUE) + 1);
    private static final Gen<int[]> randomZeroAndPositiveSmall = array(rs -> rs.nextInt(0, 1 << 8));
    private static final Gen<int[]> negatives = array(Gens.ints().between(Integer.MIN_VALUE, -1), rs -> rs.nextInt(1, 10));

    private static final Gen<int[]> monotonic = rs -> {
        int[] array = new int[rs.nextInt(0, 10)];
        for (int i = 0; i < array.length; i++)
            array[i] = i;
        return array;
    };

    private static final Gen<int[]> zeroAndPositive = rs -> {
        if (rs.decide(0.2f)) return monotonic.next(rs);
        return randomZeroAndPositive.next(rs);
    };

    private static final Gen<int[]> zeroAndPositiveSmall = rs -> {
        if (rs.decide(0.2f)) return monotonic.next(rs);
        return randomZeroAndPositiveSmall.next(rs);
    };

    private static Gen<int[]> array(Gen.IntGen valueGen)
    {
        return array(valueGen, rs -> rs.nextInt(0, 10));
    }

    private static Gen<int[]> array(Gen.IntGen valueGen, Gen.IntGen size)
    {
        return rs -> {
            int[] array = new int[size.nextInt(rs)];
            for (int i = 0; i < array.length; i++)
                array[i] = valueGen.nextInt(rs);
            Arrays.sort(array);
            return array;
        };
    }

    @Test
    public void serde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(zeroAndPositive).check(array -> Serializers.testSerde(output, PackedSortedSerializer.instance, array));
    }

    @Test
    public void serdeNegative()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(negatives).check(array -> {
            output.clear();
            Assertions.assertThatThrownBy(() -> PackedSortedSerializer.instance.serialize(array, output))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Found a negative value at offset");
        });
    }

    @Test
    public void serdeZeros()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(zeros).check(array -> Serializers.testSerde(output, PackedSortedSerializer.instance, array));
    }

    @Test
    public void serializerIsSmallerThanSimpleList()
    {
        qt().forAll(zeroAndPositiveSmall).check(array -> {
            var list = SimpleListSerializer.instance.serialize(array);
            var packed = PackedSortedSerializer.instance.serialize(array);

            Assertions.assertThat(packed.remaining()).isLessThanOrEqualTo(list.remaining());
        });
    }

    @Test
    public void randomAccess()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().check(rs -> {
            // values are non-negative, so entries are at most 63 bits wide (see rejectsUnsupportedWidth)
            int bits = rs.nextInt(0, 64);
            long max = bits == 0 ? 0 : -1L >>> (64 - bits);
            int count = rs.nextBoolean() ? rs.nextInt(0, 16) : rs.nextInt(0, 1024);
            long[] values = new long[count];
            for (int i = 0 ; i < count ; ++i)
                values[i] = bits == 0 ? 0 : rs.nextLong() & max;
            if (count > 0 && bits > 0)
                values[rs.nextInt(count)] = max; // ensure we exercise the full width

            // pad with a random prefix and suffix to ensure we respect the offset and length
            int prefix = rs.nextInt(0, 9);
            output.clear();
            for (int i = 0 ; i < prefix ; ++i)
                output.writeByte(rs.nextInt());
            SerializePacked.serializePacked((vs, i) -> vs[i], values, 0, count, max, output);
            int length = output.getLength() - prefix;
            Assertions.assertThat(length).isEqualTo(SerializePacked.serializedPackedSize(count, max));
            for (int i = 0, suffix = rs.nextInt(0, 9) ; i < suffix ; ++i)
                output.writeByte(rs.nextInt());

            byte[] bytes = output.toByteArray();
            for (int i = 0 ; i < count ; ++i)
            {
                Assertions.assertThat(SerializePacked.readPacked(bytes, prefix, count, max, i))
                          .describedAs("bits=%d count=%d index=%d", bits, count, i)
                          .isEqualTo(values[i]);
            }

            long[] deserialized = new long[count];
            try (DataInputBuffer in = new DataInputBuffer(bytes, prefix, bytes.length - prefix))
            {
                SerializePacked.deserializePacked((out, i, v) -> out[i] = v, deserialized, 0, count, max, in);
            }
            Assertions.assertThat(deserialized).describedAs("bits=%d count=%d", bits, count).isEqualTo(values);
        });
    }

    /**
     * Values must be non-negative so entries need at most 63 bits; 64-bit entries are not supported by the implementation
     * (they depend on shifts by 64, which are no-ops in Java), and may only arise from a negative max or a corrupt stream,
     * so we must reject them rather than silently mis-encode or mis-decode.
     */
    @Test
    public void rejectsUnsupportedWidth() throws IOException
    {
        long[] values = new long[] { 0, 1, Long.MAX_VALUE };
        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Assertions.assertThatThrownBy(() -> SerializePacked.serializePacked((vs, i) -> vs[i], values, 0, values.length, -1L, output))
                      .isInstanceOf(IllegalArgumentException.class)
                      .hasMessageContaining("max must be non-negative");
            Assertions.assertThat(output.getLength()).isZero();

            // the maximum supported width round-trips
            SerializePacked.serializePacked((vs, i) -> vs[i], values, 0, values.length, Long.MAX_VALUE, output);
            byte[] bytes = output.toByteArray();
            long[] deserialized = new long[values.length];
            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                SerializePacked.deserializePacked((out, i, v) -> out[i] = v, deserialized, 0, values.length, Long.MAX_VALUE, in);
            }
            Assertions.assertThat(deserialized).isEqualTo(values);

            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                Assertions.assertThatThrownBy(() -> SerializePacked.deserializePacked((out, i, v) -> out[i] = v, deserialized, 0, values.length, -1L, in))
                          .isInstanceOf(IllegalArgumentException.class);
            }
            Assertions.assertThatThrownBy(() -> SerializePacked.readPacked(bytes, 0, values.length, -1L, 0))
                      .isInstanceOf(IllegalArgumentException.class);
            Assertions.assertThatThrownBy(() -> SerializePacked.readPackedBits(bytes, 0, bytes.length, 64, 0))
                      .isInstanceOf(IllegalStateException.class)
                      .hasMessageContaining("Unsupported packed entry width");
        }
    }

    public static class PackedSortedSerializer implements UnversionedSerializer<int[]>
    {
        public static final PackedSortedSerializer instance = new PackedSortedSerializer();

        @Override
        public void serialize(int[] t, DataOutputPlus out) throws IOException
        {
            SerializePacked.serializePackedSortedIntsAndLength(t, out);
        }

        @Override
        public int[] deserialize(DataInputPlus in) throws IOException
        {
            return SerializePacked.deserializePackedSortedIntsAndLength(in);
        }

        @Override
        public long serializedSize(int[] t)
        {
            return SerializePacked.serializedSizeOfPackedSortedIntsAndLength(t);
        }
    }

    public static class SimpleListSerializer implements UnversionedSerializer<int[]>
    {
        public static final SimpleListSerializer instance = new SimpleListSerializer();

        @Override
        public void serialize(int[] t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(t.length);
            for (int i : t)
                out.writeVInt32(i);
        }

        @Override
        public int[] deserialize(DataInputPlus in) throws IOException
        {
            int size = in.readUnsignedVInt32();
            int[] array = new int[size];
            for (int i = 0; i < size; i++)
                array[i] = in.readVInt32();
            return array;
        }

        @Override
        public long serializedSize(int[] t)
        {
            long size = TypeSizes.sizeofUnsignedVInt(t.length);
            for (int i = 0; i < t.length; i++)
                size += TypeSizes.sizeofVInt(t[i]);
            return size;
        }
    }
}