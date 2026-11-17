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

import net.nicoulaj.compilecommand.annotations.Inline;

import accord.utils.BitUtils;
import accord.utils.Invariants;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A set of simple utilities to quickly serialize/deserialize arrays/lists of non-negative values that each require <= 63 bits to represent.
 * These are packed into an "array" of fixed bit width, so that the total size consumed is ceil((bits*elements)/8).
 * This can be read directly without deserialization, by indexing into the byte stream directly (see {@link #readPacked}).
 * <p/>
 * Since values must be non-negative, {@code max} must also be non-negative, so that entries are at most 63 bits wide.
 * A 64-bit width is not supported: the implementation relies on shifts by {@code 64 - bitsPerEntry} and
 * {@code -1L << bitsPerEntry}, which are not well-defined for 64-bit entries in Java (a shift by 64 is a no-op).
 * Such a width could only arise from a negative (i.e. unsigned) {@code max} or a corrupt stream, so we reject it explicitly.
 * <p/>
 * The serialized value is optimized for values in the range 0 to 256 (negative will be rejected), and should produce
 * output smaller or equal to vint serialization; when values are larger than 256, then the packing can produce 1 extra
 * serialized byte.  Serialization is safe in these cases, and faster to skip.
 */
public class SerializePacked
{
    public static void serializePackedSortedIntsAndLength(int[] vs, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt32(vs.length);
        serializePackedSortedInts(vs, out);
    }

    public static void serializePackedSortedInts(int[] vs, DataOutputPlus out) throws IOException
    {
        if (vs.length == 0)
            return;

        int last = vs[vs.length - 1];
        Invariants.require(last >= 0,
                () -> String.format("Found a negative value at offset %d; value %d", (Object) (vs.length - 1), (Object) last));
        out.writeUnsignedVInt32(last);
        serializePackedInts(vs, 0, vs.length - 1, last, out);
    }

    public static int[] deserializePackedSortedIntsAndLength(DataInputPlus in) throws IOException
    {
        return deserializePackedSortedInts(in.readUnsignedVInt32(), in);
    }

    public static int[] deserializePackedSortedInts(int length, DataInputPlus in) throws IOException
    {
        if (length == 0)
            return new int[0];

        int last = in.readUnsignedVInt32();
        int[] vs = new int[length];
        deserializePackedInts(vs, 0, length - 1, last, in);
        vs[length - 1] = last;
        return vs;
    }

    public static void skipPackedSortedIntsAndLength(DataInputPlus in) throws IOException
    {
        skipPackedSortedInts(in.readUnsignedVInt32(), in);
    }

    public static void skipPackedSortedInts(int length, DataInputPlus in) throws IOException
    {
        if (length > 0)
        {
            int last = in.readUnsignedVInt32();
            skipPackedInts(0, length - 1, last, in);
        }
    }

    public static long serializedSizeOfPackedSortedIntsAndLength(int[] vs)
    {
        return TypeSizes.sizeofUnsignedVInt(vs.length) + serializedSizeOfPackedSortedInts(vs);
    }

    public static long serializedSizeOfPackedSortedInts(int[] vs)
    {
        if (vs.length == 0)
            return 0;
        int last = vs[vs.length - 1];
        return TypeSizes.sizeofUnsignedVInt(last) + serializedPackedSize(vs.length - 1, last);
    }

    public static void serializePackedInts(int[] vs, int from, int to, long max, DataOutputPlus out) throws IOException
    {
        serializePacked((in, i) -> in[i], vs, from, to, max, out);
    }

    public static void deserializePackedInts(int[] vs, int from, int to, long max, DataInputPlus in) throws IOException
    {
        deserializePacked((out, i, v) -> out[i] = (int)v, vs, from, to, max, in);
    }

    public static void skipPackedInts(int from, int to, long max, DataInputPlus in) throws IOException
    {
        in.skipBytesFully(serializedPackedSize(to - from, max));
    }

    public static long serializedPackedIntsSize(int[] vs, int from, int to, long max)
    {
        return serializedPackedSize(to - from, max);
    }

    public interface SerializeAdapter<In>
    {
        long get(In in, int i);
    }

    @Inline
    public static <In> void serializePacked(SerializeAdapter<In> adapter, In in, int from, int to, long max, DataOutputPlus out) throws IOException
    {
        requireSupportedMax(max);
        int bitsPerEntry = BitUtils.numberOfBitsToRepresent(max);
        if (bitsPerEntry == 0)
            return;

        long outOfRange = -1L << bitsPerEntry;
        long buffer = 0L;
        int bufferCount = 0;
        for (int i = from; i < to; i++)
        {
            long v = adapter.get(in, i);
            int finalI = i;
            Invariants.require(v >= 0 && (v & outOfRange) == 0,
                    () -> String.format(v < 0 ? "Found a negative value at offset %d; value %d" : "Value out of range at offset %d; value %d", (Object) finalI, (Object) v));
            buffer |= v << bufferCount;
            bufferCount = bufferCount + bitsPerEntry;
            if (bufferCount >= 64)
            {
                out.writeLong(buffer);
                bufferCount -= 64;
                buffer = v >>> (bitsPerEntry - bufferCount);
            }
        }
        if (bufferCount > 0)
            out.writeLeastSignificantBytes(buffer, (bufferCount + 7) / 8);
    }

    public interface DeserializeAdapter<Out>
    {
        void accept(Out out, int i, long v);
    }

    @Inline
    public static <Out> void deserializePacked(DeserializeAdapter<Out> consumer, Out out, int from, int to, long max, DataInputPlus in) throws IOException
    {
        requireSupportedMax(max);
        int bitsPerEntry = BitUtils.numberOfBitsToRepresent(max);
        if (bitsPerEntry == 0)
        {
            for (int i = from; i < to ; ++i)
                consumer.accept(out, i, 0);
            return;
        }
        long mask = -1L >>> (64 - bitsPerEntry);
        int remainingBytes = (bitsPerEntry * (to - from) + 7) / 8;
        long buffer = 0L;
        int bufferCount = 0;
        for (int i = from; i < to; i++)
        {
            long v = buffer & mask;
            if (bufferCount >= bitsPerEntry)
            {
                bufferCount -= bitsPerEntry;
                buffer >>>= bitsPerEntry;
            }
            else
            {
                int newBufferCount;
                if (remainingBytes >= 8)
                {
                    buffer = in.readLong();
                    newBufferCount = 64;
                    remainingBytes -= 8;
                }
                else
                {
                    Invariants.require(remainingBytes > 0);
                    newBufferCount = remainingBytes * 8;
                    buffer = in.readLeastSignificantBytes(remainingBytes);
                    remainingBytes = 0;
                }
                int readExtra = bitsPerEntry - bufferCount;
                long extraBits = buffer & (mask >>> bufferCount);
                v |= extraBits << bufferCount;
                bufferCount = newBufferCount - readExtra;
                buffer >>>= readExtra;
            }
            Invariants.require(v <= max);
            consumer.accept(out, i, v);
        }
    }

    public static int serializedPackedSize(int count, long max)
    {
        return serializedPackedBitsSize(count, BitUtils.numberOfBitsToRepresent(max));
    }

    /**
     * Read the entry at {@code index} directly from an array serialized by {@link #serializePacked} with the provided
     * {@code count} and {@code max}, without deserializing the array.
     *
     * @param bytes the buffer containing the serialized array
     * @param offset the position in {@code bytes} at which the serialized array begins
     */
    public static long readPacked(byte[] bytes, int offset, int count, long max, int index)
    {
        requireSupportedMax(max);
        int bitsPerEntry = BitUtils.numberOfBitsToRepresent(max);
        return readPackedBits(bytes, offset, serializedPackedBitsSize(count, bitsPerEntry), bitsPerEntry, index);
    }

    /**
     * Read the entry at {@code index} directly from an array serialized by {@link #serializePacked}.
     *
     * The serialized form is a little-endian bit stream that is written in 64-bit words, each of which is written
     * big-endian, with a final partial word of {@code totalBytes % 8} bytes written as a big-endian integer
     * (see {@link DataOutputPlus#writeLeastSignificantBytes(long, int)}).
     *
     * @param bytes the buffer containing the serialized array
     * @param offset the position in {@code bytes} at which the serialized array begins
     * @param totalBytes the serialized size of the array, i.e. {@code serializedPackedBitsSize(count, bitsPerEntry)}
     */
    public static long readPackedBits(byte[] bytes, int offset, int totalBytes, int bitsPerEntry, int index)
    {
        // bitsPerEntry may be read from the stream, so validate it to fail loudly on corruption (see class comment re 64-bit entries)
        Invariants.require(bitsPerEntry >= 0 && bitsPerEntry < 64, "Unsupported packed entry width: %d", bitsPerEntry);
        if (bitsPerEntry == 0)
            return 0;

        long bitPosition = (long) index * bitsPerEntry;
        int word = (int) (bitPosition >>> 6);
        int shift = (int) (bitPosition & 63);
        long v = readPackedWord(bytes, offset, totalBytes, word) >>> shift;
        if (shift + bitsPerEntry > 64)
            v |= readPackedWord(bytes, offset, totalBytes, word + 1) << (64 - shift);
        return v & (-1L >>> (64 - bitsPerEntry));
    }

    /**
     * Values must be non-negative, so {@code max} must be too; this also ensures entries are at most 63 bits wide,
     * as 64-bit entries are not supported (see class comment). When deserializing, {@code max} may be derived from
     * the stream, so this also detects some forms of corruption.
     */
    private static void requireSupportedMax(long max)
    {
        Invariants.requireArgument(max >= 0, "Packed values must be non-negative, so max must be non-negative (and entries at most 63 bits wide); found %d", max);
    }

    private static long readPackedWord(byte[] bytes, int offset, int totalBytes, int word)
    {
        int wordOffset = word * 8;
        int length = Math.min(8, totalBytes - wordOffset);
        Invariants.require(length > 0);
        int position = offset + wordOffset;
        long v = 0;
        for (int i = 0 ; i < length ; ++i)
            v = (v << 8) | (bytes[position + i] & 0xffL);
        return v;
    }

    public static int serializedPackedBitsSize(int count, int bitsPerEntry)
    {
        return (int) (((long)bitsPerEntry * count + 7) / 8);
    }
}
