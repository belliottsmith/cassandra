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

/*
 * Renamed from org.apache.cassandra.utils.LZ4Utils and re-ported to 4.0 from 3.0.19
 * after the V5 beta protocol was updated after the changes in CASSANDRA-15299.
 */
package org.apache.cassandra.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;

import java.io.IOException;
import java.util.zip.Checksum;

/**
 * Utility methods for compressing and decompressing bytes with
 * optional checksums.
 * <p>
 * <strong>1.1. Checksumed LZ4 Compression Serialized Format</strong>
 * <p>
 * <pre>
 * {@code
 *                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Number of Compressed Chunks  |     Compressed Length (e1)    /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /  Compressed Length cont. (e1) |    Uncompressed Length (e1)   /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Uncompressed Length cont. (e1)| CRC32 Checksum of Lengths (e1)|
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Checksum of Lengths cont. (e1)|  LZ4 Compressed Bytes (e1)  +//
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                      CRC32 Checksum (e1)                     ||
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    Compressed Length (e2)                     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                   Uncompressed Length (e2)                    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                CRC32 Checksum of Lengths (e2)                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                   LZ4 Compressed Bytes (e2)                 +//
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                      CRC32 Checksum (e2)                     ||
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    Compressed Length (en)                     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                   Uncompressed Length (en)                    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                CRC32 Checksum of Lengths (en)                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                   LZ4 Compressed Bytes (en)                 +//
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                      CRC32 Checksum (en)                     ||
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }
 * </pre>
 * <p>
 * <p>
 * <strong>1.2. LZ4 Checksum Compression Description</strong>
 * <p>
 * The entire compressed payload is broken into n compressed chunks each with a checksum:
 * <ul>
 * <li>[int]: compressed length of serialized lz4 bytes for this chunk
 * <li>[int]: expected length of the decompressed bytes after lz4 decompression
 * <li>[int]: CRC32 digest of decompressed and compressed length components above
 * <li>[k bytes]: compressed lz4 payload for this chunk
 * <li>[int]: CRC32 digest of the decompressed result of the lz4 payload for this chunk
 * </ul>
 * <p>
 */
public class CieV4LZ4Utils
{
    private static final int CHUNK_HEADER_OVERHEAD = Integer.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES;
    private final static int DEFAULT_BLOCK_SIZE = 1 << 15; // 32k block size

    public static ByteBuf compress(LZ4Compressor compressor, Checksum checksum,
                                      ByteBuf bufToCompress) throws IOException
    {
        int start = bufToCompress.readerIndex();
        int length = bufToCompress.readableBytes();
        return compress(compressor, checksum, bufToCompress, start, length);
    }

    public static ByteBuf compress(LZ4Compressor compressor, Checksum checksum, ByteBuf bufToCompress,
                                      int start, int length) throws IOException
    {
        return compress(compressor, checksum, bufToCompress, DEFAULT_BLOCK_SIZE, start, length);
    }

    /**
     * Compresses and consumes the entire length from the starting offset or reader index to the length
     * in a serialization format as described in {@link org.apache.cassandra.utils.LZ4Utils} adding
     * checksums and chunking to the frame body
     * @param compressor and instance of LZ4Compressor to compress each chunk with
     * @param checksum an instance of something that implements the Checksum interface (most likely CRC32)
     * @param inputBuf the input/source buffer of what we are going to compress
     * @param blockSize how big each block/chunk will be (defaults to DEFAULT_BLOCK_SIZE kb)
     * @param start the offset to start reading from
     * @param length the amount of bytes to compress from the starting offset
     * @return a single ByteBuf with all the compressed bytes serialized in the compressed/chunk'ed/checksum'ed format
     * @throws IOException if we fail while compressing the input blocks or read from the inputBuf itself
     */
    public static ByteBuf compress(LZ4Compressor compressor, Checksum checksum, ByteBuf inputBuf,
                                      int blockSize, int start, int length) throws IOException
    {
        // be pessimistic about life and assume the compressed output will be the same
        // size as the input bytes
        int maxTotalCompressedLength = compressor.maxCompressedLength(length);
        int expectedChunks = (int) Math.ceil((double) maxTotalCompressedLength / blockSize);
        int expectedMaxSerializedLength = Short.BYTES + (expectedChunks * CHUNK_HEADER_OVERHEAD) + maxTotalCompressedLength;
        byte[] retBuf = new byte[expectedMaxSerializedLength];
        ByteBuf ret = Unpooled.wrappedBuffer(retBuf);
        ret.writerIndex(0);
        ret.readerIndex(0);

        // write out bogus short to start with to pre-allocate space as we'll encode one at the end when we finalize
        // for the number of compressed chunks to expect
        ret.writeShort((short) 0);

        byte[] inBuf = new byte[blockSize];
        byte[] outBuf = new byte[compressor.maxCompressedLength(blockSize)];

        inputBuf.readerIndex(start);

        int numCompressedChunks = 0;
        int readableBytes;
        while ((readableBytes = inputBuf.readableBytes()) > 0)
        {
            int lengthToRead = Math.min(blockSize, readableBytes);
            inputBuf.readBytes(inBuf, 0, lengthToRead);

            int written = compressor.compress(inBuf, 0, lengthToRead, outBuf, 0);

            checksum.reset();
            checksum.update(inBuf, 0, lengthToRead);
            int uncompressedChunkChecksum = (int) checksum.getValue();

            if (ret.writableBytes() < (CHUNK_HEADER_OVERHEAD + written))
            {
                // this really shouldn't ever happen -- it means we either mis-calculated the number of chunks we
                // expected to create, we gave some input to the lz4 compresser that caused the output to be much
                // larger than the input.. or some other edge condition. Regardless -- resize if necessary.
                byte[] resizedRetBuf = new byte[(retBuf.length + (CHUNK_HEADER_OVERHEAD + written)) * 3 / 2];
                System.arraycopy(retBuf, 0, resizedRetBuf, 0, retBuf.length);
                retBuf = resizedRetBuf;
                ByteBuf resizedRetByteBuf = Unpooled.wrappedBuffer(retBuf);
                resizedRetByteBuf.writerIndex(ret.writerIndex());
                ret = resizedRetByteBuf;
            }

            ret.writeInt(written); // compressed length of chunk
            ret.writeInt(lengthToRead); // uncompressed length of chunk

            // calculate the checksum of the compressed and decompressed lengths
            // protect us against a bogus length causing potential havoc on deserialization
            checksum.reset();
            checksum.update(written);
            checksum.update(lengthToRead);
            int lengthsChecksum = (int) checksum.getValue();
            ret.writeInt(lengthsChecksum);

            ret.writeBytes(outBuf, 0, written); // the actual lz4 compressed bites
            ret.writeInt(uncompressedChunkChecksum); // crc32 checksum calculated for uncompressed bytes

            numCompressedChunks++;
        }
        ret.setShort(0, (short) numCompressedChunks);

        return ret;
    }

    /**
     * Decompresses the given inputBuf in one go, where inputBuf is serialized in the checksum'ed chunked format specified
     * @param decompressor an instance of LZ4FastDecompressor
     * @param checksum an instance that implementes the Checksum interface (most likely CRC32)
     * @param inputBuf the entire compressed value serialized in the chunked and checksum'ed format described
     * @return the actual resulting decompressed bytes for usage (free of any serialization etc.)
     * @throws IOException if we failed to decompress or match a checksum check on a chunk
     */
    public static ByteBuf decompress(LZ4Decompressor decompressor, Checksum checksum, ByteBuf inputBuf) throws IOException
    {
        int numChunks = readUnsignedShort(inputBuf);

        int currentPosition = 0;

        byte[] buf = null;
        byte[] retBuf = new byte[inputBuf.readableBytes()];
        for (int i = 0; i < numChunks; i++)
        {
            int compressedLength = inputBuf.readInt();
            int decompressedLength = inputBuf.readInt();
            int lengthsChecksum = inputBuf.readInt();

            // calculate checksum on lengths (decompressed and compressed) and make sure it matches
            checksum.reset();
            checksum.update(compressedLength);
            checksum.update(decompressedLength);
            int calculatedLengthsChecksum = (int) checksum.getValue();
            // make sure checksum on lengths match
            if (lengthsChecksum != calculatedLengthsChecksum)
            {
                throw new IOException(String.format("Checksum invalid on chunk bytes lengths. Deserialized compressed " +
                                                    "length: %d decompressed length: %d. %d != %d", compressedLength,
                                                    decompressedLength, lengthsChecksum, calculatedLengthsChecksum));
            }

            if (currentPosition + decompressedLength > retBuf.length)
            {
                byte[] resizedBuf = new byte[retBuf.length + decompressedLength * 3 / 2];
                System.arraycopy(retBuf, 0, resizedBuf, 0, retBuf.length);
                retBuf = resizedBuf;
            }

            if (buf == null || buf.length < compressedLength)
            {
                buf = new byte[compressedLength];
            }

            // get the compressed bytes for this chunk
            inputBuf.readBytes(buf, 0, compressedLength);
            // decompress it

            byte[] decompressedChunk = new byte[decompressedLength];
            decompressor.decompress(buf, 0, decompressedChunk, 0, decompressedLength);
            // add the decompressed bytes into the ret buf
            System.arraycopy(decompressedChunk, 0, retBuf, currentPosition, decompressedChunk.length);
            currentPosition += decompressedChunk.length;

            // get the checksum of the decompressed bytes as calculated when serialized
            int expectedDecompressedChecksum = inputBuf.readInt();
            // calculate a crc32 checksum of the decompressed bytes we got
            checksum.reset();
            checksum.update(decompressedChunk, 0, decompressedChunk.length);
            int calculatedDecompressedChecksum = (int) checksum.getValue();
            // make sure they match
            if (expectedDecompressedChecksum != calculatedDecompressedChecksum)
            {
                throw new IOException("Decompressed checksum for chunk does not match expected checksum");
            }
        }

        ByteBuf ret = Unpooled.wrappedBuffer(retBuf, 0, currentPosition);
        ret.writerIndex(currentPosition);
        return ret;
    }

    private static int readUnsignedShort(ByteBuf buf) throws IOException
    {
        int ch1 = buf.readByte() & 0xFF;
        int ch2 = buf.readByte() & 0xFF;
        if ((ch1 | ch2) < 0)
            throw new IOException("Failed to read unsigned short as deserialized value is bogus/negative");
        return (ch1 << 8) + (ch2);
    }
}
