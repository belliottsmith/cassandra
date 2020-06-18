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

package org.apache.cassandra.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Test compressing and decompressing various randomly created blobs with
 * our Chunked and Checksumed LZ4 Implementation.
 */
public class CieV4LZ4UtilsTest
{
    private static final Random RANDOM = new Random();

    @Test
    public void compress_and_decompress_small() throws IOException
    {
        LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = lz4Factory.fastCompressor();
        LZ4Decompressor decompressor = lz4Factory.fastDecompressor();
        Checksum checksum = new CRC32();

        String randomString = generateRandomWord(10);
        byte[] expectedBytes = randomString.getBytes();
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(expectedBytes);
        expectedBuf.writerIndex(expectedBytes.length);

        ByteBuf compressedBuf = CieV4LZ4Utils.compress(compressor, checksum, expectedBuf);
        ByteBuf decompressedBuf = CieV4LZ4Utils.decompress(decompressor, checksum, compressedBuf);
        Assert.assertEquals(expectedBuf.internalNioBuffer(0, expectedBuf.writerIndex()), decompressedBuf.internalNioBuffer(0, decompressedBuf.writerIndex()));
    }

    @Test
    public void compress_and_decompress_simple() throws IOException
    {
        LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = lz4Factory.fastCompressor();
        LZ4Decompressor decompressor = lz4Factory.fastDecompressor();
        Checksum checksum = new CRC32();

        // encode with multiple block sizes to make sure they all work
        String randomString = generateRandomWord(1 << 18);
        compressAndDecompress(randomString, compressor, decompressor, checksum, 1 << 14); // 16kb
        compressAndDecompress(randomString, compressor, decompressor, checksum, 1 << 15); // 32kb
        compressAndDecompress(randomString, compressor, decompressor, checksum, 1 << 16); // 64kb
        compressAndDecompress(randomString, compressor, decompressor, checksum, 1 << 21); // 2mb

        String highlyCompressableString = "bbbbbbbbbb";
        compressAndDecompress(highlyCompressableString, compressor, decompressor, checksum, 1 << 14); // 16kb
        compressAndDecompress(highlyCompressableString, compressor, decompressor, checksum, 1 << 15); // 32kb
        compressAndDecompress(highlyCompressableString, compressor, decompressor, checksum, 1 << 16); // 64kb
        compressAndDecompress(highlyCompressableString, compressor, decompressor, checksum, 1 << 21); // 2mb
    }

    private static void compressAndDecompress(String input, LZ4Compressor compressor, LZ4Decompressor decompressor,
                                              Checksum checksum, int blockSize) throws IOException
    {
        byte[] expectedBytes = input.getBytes();
        ByteBuf expectedBuf = Unpooled.wrappedBuffer(expectedBytes);

        ByteBuf compressedBuf = CieV4LZ4Utils.compress(compressor, checksum, expectedBuf, blockSize, 0, expectedBuf.readableBytes());
        ByteBuf decompressedBuf = CieV4LZ4Utils.decompress(decompressor, checksum, compressedBuf);
        // reset reader index on expectedBuf back to 0 as it will have been entirely consumed by the compress() call
        expectedBuf.readerIndex(0);
        Assert.assertEquals(expectedBuf, decompressedBuf);
    }

    private static String generateRandomWord(int length)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            sb.append((char) (RANDOM.nextInt(26) + 'a'));
        }
        return sb.toString();
    }
}
