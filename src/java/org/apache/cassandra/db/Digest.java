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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FastByteOperations;

public class Digest
{
    private static final ThreadLocal<byte[]> localBuffer = new ThreadLocal<byte[]>()
    {
        @Override
        protected byte[] initialValue()
        {
            return new byte[4096];
        }
    };

    private final MessageDigest hasher;
    private long inputBytes = 0;

    private static Digest threadLocalMD5Digest()
    {
        return new Digest(FBUtilities.threadLocalMD5Digest());
    }

    public static Digest forReadResponse()
    {
        return threadLocalMD5Digest();
    }

    public static Digest forSchema()
    {
        return threadLocalMD5Digest();
    }

    public static Digest forValidator()
    {
        // MerkleTree uses XOR internally, so we want lots of output bits here
        return new Digest(FBUtilities.newMessageDigest("SHA-256"));
    }

    public static Digest forRepairedDataTracking()
    {
        return new Digest(FBUtilities.threadLocalMD5Digest())
        {
            @Override
            public Digest updateWithCounterContext(ByteBuffer context)
            {
                // for the purposes of repaired data tracking on the read path, exclude
                // contexts with legacy shards as these may be irrevocably different on
                // different replicas
                if (CounterContext.instance().hasLegacyShards(context))
                    return this;

                return super.updateWithCounterContext(context);
            }
        };
    }

    private Digest(MessageDigest hasher)
    {
        this.hasher = hasher;
    }

    public Digest update(byte[] input, int offset, int len)
    {
        hasher.update(input, offset, len);
        inputBytes += len;
        return this;
    }

    /**
     * Update the digest with the bytes from the supplied buffer. This does
     * not modify the position of the supplied buffer, so callers are not
     * required to duplicate() the source buffer before calling
     */
    public Digest update(ByteBuffer input)
    {
        return update(input, input.position(), input.remaining());
    }

    /**
     * Update the digest with the bytes sliced from the supplied buffer. This does
     * not modify the position of the supplied buffer, so callers are not
     * required to duplicate() the source buffer before calling
     */
    public Digest update(ByteBuffer input, int pos, int len)
    {
        if (len <= 0)
            return this;

        if (input.hasArray())
        {
            byte[] b = input.array();
            int ofs = input.arrayOffset();
            hasher.update(b, ofs + pos, len);
            inputBytes += len;
        }
        else
        {
            byte[] tempArray = localBuffer.get();
            while (len > 0)
            {
                int chunk = Math.min(len, tempArray.length);
                FastByteOperations.copy(input, pos, tempArray, 0, chunk);
                hasher.update(tempArray, 0, chunk);
                len -= chunk;
                pos += chunk;
                inputBytes += chunk;
            }
        }
        return this;
    }

    /**
     * Update a MessageDigest with the content of a context.
     * Note that this skips the header entirely since the header information
     * has local meaning only, while digests are meant for comparison across
     * nodes. This means in particular that we always have:
     *  updateDigest(ctx) == updateDigest(clearAllLocal(ctx))
     */
    public Digest updateWithCounterContext(ByteBuffer context)
    {
        // context can be empty due to the optimization from CASSANDRA-10657
        if (!context.hasRemaining())
            return this;

        int pos = context.position() + CounterContext.headerLength(context);
        int len = context.limit() - pos;
        update(context, pos, len);
        return this;
    }

    public Digest updateWithByte(int val)
    {
        hasher.update((byte) (val & 0xFF));
        inputBytes++;
        return this;
    }

    public Digest updateWithInt(int val)
    {
        hasher.update((byte) ((val >>> 24) & 0xFF));
        hasher.update((byte) ((val >>> 16) & 0xFF));
        hasher.update((byte) ((val >>>  8) & 0xFF));
        hasher.update((byte) ((val >>> 0) & 0xFF));
        inputBytes += 4;
        return this;
    }

    public Digest updateWithLong(long val)
    {
        hasher.update((byte) ((val >>> 56) & 0xFF));
        hasher.update((byte) ((val >>> 48) & 0xFF));
        hasher.update((byte) ((val >>> 40) & 0xFF));
        hasher.update((byte) ((val >>> 32) & 0xFF));
        hasher.update((byte) ((val >>> 24) & 0xFF));
        hasher.update((byte) ((val >>> 16) & 0xFF));
        hasher.update((byte) ((val >>>  8) & 0xFF));
        hasher.update((byte)  ((val >>> 0) & 0xFF));
        inputBytes += 8;
        return this;
    }

    public Digest updateWithBoolean(boolean val)
    {
        updateWithByte(val ? 0 : 1);
        return this;
    }

    public byte[] digest()
    {
        return hasher.digest();
    }

    public long inputBytes()
    {
        return inputBytes;
    }
}

