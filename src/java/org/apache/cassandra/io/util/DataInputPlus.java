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
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.vint.VIntCoding;

import com.google.common.base.Preconditions;

/**
 * Rough equivalent of BufferedInputStream and DataInputStream wrapping a ByteBuffer that can be refilled
 * via rebuffer. Implementations provide this buffer from various channels (socket, file, memory, etc).
 *
 * RebufferingInputStream is not thread safe.
 */
public abstract class DataInputPlus extends InputStream implements DataInput, Closeable
{
    protected ByteBuffer buffer;

    protected DataInputPlus(ByteBuffer buffer)
    {
        Preconditions.checkArgument(buffer == null || buffer.order() == ByteOrder.BIG_ENDIAN, "Buffer must have BIG ENDIAN byte ordering");
        this.buffer = buffer;
    }

    /**
     * Implementations must implement this method to refill the buffer.
     * They can expect the buffer to be empty when this method is invoked.
     * @throws IOException
     */
    protected abstract void reBuffer() throws IOException;

    @Override
    public void readFully(byte[] b) throws IOException
    {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        int read = read(b, off, len);
        if (read < len)
            throw new EOFException();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {

        // avoid int overflow
        if (off < 0 || off > b.length || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return 0;

        int copied = 0;
        while (copied < len)
        {
            int position = buffer.position();
            int remaining = buffer.limit() - position;
            if (remaining == 0)
            {
                reBuffer();
                position = buffer.position();
                remaining = buffer.limit() - position;
                if (remaining == 0)
                    return copied == 0 ? -1 : copied;
            }
            int toCopy = Math.min(len - copied, remaining);
            FastByteOperations.copy(buffer, position, b, off + copied, toCopy);
            buffer.position(position + toCopy);
            copied += toCopy;
        }

        return copied;
    }

    @DontInline
    protected final long readPrimitiveSlowly(int bytes) throws IOException
    {
        long result = 0;
        for (int i = 0; i < bytes; i++)
            result = (result << 8) | (readByte() & 0xFFL);
        return result;
    }

    @Override
    public final int skipBytes(int n) throws IOException
    {
        if (n < 0)
            return 0;
        int requested = n;
        int position = buffer.position(), limit = buffer.limit(), remaining;
        while ((remaining = limit - position) < n)
        {
            n -= remaining;
            buffer.position(limit);
            reBuffer();
            position = buffer.position();
            limit = buffer.limit();
            if (position == limit)
                return requested - n;
        }
        buffer.position(position + n);
        return requested;
    }

    public final void skipBytesFully(int n) throws IOException
    {
        int skipped = skipBytes(n);
        if (skipped != n)
            throw new EOFException("Only " + skipped + " bytes were remaining, but requested to fully skip " + n);
    }

    @Override
    public final boolean readBoolean() throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public final byte readByte() throws IOException
    {
        if (!buffer.hasRemaining())
        {
            reBuffer();
            if (!buffer.hasRemaining())
                throw new EOFException();
        }

        return buffer.get();
    }

    @Override
    public final int readUnsignedByte() throws IOException
    {
        return readByte() & 0xff;
    }

    @Override
    public final short readShort() throws IOException
    {
        if (buffer.remaining() >= 2)
            return buffer.getShort();
        else
            return (short) readPrimitiveSlowly(2);
    }

    @Override
    public final int readUnsignedShort() throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public final char readChar() throws IOException
    {
        if (buffer.remaining() >= 2)
            return buffer.getChar();
        else
            return (char) readPrimitiveSlowly(2);
    }

    @Override
    public final int readInt() throws IOException
    {
        if (buffer.remaining() >= 4)
            return buffer.getInt();
        else
            return (int) readPrimitiveSlowly(4);
    }

    @Override
    public final long readLong() throws IOException
    {
        if (buffer.remaining() >= 8)
            return buffer.getLong();
        else
            return readPrimitiveSlowly(8);
    }

    public final long readVInt() throws IOException
    {
        return VIntCoding.decodeZigZag64(readUnsignedVInt());
    }

    public final long readUnsignedVInt() throws IOException
    {
        //If 9 bytes aren't available use the slow path in VIntCoding
        if (buffer.remaining() < 9)
            return VIntCoding.readUnsignedVInt(this);

        byte firstByte = buffer.get();

        //Bail out early if this is one byte, necessary or it fails later
        if (firstByte >= 0)
            return firstByte;

        int extraBytes = VIntCoding.numberOfExtraBytesToRead(firstByte);

        int position = buffer.position();
        int extraBits = extraBytes * 8;

        long retval = buffer.getLong(position);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            retval = Long.reverseBytes(retval);
        buffer.position(position + extraBytes);

        // truncate the bytes we read in excess of those we needed
        retval >>>= 64 - extraBits;
        // remove the non-value bits from the first byte
        firstByte &= VIntCoding.firstByteValueMask(extraBytes);
        // shift the first byte up to its correct position
        retval |= (long) firstByte << extraBits;
        return retval;
    }

    @Override
    public final float readFloat() throws IOException
    {
        if (buffer.remaining() >= 4)
            return buffer.getFloat();
        else
            return Float.intBitsToFloat((int)readPrimitiveSlowly(4));
    }

    @Override
    public final double readDouble() throws IOException
    {
        if (buffer.remaining() >= 8)
            return buffer.getDouble();
        else
            return Double.longBitsToDouble(readPrimitiveSlowly(8));
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }

    @Override
    public final int read() throws IOException
    {
        try
        {
            return readUnsignedByte();
        }
        catch (EOFException ex)
        {
            return -1;
        }
    }

    @Override
    public void reset() throws IOException
    {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    public int available() throws IOException
    {
        return buffer.remaining();
    }
}
