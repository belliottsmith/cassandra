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
package org.apache.cassandra.utils.memory;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import sun.misc.Unsafe;

/**
 * An off-heap region of memory that must be manually free'd when no longer needed.
 */
public abstract class AbstractMemory
{

    protected abstract long internalPeer();
    protected abstract long internalSize();

    protected void internalSetByte(long offset, byte b)
    {
        checkPosition(offset, 1);
        unsafe.putByte(internalPeer() + offset, b);
    }

    protected void internalSetShort(long offset, short s)
    {
        checkPosition(offset, 1);
        unsafe.putShort(internalPeer() + offset, s);
    }

    private static void putShortByByte(long address, int value)
    {
        if (bigEndian)
        {
            unsafe.putByte(address, (byte) (value >> 8));
            unsafe.putByte(address + 1, (byte) value);
        }
        else
        {
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address, (byte) (value));
        }
    }

    protected void internalSetInt(long offset, int l)
    {
        checkPosition(offset, 4);
        if (unaligned)
        {
            unsafe.putInt(internalPeer() + offset, l);
        }
        else
        {
            putIntByByte(internalPeer() + offset, l);
        }
    }

    void putIntByByte(long address, int value)
    {
        if (bigEndian)
        {
            unsafe.putByte(address, (byte) (value >> 24));
            unsafe.putByte(address + 1, (byte) (value >> 16));
            unsafe.putByte(address + 2, (byte) (value >> 8));
            unsafe.putByte(address + 3, (byte) (value));
        }
        else
        {
            unsafe.putByte(address + 3, (byte) (value >> 24));
            unsafe.putByte(address + 2, (byte) (value >> 16));
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address, (byte) (value));
        }
    }

    protected void internalGetLong(long offset, long l)
    {
        checkPosition(offset, 8);
        if (unaligned)
        {
            unsafe.putLong(internalPeer() + offset, l);
        }
        else
        {
            putLongByByte(internalPeer() + offset, l);
        }
    }

    private void putLongByByte(long address, long value)
    {
        if (bigEndian)
        {
            unsafe.putByte(address, (byte) (value >> 56));
            unsafe.putByte(address + 1, (byte) (value >> 48));
            unsafe.putByte(address + 2, (byte) (value >> 40));
            unsafe.putByte(address + 3, (byte) (value >> 32));
            unsafe.putByte(address + 4, (byte) (value >> 24));
            unsafe.putByte(address + 5, (byte) (value >> 16));
            unsafe.putByte(address + 6, (byte) (value >> 8));
            unsafe.putByte(address + 7, (byte) (value));
        }
        else
        {
            unsafe.putByte(address + 7, (byte) (value >> 56));
            unsafe.putByte(address + 6, (byte) (value >> 48));
            unsafe.putByte(address + 5, (byte) (value >> 40));
            unsafe.putByte(address + 4, (byte) (value >> 32));
            unsafe.putByte(address + 3, (byte) (value >> 24));
            unsafe.putByte(address + 2, (byte) (value >> 16));
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address, (byte) (value));
        }
    }

    /**
     * Transfers count bytes from buffer to Memory
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    protected void internalSetBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        assert buffer != null;
        assert !(bufferOffset < 0
                 || count < 0
                 || bufferOffset + count > buffer.length);
        checkPosition(memoryOffset, count);
        internalSetBytes(buffer, bufferOffset, internalPeer() + memoryOffset, count);
    }

    private static final long UNSAFE_COPY_THRESHOLD = 1024 * 1024L; // copied from java.nio.Bits

    protected void internalSetBytes(long offset, ByteBuffer buffer)
    {
        int start = buffer.position();
        int count = buffer.limit() - start;
        if (count == 0)
            return;
        checkPosition(offset, count);
        if (buffer.isDirect())
            internalSetBytes(unsafe.getLong(buffer, directByteBufferAddressOffset) + start, internalPeer() + offset, count);
        else
            internalSetBytes(offset, buffer.array(), start, count);
    }

    void internalSetBytes(long src, long trg, long count)
    {
        while (count > 0) {
            long size = (count> UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
            unsafe.copyMemory(src, trg, size);
            count -= size;
            src += size;
            trg+= size;
        }
    }

    void internalSetBytes(byte[] src, int offset, long trg, long count)
    {
        while (count > 0) {
            long size = (count> UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
            unsafe.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + offset, null, trg, size);
            count -= size;
            offset += size;
            trg += size;
        }
    }

    protected void internalSetMemory(long offset, long bytes, byte b)
    {
        // check if the last element will fit into the memory
        checkPosition(offset, bytes);
        unsafe.setMemory(internalPeer() + offset, bytes, b);
    }

    protected byte internalGetByte(long offset)
    {
        checkPosition(offset, 1);
        return unsafe.getByte(internalPeer() + offset);
    }

    protected long internalGetLong(long offset)
    {
        checkPosition(offset, 8);
        if (unaligned) {
            return unsafe.getLong(internalPeer() + offset);
        } else {
            return getLongByByte(internalPeer() + offset);
        }
    }

    private long getLongByByte(long address) {
        if (bigEndian) {
            return  (((long) unsafe.getByte(address    )       ) << 56) |
                    (((long) unsafe.getByte(address + 1) & 0xff) << 48) |
                    (((long) unsafe.getByte(address + 2) & 0xff) << 40) |
                    (((long) unsafe.getByte(address + 3) & 0xff) << 32) |
                    (((long) unsafe.getByte(address + 4) & 0xff) << 24) |
                    (((long) unsafe.getByte(address + 5) & 0xff) << 16) |
                    (((long) unsafe.getByte(address + 6) & 0xff) <<  8) |
                    (((long) unsafe.getByte(address + 7) & 0xff)      );
        } else {
            return  (((long) unsafe.getByte(address + 7)       ) << 56) |
                    (((long) unsafe.getByte(address + 6) & 0xff) << 48) |
                    (((long) unsafe.getByte(address + 5) & 0xff) << 40) |
                    (((long) unsafe.getByte(address + 4) & 0xff) << 32) |
                    (((long) unsafe.getByte(address + 3) & 0xff) << 24) |
                    (((long) unsafe.getByte(address + 2) & 0xff) << 16) |
                    (((long) unsafe.getByte(address + 1) & 0xff) <<  8) |
                    (((long) unsafe.getByte(address    ) & 0xff)      );
        }
    }

    protected int internalGetInt(long offset)
    {
        checkPosition(offset, 4);
        if (unaligned) {
            return unsafe.getInt(internalPeer() + offset);
        } else {
            return getIntByByte(internalPeer() + offset);
        }
    }

    static int getIntByByte(long address) {
        if (bigEndian) {
            return  (((int) unsafe.getByte(address    )       ) << 24) |
                    (((int) unsafe.getByte(address + 1) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 8 ) |
                    (((int) unsafe.getByte(address + 3) & 0xff)      );
        } else {
            return  (((int) unsafe.getByte(address + 3)       ) << 24) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 1) & 0xff) <<  8) |
                    (((int) unsafe.getByte(address    ) & 0xff)      );
        }
    }

    protected int internalGetShort(long offset)
    {
        checkPosition(offset, 2);
        if (unaligned) {
            return unsafe.getShort(internalPeer() + offset);
        } else {
            return getShortByByte(internalPeer() + offset);
        }
    }

    private static int getShortByByte(long address) {
        if (bigEndian) {
            return  (((int) unsafe.getByte(address    )       ) << 8) |
                    (((int) unsafe.getByte(address + 1) & 0xff)     );
        } else {
            return  (((int) unsafe.getByte(address + 1)       ) <<  8) |
                    (((int) unsafe.getByte(address    ) & 0xff)      );
        }
    }

    /**
     * Transfers count bytes from Memory starting at memoryOffset to buffer starting at bufferOffset
     *
     * @param memoryOffset start offset in the memory
     * @param buffer the data buffer
     * @param bufferOffset start offset of the buffer
     * @param count number of bytes to transfer
     */
    protected void internalGetBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        if (buffer == null)
            throw new NullPointerException();
        else if (bufferOffset < 0 || count < 0 || count > buffer.length - bufferOffset)
            throw new IndexOutOfBoundsException();
        else if (count == 0)
            return;

        checkPosition(memoryOffset, count);
        unsafe.copyMemory(null, internalPeer() + memoryOffset, buffer, BYTE_ARRAY_BASE_OFFSET + bufferOffset, count);
    }

    protected ByteBuffer internalGetByteBuffer(long offset, int length)
    {
        ByteBuffer instance;
        try
        {
            instance = (ByteBuffer) unsafe.allocateInstance(directByteBufferClass);
        }
        catch (InstantiationException e)
        {
            throw new AssertionError(e);
        }
        checkPosition(offset, length);
        unsafe.putLong(instance, directByteBufferAddressOffset, internalPeer() + offset);
        unsafe.putInt(instance, directByteBufferCapacityOffset, length);
        unsafe.putInt(instance, directByteBufferLimitOffset, length);
        return instance;
    }

    private void checkPosition(long offset, long size)
    {
        assert size >= 0;
        assert internalPeer() != 0 : "Memory was freed";
        assert offset >= 0 && offset + size <= internalSize() : String.format("Illegal range: [%d..%d), size: %s", offset, offset + size, internalSize());
    }

    static final Unsafe unsafe;
    private static final Class<?> directByteBufferClass;
    private static final long directByteBufferAddressOffset;
    private static final long directByteBufferCapacityOffset;
    private static final long directByteBufferLimitOffset;
    private static final boolean bigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    static final boolean unaligned;

    static
    {
        String arch = System.getProperty("os.arch");
        unaligned = arch.equals("i386") || arch.equals("x86")
                    || arch.equals("amd64") || arch.equals("x86_64");
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
            Class<?> clazz = ByteBuffer.allocateDirect(0).getClass();
            directByteBufferAddressOffset = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            directByteBufferCapacityOffset = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            directByteBufferLimitOffset = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
            directByteBufferClass = clazz;
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private static final long BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

}