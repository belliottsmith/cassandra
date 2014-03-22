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
package org.apache.cassandra.db.data;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

public class BufferCell implements Cell
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferCell(CellNames.simpleDense(ByteBuffer.allocate(1))));

    private final CellName name;
    private final ByteBuffer value;
    private final long timestamp;

    public BufferCell(CellName name)
    {
        this(name, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public BufferCell(CellName name, ByteBuffer value)
    {
        this(name, value, 0);
    }

    public BufferCell(CellName name, ByteBuffer value, long timestamp)
    {
        assert name != null;
        assert value != null;
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }

    public CellName name()
    {
        return name;
    }
    public ByteBuffer value()
    {
        return value;
    }
    public long timestamp()
    {
        return timestamp;
    }
    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + name().unsharedHeapSizeExcludingData() + ObjectSizes.sizeOnHeapExcludingData(value());
    }

    public Cell withUpdatedName(CellName newName)
    {
        return new BufferCell(newName, value(), timestamp());
    }
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        return new BufferCell(name(), value(), newTimestamp);
    }
    public boolean isMarkedForDelete(long now)
    {
        return Impl.isMarkedForDelete(this, now);
    }
    public boolean isLive(long now)
    {
        return Impl.isLive(this, now);
    }
    public long getMarkedForDeleteAt()
    {
        return Impl.getMarkedForDeleteAt();
    }
    public int cellDataSize()
    {
        return Impl.cellDataSize(this);
    }
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return Impl.serializedSize(this, type, typeSizes);
    }
    public int serializationFlags()
    {
        return Impl.serializationFlags();
    }
    public Cell diff(Cell cell)
    {
        return Impl.diff(this, cell);
    }
    public void updateDigest(MessageDigest digest)
    {
        Impl.updateDigest(this, digest);
    }
    public int getLocalDeletionTime()
    {
        return Impl.getLocalDeletionTime();
    }
    public Cell reconcile(Cell cell)
    {
        return Impl.reconcile(this, cell);
    }
    public Cell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return Impl.localCopy(this, cfMetaData, allocator);
    }
    public Cell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp)
    {
        return allocator.clone(this, cfMetaData, writeOp);
    }
    public String getString(CellNameType comparator)
    {
        return Impl.getString(this, comparator);
    }
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        Impl.validateFields(this, metadata);
    }
    public static Cell create(CellName name, ByteBuffer value, long timestamp, int ttl, CFMetaData metadata)
    {
        if (ttl <= 0)
            ttl = metadata.getDefaultTimeToLive();

        return ttl > 0
               ? new BufferExpiringCell(name, value, timestamp, ttl)
               : new BufferCell(name, value, timestamp);
    }

    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public boolean equals(Object obj)
    {
        return Cell.Impl.equals(this, obj);
    }
}
