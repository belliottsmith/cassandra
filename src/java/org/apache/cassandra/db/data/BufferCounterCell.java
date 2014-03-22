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

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

/**
 * A column that represents a partitioned counter.
 */
public class BufferCounterCell extends BufferCell implements CounterCell
{
    private final long timestampOfLastDelete;
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferCounterCell(CellNames.simpleDense(ByteBuffer.allocate(1)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 1));

    public BufferCounterCell(CellName name, ByteBuffer value, long timestamp)
    {
        this(name, value, timestamp, Long.MIN_VALUE);
    }

    public BufferCounterCell(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete)
    {
        super(name, value, timestamp);
        this.timestampOfLastDelete = timestampOfLastDelete;
    }

    public long timestampOfLastDelete()
    {
        return timestampOfLastDelete;
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + name().unsharedHeapSizeExcludingData() + ObjectSizes.sizeOnHeapExcludingData(value());
    }


    public Cell withUpdatedName(CellName newName)
    {
        return new BufferCounterCell(newName, value(), timestamp(), timestampOfLastDelete());
    }
    public Cell withUpdatedTimestamp(long timestamp)
    {
        return new BufferCounterCell(name(), value(), timestamp, timestampOfLastDelete());
    }
    public long total()
    {
        return CounterCell.Impl.total(this);
    }
    public int cellDataSize()
    {
        return CounterCell.Impl.cellDataSize(this);
    }
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return CounterCell.Impl.serializedSize(this, type, typeSizes);
    }
    public Cell diff(Cell cell)
    {
        return CounterCell.Impl.diff(this, cell);
    }
    public void updateDigest(MessageDigest digest)
    {
        CounterCell.Impl.updateDigest(this, digest);
    }
    public Cell reconcile(Cell cell)
    {
        return CounterCell.Impl.reconcile(this, cell);
    }
    public CounterCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return CounterCell.Impl.localCopy(this, cfMetaData, allocator);
    }
    public CounterCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp)
    {
        return allocator.clone(this, cfMetaData, writeOp);
    }
    public String getString(CellNameType comparator)
    {
        return CounterCell.Impl.getString(this, comparator);
    }
    public int serializationFlags()
    {
        return CounterCell.Impl.serializationFlags();
    }
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        CounterCell.Impl.validateFields(this, metadata);
    }
    public Cell markLocalToBeCleared()
    {
        return CounterCell.Impl.markLocalToBeCleared(this);
    }

    public static CounterCell create(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete, ColumnSerializer.Flag flag)
    {
        if (flag == ColumnSerializer.Flag.FROM_REMOTE || (flag == ColumnSerializer.Flag.LOCAL && CounterCell.Impl.contextManager.shouldClearLocal(value)))
            value = CounterCell.Impl.contextManager.clearAllLocal(value);
        return new BufferCounterCell(name, value, timestamp, timestampOfLastDelete);
    }

    // For use by tests of compatibility with pre-2.1 counter only.
    public static CounterCell createLocal(CellName name, long value, long timestamp, long timestampOfLastDelete)
    {
        return new BufferCounterCell(name, CounterCell.Impl.contextManager.createLocal(value), timestamp, timestampOfLastDelete);
    }

    public boolean equals(Object obj)
    {
        return CounterCell.Impl.equals(this, obj);
    }

}
