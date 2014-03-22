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
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

/**
 * Alternative to Cell that have an expiring time.
 * ExpiringCell is immutable (as Cell is).
 *
 * Note that ExpiringCell does not override Cell.getMarkedForDeleteAt,
 * which means that it's in the somewhat unintuitive position of being deleted (after its expiration)
 * without having a time-at-which-it-became-deleted.  (Because ttl is a server-side measurement,
 * we can't mix it with the timestamp field, which is client-supplied and whose resolution we
 * can't assume anything about.)
 */
public class BufferExpiringCell extends BufferCell implements ExpiringCell
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferExpiringCell(CellNames.simpleDense(ByteBuffer.allocate(1)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 1, 1));

    private final int localExpirationTime;
    private final int timeToLive;

    public BufferExpiringCell(CellName name, ByteBuffer value, long timestamp, int timeToLive)
    {
        this(name, value, timestamp, timeToLive, (int) (System.currentTimeMillis() / 1000) + timeToLive);
    }

    public BufferExpiringCell(CellName name, ByteBuffer value, long timestamp, int timeToLive, int localExpirationTime)
    {
        super(name, value, timestamp);
        assert timeToLive > 0 : timeToLive;
        assert localExpirationTime > 0 : localExpirationTime;
        this.timeToLive = timeToLive;
        this.localExpirationTime = localExpirationTime;
    }

    /** @return Either a DeletedCell, or an ExpiringCell. */
    public static Cell create(CellName name, ByteBuffer value, long timestamp, int timeToLive, int localExpirationTime, int expireBefore, ColumnSerializer.Flag flag)
    {
        if (localExpirationTime >= expireBefore || flag == ColumnSerializer.Flag.PRESERVE_SIZE)
            return new BufferExpiringCell(name, value, timestamp, timeToLive, localExpirationTime);
        // The column is now expired, we can safely return a simple tombstone. Note that
        // as long as the expiring column and the tombstone put together live longer than GC grace seconds,
        // we'll fulfil our responsibility to repair.  See discussion at
        // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
        return new BufferDeletedCell(name, localExpirationTime - timeToLive, timestamp);
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    @Override
    public int getLocalDeletionTime()
    {
        return localExpirationTime;
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new BufferExpiringCell(newName, value(), timestamp(), timeToLive, localExpirationTime);
    }
    @Override
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        return new BufferExpiringCell(name(), value(), newTimestamp, timeToLive, localExpirationTime);
    }
    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + name().unsharedHeapSizeExcludingData() + ObjectSizes.sizeOnHeapExcludingData(value());
    }

    @Override
    public int cellDataSize()
    {
        return ExpiringCell.Impl.cellDataSize(this);
    }
    @Override
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return ExpiringCell.Impl.serializedSize(this, type, typeSizes);
    }
    @Override
    public void updateDigest(MessageDigest digest)
    {
        ExpiringCell.Impl.updateDigest(this, digest);
    }
    @Override
    public ExpiringCell localCopy(CFMetaData cfMetaData, ByteBufferAllocator allocator)
    {
        return ExpiringCell.Impl.localCopy(this, cfMetaData, allocator);
    }
    @Override
    public ExpiringCell localCopy(CFMetaData cfMetaData, DataAllocator allocator, OpOrder.Group writeOp)
    {
        return allocator.clone(this, cfMetaData, writeOp);
    }
    @Override
    public String getString(CellNameType comparator)
    {
        return ExpiringCell.Impl.getString(this, comparator);
    }
    @Override
    public boolean isMarkedForDelete(long now)
    {
        return ExpiringCell.Impl.isMarkedForDelete(this, now);
    }
    @Override
    public long getMarkedForDeleteAt()
    {
        return ExpiringCell.Impl.getMarkedForDeleteAt(this);
    }
    @Override
    public int serializationFlags()
    {
        return ExpiringCell.Impl.serializationFlags();
    }
    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        ExpiringCell.Impl.validateFields(this, metadata);
    }
    @Override
    public boolean equals(Object that)
    {
        return ExpiringCell.Impl.equals(this, that);
    }
    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }
}
