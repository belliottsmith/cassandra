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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferPool;
import org.apache.cassandra.utils.memory.PoolAllocator;
import org.apache.cassandra.utils.memory.RefAction;

public class BufferDataAllocator implements DataAllocator
{

    public static final class BufferDataPool implements DataPool
    {
        final ByteBufferPool wrapped;
        public BufferDataPool(ByteBufferPool wrapped)
        {
            this.wrapped = wrapped;
        }

        public DataGroup newGroup(String name, OpOrder reads, OpOrder writeOps)
        {
            return new BufferDataGroup(wrapped.newAllocatorGroup(name, reads, writeOps));
        }

        public boolean needToCopyOnHeap()
        {
            return wrapped.needToCopyOnHeap();
        }

        public OpOrder.Barrier getGCBarrier()
        {
            return null;
        }
    }

    public static final class BufferDataGroup implements DataGroup
    {
        final ByteBufferPool.Group wrapped;
        public BufferDataGroup(ByteBufferPool.Group wrapped)
        {
            this.wrapped = wrapped;
        }

        public DataAllocator newAllocator()
        {
            return new BufferDataAllocator(wrapped.newAllocator());
        }

        public void complete(RefAction refAction, OpOrder.Group readOp, Object referent)
        {
        }
    }

    public static final class BufferDataReclaimer implements DataReclaimer
    {
        final ByteBufferPool.Allocator allocator;
        List<Cell> delayed;
        public BufferDataReclaimer(ByteBufferPool.Allocator allocator)
        {
            this.allocator = allocator;
        }

        public DataReclaimer reclaim(Cell cell)
        {
            if (delayed == null)
                delayed = new ArrayList<>();
            delayed.add(cell);
            return this;
        }

        public DataReclaimer reclaimImmediately(Cell cell)
        {
            cell.name().free(allocator);
            allocator.free(cell.value());
            return this;
        }

        public DataReclaimer reclaimImmediately(DecoratedKey key)
        {
            allocator.free(key.key());
            return this;
        }

        public void cancel()
        {
            if (delayed != null)
                delayed.clear();
        }

        public void commit()
        {
            if (delayed != null)
                for (Cell cell : delayed)
                    reclaimImmediately(cell);
        }
    }

    private final ByteBufferPool.Allocator allocator;
    public BufferDataAllocator(ByteBufferPool.Allocator allocator)
    {
        this.allocator = allocator;
    }

    public Cell clone(Cell cell, CFMetaData cfMetaData, OpOrder.Group writeOp)
    {
        return cell.localCopy(cfMetaData, allocator.context(writeOp));
    }

    public CounterCell clone(CounterCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp)
    {
        return cell.localCopy(cfMetaData, allocator.context(writeOp));
    }

    public DeletedCell clone(DeletedCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp)
    {
        return cell.localCopy(cfMetaData, allocator.context(writeOp));
    }

    public ExpiringCell clone(ExpiringCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp)
    {
        return cell.localCopy(cfMetaData, allocator.context(writeOp));
    }

    public DecoratedKey clone(DecoratedKey key, OpOrder.Group writeOp)
    {
        return new BufferDecoratedKey(key.token(), allocator.clone(key.key(), writeOp));
    }

    public DataReclaimer reclaimer()
    {
        return new BufferDataReclaimer(allocator);
    }

    public void setDiscarding()
    {
        allocator.setDiscarding();
    }

    public void setDiscarded()
    {
        allocator.setDiscarded();
    }

    public boolean isLive()
    {
        return allocator.isLive();
    }

    public PoolAllocator.SubAllocator onHeap()
    {
        return allocator.onHeap();
    }

    public PoolAllocator.SubAllocator offHeap()
    {
        return allocator.offHeap();
    }
}
