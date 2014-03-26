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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.NativeAllocation;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.RefAction;

public class NativeDataAllocator extends NativeAllocator implements DataAllocator
{

    public static final class NativeDataGroup extends NativePool.Group implements DataGroup
    {
        NativeDataGroup(String name, NativePool parent, OpOrder reads, OpOrder writes)
        {
            super(name, parent, reads, writes);
        }
        public NativeDataAllocator newAllocator()
        {
            NativeDataAllocator allocator = new NativeDataAllocator(this);
            live.put(allocator, Boolean.TRUE);
            return allocator;
        }

        public void complete(RefAction refAction, OpOrder.Group readOp, Object referent)
        {
            refAction.complete(this, readOp, referent);
        }
    }

    public static final class NativeDataPool extends NativePool implements DataPool
    {
        public NativeDataPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanupThreshold, Runnable cleaner)
        {
            super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
        }

        public NativeDataGroup newGroup(String name, OpOrder readOps, OpOrder writeOps)
        {
            return new NativeDataGroup(name, this, readOps, writeOps);
        }

        public boolean needToCopyOnHeap()
        {
            return true;
        }

        public NativeDataPool setGcEnabled(boolean enabled)
        {
            super.setGcEnabled(enabled);
            return this;
        }
    }

    public static class NativeDataReclaimer implements DataReclaimer
    {
        private static final NativeDataReclaimer instance = new NativeDataReclaimer();

        public DataReclaimer reclaim(Cell cell)
        {
            ((NativeAllocation) cell).free();
            return this;
        }

        public DataReclaimer reclaimImmediately(Cell cell)
        {
            ((NativeAllocation) cell).free();
            return this;
        }

        public DataReclaimer reclaimImmediately(DecoratedKey key)
        {
            ((NativeAllocation) key).free();
            return this;
        }

        public void cancel()
        {
        }

        public void commit()
        {
        }
    }

    NativeDataAllocator(NativePool.Group group)
    {
        super(group);
    }

    public Cell clone(Cell cell, CFMetaData metadata, OpOrder.Group writeOp)
    {
        return new NativeCell(this, writeOp, cell);
    }

    public CounterCell clone(CounterCell cell, CFMetaData metadata, OpOrder.Group writeOp)
    {
        return new NativeCounterCell(this, writeOp, cell);
    }

    public DeletedCell clone(DeletedCell cell, CFMetaData metadata, OpOrder.Group writeOp)
    {
        return new NativeDeletedCell(this, writeOp, cell);
    }

    public ExpiringCell clone(ExpiringCell cell, CFMetaData metadata, OpOrder.Group writeOp)
    {
        return new NativeExpiringCell(this, writeOp, cell);
    }

    public DecoratedKey clone(DecoratedKey key, OpOrder.Group writeOp)
    {
        return new NativeDecoratedKey(key.key(), key.token(), this, writeOp);
    }

    public DataReclaimer reclaimer()
    {
        return NativeDataReclaimer.instance;
    }
}
