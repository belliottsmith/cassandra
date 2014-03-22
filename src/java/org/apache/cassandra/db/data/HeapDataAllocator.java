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
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.utils.memory.PoolAllocator;

public class HeapDataAllocator implements DataAllocator
{

    public static final HeapDataAllocator instance = new HeapDataAllocator();

    private HeapDataAllocator() {}

    public Cell clone(Cell cell, CFMetaData cfMetaData, OpOrder.Group writeOp)
    {
        return cell.localCopy(cfMetaData, HeapAllocator.instance);
    }

    public CounterCell clone(CounterCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp)
    {
        return cell.localCopy(cfMetaData, HeapAllocator.instance);
    }

    public DeletedCell clone(DeletedCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp)
    {
        return cell.localCopy(cfMetaData, HeapAllocator.instance);
    }

    public ExpiringCell clone(ExpiringCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp)
    {
        return cell.localCopy(cfMetaData, HeapAllocator.instance);
    }

    public DecoratedKey clone(DecoratedKey key, OpOrder.Group writeOp)
    {
        return new BufferDecoratedKey(key.token(), HeapAllocator.instance.clone(key.key()));
    }

    public DataReclaimer reclaimer()
    {
        throw new IllegalStateException();
    }

    public void setDiscarding()
    {
        throw new IllegalStateException();
    }

    public void setDiscarded()
    {
        throw new IllegalStateException();
    }

    public boolean isLive()
    {
        throw new IllegalStateException();
    }

    public PoolAllocator.SubAllocator onHeap()
    {
        throw new IllegalStateException();
    }

    public PoolAllocator.SubAllocator offHeap()
    {
        throw new IllegalStateException();
    }
}
