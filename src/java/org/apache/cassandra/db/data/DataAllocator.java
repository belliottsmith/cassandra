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
import org.apache.cassandra.utils.memory.PoolAllocator;

public interface DataAllocator
{

    public static interface DataPool
    {
        DataAllocator newAllocator();
        boolean needToCopyOnHeap();
    }

    public static interface DataReclaimer
    {
        // reclaim should only be called when it is known that it WILL be reclaimed
        DataReclaimer reclaim(Cell cell);
        DataReclaimer reclaimImmediately(Cell cell);
        DataReclaimer reclaimImmediately(DecoratedKey key);
        // cancel means we failed to complete the action that would reclaim the objects registered,
        // however if it has not already been reclaimed by somebody else we guarantee that we will reclaim it again
        void cancel();
        void commit();
    }

    Cell clone(Cell cell, CFMetaData cfMetaData, OpOrder.Group writeOp);
    CounterCell clone(CounterCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp);
    DeletedCell clone(DeletedCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp);
    ExpiringCell clone(ExpiringCell cell, CFMetaData cfMetaData, OpOrder.Group writeOp);
    DecoratedKey clone(DecoratedKey key, OpOrder.Group writeOp);
    DataReclaimer reclaimer();

    void setDiscarding();
    void setDiscarded();
    boolean isLive();

    PoolAllocator.SubAllocator onHeap();
    PoolAllocator.SubAllocator offHeap();
}
