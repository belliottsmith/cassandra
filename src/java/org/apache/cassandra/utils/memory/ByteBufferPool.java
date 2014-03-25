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

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class ByteBufferPool extends Pool
{
    ByteBufferPool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, maxOffHeapMemory, cleanThreshold, cleaner);
    }

    public abstract Group newAllocatorGroup(String name, OpOrder reads, OpOrder writes);
    public abstract boolean needToCopyOnHeap();

    public static abstract class Group<P extends ByteBufferPool> extends AllocatorGroup<P>
    {
        public Group(String name, P pool, OpOrder reads, OpOrder writes)
        {
            super(name, pool, reads, writes);
        }

        public abstract Allocator<? extends Group<P>, P> newAllocator();
    }

    public static abstract class Allocator<G extends AllocatorGroup<P>, P extends Pool> extends PoolAllocator<G, P>
    {

        Allocator(G group)
        {
            super(group);
        }

        public abstract ByteBuffer allocate(int size, OpOrder.Group writeOp);

        public ByteBuffer clone(ByteBuffer buffer, OpOrder.Group writeOp)
        {
            assert buffer != null;
            if (buffer.remaining() == 0)
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;
            ByteBuffer cloned = allocate(buffer.remaining(), writeOp);

            cloned.mark();
            cloned.put(buffer.duplicate());
            cloned.reset();
            return cloned;
        }

        public abstract void free(ByteBuffer buffer);

        public ByteBufferAllocator context(final OpOrder.Group writeOp)
        {
            return new ByteBufferAllocator.AbstractAllocator()
            {
                public ByteBuffer allocate(int size)
                {
                    return Allocator.this.allocate(size, writeOp);
                }
            };
        }
    }

}
