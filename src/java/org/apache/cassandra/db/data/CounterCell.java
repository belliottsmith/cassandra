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

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.utils.memory.ByteBufferAllocator;

public interface CounterCell extends Cell
{
    static final CounterContext contextManager = CounterContext.instance();

    long timestampOfLastDelete();

    long total();

    /*
         * We have to special case digest creation for counter column because
         * we don't want to include the information about which shard of the
         * context is a delta or not, since this information differs from node to
         * node.
         */

    Cell markLocalToBeCleared();

    CounterCell localCopy(ByteBufferAllocator allocator);
}
