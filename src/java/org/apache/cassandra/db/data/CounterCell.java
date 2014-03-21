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

import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public interface CounterCell extends Cell
{
    static final CounterContext contextManager = CounterContext.instance();

    Cell withUpdatedName(CellName newName);

    long timestampOfLastDelete();

    long total();

    int dataSize();

    int serializedSize(CellNameType type, TypeSizes typeSizes);

    Cell diff(Cell cell);

    /*
         * We have to special case digest creation for counter column because
         * we don't want to include the information about which shard of the
         * context is a delta or not, since this information differs from node to
         * node.
         */
    void updateDigest(MessageDigest digest);

    boolean equals(Object o);

    int hashCode();

    Cell localCopy(ColumnFamilyStore cfs, AbstractAllocator allocator);

    String getString(CellNameType comparator);

    int serializationFlags();

    void validateFields(CFMetaData metadata) throws MarshalException;

    Cell markLocalToBeCleared();
}
