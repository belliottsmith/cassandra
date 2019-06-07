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
package com.apple.cie.db.marshal;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;

/**
 * Cell resolver for CappedCoalescingMapType.  In addition to the length cap, the coalescing map expects the
 * value of the map to be a tuple of a coalescing key (a blob) and a user supplied type.  Only the first
 * (and newest) coalescing key is resolved - all subsequent keys of the same value are dropped.
 */
public class CappedCoalescingMapCellsResolver extends AbstractCappedMapCellsResolver
{
    private static final String typeName = CappedCoalescingMapType.class.getSimpleName();
    private static final String MBEAN_NAME = "com.apple.cie.db:type=" + typeName;

    static final CappedCoalescingMapCellsResolver instance = new CappedCoalescingMapCellsResolver();

    public CappedCoalescingMapCellsResolver()
    {
        super(MBEAN_NAME, Integer.getInteger("com.apple.cie.db.marshal.cappedcoalescingmap.defaultcap", 10));
    }

    Cells.Builder wrapCellsBuilder(Cells.Builder resolver, TupleType coalesingValueTupleType)
    {
        return enabled ? new WrapperBuilder(resolver, coalesingValueTupleType) : resolver;
    }

    String typeName()
    {
        return typeName;
    }

    static private <V> ByteBuffer toByteBuffer(Cell<V> cell)
    {
        return cell.accessor().toBuffer(cell.value());
    }

    static private <V> boolean isEmpty(Cell<V> cell)
    {
        return cell.accessor().isEmpty(cell.value());
    }

    private class WrapperBuilder extends AbstractCappedMapCellsResolver.WrapperBuilder
    {
        private final TupleType coalesingValueTupleType;
        private final Set<ByteBuffer> seenKeys;

        private WrapperBuilder(Cells.Builder builder, TupleType coalesingValueTupleType)
        {
            super(builder, defaultCap);
            this.coalesingValueTupleType = coalesingValueTupleType;
            this.seenKeys = new HashSet<>();
        }

        protected void onRegularCell(Cell<?> cell)
        {
            if (regularsOut < cap)
            {
                if (cell.value() != null && !isEmpty(cell))
                {
                    ByteBuffer[] components = coalesingValueTupleType.split(toByteBuffer(cell));
                    ByteBuffer coalescingKey = components[0];
                    if (!seenKeys.add(coalescingKey))
                        return; // have seen a more recent version of this key already
                }
                // If coalescing key not seen before, or cell is a tombstone - include in output
                keepCell(cell);
            }
        }

        @Override
        public void endColumn()
        {
            super.endColumn();
            seenKeys.clear();
        }
    }
}
