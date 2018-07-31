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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;

public abstract class Conflicts
{
    private Conflicts() {}

    public static Cell resolveRegular(Cell left, Cell right)
    {
        long leftTimestamp = left.timestamp();
        long rightTimestamp = right.timestamp();
        if (leftTimestamp != rightTimestamp)
            return leftTimestamp > rightTimestamp ? left : right;

        int leftLocalDeletionTime = left.localDeletionTime();
        int rightLocalDeletionTime = right.localDeletionTime();

        boolean leftIsExpiringOrTombstone = leftLocalDeletionTime != Cell.NO_DELETION_TIME;
        boolean rightIsExpiringOrTombstone = rightLocalDeletionTime != Cell.NO_DELETION_TIME;
        if (leftIsExpiringOrTombstone != rightIsExpiringOrTombstone)
            return leftIsExpiringOrTombstone ? left : right;

        ByteBuffer leftValue = left.value();
        ByteBuffer rightValue = right.value();
        int c = leftValue.compareTo(rightValue);
        if (c > 0)
            return left;
        else if (c < 0)
            return right;

        // Prefer the longest ttl if relevant
        return leftLocalDeletionTime > rightLocalDeletionTime ? left : right;
    }

    public static Cell resolveCounter(Cell left, Cell right)
    {
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        boolean leftIsTombstone = left.isTombstone();
        boolean rightIsTombstone = right.isTombstone();
        if (leftIsTombstone != rightIsTombstone)
            return leftIsTombstone ? left : right;

        long leftTimestamp = left.timestamp();
        long rightTimestamp = right.timestamp();
        if (leftIsTombstone) // ==> && rightIsTombstone
            return leftTimestamp > rightTimestamp ? left : right;

        ByteBuffer leftValue = left.value();
        ByteBuffer rightValue = right.value();
        // Handle empty values. Counters can't truly have empty values, but we can have a counter cell that temporarily
        // has one on read if the column for the cell is not queried by the user due to the optimization of #10657. We
        // thus need to handle this (see #11726 too).
        boolean leftIsEmpty = !leftValue.hasRemaining();
        boolean rightIsEmpty = !rightValue.hasRemaining();
        if (leftIsEmpty || rightIsEmpty)
        {
            if (leftIsEmpty != rightIsEmpty)
                return leftIsEmpty ? left : right;
            return leftTimestamp > rightTimestamp ? left : right;
        }

        ByteBuffer merged = CounterContext.instance().merge(leftValue, rightValue);
        long timestamp = Math.max(leftTimestamp, rightTimestamp);

        // We save allocating a new cell object if it turns out that one cell was
        // a complete superset of the other
        if (merged == leftValue && timestamp == leftTimestamp)
            return left;
        else if (merged == rightValue && timestamp == rightTimestamp)
            return right;
        else // merge clocks and timestamps.
            return new BufferCell(left.column(), timestamp, Cell.NO_TTL, Cell.NO_DELETION_TIME, merged, left.path());
    }

}
