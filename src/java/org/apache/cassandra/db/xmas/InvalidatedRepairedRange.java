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

package org.apache.cassandra.db.xmas;

import java.util.Objects;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class InvalidatedRepairedRange
{
    public final int invalidatedAtSeconds;
    public final int minLDTSeconds;
    public final Range<Token> range;

    public InvalidatedRepairedRange(int invalidatedAtSeconds, int minLDTSeconds, Range<Token> range)
    {
        this.invalidatedAtSeconds = invalidatedAtSeconds;
        this.minLDTSeconds = minLDTSeconds;
        this.range = range;
    }

    /**
     * Merge this {@link InvalidatedRepairedRange} with other.
     *
     * ignores this or other if their invalidatedAtSeconds times are before the last repair time given, returns null if
     * both are "inactive" (= have had a repair run after it was invalidated)
     *
     * it keeps the maximum of the invalidated at and the minimum of the minLDTs - this basically
     * emulates running nodetool import once with all sstables at the max invalidatedAtSeconds time instead of
     * twice when there was no repair run in between the refreshes.
     */
    public InvalidatedRepairedRange merge(long lastRepairTime, InvalidatedRepairedRange other)
    {
        assert range.equals(other.range);
        // can we ignore this or other? If there has been a repair running for this range after
        // the invalidatedAtSeconds time, we can just ignore it.
        boolean otherActive = lastRepairTime < other.invalidatedAtSeconds;
        boolean thisActive = lastRepairTime < invalidatedAtSeconds;

        if (thisActive && otherActive)
        {
            return new InvalidatedRepairedRange(Math.max(invalidatedAtSeconds, other.invalidatedAtSeconds),
                                                Math.min(minLDTSeconds, other.minLDTSeconds),
                                                range);
        }
        else if (thisActive)
        {
            return this;
        }
        else if (otherActive)
        {
            return other;
        }
        else
        {
            return null;
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof InvalidatedRepairedRange)) return false;
        InvalidatedRepairedRange that = (InvalidatedRepairedRange) o;
        return invalidatedAtSeconds == that.invalidatedAtSeconds &&
               minLDTSeconds == that.minLDTSeconds &&
               Objects.equals(range, that.range);
    }

    public int hashCode()
    {
        return Objects.hash(invalidatedAtSeconds, minLDTSeconds, range);
    }

    @Override
    public String toString()
    {
        return "InvalidatedRepairedRange{" +
               "invalidatedAtSeconds=" + invalidatedAtSeconds +
               ", minLDTSeconds=" + minLDTSeconds +
               ", range=" + range +
               '}';
    }
}
