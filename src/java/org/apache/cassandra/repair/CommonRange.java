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

package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * Groups ranges with identical endpoints/transient endpoints
 */
public class CommonRange
{
    public final Set<InetAddress> endpoints;
    public final Collection<Range<Token>> ranges;
    public final boolean hasSkippedReplicas;

    public CommonRange(Set<InetAddress> endpoints, Collection<Range<Token>> ranges)
    {
        this(endpoints, ranges, false);
    }

    public CommonRange(Set<InetAddress> endpoints, Collection<Range<Token>> ranges, boolean hasSkippedReplicas)
    {
        Preconditions.checkArgument(endpoints != null && !endpoints.isEmpty(), "Endpoints can not be empty");
        Preconditions.checkArgument(ranges != null && !ranges.isEmpty(), "ranges can not be empty");

        this.endpoints = endpoints;
        this.ranges = ranges;
        this.hasSkippedReplicas = hasSkippedReplicas;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommonRange that = (CommonRange) o;
        return hasSkippedReplicas == that.hasSkippedReplicas && Objects.equals(endpoints, that.endpoints) && Objects.equals(ranges, that.ranges);
    }

    public int hashCode()
    {
        return Objects.hash(endpoints, ranges, hasSkippedReplicas);
    }

    public String toString()
    {
        return "CommonRange{" +
               "endpoints=" + endpoints +
               ", ranges=" + ranges +
               ", hasSkippedReplicas=" + hasSkippedReplicas +
               '}';
    }
}
