/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.metrics;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class CassandraMetricsRegistryTest
{

    @Test
    public void testDeltaBaseCase()
    {
        long[] last = new long[10];
        long[] now = new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        // difference between all zeros and a value should be the value
        assertArrayEquals(now, CassandraMetricsRegistry.delta(now, last));
        // the difference between itself should be all 0s
        assertArrayEquals(last, CassandraMetricsRegistry.delta(now, now));
        // verifying each value is calculated
        assertArrayEquals(new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
                CassandraMetricsRegistry.delta(new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, now));
    }

    @Test
    public void testDeltaHistogramSizeChange()
    {
        long[] count = new long[]{0, 1, 2, 3, 4, 5};
        assertArrayEquals(count, CassandraMetricsRegistry.delta(count, new long[3]));
        assertArrayEquals(new long[6], CassandraMetricsRegistry.delta(count, new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    }
}
