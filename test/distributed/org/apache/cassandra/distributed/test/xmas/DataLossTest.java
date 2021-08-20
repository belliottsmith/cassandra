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

package org.apache.cassandra.distributed.test.xmas;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class DataLossTest extends ChristmasPatchTestBase
{
    public DataLossTest(int[] insertIds, int deletionId)
    {
        super(insertIds, deletionId);
    }

    @Test
    public void showDataLossTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2, c ->
                                                      c.with(Feature.NETWORK)
                                                       .set("write_request_timeout_in_ms", 5000l))))
        {
            cluster.schemaChange(String.format("CREATE TABLE %s.christmas (id int PRIMARY KEY, b text) WITH gc_grace_seconds=1", KEYSPACE));
            // when we dont set anything related to the christmas patch, we expect data resurection and no last repair logs
            christmasPatchTesterUtil(cluster, "christmas", true, false);
        }
    }

    @Test
    public void showDataLossWithTrackedRepairTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2, c ->
                                                      c.with(Feature.NETWORK)
                                                       .set("write_request_timeout_in_ms", 5000l)
                                                       .set("enable_shadow_christmas_patch", true))))
        {
            cluster.schemaChange(String.format("CREATE TABLE %s.christmas (id int PRIMARY KEY, b text) with gc_grace_seconds=1", KEYSPACE));
            // when we only shadow the christmas patch, we expect data resurection but we expect last repair logs
            christmasPatchTesterUtil(cluster, "christmas", true, true);
        }
    }
}
