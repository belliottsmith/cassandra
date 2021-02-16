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

package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.insert;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.options;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.repair;

public class XmasPatchDistributedTest extends TestBaseImpl
{
    @Test
    public void testPopulateAndDropTable() throws IOException, InterruptedException
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.set("enable_christmas_patch", true)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE \"" + KEYSPACE.toUpperCase() + "\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            cluster.schemaChange("create table " + KEYSPACE + ".\"tbL\" (id int primary key, t int)");
            cluster.schemaChange("create table " + KEYSPACE + ".\"tBl\" (id int primary key, t int)");
            cluster.schemaChange("create table \"" + KEYSPACE.toUpperCase() + "\".\"tBl\" (id int primary key, t int)");
            cluster.schemaChange("create table \"" + KEYSPACE.toUpperCase() + "\".\"tbl\" (id int primary key, t int)");
            insert(cluster.coordinator(1), 0, 100, "tbl");
            insert(cluster.coordinator(1), 0, 100, "\"tBl\"");
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).callOnInstance(repair(options(false, true)));
            cluster.get(1).callOnInstance(repair(options(false, true), KEYSPACE.toUpperCase()));
            for (int i = 1; i <= 2; i++)
                assertRows(cluster.get(i).executeInternal("select distinct keyspace_name, columnfamily_name from system.repair_history"),
                           row(KEYSPACE, "tbl"),
                           row(KEYSPACE, "tbL"),
                           row(KEYSPACE.toUpperCase(), "tbl"),
                           row(KEYSPACE, "tBl"),
                           row(KEYSPACE.toUpperCase(), "tBl"));

            cluster.schemaChange("DROP TABLE "+KEYSPACE+".tbl");
            for (int i = 1; i <= 2; i++)
                assertRows(cluster.get(i).executeInternal("select distinct keyspace_name, columnfamily_name from system.repair_history"),
                           row(KEYSPACE, "tbL"),
                           row(KEYSPACE.toUpperCase(), "tbl"),
                           row(KEYSPACE, "tBl"),
                           row(KEYSPACE.toUpperCase(), "tBl"));

            cluster.schemaChange("DROP KEYSPACE \""+KEYSPACE.toUpperCase()+"\"");
            for (int i = 1; i <= 2; i++)
                assertRows(cluster.get(i).executeInternal("select distinct keyspace_name, columnfamily_name from system.repair_history"),
                           row(KEYSPACE, "tbL"),
                           row(KEYSPACE, "tBl"));

        }
    }
}
