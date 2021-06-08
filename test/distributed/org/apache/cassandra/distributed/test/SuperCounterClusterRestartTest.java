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
import java.util.stream.LongStream;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.thrift.TException;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class SuperCounterClusterRestartTest extends TestBaseImpl
{
    @Test
    public void superCounter() throws IOException, TException
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(c -> c.with(Feature.NATIVE_PROTOCOL)).start()))
        {
            int NUM_SUBCOLS = 100;
            int NUM_ADDS = 100;
            ThriftClientUtils.thriftClient(cluster.get(1), thrift -> {
                thrift.set_keyspace(KEYSPACE);

                CfDef table = new CfDef();
                table.keyspace = KEYSPACE;
                table.name = "cf";
                table.column_type = "Super";
                table.default_validation_class = "CounterColumnType";
                thrift.system_add_column_family(table);
                for (int subcol = 0; subcol < NUM_SUBCOLS; subcol++)
                {
                    for (int add = 0; add < NUM_ADDS; add++)
                    {
                        ColumnParent columnParent = new ColumnParent();
                        columnParent.column_family = "cf";
                        columnParent.super_column = bytes(String.format("subcol_%d", subcol));
                        CounterColumn counterColumn = new CounterColumn();
                        counterColumn.name = bytes("col_0");
                        counterColumn.value = 1;
                        thrift.add(bytes("row_0"), columnParent, counterColumn, org.apache.cassandra.thrift.ConsistencyLevel.QUORUM);
                    }
                }
            });

            cluster.forEach(n -> n.flush(KEYSPACE));
            ClusterUtils.stopAll(cluster);

            cluster.forEach(n -> n.startup()); // cluster.startup() is failing with "Can't use shut down instances, delegate is null"
            ThriftClientUtils.thriftClient(cluster.get(1), thrift -> {
                thrift.set_keyspace(KEYSPACE);

                long[] actuals = new long[NUM_SUBCOLS];
                for (int i = 0; i < NUM_SUBCOLS; i++)
                {
                    ColumnPath path = new ColumnPath();
                    path.column_family = "cf";
                    path.column = bytes("col_0");
                    path.super_column = bytes(String.format("subcol_%d", i));
                    long result = thrift.get(bytes("row_0"), path, org.apache.cassandra.thrift.ConsistencyLevel.QUORUM).counter_column.value;
                    actuals[i] = result;
                }

                long[] expected = LongStream.range(0, NUM_SUBCOLS).map(ignore -> NUM_ADDS).toArray();
                Assertions.assertThat(actuals).isEqualTo(expected);
            });
        }
    }
}
