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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;


public class NodeDownTest extends TestBaseImpl
{
    // checks a QUORUM read query can tolerate 1 node being down in a 3 nodes cluster
    // and the coordinator should select the other 2 living nodes to contact.
    @Test
    public void nodeDownTest() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        try(Cluster cluster = init(Cluster.build(3)
                                          .withConfig(config -> config.with(Feature.GOSSIP))
                                          .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key) with speculative_retry='NEVER'"));
            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (1)"), ConsistencyLevel.ALL);
            for (int coordinator = 1; coordinator <= cluster.size() ; coordinator++ )
            {
                for (int downNode = 1; downNode <= cluster.size(); downNode++)
                {
                    if (coordinator == downNode)
                        continue;

                    long mark = cluster.get(coordinator).logs().mark();
                    cluster.get(downNode).shutdown().get();
                    cluster.get(coordinator).logs().watchFor(mark, "is now DOWN");
                    cluster.coordinator(coordinator).execute(withKeyspace("select * from %s.tbl where id = 1"), ConsistencyLevel.QUORUM);
                    mark = cluster.get(coordinator).logs().mark();
                    cluster.get(downNode).startup();
                    cluster.get(coordinator).logs().watchFor(mark, "is now UP");
                }
            }
        }
    }
}
