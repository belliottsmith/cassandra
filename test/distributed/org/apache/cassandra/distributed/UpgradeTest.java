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

package org.apache.cassandra.distributed;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;

public class UpgradeTest extends DistributedTestBase
{

    @Test
    public void upgradeTest() throws IOException
    {
        Versions versions = Versions.find();
        try (MultiVersionCluster cluster = init(MultiVersionCluster.create(3, versions.getLatest(Versions.Major.v3X))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");

//            for (Versions.Major major : new Versions.Major[] { Versions.Major.v30, Versions.Major.v3X, Versions.Major.v4 })
            for (Versions.Major major : new Versions.Major[] { Versions.Major.v4 })
            {
                for (int i = 1 ; i <= 3 ; ++i)
                {
                    cluster.get(i).shutdown();
                    cluster.get(i).setVersion(versions.getLatest(major));
                    cluster.get(i).startup();
                }

                assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                          ConsistencyLevel.ALL,
                                                          1),
                           row(1, 1, 1),
                           row(1, 2, 2),
                           row(1, 3, 3));
            }
        }
    }

}
