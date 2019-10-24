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

import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;

public class AlterSystemKeyspacesTest extends TestBaseImpl
{
    /**
     * See rdar://56247937 (Permit altering tables in cie_internal and system_auth keyspaces).
     */
    @Test
    public void alterCIEInternalTablesTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(1).withConfig(c -> c.with(Feature.GOSSIP)).start()))
        {
            ICoordinator coordinator = cluster.coordinator(1);

            coordinator.execute("ALTER TABLE cie_internal.schema_drop_log " +
                                "WITH comment = 'Store all dropped tables for apple internal patch' " +
                                "AND gc_grace_seconds = 864000 " +
                                "AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'enabled': 'true'};",
                                ConsistencyLevel.ONE);
            coordinator.execute("ALTER TABLE cie_internal.partition_blacklist " +
                                "WITH comment = 'Partition keys which have been blacklisted' " +
                                "AND gc_grace_seconds = 864000 " +
                                "AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'enabled': 'true'};",
                                ConsistencyLevel.ONE);
        }
    }
}
