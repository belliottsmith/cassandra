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

import java.util.function.Consumer;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.impl.Instance;

import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;

public class LegacyCASTest extends CASTestBase
{
    protected Consumer<IInstanceConfig> config()
    {
        return config -> config
                .set("paxos_variant", "legacy")
                .set("write_request_timeout_in_ms", 200L)
                .set("cas_contention_timeout_in_ms", 200L)
                .set("request_timeout_in_ms", 200L);
    }

    /**
     * This particular variant is unique to legacy Paxos because of the differing quorums for consensus, read and commit.
     * It is also unique to range movements with an even-numbered RF under legacy paxos.
     *
     * Range movements do not necessarily complete; they may be aborted.
     * CAS consistency should not be affected by this.
     *
     *  - Range moving from {1, 2} to {2, 3}; witnessed by all
     *  - Promised and Accepted on {2, 3}; Commits are delayed and arrive after next commit (or perhaps vanish)
     *  - Range move cancelled; a new one starts moving {1, 2} to {2, 4}; witnessed by all
     *  - Promised, Accepted and Committed on {1, 4}
     */
    @Ignore // known to be unsafe, just documents issue
    @Test
    public void testAbortedRangeMovement() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");
            int pk = pk(cluster, 1, 2);

            // set {3} bootstrapping, {4} not in ring
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(3));
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(3));

            // {3} promises and accepts on !{1} => {2, 3}
            // {3} commits do not YET arrive on either of {1, 2} (must be either due to read quorum differing on legacy Paxos)
            drop(cluster, 3, to(1), to(), to(1), to(1, 2));
            assertRows(cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ANY, pk),
                    row(true));

            // abort {3} bootstrap, start {4} bootstrap
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(3));
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            // {4} promises and accepts on !{2} => {1, 4}
            // {4} commits on {1, 2, 4}
            drop(cluster, 4, to(2), to(), to(2), to());
            assertRows(cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", QUORUM, pk),
                    row(false, pk, 1, 1, null));
        }
    }

}
