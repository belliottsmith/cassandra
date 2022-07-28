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

package org.apache.cassandra.distributed.upgrade;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

public class MixedModeApplePaxosTest extends UpgradeTestBase
{
    public static final Semver v3019 = new Semver("3.0.19", Semver.SemverType.STRICT);

    @Test
    public void applePaxosUpgradeTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .singleUpgrade(v3019)
        .nodesToUpgrade(1)
        .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                    .set("paxos_variant", "apple_rrl"))
        .setup(cluster -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl(pk,ck,v) VALUES (1, 1, 1) IF NOT EXISTS",
                                           ConsistencyLevel.QUORUM);
            Object[][] result = cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.SERIAL);
            Assert.assertArrayEquals(new Object[]{1, 1, 1}, result[0]);

            // 3.0.19.61-hotfix uses ordinals for message filters, this blocks apple paxos prepare messages
            // this only affects message filters though, the actual message is routed correctly since the verb
            // id is encoded in the message header.
            cluster.filters().verbs(47).to(2).drop();
            try
            {
                cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v=2 WHERE pk=1 AND ck=1 IF EXISTS",
                                               ConsistencyLevel.QUORUM);
                Assert.fail("Timeout expected");
            } catch (RuntimeException e)
            {
                Assert.assertEquals(e.getCause().getClass().getName(), WriteTimeoutException.class.getName());
            }
            cluster.filters().reset();

        }).runAfterClusterUpgrade(cluster -> {
            // this will recover, in mixed mode, the failed operation in the previous section
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v=2 WHERE pk=1 AND ck=1 IF EXISTS",
                                           ConsistencyLevel.QUORUM);
            Object[][] result1 = cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.SERIAL);
            Assert.assertArrayEquals(new Object[]{1, 1, 2}, result1[0]);
            cluster.coordinator(2).execute("UPDATE " + KEYSPACE + ".tbl SET v=3 WHERE pk=1 AND ck=1 IF EXISTS",
                                           ConsistencyLevel.QUORUM);
            Object[][] result2 = cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.SERIAL);
            Assert.assertArrayEquals(new Object[]{1, 1, 3}, result2[0]);
        })
        .run();
    }

    @Test
    public void applePaxosUpgradePermittedReadTest() throws Throwable
    {
        ExecutorService exec = Executors.newFixedThreadPool(1, new NamedThreadFactory("PaxosTest"));
        new TestCase()
        .nodes(3)
        .singleUpgrade(v3019)
        .nodesToUpgrade(1)
        .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                    .set("paxos_variant", "apple_rrl"))
        .setup(cluster -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl(pk,ck,v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
        }).runAfterClusterUpgrade(cluster -> {
            CountDownLatch hasInitiatedRead1 = CountDownLatch.newCountDownLatch(1);
            CountDownLatch hasPerformedRead2 = CountDownLatch.newCountDownLatch(1);
            cluster.filters().verbs(47).from(3).to(2).drop(); // make sure we contact the upgraded node
            IMessageFilters.Filter filter = cluster.filters().verbs(47).from(3).outbound().messagesMatching((from, to, msg) -> {
                hasInitiatedRead1.decrement();
                hasPerformedRead2.awaitUninterruptibly();
                return false;
            }).drop();
            Future<Object[][]> result = exec.submit(() -> cluster.coordinator(3).execute("SELECT pk, ck, v FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL));
            hasInitiatedRead1.awaitUninterruptibly();
            filter.off();
            Thread.sleep(1); // ensure we pick a higher ballot for the second read
            cluster.coordinator(2).execute("SELECT pk, ck, v FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL);
            hasPerformedRead2.decrement();
            Assert.assertArrayEquals(new Object[]{1, 1, 1}, result.get()[0]);
            cluster.filters().reset();
        })
        .run();
        exec.shutdown();
    }

    @Test
    public void applePaxosRepairUpgradeTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .singleUpgrade(v3019)
        .nodesToUpgrade(1)
        .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                    .set("paxos_variant", "apple_rrl"))
        .setup(cluster -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl(pk,ck,v) VALUES (1, 1, 1) IF NOT EXISTS",
                                           ConsistencyLevel.QUORUM);
            Object[][] result = cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.SERIAL);
            Assert.assertArrayEquals(new Object[]{1, 1, 1}, result[0]);

            // 3.0.19.61-hotfix uses ordinals for message filters, this blocks apple paxos prepare messages
            // this only affects message filters though, the actual message is routed correctly since the verb
            // id is encoded in the message header
            cluster.filters().verbs(47).to(2).drop();
            try
            {
                cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v=2 WHERE pk=1 AND ck=1 IF EXISTS",
                                               ConsistencyLevel.QUORUM);
                Assert.fail("Timeout expected");
            } catch (RuntimeException e)
            {
                Assert.assertEquals(e.getCause().getClass().getName(), WriteTimeoutException.class.getName());
            }
            cluster.filters().reset();

        }).runAfterClusterUpgrade(cluster -> {
            // uncommitteed operation introduced before upgrade will be repaired here
            cluster.get(1).nodetool("repair", "--paxos-only");
        })
        .run();
    }
}
