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

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.ContentionStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE_REQ;

public class CASContentionTest extends CASTestBase
{
    private static Cluster THREE_NODES;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        System.setProperty("cassandra.paxos.use_apple_paxos_self_execution", "false");
        TestBaseImpl.beforeClass();
        Consumer<IInstanceConfig> conf = config -> config
                .set("paxos_variant", "apple_rrl")
                .set("write_request_timeout_in_ms", 20000L)
                .set("cas_contention_timeout_in_ms", 20000L)
                .set("request_timeout_in_ms", 20000L);
        THREE_NODES = init(Cluster.create(3, conf));
    }

    @AfterClass
    public static void afterClass()
    {
        if (THREE_NODES != null)
            THREE_NODES.close();
    }

    @Test
    public void testDynamicContentionTracing() throws Throwable
    {
        try
        {

            String tableName = tableName("tbl");
            THREE_NODES.schemaChange("CREATE TABLE " + KEYSPACE + '.' + tableName + " (pk int, v int, PRIMARY KEY (pk))");

            int attempts = 0;
            do
            {
                Assert.assertTrue(String.format("Unable to detect contention after %s attempts", attempts), attempts < 10);
                CountDownLatch haveInvalidated = new CountDownLatch(1);
                THREE_NODES.verbs(APPLE_PAXOS_PREPARE_REQ).from(1).messagesMatching((from, to, verb) -> {
                    Uninterruptibles.awaitUninterruptibly(haveInvalidated);
                    return false;
                }).drop();
                THREE_NODES.get(1).runOnInstance(() -> ContentionStrategy.setStrategy("trace=1"));
                Future<?> insert = THREE_NODES.get(1).async(() -> {
                    THREE_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + tableName + " (pk, v) VALUES (1, 1) IF NOT EXISTS", QUORUM);
                }).call();
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                THREE_NODES.coordinator(2).execute("INSERT INTO " + KEYSPACE + '.' + tableName + " (pk, v) VALUES (1, 1) IF NOT EXISTS", QUORUM);
                haveInvalidated.countDown();
                THREE_NODES.filters().reset();
                insert.get();
                THREE_NODES.forEach(i -> i.runOnInstance(() -> FBUtilities.waitOnFuture(StageManager.getStage(Stage.TRACING).submit(StageManager.NO_OP_TASK))));
                attempts++;
            } while (THREE_NODES.get(1).callOnInstance(() -> StorageProxy.casWriteMetrics.contention.getCount()) == 0);

            Object[][] result = THREE_NODES.coordinator(1).execute("SELECT parameters FROM system_traces.sessions", ALL);
            Assert.assertEquals(1, result.length);
            Assert.assertEquals(1, result[0].length);
            Assert.assertTrue(Map.class.isAssignableFrom(result[0][0].getClass()));
            Map<?, ?> params = (Map<?, ?>) result[0][0];
            Assert.assertEquals("SERIAL", params.get("consistency"));
            Assert.assertEquals(tableName, params.get("table"));
            Assert.assertEquals(KEYSPACE, params.get("keyspace"));
            Assert.assertEquals("1", params.get("partitionKey"));
            Assert.assertEquals("write", params.get("kind"));
        }
        finally
        {
            THREE_NODES.filters().reset();
        }
    }


}
