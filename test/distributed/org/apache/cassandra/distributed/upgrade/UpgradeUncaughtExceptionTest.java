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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.ShutdownException;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

public class UpgradeUncaughtExceptionTest extends UpgradeTestBase
{
    @Test
    public void uncaughtExceptionTest() throws Throwable
    {
        new TestCase()
        .nodes(1)
        .nodesToUpgrade(1)
        .upgradesFrom(v3X)
        .setup(cluster -> {})
        .runBeforeNodeRestart(UpgradeUncaughtExceptionTest::testUncaught)
        .runAfterNodeUpgrade(UpgradeUncaughtExceptionTest::testUncaught).run();
    }

    private static void testUncaught(AbstractCluster cluster, int node) throws InterruptedException
    {
        IInstance instance = cluster.get(node);
        if (instance.isShutdown())
            return;

        instance.sync(() -> {
            Thread thread = new Thread(() -> {
                throw new RuntimeException();
            });
            thread.start();
            try
            {
                thread.join();
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
        }).run();
        int count;
        try
        {
            try
            {
                ((ExecutorService)cluster.get(node).executorFor(Verb.MUTATION_REQ.id)).submit(() -> {
                    throw new RuntimeException();
                }).get();
            }
            catch (ExecutionException ignore)
            {
            }
            count = 2;
        }
        catch (UnsupportedOperationException ignore)
        {
            count = 1;
        }
        try
        {
            cluster.checkAndResetUncaughtExceptions();
            Assert.fail();
        }
        catch (ShutdownException uncaught)
        {
            Assert.assertEquals(count, uncaught.getSuppressed().length);
        }
    }

}
