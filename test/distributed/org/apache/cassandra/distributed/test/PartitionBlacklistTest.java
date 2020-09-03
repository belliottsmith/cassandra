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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class PartitionBlacklistTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionBlacklistTest.class);
    private static final int testReplicationFactor = 3;

    // Test fix for rdar://49631939 (Partition blacklist read unavailable on startup)
    // Create a four node cluster, populate with some blacklist entries, stop all
    // the nodes, then bring them up one by one, waiting for each node to complete
    // startup before starting the next.
    // On startup each node runs a SELECT * query on the partition blacklist table
    // to populate the cache.  The whole keyspace is unlikely to be available until
    // three of the four nodes are started, so the early nodes will go through several
    // cycles of failing to retrieve the partition blacklist before succeeding.
    //
    // with({NETWORK,GOSSIP} is currently required for in-JVM dtests to create
    // the distributed system tables.
    @Test
    public void checkStartupWithoutTriggeringUnavailable() throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        int nodeCount = 4;
        System.setProperty("cassandra.ring_delay_ms", "5000"); // down from 30s default
        System.setProperty("cassandra.consistent.rangemovement", "false");
        System.setProperty("cassandra.consistent.simultaneousmoves.allow", "true");
        try (Cluster cluster = Cluster.build(nodeCount)
                                      .withConfig(config -> config
                                                            .with(NETWORK)
                                                            .with(GOSSIP)
                                                            .set("enable_partition_blacklist", true)
                                                            .set("blacklist_initial_load_retry_seconds", 1))
                                      .createWithoutStarting())
        {
            cluster.forEach(i -> {
                i.startup();
                i.runOnInstance(PartitionBlacklistTest::waitUntilStarted);
            });

            // Do a cluster-wide no unavailables were recorded while the blacklist
            // was loaded.
            cluster.forEach(i -> i.runOnInstance(PartitionBlacklistTest::checkNoUnavailables));
        }
    }

    static private void waitUntilStarted()
    {
        waitUntilStarted(60, TimeUnit.SECONDS);
    }

    // To be called inside the instance with runOnInstance
    static private void waitUntilStarted(int waitDuration, TimeUnit waitUnits)
    {
        long deadlineInMillis = System.currentTimeMillis() + Math.max(1, waitUnits.toMillis(waitDuration));
        while (!StorageService.instance.getOperationMode().equals("NORMAL"))
        {
            if (System.currentTimeMillis() >= deadlineInMillis)
            {
                throw new RuntimeException("Instance did not reach application state NORMAL before timeout");
            }
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
    }

    // To be called inside the instance with runOnInstance
    static private void checkNoUnavailables()
    {
        long deadlineInMillis = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);

        while (System.currentTimeMillis() < deadlineInMillis &&
               StorageProxy.instance.getPartitionBlacklistLoadSuccesses() == 0)
        {
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        }

        Assert.assertTrue("Partition blacklist must have loaded before checking unavailables",
                          StorageProxy.instance.getPartitionBlacklistLoadSuccesses() > 0);
        Assert.assertEquals("Initial load of partition blacklist should not trigger unavailables",
                            0L, StorageProxy.rangeMetrics.unavailables.getCount());
    }

    // to be called inside the isntance with runOnInstance, no nodes are started/stopped
    // and not enough nodes are available to succeed, so it should just retry a few times
    static private void checkTimerActive()
    {
        long deadlineInMillis = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);

        do
        {
            // Make sure at least two load attempts have happened,
            // in case we received a node up event about this node
            if (StorageProxy.instance.getPartitionBlacklistLoadAttempts() > 2)
            {
                return;
            }
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        } while (System.currentTimeMillis() < deadlineInMillis);

        Assert.fail("Node did not retry loading on timeout in 30s");
    }

    @Test
    public void checkTimerRetriesLoad() throws IOException
    {
        int nodeCount = 3;

        try (Cluster cluster = Cluster.build(nodeCount)
                                      .withConfig(config -> config
                                                            .with(NETWORK)
                                                            .with(GOSSIP)
                                                            .set("enable_partition_blacklist", true)
                                                            .set("blacklist_initial_load_retry_seconds", 1))
                                      .createWithoutStarting())
        {
            // Starting without networking enabled in the hope it doesn't trigger
            // node lifecycle events when nodes start up.
            cluster.get(1).startup();
            cluster.get(1).runOnInstance(PartitionBlacklistTest::checkTimerActive);
        }
    }
}
