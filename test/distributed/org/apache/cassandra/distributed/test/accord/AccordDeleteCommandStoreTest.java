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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.primitives.Ranges;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.Assert.assertFalse;

import org.junit.BeforeClass;
import org.junit.Test;

public class AccordDeleteCommandStoreTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordRegainRangesTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder
                                               .withoutVNodes()
                                               .withConfig(config ->
                                                           config
                                                           .set("accord.shard_durability_target_splits", "1")
                                                           .set("accord.shard_durability_cycle", "20s")
                                                           .with(Feature.NETWORK, Feature.GOSSIP)), 6);
    }

    @Test
    public void deleteCommandStoresTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");
        test(ddls, cluster -> {
            String newToken = cluster.get(1).callOnInstance(() -> getOnlyElement(StorageService.instance.getTokens()));
            String originalToken = cluster.get(2).callOnInstance(() -> getOnlyElement(StorageService.instance.getTokens()));

            cluster.get(2).runOnInstance(() -> {
                StorageService.instance.move(Long.toString(Long.parseLong(newToken) + 100));
            });

            cluster.get(2).runOnInstance(() -> {
                Set<Integer> commandStoresThatWillBeRemoved = new HashSet<>();

                for (CommandStore commandStore : AccordService.instance().node().commandStores().all())
                {
                    Ranges ranges = getBlocking(commandStore.submit((PreLoadContext.Empty) () -> "Get rangesForEpoch", safeCommandStore -> safeCommandStore.ranges().currentRanges()));

                    if (ranges.isEmpty())
                        commandStoresThatWillBeRemoved.add(commandStore.id());
                }

                StorageService.instance.move(originalToken);

                for (CommandStore commandStore : AccordService.instance().node().commandStores().all())
                {
                    assertFalse(commandStoresThatWillBeRemoved.contains(commandStore.id()));
                }
            });
        });
    }
}

