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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;

public class HintRetryTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(HintRetryTest.class);

    // port of hintedhandoff_test.py::TestHintedHandoffConfig::test_hintedhandoff_dc_reenabled but adds failed messages
    // this will make sure we still work if ephemeral network issues come up
    @Test
    public void hintedhandoffDcReenabledWithFailedMessage() throws IOException, TimeoutException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c ->
                                                       c.with(Feature.values())
                                                        .set("hinted_handoff_enabled", true)
                                                        .set("hinted_handoff_disabled_datacenters", Arrays.asList("datacenter1")))
                                           .start()))
        {
            // validate HH is off
            cluster.forEach(node -> {
                NodeToolResult result = node.nodetoolResult("statushandoff");
                result.asserts().success();
                Assertions.assertThat(result.getStdout())
                          .isEqualTo("Hinted handoff is running\nData center datacenter1 is disabled\n");

                node.nodetoolResult("enablehintsfordc", "datacenter1").asserts().success();

                result = node.nodetoolResult("statushandoff");
                result.asserts().success();
                Assertions.assertThat(result.getStdout())
                          .isEqualTo("Hinted handoff is running\n");
            });

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY)"));

            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);

            stopUnchecked(node2);

            int numRows = 100;
            for (int i = 0; i < numRows; i++)
                node1.coordinator().execute(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), ConsistencyLevel.ONE, i);

            long node1Offset = node1.logs().mark();

            // make sure a hint is dropped due to networking
            AtomicReference<IMessageFilters.Filter> filter = new AtomicReference<>();
            filter.set(cluster.filters()
                              .inbound()
                              .to(2)
                              .verbs(MessagingService.Verb.HINT.getId())
                              .messagesMatching((from, to, msg) -> {
                                  filter.get().off();
                                  return true;
                              })
                              .drop());
            node2.startup(cluster);

            LogResult<List<String>> watch = node1.logs().watchFor(node1Offset, "Finished hinted");
            logger.info("Watched results {}", watch);
            Assertions.assertThat(watch.getResult())
                      .isNotEmpty()
                      .as("Checking if string ends with ', partially'")
                      .allMatch(s -> s.endsWith(", partially"));
            node1Offset = watch.getMark();

            // watch for the retry
            watch = node1.logs().watchFor(node1Offset, "Finished hinted");
            logger.info("Watched results {}", watch);
            Assertions.assertThat(watch.getResult())
                      .isNotEmpty()
                      .as("Checking if string does notends with ', partially'")
                      .allMatch(s -> !s.endsWith(", partially"));

            stopUnchecked(node1);

            for (int i = 0; i < numRows; i++)
            {
                SimpleQueryResult result = node2.executeInternalWithResult(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"), i);
                AssertUtils.assertRows(result, QueryResults.builder()
                                                           .columns("pk")
                                                           .row(i)
                                                           .build());
            }
        }
    }
}
