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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;

public class ProfileLoadTest extends TestBaseImpl
{
    @Test
    public void testScheduledSamplingTaskLogs() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            // start the scheduled profileload task that samples for 1 second and every second.
            cluster.get(1).nodetoolResult("profileload", "1000", "-i", "1")
                   .asserts().success();

            Random rnd = new Random();
            // 600 * 2ms = 1.2 seconds. It logs every second. So it logs at least once.
            for (int i = 0; i < 600; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"),
                                ConsistencyLevel.QUORUM, rnd.nextInt(), rnd.nextInt(), i);
                Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MILLISECONDS);
            }
            // test --list should display all active tasks.
            cluster.get(1).nodetoolResult("profileload", "--list")
                   .asserts()
                   .success()
                   .stdoutContains("*.*");
            // stop the scheduled sampling
            cluster.get(1).nodetoolResult("profileload", "--stop")
                   .asserts().success();
            List<String> freqHeadings = cluster.get(1)
                                               .logs()
                                               .grep("Frequency of (reads|writes|cas contentions) by partition")
                                               .getResult();
            Assert.assertTrue("The scheduled task should at least run and log once",
                              freqHeadings.size() > 3);
        }
    }

    @Test
    public void testPreventDuplicatedSchedule() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            // new sampling, we are good
            cluster.get(1).nodetoolResult("profileload", KEYSPACE, "tbl", "-i", "10")
                   .asserts()
                   .success()
                   .stdoutNotContains("Unable to schedule sampling for keyspace");

            // duplicated sampling (against the same table) but different interval. Nodetool should reject
            cluster.get(1).nodetoolResult("profileload", KEYSPACE, "tbl", "-i", "20")
                   .asserts()
                   .success()
                   .stdoutContains("Unable to schedule sampling for keyspace");

            // the later sampling all request creates overlaps, so it should be rejected too.
            cluster.get(1).nodetoolResult("profileload", "-i", "20")
                   .asserts()
                   .success()
                   .stdoutContains("Unable to schedule sampling for keyspace");

            cluster.get(1).nodetoolResult("profileload", KEYSPACE, "tbl", "--stop").asserts().success();

            cluster.get(1).nodetoolResult("profileload", "nonexistks", "nonexisttbl", "--stop")
                   .asserts()
                   .success()
                   .stdoutContains("Unable to stop the non-exist scheduled sampling");
        }
    }
}
