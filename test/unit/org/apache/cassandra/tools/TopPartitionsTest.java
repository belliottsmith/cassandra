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

package org.apache.cassandra.tools;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.openmbean.CompositeData;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// Includes test cases for 'toppartitions' and the successor 'profileload'
public class TopPartitionsTest
{
    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testServiceTopPartitionsNoArg() throws Exception
    {
        BlockingQueue<Map<String, List<CompositeData>>> q = new ArrayBlockingQueue<>(1);
        ColumnFamilyStore.all();
        Executors.newCachedThreadPool().execute(() ->
        {
            try
            {
                q.put(StorageService.instance.samplePartitions(null, 1000, 100, 10, Lists.newArrayList("READS", "WRITES")));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        });
        Thread.sleep(100);
        SystemKeyspace.persistLocalMetadata();
        Map<String, List<CompositeData>> result = q.poll(5, TimeUnit.SECONDS);
        List<CompositeData> cd = result.get("WRITES");
        assertEquals(1, cd.size());
    }

    @Test
    public void testServiceTopPartitionsSingleTable() throws Exception
    {
        ColumnFamilyStore.getIfExists("system", "local").beginLocalSampling("READS", 5, 100000);
        String req = "SELECT * FROM system.%s WHERE key='%s'";
        executeInternal(format(req, SystemKeyspace.LOCAL, SystemKeyspace.LOCAL));
        List<CompositeData> result = ColumnFamilyStore.getIfExists("system", "local").finishLocalSampling("READS", 5);
        assertEquals(1, result.size());
    }

    @Test
    public void testStartAndStopScheduledSampling()
    {
        List<String> allSamplers = Arrays.stream(Sampler.SamplerType.values()).map(Enum::toString).collect(Collectors.toList());
        StorageService ss = StorageService.instance;
        assertTrue("It should allow a new scheduled sampling task",
                   ss.startSamplingPartitions(null, null, 10, 10, 100, 10, allSamplers));
        assertEquals(Collections.singletonList("*.*"), ss.getSampleTasks());
        assertFalse("It should not allow start sampling with the same key",
                    ss.startSamplingPartitions(null, null, 20, 20, 100, 10, allSamplers));
        assertTrue("It should allow to stop an existing scheduled sampling task",
                   ss.stopSamplingPartitions(null, null));
        int timeout = 3;
        while (timeout-- > 0 && ss.getSampleTasks().size() > 0)
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        assertEquals("It should remove the scheduled sample task.", Collections.emptyList(), ss.getSampleTasks());
        assertTrue("It should allow stop all scheduled sampling task when nothing scheduled",
                    ss.stopSamplingPartitions(null, null));
    }
}
