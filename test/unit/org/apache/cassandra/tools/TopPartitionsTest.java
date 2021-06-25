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
        // in 3.0 all CFs seem empty so all querties return no records; so need to insert one just for this test
        executeInternal("INSERT INTO system.local (key) VALUES (?)", "testServiceTopPartitionsSingleTable");

        ColumnFamilyStore.getIfExists("system", "local").beginLocalSampling("READS", 5, 100000);
        String req = "SELECT * FROM system.%s WHERE key='%s'";
        executeInternal(format(req, SystemKeyspace.LOCAL, "testServiceTopPartitionsSingleTable"));
        List<CompositeData> result = ColumnFamilyStore.getIfExists("system", "local").finishLocalSampling("READS", 5);
        assertEquals(1, result.size());
    }

    @Test
    public void testStartAndStopScheduledSampling()
    {
        List<String> allSamplers = Arrays.stream(Sampler.SamplerType.values()).map(Enum::toString).collect(Collectors.toList());
        StorageService ss = StorageService.instance;
        assertTrue("It should allow a new scheduled sampling task",
                   ss.startSamplePartitions(null, null, 1000, 1000, 100, 10, allSamplers));
        assertEquals(Collections.singletonList("*.*"), ss.getSampleTasks());
        assertFalse("It should not allow start sampling with the same key",
                    ss.startSamplePartitions(null, null, 2000, 2000, 100, 10, allSamplers));
        assertTrue("It should allow to stop an existing scheduled sampling task",
                   ss.stopSamplePartitions(null, null));
        assertEquals(Collections.emptyList(), ss.getSampleTasks());
        assertFalse("It should not allow stop a non-existing scheduled sampling task",
                    ss.stopSamplePartitions(null, null));
    }
}
