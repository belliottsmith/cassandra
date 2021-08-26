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
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.TombstoneAbortException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.assertj.core.api.Assertions;

public class ClientTombstoneWarningTest extends TestBaseImpl
{
    private static final int TOMBSTONE_WARN = 50;
    private static final int TOMBSTONE_FAIL = 100;
    private static ICluster<IInvokableInstance> CLUSTER;
    private static com.datastax.driver.core.Cluster JAVA_DRIVER;
    private static com.datastax.driver.core.Session JAVA_DRIVER_SESSION;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        Cluster.Builder builder = Cluster.build(3);
        builder.withConfig(c -> c.set("tombstone_warn_threshold", TOMBSTONE_WARN)
                                 .set("tombstone_failure_threshold", TOMBSTONE_FAIL)
                                 .with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP));
        CLUSTER = builder.start();
        JAVA_DRIVER = JavaDriverUtils.create(CLUSTER);
        JAVA_DRIVER_SESSION = JAVA_DRIVER.connect();
    }

    @AfterClass
    public static void teardown()
    {
        if (JAVA_DRIVER_SESSION != null)
            JAVA_DRIVER_SESSION.close();
        if (JAVA_DRIVER != null)
            JAVA_DRIVER.close();
    }

    @Before
    public void setup()
    {
        CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(CLUSTER);
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void noWarningsSinglePartition()
    {
        noWarnings("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1");
    }

    @Test
    public void noWarningsScan()
    {
        noWarnings("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1");
    }

    public void noWarnings(String cql)
    {
        Consumer<List<String>> test = warnings ->
                                      Assert.assertEquals(Collections.emptyList(), warnings);

        for (int i=0; i<TOMBSTONE_WARN; i++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, null)", ConsistencyLevel.ALL, i);

        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(cql, ConsistencyLevel.ALL);
        test.accept(result.warnings());
        test.accept(driverQueryAll(cql).getExecutionInfo().getWarnings());
    }

    @Test
    public void warnThresholdSinglePartition()
    {
        warnThreshold("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", false);
    }

    @Test
    public void warnThresholdScan()
    {
        warnThreshold("SELECT * FROM " + KEYSPACE + ".tbl", true);
    }

    private void warnThreshold(String cql, boolean isScan)
    {
        for (int i = 0; i < TOMBSTONE_WARN + 1; i++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, null)", ConsistencyLevel.ALL, i);

        Consumer<List<String>> testEnabled = warnings ->
                                             Assertions.assertThat(Iterables.getOnlyElement(warnings))
                                                       .contains("nodes scanned up to " + (TOMBSTONE_WARN + 1) + " tombstones and issued tombstone warnings for query " + cql);

        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(cql, ConsistencyLevel.ALL);
        testEnabled.accept(result.warnings());
        testEnabled.accept(driverQueryAll(cql).getExecutionInfo().getWarnings());
    }

    @Test
    public void failThresholdSinglePartition() throws UnknownHostException
    {
        failThreshold("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", false);
    }

    @Test
    public void failThresholdScan() throws UnknownHostException
    {
        failThreshold("SELECT * FROM " + KEYSPACE + ".tbl", true);
    }

    private void failThreshold(String cql, boolean isScan) throws UnknownHostException
    {
        for (int i = 0; i < TOMBSTONE_FAIL + 1; i++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, null)", ConsistencyLevel.ALL, i);

        List<String> warnings = CLUSTER.get(1).callsOnInstance(() -> {
            ClientWarn.instance.captureWarnings();
            try
            {
                QueryProcessor.execute(cql, org.apache.cassandra.db.ConsistencyLevel.ALL, QueryState.forInternalCalls());
                Assert.fail("Expected query failure");
            }
            catch (TombstoneAbortException e)
            {
                Assert.assertTrue(e.nodes >= 1 && e.nodes <= 3);
                Assert.assertEquals(TOMBSTONE_FAIL + 1, e.tombstones);
                // expected, client transport returns an error message and includes client warnings
            }
            return ClientWarn.instance.getWarnings();
        }).call();
        Assertions.assertThat(Iterables.getOnlyElement(warnings))
                  .contains("nodes scanned over " + (TOMBSTONE_FAIL + 1) + " tombstones and aborted the query " + cql);

        try
        {
            driverQueryAll(cql);
            Assert.fail("Query should have thrown ReadFailureException");
        }
        catch (com.datastax.driver.core.exceptions.ReadFailureException e)
        {
            // without changing the client can't produce a better message...
            // client does NOT include the message sent from the server in the exception; so the message doesn't work
            // well in this case
            Assertions.assertThat(e.getMessage()).contains("(3 responses were required but only 0 replica responded"); // can't include ', 3 failed)' as some times its 2
        }
    }

    private static ResultSet driverQueryAll(String cql)
    {
        return JAVA_DRIVER_SESSION.execute(new SimpleStatement(cql).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));
    }
}
