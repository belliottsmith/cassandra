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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.ReadSizeAbortException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.assertj.core.api.Assertions;

/**
 * ReadSize client warn/abort is coordinator only, so the fact ClientMetrics is coordinator only does not
 * impact the user experience
 */
public class ClientReadSizeWarningTest extends TestBaseImpl
{
    private static final Random RANDOM = new Random(0);
    private static ICluster<IInvokableInstance> CLUSTER;
    private static com.datastax.driver.core.Cluster JAVA_DRIVER;
    private static com.datastax.driver.core.Session JAVA_DRIVER_SESSION;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        Cluster.Builder builder = Cluster.build(3);
        builder.withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP));
        CLUSTER = builder.start();
        JAVA_DRIVER = JavaDriverUtils.create(CLUSTER);
        JAVA_DRIVER_SESSION = JAVA_DRIVER.connect();

        // setup threshold after init to avoid driver issues loading
        // the test uses a rather small limit, which causes driver to fail while loading metadata
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            DatabaseDescriptor.setClientLargeReadWarnThresholdKB(1);
            DatabaseDescriptor.setClientLargeReadAbortThresholdKB(2);
        }));
    }

    @Before
    public void setup()
    {
        CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(CLUSTER);
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v blob, PRIMARY KEY (pk, ck))");
    }

    private static void assertPrefix(String expectedPrefix, String actual)
    {
        if (!actual.startsWith(expectedPrefix))
            throw new AssertionError(String.format("expected \"%s\" to begin with \"%s\"", actual, expectedPrefix));
    }

    private static ByteBuffer bytes(int size)
    {
        byte[] b = new byte[size];
        RANDOM.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    @Test
    public void noWarningsSinglePartition()
    {
        noWarnings("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1");
    }

    @Test
    public void noWarningsScan()
    {
        noWarnings("SELECT * FROM " + KEYSPACE + ".tbl");
    }

    public void noWarnings(String cql)
    {
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)", ConsistencyLevel.ALL, bytes(128));
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, bytes(128));

        Consumer<List<String>> test = warnings ->
                                      Assert.assertEquals(Collections.emptyList(), warnings);

        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(cql, ConsistencyLevel.ALL);
        test.accept(result.warnings());
        test.accept(driverQueryAll(cql).getExecutionInfo().getWarnings());
    }

    @Test
    public void warnThresholdSinglePartition()
    {
        warnThreshold("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1");
    }

    @Test
    public void warnThresholdScan()
    {
        warnThreshold("SELECT * FROM " + KEYSPACE + ".tbl");
    }

    public void warnThreshold(String cql)
    {
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)", ConsistencyLevel.ALL, bytes(512));
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, bytes(512));

        Consumer<List<String>> testEnabled = warnings ->
                                             assertPrefix("Read on table " + KEYSPACE + ".tbl has exceeded the size warning threshold", Iterables.getOnlyElement(warnings));

        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(cql, ConsistencyLevel.ALL);
        testEnabled.accept(result.warnings());
        testEnabled.accept(driverQueryAll(cql).getExecutionInfo().getWarnings());
    }

    @Test
    public void failThresholdSinglePartition() throws UnknownHostException
    {
        failThreshold("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1");
    }

    @Test
    public void failThresholdScan() throws UnknownHostException
    {
        failThreshold("SELECT * FROM " + KEYSPACE + ".tbl");
    }

    public void failThreshold(String cql) throws UnknownHostException
    {
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)", ConsistencyLevel.ALL, bytes(512));
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, bytes(512));
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, ?)", ConsistencyLevel.ALL, bytes(512));
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 4, ?)", ConsistencyLevel.ALL, bytes(512));
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 5, ?)", ConsistencyLevel.ALL, bytes(512));

        List<String> warnings = CLUSTER.get(1).callsOnInstance(() -> {
            ClientWarn.instance.captureWarnings();
            try
            {
                QueryProcessor.execute(cql, org.apache.cassandra.db.ConsistencyLevel.ALL, QueryState.forInternalCalls());
                Assert.fail("Expected query failure");
            }
            catch (ReadSizeAbortException e)
            {
                // expected, client transport returns an error message and includes client warnings
            }
            return ClientWarn.instance.getWarnings();
        }).call();
        Assertions.assertThat(warnings).hasSize(1);
        assertPrefix("Read on table " + KEYSPACE + ".tbl has exceeded the size failure threshold", warnings.get(0));

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
            Assertions.assertThat(e.getMessage()).endsWith("(1 responses were required but only 0 replica responded, 1 failed)");
        }
    }

    private static ResultSet driverQueryAll(String cql)
    {
        return JAVA_DRIVER_SESSION.execute(new SimpleStatement(cql).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));
    }
}
