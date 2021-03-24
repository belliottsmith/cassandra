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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Iterables;
import org.junit.*;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.*;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.ReadSizeFailureException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;

public class ReadSizeWarningTest extends TestBaseImpl
{
    private static final ICluster<IInvokableInstance> cluster;
    private static final Random random = new Random(0);

    static
    {
        try
        {
            Cluster.Builder builder = Cluster.build(3);
            builder.withConfig(c -> c.set("client_large_read_warn_threshold_kb", 1)
                                     .set("client_large_read_block_threshold_kb", 2));
            cluster = builder.createWithoutStarting();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void setupClass()
    {
        cluster.startup();
    }

    @Before
    public void setup()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(cluster);
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v blob, PRIMARY KEY (pk, ck))");
    }

    private static void assertPrefix(String expectedPrefix, String actual)
    {
        if (!actual.startsWith(expectedPrefix))
            throw new AssertionError(String.format("expected \"%s\" to begin with \"%s\"", actual, expectedPrefix));
    }

    private static ByteBuffer bytes(int size)
    {
        byte[] b = new byte[size];
        random.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    @Test
    public void noWarnings()
    {
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)", ConsistencyLevel.ALL, bytes(128));
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, bytes(128));

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.ALL);
        Assert.assertEquals(Collections.emptyList(), result.warnings());
    }

    @Test
    public void warnThreshold()
    {
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)", ConsistencyLevel.ALL, bytes(512));
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, bytes(512));

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.ALL);
        assertPrefix("Read has exceeded the size warning threshold", Iterables.getOnlyElement(result.warnings()));
    }

    @Test
    public void failThreshold()
    {
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)", ConsistencyLevel.ALL, bytes(512));
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, bytes(512));
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, ?)", ConsistencyLevel.ALL, bytes(512));
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 4, ?)", ConsistencyLevel.ALL, bytes(512));
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 5, ?)", ConsistencyLevel.ALL, bytes(512));

        List<String> warnings = cluster.get(1).callsOnInstance(() -> {
            ClientWarn.instance.captureWarnings();
            try
            {
                QueryProcessor.execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", org.apache.cassandra.db.ConsistencyLevel.ALL, QueryState.forInternalCalls());
                Assert.fail("Expected query failure");
            }
            catch (ReadSizeFailureException e)
            {
                // expected, client transport returns an error message and includes client warnings
            }
            return ClientWarn.instance.getWarnings();
        }).call();
        Assert.assertEquals(2, warnings.size());
        assertPrefix("Read has exceeded the size warning threshold", warnings.get(0));
        assertPrefix("Read has exceeded the size failure threshold", warnings.get(1));
    }}
