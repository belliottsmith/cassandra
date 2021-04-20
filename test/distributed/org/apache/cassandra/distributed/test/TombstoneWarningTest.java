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
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.TombstoneAbortException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;

import static org.apache.cassandra.service.ReadCallback.tombstoneAbortMessage;
import static org.apache.cassandra.service.ReadCallback.tombstoneWarnMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TombstoneWarningTest extends TestBaseImpl
{
    private static final int TOMBSTONE_WARN = 50;
    private static final int TOMBSTONE_FAIL = 100;
    private static final ICluster<IInvokableInstance> cluster;

    static
    {
        try
        {
            Cluster.Builder builder = Cluster.build(3);
            builder.withConfig(c -> c.set("tombstone_warn_threshold", TOMBSTONE_WARN)
                                     .set("tombstone_failure_threshold", TOMBSTONE_FAIL));
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
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
    }

    private static void assertPrefix(String expectedPrefix, String actual)
    {
        if (!actual.startsWith(expectedPrefix))
            throw new AssertionError(String.format("expected \"%s\" to begin with \"%s\"", actual, expectedPrefix));
    }

    @Test
    public void noWarnings()
    {
        for (int i=0; i<TOMBSTONE_WARN; i++)
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, null)", ConsistencyLevel.ALL, i);

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.ALL);
        Assert.assertEquals(Collections.emptyList(), result.warnings());
    }

    @Test
    public void warnThreshold()
    {
        for (int i=0; i<TOMBSTONE_WARN + 1; i++)
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, null)", ConsistencyLevel.ALL, i);

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.ALL);
        assertPrefix(tombstoneWarnMessage(3, TOMBSTONE_WARN + 1), Iterables.getOnlyElement(result.warnings()));
    }

    @Test
    public void failThreshold()
    {
        for (int i=0; i<TOMBSTONE_FAIL + 1; i++)
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, null)", ConsistencyLevel.ALL, i);

        List<String> warnings = cluster.get(1).callsOnInstance(() -> {
            ClientWarn.instance.captureWarnings();
            try
            {
                QueryProcessor.execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", org.apache.cassandra.db.ConsistencyLevel.ALL, QueryState.forInternalCalls());
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
        List<String> expected = IntStream.of(1, 2, 3).mapToObj(i -> tombstoneAbortMessage(i, TOMBSTONE_FAIL + 1)).collect(Collectors.toList());
        Assert.assertTrue(warnings + " not contained in " + expected, Iterables.any(expected, Iterables.getOnlyElement(warnings)::startsWith));
    }

    @Test
    public void regularTombstonesLogTest()
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("update %s.tbl set v = null where pk = ? and ck = ?"), ConsistencyLevel.ALL, i, j);
        assertTombstoneLogs(TOMBSTONE_WARN - 1, false);
    }

    @Test
    public void rowTombstonesLogTest()
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("delete from %s.tbl where pk = ? and ck = ?"), ConsistencyLevel.ALL, i, j);
        assertTombstoneLogs(TOMBSTONE_WARN - 1, false);
    }

    @Test
    public void rangeTombstonesLogTest()
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("delete from %s.tbl where pk = ? and ck >= ? and ck <= ?"), ConsistencyLevel.ALL, i, j, j);
        assertTombstoneLogs((TOMBSTONE_WARN - 1) + TOMBSTONE_WARN / 2, true);
    }

    @Test
    public void noTombstonesLogTest()
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v) values (?, ?, ?)"), ConsistencyLevel.ALL, i, j, j);
        assertTombstoneLogs(0, false);
    }

    private void assertTombstoneLogs(long expectedCount, boolean isRangeTombstones)
    {
        long mark = cluster.get(1).logs().mark();
        cluster.get(1).flush(KEYSPACE);
        String pattern = ".*Writing (?<tscount>\\d+) tombstones to distributed_test_keyspace/tbl:(?<key>\\d+).*";
        LogResult<List<String>> res = cluster.get(1).logs().grep(mark, pattern);
        assertEquals(expectedCount, res.getResult().size());
        Pattern p = Pattern.compile(pattern);
        for (String r : res.getResult())
        {
            Matcher m = p.matcher(r);
            assertTrue(m.matches());
            long tombstoneCount = Integer.parseInt(m.group("tscount"));
            assertTrue(tombstoneCount > TOMBSTONE_WARN);
            assertEquals(r, Integer.parseInt(m.group("key")) * (isRangeTombstones ? 2 : 1), tombstoneCount);
        }
    }
}
