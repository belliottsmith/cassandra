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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.IndexSizeAbortException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;

import static org.apache.cassandra.db.RowIndexEntry.Serializer.estimateMaterializedIndexSize;

public class ClientIndexSizeWarningTest extends TestBaseImpl
{
    private static final int COLUMN_IDX_SIZE_KB = 1;
    private static final int INDEX_SIZE_WARN_KB = 1;
    private static final int INDEX_SIZE_ABORT_KB = 2;

    private static final int COLUMN_IDX_SIZE = COLUMN_IDX_SIZE_KB * 1024;
    private static final int INDEX_WARN_SIZE = INDEX_SIZE_WARN_KB * 1024;
    private static final int INDEX_ABORT_SIZE = INDEX_SIZE_ABORT_KB * 1024;
    private static final String TABLE = "tbl";
    private static final String KS_TBL = KEYSPACE + '.' + TABLE;
    private static final Cluster cluster;

    static
    {
        try
        {
            Cluster.Builder builder = Cluster.build(3);
            builder.withConfig(c -> c.set("column_index_size_in_kb", COLUMN_IDX_SIZE_KB)
                                     .set("large_partition_index_warning_threshold_kb", INDEX_SIZE_WARN_KB)
                                     .set("large_partition_index_failure_threshold_kb", INDEX_SIZE_ABORT_KB));
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
        cluster.schemaChange("CREATE TABLE " + KS_TBL + " (pk int, ck blob, v blob, PRIMARY KEY (pk, ck))");
    }

    private static ByteBuffer blob(int val, int size)
    {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        assert buffer.remaining() == size;
        buffer.putInt(size - Integer.BYTES, val);
        return buffer;
    }

    private static void flush(long minIdxSize, long maxIdxSize)
    {
        cluster.forEach(i -> i.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            Assert.assertEquals(0, cfs.getLiveSSTables().size());
            cfs.forceBlockingFlush();
            SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
            long ifileSize = sstable.getIndexChannel().size();
            Assert.assertTrue(String.format("%s is not > %s", ifileSize, minIdxSize), ifileSize > minIdxSize);
            Assert.assertTrue(String.format("%s is not < %s", ifileSize, maxIdxSize), ifileSize < maxIdxSize);
        }));
    }

    private static void insert(ByteBuffer clustering, ByteBuffer value)
    {
        cluster.coordinator(1).execute("INSERT INTO " + KS_TBL + " (pk, ck, v) VALUES (1, ?, ?)", ConsistencyLevel.ALL,
                                       clustering, value);
    }

    private static void assertIndexSizeMetrics(int expectedWarnings, int expectedAborts)
    {
        cluster.forEach(() -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(TABLE);

            Assert.assertEquals(expectedWarnings, keyspace.metric.clientIndexSizeWarnings.getCount());
            Assert.assertEquals(expectedWarnings, cfs.metric.clientIndexSizeWarnings.table.getCount());

            Assert.assertEquals(expectedAborts, keyspace.metric.clientIndexSizeAborts.getCount());
            Assert.assertEquals(expectedAborts, cfs.metric.clientIndexSizeAborts.table.getCount());
        });
    }

    @Test
    public void noWarnings()
    {
        insert(blob(1, INDEX_WARN_SIZE / 8), blob(1, COLUMN_IDX_SIZE + 1));
        insert(blob(2, INDEX_WARN_SIZE / 8), blob(1, COLUMN_IDX_SIZE + 1));
        flush(INDEX_WARN_SIZE / 2, INDEX_WARN_SIZE);
        assertIndexSizeMetrics(0, 0);
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.ALL);
        assertIndexSizeMetrics(0, 0);
        Assert.assertEquals(Collections.emptyList(), result.warnings());
    }

    @Test
    public void warnThreshold()
    {
        long keySize = estimateMaterializedIndexSize(2, INDEX_WARN_SIZE + 16) / 4;
        insert(blob(1, (int) keySize), blob(1, COLUMN_IDX_SIZE + 1));
        insert(blob(2, (int) keySize), blob(1, COLUMN_IDX_SIZE + 1));
        flush(INDEX_WARN_SIZE, INDEX_ABORT_SIZE);
        assertIndexSizeMetrics(0, 0);

        SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", ConsistencyLevel.ALL);
        assertIndexSizeMetrics(1, 0);
        String warning = Iterables.getOnlyElement(result.warnings());
        Matcher matcher = Pattern.compile("3 nodes encountered materialized row index sizes of up to (\\d+)b and issued index size warnings.*").matcher(warning);

        Assert.assertTrue(warning, matcher.matches());
        long size = Long.parseLong(matcher.group(1));
        Assert.assertTrue(Long.toString(size), size >= INDEX_WARN_SIZE);
        Assert.assertTrue(Long.toString(size), size < INDEX_ABORT_SIZE);
    }

    @Test
    public void failThreshold()
    {
        long keySize = estimateMaterializedIndexSize(2, INDEX_ABORT_SIZE + 16) / 4;
        insert(blob(1, (int) keySize), blob(1, COLUMN_IDX_SIZE + 1));
        insert(blob(2, (int) keySize), blob(1, COLUMN_IDX_SIZE + 1));
        flush(INDEX_ABORT_SIZE, INDEX_ABORT_SIZE * 2);
        assertIndexSizeMetrics(0, 0);

        List<String> warnings = cluster.get(1).callsOnInstance(() -> {
            ClientWarn.instance.captureWarnings();
            try
            {
                QueryProcessor.execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1", org.apache.cassandra.db.ConsistencyLevel.ALL, QueryState.forInternalCalls());
                Assert.fail("Expected query failure");
            }
            catch (IndexSizeAbortException e)
            {
                // expected, client transport returns an error message and includes client warnings
            }
            return ClientWarn.instance.getWarnings();
        }).call();

        assertIndexSizeMetrics(0, 1);
        String warning = Iterables.getOnlyElement(warnings);
        Matcher matcher = Pattern.compile("(\\d+) nodes encountered estimated materialized row index sizes of up to (\\d+)b and aborted the query.*").matcher(warning);

        Assert.assertTrue(warning, matcher.matches());
        int failures = Integer.parseInt(matcher.group(1));
        long size = Long.parseLong(matcher.group(2));
        Assert.assertTrue(failures >= 1 && failures <= 3);
        Assert.assertTrue(Long.toString(size), size >= INDEX_ABORT_SIZE);
    }
}
