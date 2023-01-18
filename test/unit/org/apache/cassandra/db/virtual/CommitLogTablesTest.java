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

package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.commitlog.CommitLogSegment;

import static org.assertj.core.api.Assertions.assertThat;

public class CommitLogTablesTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    private Config config;
    static private CommitLogSegmentsMapTable segmentsMapTable;
    static private CommitLogSegmentsTable segmentsTable;
    static private CommitLogTablesTable summaryTable;
    private final static Random random = new Random();

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setCommitLogSegmentSize(1);
        CQLTester.setUpClass();
        segmentsMapTable = new CommitLogSegmentsMapTable(KS_NAME);
        segmentsTable = new CommitLogSegmentsTable(KS_NAME);
        summaryTable = new CommitLogTablesTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(segmentsMapTable,
                                                                                                segmentsTable,
                                                                                                summaryTable)));
    }

    @Before
    public void config()
    {
        schemaChange("CREATE TABLE " + KEYSPACE + ".tbl1(" +
                     "pk int," +
                     "b blob," +
                     "PRIMARY KEY (pk))");
        schemaChange("CREATE TABLE " + KEYSPACE + ".tbl2(" +
                     "pk int," +
                     "b blob," +
                     "PRIMARY KEY (pk))");
    }
    int pk = 0;

    ByteBuffer randomBytes(int size)
    {
        byte[] randomBytes = new byte[size];
        random.nextBytes(randomBytes);
        return ByteBuffer.wrap(randomBytes);
    }

    void writeData() throws Throwable
    {
        for (int i = 0; i < 4; i++)
        {
            execute("INSERT INTO " + KEYSPACE + ".tbl1(pk,b) VALUES (?, ?)", pk, randomBytes(1 << 19)); // 512KiB
            execute("INSERT INTO " + KEYSPACE + ".tbl2(pk,b) VALUES (?, ?)", pk++, randomBytes(1 << 19)); // 512KiB
        }
    }

    @Test
    public void testSelectAll() throws Throwable
    {
        int paging = (int) (Math.random() * 100 + 1);
        flush(KEYSPACE); // make sure seeing fresh commitlog for the keyspace

        ResultSet flushedDetails = executeNetWithPaging("SELECT * FROM vts.commitlog_segments_map", paging);
        for (Row r : flushedDetails)
        {
            String keyspace = r.getString("keyspace_name");
            assertThat(keyspace).isNotEqualTo(KEYSPACE);
        }

        ResultSet flushedSummary = executeNetWithPaging("SELECT * FROM vts.commitlog_tables", paging);
        for (Row r : flushedSummary)
        {
            String keyspace = r.getString("keyspace_name");
            assertThat(keyspace).isNotEqualTo(KEYSPACE);
        }


        // Write enough data to fill a few commitlog segments
        writeData();

        // Check the dirty tables include the tables written to
        List<String> dirtyTableNames = new ArrayList<>();
        ResultSet writtenDetails = executeNetWithPaging("SELECT * FROM vts.commitlog_segments_map", paging);
        assertThat(writtenDetails.getColumnDefinitions().asList()
                                 .stream().map(ColumnDefinitions.Definition::getName).collect(Collectors.toSet()))
            .containsAll(Arrays.asList(CommitLogSegmentsMapTable.SEGMENT_PATH,
                                       CommitLogSegmentsMapTable.KEYSPACE_NAME,
                                       CommitLogSegmentsMapTable.TABLE_NAME));
        for (Row r : writtenDetails)
        {
            String keyspace = r.getString("keyspace_name");
            if (KEYSPACE.equals(keyspace))
            {
                dirtyTableNames.add(r.getString("table_name"));
            }
        }
        assertThat(dirtyTableNames).containsAll(Arrays.asList("tbl1", "tbl2"));

        // Check the summary includes the test tables

        ResultSet writtenSummary = executeNetWithPaging("SELECT * FROM vts.commitlog_tables", paging);
        assertThat(writtenSummary.getColumnDefinitions().asList()
                                 .stream().map(ColumnDefinitions.Definition::getName).collect(Collectors.toSet()))
            .containsAll(Arrays.asList(CommitLogTablesTable.KEYSPACE_NAME,
                                       CommitLogTablesTable.TABLE_NAME,
                                       CommitLogTablesTable.NUM_DIRTY_SEGMENTS,
                                       CommitLogTablesTable.ON_DISK_BYTES_BLOCKED,
                                       CommitLogTablesTable.EARLIEST_SEGMENT_PATH));
        dirtyTableNames.clear();
        for (Row r : writtenSummary)
        {
            String keyspace = r.getString("keyspace_name");
            if (KEYSPACE.equals(keyspace))
            {
                assertThat(r.getInt(CommitLogTablesTable.NUM_DIRTY_SEGMENTS)).isGreaterThan(1);
                assertThat(r.getLong(CommitLogTablesTable.ON_DISK_BYTES_BLOCKED)).isGreaterThan(0L);
                dirtyTableNames.add(r.getString("table_name"));
            }
        }
        assertThat(dirtyTableNames).containsAll(Arrays.asList("tbl1", "tbl2"));

        // Check the commitlog segment table
        ResultSet segments1 = executeNetWithPaging("SELECT * FROM vts.commitlog_segments", paging);
        assertThat(segments1.getColumnDefinitions().asList()
                            .stream().map(ColumnDefinitions.Definition::getName).collect(Collectors.toSet()))
            .containsAll(Arrays.asList(CommitLogSegmentsTable.segment_path,
                                       CommitLogSegmentsTable.ON_DISK_SIZE,
                                       CommitLogSegmentsTable.NUM_DIRTY_TABLES,
                                       CommitLogSegmentsTable.CONTENT_SIZE,
                                       CommitLogSegmentsTable.IS_STILL_ALLOCATING,
                                       CommitLogSegmentsTable.CDC_STATE));
        int numSegments = 0;
        for (Row r : segments1)
        {
            assertThat(r.getString(CommitLogSegmentsTable.segment_path)).satisfies(p -> Files.exists(Paths.get(p)));
            assertThat(r.getLong(CommitLogSegmentsTable.ON_DISK_SIZE)).isGreaterThan(0L);
            assertThat(r.getLong(CommitLogSegmentsTable.CONTENT_SIZE)).isGreaterThan(0L);
            assertThat(r.getInt(CommitLogSegmentsTable.NUM_DIRTY_TABLES)).isGreaterThan(0);
            assertThat(r.getString(CommitLogSegmentsTable.CDC_STATE)).isEqualTo(CommitLogSegment.CDCState.PERMITTED.name());
            numSegments++;
        }
        assertThat(numSegments).isGreaterThanOrEqualTo(4); // wrote 2 x 4 512Kb+ rows with max commitlog size 1Mb
    }
}
