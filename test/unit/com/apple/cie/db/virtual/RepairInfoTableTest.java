/*
 * Copyright (c) 2018-2022 Apple, Inc. All rights reserved.
 */

package com.apple.cie.db.virtual;

import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;

import static org.apache.cassandra.db.ColumnFamilyStoreTest.CF_STANDARD1;
import static org.apache.cassandra.db.ColumnFamilyStoreTest.CF_STANDARD2;
import static org.apache.cassandra.db.ColumnFamilyStoreTest.KEYSPACE1;
import static org.apache.cassandra.db.ColumnFamilyStoreTest.KEYSPACE2;
import static org.assertj.core.api.Assertions.assertThat;

public class RepairInfoTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        RepairInfoTable table = new RepairInfoTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @After
    public void clearRepairedRanges()
    {
        SystemKeyspace.clearRepairedRanges(KEYSPACE1, CF_STANDARD1);
        SystemKeyspace.clearRepairedRanges(KEYSPACE1, CF_STANDARD2);
        SystemKeyspace.clearRepairedRanges(KEYSPACE2, CF_STANDARD2);
    }

    @Test
    public void testSelectAllWhenRepairInfoIsEmpty() throws Throwable
    {
        assertEmpty(execute("SELECT * FROM vts.xmas_repair_info"));
    }

    @Test
    public void testSingleKeyspaceSingleCFSingleRange() throws Throwable
    {
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(0),
                                                              new Murmur3Partitioner.LongToken(100)),
                                                  5_000L);
        UntypedResultSet resultSet = execute("SELECT * FROM vts.xmas_repair_info");

        assertThat(resultSet.size()).isEqualTo(1);

        UntypedResultSet.Row row = resultSet.one();
        assertThat(row.getColumns().size()).isEqualTo(5);

        assertThat(row.getString("keyspace_name")).isEqualTo(KEYSPACE1);
        assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD1);
        assertThat(row.getString("start_token")).isEqualTo("0");
        assertThat(row.getString("end_token")).isEqualTo("100");
        assertThat(row.getLong("last_repair_time")).isEqualTo(5_000L);
    }

    @Test
    public void testSingleKeyspaceSingleCFSingleRangeWithFilter() throws Throwable
    {
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(0),
                                                              new Murmur3Partitioner.LongToken(100)),
                                                  5_000L);
        UntypedResultSet resultSet = execute("SELECT * FROM vts.xmas_repair_info WHERE keyspace_name = ? AND table_name = ?", KEYSPACE1, CF_STANDARD1);

        assertThat(resultSet.size()).isEqualTo(1);

        UntypedResultSet.Row row = resultSet.one();
        assertThat(row.getColumns().size()).isEqualTo(5);

        assertThat(row.getString("keyspace_name")).isEqualTo(KEYSPACE1);
        assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD1);
        assertThat(row.getString("start_token")).isEqualTo("0");
        assertThat(row.getString("end_token")).isEqualTo("100");
        assertThat(row.getLong("last_repair_time")).isEqualTo(5_000L);
    }

    @Test
    public void testSingleKeyspaceMultipleCF() throws Throwable
    {
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(0),
                                                              new Murmur3Partitioner.LongToken(100)),
                                                  5_000L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD2,
                                                  new Range<>(new Murmur3Partitioner.LongToken(101),
                                                              new Murmur3Partitioner.LongToken(200)),
                                                  10_000L);
        UntypedResultSet resultSet = execute("SELECT * FROM vts.xmas_repair_info");
        assertThat(resultSet.size()).isEqualTo(2);

        int i = 0;
        Iterator<UntypedResultSet.Row> iterator = resultSet.iterator();
        while (iterator.hasNext())
        {
            UntypedResultSet.Row row = iterator.next();
            assertThat(row.getColumns().size()).isEqualTo(5);

            assertThat(row.getString("keyspace_name")).isEqualTo(KEYSPACE1);

            if (i == 0)
            {
                assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD1);
                assertThat(row.getString("start_token")).isEqualTo("0");
                assertThat(row.getString("end_token")).isEqualTo("100");
                assertThat(row.getLong("last_repair_time")).isEqualTo(5_000L);
            }
            else if (i == 1)
            {
                assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD2);
                assertThat(row.getString("start_token")).isEqualTo("101");
                assertThat(row.getString("end_token")).isEqualTo("200");
                assertThat(row.getLong("last_repair_time")).isEqualTo(10_000L);
            }
            i++;
        }
    }

    @Test
    public void testSingleKeyspaceMultipleCFWithFilter() throws Throwable
    {
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(0),
                                                              new Murmur3Partitioner.LongToken(100)),
                                                  5_000L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD2,
                                                  new Range<>(new Murmur3Partitioner.LongToken(101),
                                                              new Murmur3Partitioner.LongToken(200)),
                                                  10_000L);
        UntypedResultSet resultSet = execute("SELECT * FROM vts.xmas_repair_info WHERE keyspace_name = ? AND table_name = ?", KEYSPACE1, CF_STANDARD1);

        assertThat(resultSet.size()).isEqualTo(1);

        UntypedResultSet.Row row = resultSet.one();
        assertThat(row.getColumns().size()).isEqualTo(5);

        assertThat(row.getString("keyspace_name")).isEqualTo(KEYSPACE1);
        assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD1);
        assertThat(row.getString("start_token")).isEqualTo("0");
        assertThat(row.getString("end_token")).isEqualTo("100");
        assertThat(row.getLong("last_repair_time")).isEqualTo(5_000L);

        resultSet = execute("SELECT * FROM vts.xmas_repair_info WHERE keyspace_name = ? AND table_name = ?", KEYSPACE1, CF_STANDARD2);

        assertThat(resultSet.size()).isEqualTo(1);

        row = resultSet.one();
        assertThat(row.getColumns().size()).isEqualTo(5);

        assertThat(row.getString("keyspace_name")).isEqualTo(KEYSPACE1);
        assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD2);
        assertThat(row.getString("start_token")).isEqualTo("101");
        assertThat(row.getString("end_token")).isEqualTo("200");
        assertThat(row.getLong("last_repair_time")).isEqualTo(10_000L);
    }

    @Test
    public void testMultipleKeyspaces() throws Throwable
    {
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(0),
                                                              new Murmur3Partitioner.LongToken(100)),
                                                  5_000L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE2, CF_STANDARD2,
                                                  new Range<>(new Murmur3Partitioner.LongToken(0),
                                                              new Murmur3Partitioner.LongToken(100)),
                                                  10_000L);
        UntypedResultSet resultSet = execute("SELECT * FROM vts.xmas_repair_info");
        assertThat(resultSet.size()).isEqualTo(2);

        int i = 0;
        Iterator<UntypedResultSet.Row> iterator = resultSet.iterator();
        while (iterator.hasNext())
        {
            UntypedResultSet.Row row = iterator.next();
            assertThat(row.getColumns().size()).isEqualTo(5);

            if (i == 0)
            {
                assertThat(row.getString("keyspace_name")).isEqualTo(KEYSPACE1);
                assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD1);
                assertThat(row.getString("start_token")).isEqualTo("0");
                assertThat(row.getString("end_token")).isEqualTo("100");
                assertThat(row.getLong("last_repair_time")).isEqualTo(5_000L);
            }
            else if (i == 1)
            {
                assertThat(row.getString("keyspace_name")).isEqualTo(KEYSPACE2);
                assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD2);
                assertThat(row.getString("start_token")).isEqualTo("0");
                assertThat(row.getString("end_token")).isEqualTo("100");
                assertThat(row.getLong("last_repair_time")).isEqualTo(10_000L);
            }
            i++;
        }
    }

    @Test
    public void testTokenRangeWrapAround() throws Throwable
    {
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(0),
                                                              new Murmur3Partitioner.LongToken(100)),
                                                  5_000L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(101),
                                                              new Murmur3Partitioner.LongToken(200)),
                                                  10_000L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(201),
                                                              new Murmur3Partitioner.LongToken(300)),
                                                  15_000L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(301),
                                                              new Murmur3Partitioner.LongToken(-1)),
                                                  20_000L);
        UntypedResultSet resultSet = execute("SELECT * FROM vts.xmas_repair_info");
        assertThat(resultSet.size()).isEqualTo(5);

        int i = 0;
        Iterator<UntypedResultSet.Row> iterator = resultSet.iterator();
        while (iterator.hasNext())
        {
            UntypedResultSet.Row row = iterator.next();
            assertThat(row.getColumns().size()).isEqualTo(5);

            assertThat(row.getString("keyspace_name")).isEqualTo(KEYSPACE1);
            assertThat(row.getString("table_name")).isEqualTo(CF_STANDARD1);

            if (i == 0)
            {
                assertThat(row.getString("start_token")).isEqualTo("-9223372036854775808"); // min token
                assertThat(row.getString("end_token")).isEqualTo("-1");
                assertThat(row.getLong("last_repair_time")).isEqualTo(20_000L);
            }
            else if (i == 1)
            {
                assertThat(row.getString("start_token")).isEqualTo("0");
                assertThat(row.getString("end_token")).isEqualTo("100");
                assertThat(row.getLong("last_repair_time")).isEqualTo(5_000L);
            }
            else if (i == 2)
            {
                assertThat(row.getString("start_token")).isEqualTo("101");
                assertThat(row.getString("end_token")).isEqualTo("200");
                assertThat(row.getLong("last_repair_time")).isEqualTo(10_000L);
            }
            else if (i == 3)
            {
                assertThat(row.getString("start_token")).isEqualTo("201");
                assertThat(row.getString("end_token")).isEqualTo("300");
                assertThat(row.getLong("last_repair_time")).isEqualTo(15_000L);
            }
            else if (i == 4)
            {
                assertThat(row.getString("start_token")).isEqualTo("301");
                assertThat(row.getString("end_token")).isEqualTo("9223372036854775807");
                assertThat(row.getLong("last_repair_time")).isEqualTo(20_000L);
            }
            i++;
        }
    }

    @Test
    public void testRangeContains() throws Throwable
    {
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(-9223372036854775808L),
                                                              new Murmur3Partitioner.LongToken(-4611686018427387908L)),
                                                  1655316229290L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(-4611686018427387908L),
                                                              new Murmur3Partitioner.LongToken(-4304240283865562048L)),
                                                  1655323091292L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(-4304240283865562048L),
                                                              new Murmur3Partitioner.LongToken(-3996794549303736188L)),
                                                  1655344755637L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(-3996794549303736188L),
                                                              new Murmur3Partitioner.LongToken(1537228672809129297L)),
                                                  1655316229294L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(1537228672809129297L),
                                                              new Murmur3Partitioner.LongToken(1844674407370955157L)),
                                                  1655330291081L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(1844674407370955157L),
                                                              new Murmur3Partitioner.LongToken(7993589098607472362L)),
                                                  1655316229296L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(7993589098607472362L),
                                                              new Murmur3Partitioner.LongToken(8301034833169298222L)),
                                                  1655337491549L);
        SystemKeyspace.updateLastSuccessfulRepair(KEYSPACE1, CF_STANDARD1,
                                                  new Range<>(new Murmur3Partitioner.LongToken(8301034833169298222L),
                                                              new Murmur3Partitioner.LongToken(Murmur3Partitioner.MAXIMUM)),
                                                  1655316229293L);

        // This is the equivalent of the curl request for the /repair/lastRepairSuccess endpoint
        // with the following payload '{"keyspace": "ks", "tokens": [["cf", "0"]]}'
        //   bash-4.2# curl -s -k --cert client_keystore -X POST -d '{"keyspace": "ks", "tokens": [["cf", "0"]]}' https://lo11-ztba010101.local.local.apple.com:8010/repair/lastRepairSuccess
        //   [ 1655316229294 ]
        UntypedResultSet resultSet = execute("SELECT max(last_repair_time) AS most_recent_repair FROM vts.xmas_repair_info WHERE keyspace_name = ? AND table_name = ? AND start_token < ? AND end_token >= ? ALLOW FILTERING",
                                             KEYSPACE1, CF_STANDARD1, "0", "0");

        assertThat(resultSet.size()).isEqualTo(1);
        UntypedResultSet.Row row = resultSet.one();
        assertThat(row.getColumns().size()).isEqualTo(1);
        assertThat(row.getLong("most_recent_repair")).isEqualTo(1655316229294L);
    }
}
