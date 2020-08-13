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

package org.apache.cassandra.config;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.clustering;
import static org.junit.Assert.*;

public class RepairedDataTrackingExclusionsTest
{
    // Light(er)weight stand-in for CFMetadata (so we don't need to configure column metadata etc)
    private static class TableInfo
    {
        final String ks;
        final String table;
        final UUID cfid;
        final ClusteringComparator comparator;
        final CFMetaData metadata;
        TableInfo(String ks, String table, ClusteringComparator comparator)
        {
            this.ks = ks;
            this.table = table;
            this.cfid = UUID.randomUUID();
            this.comparator = comparator;
            this.metadata = CFMetaData.Builder.create(ks, table)
                                              .withId(cfid)
                                              .addPartitionKey("key", BytesType.instance)
                                              .build();
        }
    }

    private final TableInfo t1 = new TableInfo("ks1", "t1", new ClusteringComparator(UTF8Type.instance));
    private final TableInfo t2 = new TableInfo("ks1", "t2", new ClusteringComparator(UTF8Type.instance,
                                                                                     UTF8Type.instance));
    private final TableInfo t3 = new TableInfo("ks1", "t3", new ClusteringComparator(Int32Type.instance));
    private final TableInfo t4 = new TableInfo("ks1", "t4", new ClusteringComparator(BytesType.instance));
    private final TableInfo t5 = new TableInfo("ks1", "t5", new ClusteringComparator(BytesType.instance,
                                                                                     BytesType.instance));
    private final TableInfo t6 = new TableInfo("ks1", "t6", new ClusteringComparator(BytesType.instance,
                                                                                     UTF8Type.instance,
                                                                                     Int32Type.instance));
    private final TableInfo t99 = new TableInfo("ks1", "t99", new ClusteringComparator(UTF8Type.instance));
    private final TableInfo t55 = new TableInfo("ks3", "t55", new ClusteringComparator(UTF8Type.instance));
    private final TableInfo t66 = new TableInfo("ks4", "t66", new ClusteringComparator(UTF8Type.instance));

    private RepairedDataTrackingExclusions exclusions;

    @Before
    public void setup()
    {
        String conf = "ks1:t1:6161," +             // text type [aa, efgh]
                      "ks1:t1:65666768," +
                      "ks1:t2:6161," +             // (text, text) type [aa, efgh]
                      "ks1:t2:65666768," +
                      "ks1:t3:0012d687," +         // (int) [1234567, 99999, (any int < 65536)]
                      "ks1:t3:0001869f," +
                      "ks1:t3:0000," +
                      "ks1:t4:02," +               // (bytes) [0x02, 0x05, 0xDEADBEEF]
                      "ks1:t4:05," +
                      "ks1:t4:DEADBEEF," +
                      "ks1:t5:02," +               // (bytes, bytes) [0x02, 0x050505, 0xDEADBEEF]
                      "ks1:t5:050505," +
                      "ks1:t5:DEADBEEF," +
                      "ks1:t6:0261610001869f," +   // Prefixes: [0x02:aa:99999, 0x04AF34, 0x08070605:foo:0000] (0000 == any int < 65536)
                      "ks1:t6:04AF34," +
                      "ks1:t6:0807666F6F0000," +
                      "ks1:t99," +                 // all rows excluded
                      "ks3";                       // all tables/rows excluded
        exclusions = RepairedDataTrackingExclusions.fromConfig(conf);

        DatabaseDescriptor.setRepairedDataTrackingExclusionsEnabled(true);
    }

    @Test
    public void textClusteringWithSingleComponent()
    {
        // Clustering type: (text). Prefixes: ['aa', 'efgh']
        TableInfo t = t1;
        RepairedDataExclusion exclusion = exclusions.getExclusion(t.ks, t.table);
        assertTrue(exclusion instanceof RepairedDataExclusion.ExcludeSome);

        // exclusions are based on clustering
        assertFalse(exclusion.excludePartition(partition(t)));

        // clustering shorter than any prefix
        assertFalse(exclusion.excludeRow(row(t.comparator, "foo")));

        // clustering longer than any prefix, but none matching
        assertFalse(exclusion.excludeRow(row(t.comparator, "foooooooooooo")));

        // clustering exact match for one prefix
        assertTrue(exclusion.excludeRow(row(t.comparator, "efgh")));

        // clustering prefix match for one prefix
        assertTrue(exclusion.excludeRow(row(t.comparator, "efghijklmno")));

        // no match
        assertFalse(exclusion.excludeRow(row(t.comparator, "zzzzzzzzzz")));
    }

    @Test
    public void textClusteringWithMultipleComponents()
    {
        // Clustering type: (text,text). Prefixes: ['aa', 'efgh']
        TableInfo t = t2;
        RepairedDataExclusion exclusion = exclusions.getExclusion(t.ks, t.table);
        assertTrue(exclusion instanceof RepairedDataExclusion.ExcludeSome);

        // exclusions are based on clustering
        assertFalse(exclusion.excludePartition(partition(t)));

        // clustering shorter than any prefix
        assertFalse(exclusion.excludeRow(row(t.comparator, "a", "b")));

        // clustering longer than any prefix, but none matching
        assertFalse(exclusion.excludeRow(row(t.comparator, "ab", "ccc")));

        // clustering exact match for one prefix
        assertTrue(exclusion.excludeRow(row(t.comparator, "ef", "gh")));

        // clustering prefix match for one prefix
        assertTrue(exclusion.excludeRow(row(t.comparator, "efg", "hijklmno")));

        // no match
        assertFalse(exclusion.excludeRow(row(t.comparator, "zz", "zzzzzzzz")));
    }

    @Test
    public void intClusteringWithSingleComponent()
    {
        // ClusteringType (int). Prefixes: [1234567, 99999, (any int < 65536)]

        // For numeric types, the conversion to hex and comparison is less obvious than with text/bytes
        // For example, an int value 1 has 4 byte binary representation 00000000 00000000 00000000 00000001
        // which in hex is 00000001
        // So a prefix specified as a hex string 00000001 will match an int value 1, but also a long
        // value where the most significant 4 bytes == 00000000 00000000 00000000 00000001
        // As we compare lexicographically, a short hex-formatted numeric prefix will match many more
        // potential values than string or plain byte types.
        // For example, a prefix 0000 will match any int < 65536 (which in hex is 00010000)
        TableInfo t = t3;
        RepairedDataExclusion exclusion = exclusions.getExclusion(t.ks, t.table);
        assertTrue(exclusion instanceof RepairedDataExclusion.ExcludeSome);

        // exclusions are based on clustering
        assertFalse(exclusion.excludePartition(partition(t)));

        // no direct match and > than the prefix '0000' (anything < 65536)
        assertFalse(exclusion.excludeRow(row(t.comparator, 999999)));

        // clustering exact match for one prefix
        assertTrue(exclusion.excludeRow(row(t.comparator, 1234567)));

        // matches the prefix '0000' - anything < 65536
        assertTrue(exclusion.excludeRow(row(t.comparator, 65535)));
        // doesn't matches the prefix '0000' (or any other)
        assertFalse(exclusion.excludeRow(row(t.comparator, 65536)));
    }

    @Test
    public void bytesClusteringWithSingleComponent()
    {
        // ClusteringType (bytes). Prefixes: [0x02, 0x05, 0xDEADBEEF]
        TableInfo t = t4;
        RepairedDataExclusion exclusion = exclusions.getExclusion(t.ks, t.table);
        assertTrue(exclusion instanceof RepairedDataExclusion.ExcludeSome);

        // exclusions are based on clustering
        assertFalse(exclusion.excludePartition(partition(t)));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x01))));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x08))));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x01, 0x08))));

        // matches the prefix '0x02'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0x02, 0x04, 0xFF))));

        // matches the prefix '0x05'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0x05, 0x05, 0x05))));

        // matches the prefix '0xDEADBEEF'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE))));
    }

    @Test
    public void bytesClusteringWithMultipleComponents()
    {
        // ClusteringType (bytes). Prefixes: [0x02, 0x050505, 0xDEADBEEF]
        TableInfo t = t5;
        RepairedDataExclusion exclusion = exclusions.getExclusion(t.ks, t.table);
        assertTrue(exclusion instanceof RepairedDataExclusion.ExcludeSome);

        // exclusions are based on clustering
        assertFalse(exclusion.excludePartition(partition(t)));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x01), bytes(0x02))));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x08), bytes(0x02))));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x01, 0x08), bytes(0x1A, 0x1A, 0xCE))));

        // matches the prefix '0x02'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0x02), bytes(0x04, 0xFF))));

        // matches the prefix '0x050505'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0x05, 0x05), bytes(0x05, 0x06))));

        // matches the prefix '0xDEADBEEF'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0xDE, 0xAD), bytes(0xBE, 0xEF, 0xCA, 0xFE))));
    }

    @Test
    public void multipleMixedClusteringTypes()
    {
        // This use case is pretty dubious. As we don't take the length of each component into account
        // we can only greedily match the prefixes. Equivalent to only being able to restrict a clustering
        // component in CQL if all the preceding components are restricted.
        // So for instance, given a clustering (bytes, text) the hex prefix 0x026161 would match rows with
        // all the following clusterings: (0x02:aa...), (0x0261, a...), (0x026161, ...), (0x026161..., ...)

        // ClusteringType (bytes, text, int).
        // Prefixes: [0x02:aa:99999, 0x04AF34, 0x0807:foo:0000] (0000 == any int < 65536)
        TableInfo t = t6;
        RepairedDataExclusion exclusion = exclusions.getExclusion(t.ks, t.table);
        assertTrue(exclusion instanceof RepairedDataExclusion.ExcludeSome);

        // exclusions are based on clustering
        assertFalse(exclusion.excludePartition(partition(t)));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x01), "xx", 999999)));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x08), "foo", 1)));

        // no direct match
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x01, 0x08), "abc", Integer.MAX_VALUE)));

        // matches the prefix '0x02:aa:99999'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0x02), "aa", 99999)));

        // also matches the prefix '0x02:aa:99999'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0x02, 0x61), "a", 99999)));

        // matches the prefix '0x04AF34'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0x04, 0xAF, 0x34, 0xFF), "somestring", 100)));

        // matches the prefix '0x08:foo:0000'
        assertTrue(exclusion.excludeRow(row(t.comparator, bytes(0x08, 0x07), "foo", 65535)));

        // doesn't match the prefix '0x08:foo:0000'
        assertFalse(exclusion.excludeRow(row(t.comparator, bytes(0x08, 0x07), "foo", 65536)));
    }

    @Test
    public void excludeAllRowsInTable()
    {
        RepairedDataExclusion exclusion = exclusions.getExclusion(t99.ks, t99.table);
        assertEquals(RepairedDataExclusion.ALL, exclusion);

        assertTrue(DatabaseDescriptor.getRepairedDataTrackingExclusionsEnabled());
        assertTrue(exclusion.excludePartition(partition(t99)));
        assertTrue(exclusion.excludeRow(row(t99.comparator, "foo")));

        DatabaseDescriptor.setRepairedDataTrackingExclusionsEnabled(false);
        assertFalse(exclusion.excludePartition(partition(t99)));
        assertFalse(exclusion.excludeRow(row(t99.comparator, "foo")));
    }

    @Test
    public void excludeAllRowsInKeyspace()
    {
        RepairedDataExclusion exclusion = exclusions.getExclusion(t55.ks, t55.table);
        assertEquals(RepairedDataExclusion.ALL, exclusion);

        assertTrue(DatabaseDescriptor.getRepairedDataTrackingExclusionsEnabled());
        assertTrue(exclusion.excludePartition(partition(t55)));
        assertTrue(exclusion.excludeRow(row(t55.comparator, "foo")));

        DatabaseDescriptor.setRepairedDataTrackingExclusionsEnabled(false);
        assertFalse(exclusion.excludePartition(partition(t55)));
        assertFalse(exclusion.excludeRow(row(t55.comparator, "foo")));
    }

    @Test
    public void excludeNoneIfNotConfigured()
    {
        RepairedDataExclusion exclusion = exclusions.getExclusion(t66.ks, t66.table);
        assertEquals(RepairedDataExclusion.NONE, exclusion);

        assertFalse(exclusion.excludePartition(partition(t66)));
        assertFalse(exclusion.excludeRow(row(t66.comparator, "foo")));
    }

    @Test
    public void emptyClustering()
    {
        // It doesn't make sense to specify exclusion prefixes for a table
        // without any clustering columns, so just verify that a
        // misconfiguration doesn't cause errors
        RepairedDataExclusion exclusion = exclusions.getExclusion(t1.ks, t1.table);
        assertTrue(exclusion instanceof RepairedDataExclusion.ExcludeSome);

        ClusteringComparator comparator = new ClusteringComparator();
        Row row = row(comparator);
        assertFalse(exclusion.excludeRow(row));

        // it's still valid to exclude at the keyspace or table level though
        exclusion = exclusions.getExclusion(t55.ks, t55.table);
        assertTrue(exclusion.excludeRow(row));
        exclusion = exclusions.getExclusion(t99.ks, t99.table);
        assertTrue(exclusion.excludeRow(row));
    }

    @Test
    public void testStaticClustering()
    {
        // Like an empty clustering, a static row should only be
        // excluded if the keyspace or table is entirely excluded
        RepairedDataExclusion exclusion = exclusions.getExclusion(t1.ks, t1.table);
        assertTrue(exclusion instanceof RepairedDataExclusion.ExcludeSome);

        ClusteringComparator comparator = new ClusteringComparator();
        Row row = staticRow();
        assertFalse(exclusion.excludeRow(row));

        // it's still valid to exclude at the keyspace or table level though
        exclusion = exclusions.getExclusion(t55.ks, t55.table);
        assertTrue(exclusion.excludeRow(row));
        exclusion = exclusions.getExclusion(t99.ks, t99.table);
        assertTrue(exclusion.excludeRow(row));
    }

    @Test(expected = ConfigurationException.class)
    public void testInvalidKeyspace()
    {
        RepairedDataTrackingExclusions.fromConfig("ks1,ks.abc:iii:ccc");
    }

    @Test(expected = ConfigurationException.class)
    public void testInvalidTable()
    {
        RepairedDataTrackingExclusions.fromConfig("ks1,ks:abc.iii:ccc");
    }

    @Test(expected = ConfigurationException.class)
    public void testInvalidRow()
    {
        RepairedDataTrackingExclusions.fromConfig("ks1,ks:abc:ccc");
    }


    private static ByteBuffer bytes(int...bytes)
    {
        ByteBuffer buf = ByteBuffer.allocate(bytes.length);
        for (int b : bytes)
            buf.put((byte)b);
        buf.flip();
        return buf;
    }

    private static UnfilteredRowIterator partition(TableInfo table)
    {
        return EmptyIterators.unfilteredRow(table.metadata, table.metadata.partitioner.decorateKey(bytes(0)), false);
    }

    private static Row row(ClusteringComparator comparator, Object...o)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
        builder.newRow(clustering(comparator, o));
        return builder.build();
    }

    private static Row staticRow()
    {
        Row.Builder builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
        builder.newRow(Clustering.STATIC_CLUSTERING);
        return builder.build();
    }
}
