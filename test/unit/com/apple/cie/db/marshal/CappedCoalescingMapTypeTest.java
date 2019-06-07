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

package com.apple.cie.db.marshal;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.apple.cie.cql3.functions.CappedSortedMap;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class CappedCoalescingMapTypeTest extends CQLTester
{
    static ByteBuffer makeCoalescingKey(int key)
    {
        return  ByteBufferUtil.bytes(key); // just make a blob out of the text representation of the int
    }

    static private UUID makeMessageId(int msgIdx)
    {
        final long start = 0L;
        return UUIDs.startOf(start + msgIdx);
    }

    static ByteBuffer makeCoalescingKeyAndValue(int key, int value)
    {
        return TupleType.buildValue(new ByteBuffer[] {makeCoalescingKey(key), Int32Type.instance.decompose(value) });
    }

    @Test
    public void basicFunctionality() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, ccm 'com.apple.cie.db.marshal.CappedCoalescingMapType(Int32Type)', PRIMARY KEY (pk, ck))");

        assertRows(execute(String.format("SELECT type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s' AND column_name = 'ccm'", KEYSPACE, currentTable())),
                   row("'com.apple.cie.db.marshal.CappedCoalescingMapType(org.apache.cassandra.db.marshal.Int32Type)'"));

        int pk = 1;
        int ck = 1;
        final UUID cap = CappedSortedMap.withCap(3);
        for (int msgIdx = 1; msgIdx <= 5; msgIdx++)
        {
            UUID messageId = makeMessageId(msgIdx);
            ByteBuffer coalescingKey = makeCoalescingKey(msgIdx % 2);
            execute("UPDATE %s SET ccm[?] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                    cap, messageId, coalescingKey, msgIdx, pk, ck);
        }

        assertRows(execute("SELECT pk, ck, ccm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(4), makeCoalescingKeyAndValue(0, 4), // coalescing key 0, most recent is 4
                       makeMessageId(5), makeCoalescingKeyAndValue(1, 5)) // coalescing key 1, most recent is 5
                   ));
    }


    @Test
    public void varyCap() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, ccm 'com.apple.cie.db.marshal.CappedCoalescingMapType(Int32Type)', PRIMARY KEY (pk, ck))");

        assertRows(execute(String.format("SELECT type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s' AND column_name = 'ccm'", KEYSPACE, currentTable())),
                   row("'com.apple.cie.db.marshal.CappedCoalescingMapType(org.apache.cassandra.db.marshal.Int32Type)'"));

        int pk = 1;
        int ck = 1;
        final UUID cap1 = CappedSortedMap.withCap(1);
        final UUID cap2 = CappedSortedMap.withCap(2);
        final UUID cap4 = CappedSortedMap.withCap(4);

        // Write three messages with cap=2
        int msgIdx = 1;
        while (msgIdx <= 3)
        {
            execute("UPDATE %s SET ccm[?] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                    cap2, makeMessageId(msgIdx), makeCoalescingKey(msgIdx * 100), msgIdx, pk, ck);
            msgIdx++;
        }

        assertRows(execute("SELECT pk, ck, ccm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(3), makeCoalescingKeyAndValue(300, 3),
                       makeMessageId(2), makeCoalescingKeyAndValue(200, 2))
                   ));

        // Write one message with cap=1, should be the last
        while (msgIdx <= 4)
        {
            execute("UPDATE %s SET ccm[?] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                    cap1, makeMessageId(msgIdx), makeCoalescingKey(msgIdx * 100), msgIdx, pk, ck);
            msgIdx++;
        }

        assertRows(execute("SELECT pk, ck, ccm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(4), makeCoalescingKeyAndValue(400, 4))
                   ));

        // Increase back up to 4 and see what we get
        while (msgIdx <= 5)
        {
            execute("UPDATE %s SET ccm[?] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                    cap4, makeMessageId(msgIdx), makeCoalescingKey(msgIdx * 100), msgIdx, pk, ck);
            msgIdx++;
        }

        // As the ccm is (probably) still in the memtable, the items pushed out of the end
        // of the ccm have been dropped before making it to an sstable, so increasing the
        // cap will only return the existing values.
        assertRows(execute("SELECT pk, ck, ccm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(4), makeCoalescingKeyAndValue(400, 4),
                       makeMessageId(5), makeCoalescingKeyAndValue(500, 5))
                   ));

        // Write 3 more to push msgIdx 4 out
        while (msgIdx <= 8)
        {
            execute("UPDATE %s SET ccm[?] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                    cap4, makeMessageId(msgIdx), makeCoalescingKey(msgIdx * 100), msgIdx, pk, ck);
            msgIdx++;
        }
        assertRows(execute("SELECT pk, ck, ccm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(5), makeCoalescingKeyAndValue(500, 5),
                       makeMessageId(6), makeCoalescingKeyAndValue(600, 6),
                       makeMessageId(7), makeCoalescingKeyAndValue(700, 7),
                       makeMessageId(8), makeCoalescingKeyAndValue(800, 8))
                   ));
    }

    @Test
    public void adjustHotProperties() throws Throwable
    {
        int defaultCap = CappedCoalescingMapCellsResolver.instance.getDefaultCap();
        int minEffectiveCap = CappedCoalescingMapCellsResolver.instance.getMinEffectiveCap();
        int maxEffectiveCap = CappedCoalescingMapCellsResolver.instance.getMaxEffectiveCap();

        try
        {
            // Check the minimum effective cap can only be set in bounds
            CappedCoalescingMapCellsResolver.instance.setMinEffectiveCap(-1);
            assertEquals(0, CappedCoalescingMapCellsResolver.instance.getMinEffectiveCap());
            CappedCoalescingMapCellsResolver.instance.setMinEffectiveCap(CappedSortedMap.CAP_MAX + 1);
            assertEquals(CappedSortedMap.CAP_MAX, CappedCoalescingMapCellsResolver.instance.getMinEffectiveCap());

            // Check the Maximum effective cap can only be set in bounds
            // and that the minimum drops to be less than or equal the max
            CappedCoalescingMapCellsResolver.instance.setMaxEffectiveCap(CappedSortedMap.CAP_MAX + 1);
            assertEquals(CappedSortedMap.CAP_MAX, CappedCoalescingMapCellsResolver.instance.getMaxEffectiveCap());
            assertEquals(CappedSortedMap.CAP_MAX, CappedCoalescingMapCellsResolver.instance.getMinEffectiveCap());

            CappedCoalescingMapCellsResolver.instance.setMaxEffectiveCap(-1);
            assertEquals(0, CappedCoalescingMapCellsResolver.instance.getMaxEffectiveCap());
            assertEquals(0, CappedCoalescingMapCellsResolver.instance.getMinEffectiveCap());

            CappedCoalescingMapCellsResolver.instance.setMinEffectiveCap(0);
            CappedCoalescingMapCellsResolver.instance.setMaxEffectiveCap(CappedSortedMap.CAP_MAX);

            createTable("CREATE TABLE %s (pk int, ck int, " +
                        "ccm 'com.apple.cie.db.marshal.CappedCoalescingMapType(Int32Type)', PRIMARY KEY (pk, ck))" +
                        " WITH gc_grace_seconds = 0");

            // Reduce the default cap to 2
            CappedCoalescingMapCellsResolver.instance.setDefaultCap(2);
            assertEquals(2, CappedCoalescingMapCellsResolver.instance.getDefaultCap());

            // Write three entries to a ccm limited to two
            final int pk = 1;
            final int ck = 1;
            for (int msgIdx = 1; msgIdx <= 3; msgIdx++)
            {
                execute("UPDATE %s SET ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                        makeMessageId(msgIdx), makeCoalescingKey(msgIdx * 100), msgIdx, pk, ck);
            }
            assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), makeCoalescingKeyAndValue(200, 2),
                               makeMessageId(3), makeCoalescingKeyAndValue(300, 3))));

            assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), makeCoalescingKeyAndValue(200, 2),
                               makeMessageId(3), makeCoalescingKeyAndValue(300, 3))));

            // Lower the effective limit to 1 and check result is limited.
            flush(); // otherwise CCM served out of Memtable
            CappedCoalescingMapCellsResolver.instance.setMaxEffectiveCap(1);
            assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(3), makeCoalescingKeyAndValue(300, 3))));

            // Raise again and see the default limit is back in place
            CappedCoalescingMapCellsResolver.instance.setMaxEffectiveCap(CappedSortedMap.CAP_MAX);

            assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), makeCoalescingKeyAndValue(200, 2),
                               makeMessageId(3), makeCoalescingKeyAndValue(300, 3))));
        }
        finally
        {
            CappedCoalescingMapCellsResolver.instance.setDefaultCap(defaultCap);
            CappedCoalescingMapCellsResolver.instance.setMinEffectiveCap(minEffectiveCap);
            CappedCoalescingMapCellsResolver.instance.setMaxEffectiveCap(maxEffectiveCap);
        }
    }

    @Test
    public void disableCellsResolver() throws Throwable
    {
        try
        {
            CappedCoalescingMapCellsResolver.instance.setCellResolverEnabled(true);
            createTable("CREATE TABLE %s (pk int, ck int, " +
                        "ccm 'com.apple.cie.db.marshal.CappedCoalescingMapType(Int32Type)', PRIMARY KEY (pk, ck))" +
                        " WITH gc_grace_seconds = 0");

            final int pk = 1;
            final int ck = 1;
            final int cap = 2;

            // Write three entries to a ccm limited to two
            for (int msgIdx = 1; msgIdx <= 3; msgIdx++)
            {
                execute("UPDATE %s SET ccm[cap(?)] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                        cap, makeMessageId(msgIdx), makeCoalescingKey(msgIdx * 100), msgIdx, pk, ck);
            }
            assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), makeCoalescingKeyAndValue(200, 2),
                               makeMessageId(3), makeCoalescingKeyAndValue(300, 3))));

            CappedCoalescingMapCellsResolver.instance.setCellResolverEnabled(false);

            // Write three more entries to a ccm limited to two
            for (int msgIdx = 4; msgIdx <= 6; msgIdx++)
            {
                execute("UPDATE %s SET ccm[cap(?)] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                        cap, makeMessageId(msgIdx), makeCoalescingKey(msgIdx * 100), msgIdx, pk, ck);
            }
            assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), makeCoalescingKeyAndValue(200, 2),
                               makeMessageId(3), makeCoalescingKeyAndValue(300, 3),
                               makeMessageId(4), makeCoalescingKeyAndValue(400, 4),
                               makeMessageId(5), makeCoalescingKeyAndValue(500, 5),
                               makeMessageId(6), makeCoalescingKeyAndValue(600, 6))));

            // Reenable and check previously set cap respected
            CappedCoalescingMapCellsResolver.instance.setCellResolverEnabled(true);

            for (int msgIdx = 7; msgIdx <=7; msgIdx++)
            {
                execute("UPDATE %s SET ccm[cap(?)] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ?",
                        cap, makeMessageId(msgIdx), makeCoalescingKey(msgIdx * 100), msgIdx, pk, ck);
            }
            assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(6), makeCoalescingKeyAndValue(600, 6),
                               makeMessageId(7), makeCoalescingKeyAndValue(700, 7))));
        }
        finally
        {
            CappedCoalescingMapCellsResolver.instance.setCellResolverEnabled(true);
        }
    }

    @Test
    public void emptyPartitionExpirationNonPurged() throws Throwable
    {
        int pk = 1;
        int ck = 1;

        emptyPartitionExpirationSetup(pk, ck);
        Thread.sleep(6_000); // expire but don't purge tombstone
        assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck));
        compact();
        Assert.assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        compact();
        Assert.assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    @Test
    public void emptyPartitionExpirationPurged() throws Throwable
    {
        int pk = 1;
        int ck = 1;

        emptyPartitionExpirationSetup(pk, ck);
        Thread.sleep(11_000); // 5 seconds of TTL + 5 seconds of GC Grace
        assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck));
        compact();
        Assert.assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
        compact();
        Assert.assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    public void emptyPartitionExpirationSetup(int pk, int ck) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, ccm 'com.apple.cie.db.marshal.CappedCoalescingMapType(Int32Type)', PRIMARY KEY (pk, ck))" +
                    " WITH gc_grace_seconds = 5");

        final UUID cap = CappedSortedMap.withCap(3);
        for (int msgIdx = 1; msgIdx <= 5; msgIdx++)
        {
            UUID messageId = makeMessageId(msgIdx);
            ByteBuffer coalescingKey = makeCoalescingKey(msgIdx % 2);
            execute("UPDATE %s USING TTL 5 SET ccm[?] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ? ",
                    cap, messageId, coalescingKey, msgIdx, pk, ck);
            flush();
        }
    }

    @Test
    public void mixedExpiredAndAliveRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, ccm 'com.apple.cie.db.marshal.CappedCoalescingMapType(Int32Type)', " +
                    "PRIMARY KEY (pk, ck))");

        int pk = 1;
        int ck = 1;
        final UUID cap = CappedSortedMap.withCap(3);

        write(pk, ck, cap, 1, 1, 5);
        flush();
        write(pk, ck, cap, 2, 0, 10);
        flush();
        Thread.sleep(6_000);
        compact();

        assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                   row(map(makeMessageId(2), makeCoalescingKeyAndValue(0, 2))));

        compact();
        Assert.assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    @Test
    public void mixedTtlAndAliveRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, ccm 'com.apple.cie.db.marshal.CappedCoalescingMapType(Int32Type)', " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 5");

        int pk = 1;
        int ck = 1;
        final UUID cap = CappedSortedMap.withCap(3);

        // Write one non-expiring cell
        write(pk, ck, cap, 1, 1, 0);
        flush();
        write(pk, ck, cap, 2, 2, 5);
        write(pk, ck, cap, 3, 3, 5);
        write(pk, ck, cap, 4, 4, 5);
        flush();
        compact();

        // Non-expiring cell is kicked out by the later writes
        assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                   row(map(makeMessageId(2), makeCoalescingKeyAndValue(2, 2),
                           makeMessageId(3), makeCoalescingKeyAndValue(3, 3),
                           makeMessageId(4), makeCoalescingKeyAndValue(4, 4))));

        // After compaction, we have one sstable, where cap cell has MAX_DELETION_TIME ldt
        Assert.assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        Thread.sleep(11_000);
        compact();

        // All cells are now expired
        assertRows(execute("SELECT ccm FROM %s WHERE pk = ? AND ck = ?", pk, ck));

        // Now, cell is compacted away
        Assert.assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }


    public void write(int pk, int ck, UUID cap, int msgIdx, int msgCoalescingIdx, int ttl) throws Throwable
    {
        UUID messageId = makeMessageId(msgIdx);
        ByteBuffer coalescingKey = makeCoalescingKey(msgCoalescingIdx);
        if (ttl > 0)
        {
            execute("UPDATE %s USING TTL ? SET ccm[?] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ? ",
                    ttl, cap, messageId, coalescingKey, msgIdx, pk, ck);
        }
        else
        {
            execute("UPDATE %s SET ccm[?] = null, ccm[?] = (?, ?) WHERE pk = ? AND ck = ? ",
                    cap, messageId, coalescingKey, msgIdx, pk, ck);

        }
    }
}
