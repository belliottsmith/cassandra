/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */

package com.apple.cie.db.marshal;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.apple.cie.cql3.functions.CappedSortedMap;
import com.datastax.driver.core.utils.UUIDs;
import junit.framework.Assert;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;

public class CappedSortedMapTypeTest extends CQLTester
{
    // abstract out mapElement representation as CQLTester support for
    // tuples/UDTs changes in future versions, this isolates the change.
    Object mapElement(int cap, int numRetries, Long timestamp, String message)
    {
        return userType((byte) cap, (byte) numRetries, timestamp, message);
    }

    Object mapElement(Object cap, int numRetries, Long timestamp, String message)
    {
        return userType(cap, (byte) numRetries, timestamp, message);
    }

    @Test
    public void basicFunctionality() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', PRIMARY KEY (pk, ck))");

        assertRows(execute(String.format("SELECT type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s' AND column_name = 'csm'", KEYSPACE, currentTable())),
                   row("'com.apple.cie.db.marshal.CappedSortedMapType(org.apache.cassandra.db.marshal.Int32Type)'"));

        int pk = 1;
        int ck = 1;
        long start = 0L; // Test in the 1970s!
        final UUID cap = CappedSortedMap.withCap(3);
        for (int msgIdx = 1; msgIdx <= 5; msgIdx++)
        {
            UUID messageId = UUIDs.startOf(start + msgIdx);
            execute("UPDATE %s SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                    cap, messageId, msgIdx, pk, ck);
        }

        assertRows(execute("SELECT pk, ck, csm FROM %s"),
                   row(pk, ck,
                       map(
                       UUIDs.startOf(start + 3), 3,
                       UUIDs.startOf(start + 4), 4,
                       UUIDs.startOf(start + 5), 5)
                   ));
    }

    static private UUID makeMessageId(int msgIdx)
    {
        final long start = 0L;
        return UUIDs.startOf(start + msgIdx);
    }

    @Test
    public void varyCap() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', PRIMARY KEY (pk, ck))");

        assertRows(execute(String.format("SELECT type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s' AND column_name = 'csm'", KEYSPACE, currentTable())),
                   row("'com.apple.cie.db.marshal.CappedSortedMapType(org.apache.cassandra.db.marshal.Int32Type)'"));

        int pk = 1;
        int ck = 1;
        final UUID cap1 = CappedSortedMap.withCap(1);
        final UUID cap2 = CappedSortedMap.withCap(2);
        final UUID cap4 = CappedSortedMap.withCap(4);

        // Write three messages with cap=2
        int msgIdx = 1;
        while (msgIdx <= 3)
        {
            execute("UPDATE %s SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                    cap2, makeMessageId(msgIdx), msgIdx, pk, ck);
            msgIdx++;
        }

        assertRows(execute("SELECT pk, ck, csm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(3), 3,
                       makeMessageId(2), 2)
                   ));

        // Write one message with cap=1, should be the last
        while (msgIdx <= 4)
        {
            execute("UPDATE %s SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                    cap1, makeMessageId(msgIdx), msgIdx, pk, ck);
            msgIdx++;
        }

        assertRows(execute("SELECT pk, ck, csm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(4), 4)
                   ));

        // Increase back up to 4 and see what we get
        while (msgIdx <= 5)
        {
            execute("UPDATE %s SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                    cap4, makeMessageId(msgIdx), msgIdx, pk, ck);
            msgIdx++;
        }

        // As the csm is (probably) still in the memtable, the items pushed out of the end
        // of the csm have been dropped before making it to an sstable, so increasing the
        // cap will only return the existing values.
        assertRows(execute("SELECT pk, ck, csm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(4), 4,
                       makeMessageId(5), 5)
                   ));

        // Write 3 more to push msgIdx 4 out
        while (msgIdx <= 8)
        {
            execute("UPDATE %s SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                    cap4, makeMessageId(msgIdx), msgIdx, pk, ck);
            msgIdx++;
        }
        assertRows(execute("SELECT pk, ck, csm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(5), 5,
                       makeMessageId(6), 6,
                       makeMessageId(7), 7,
                       makeMessageId(8), 8)
                   ));
    }

    void assertPartitionsEmpty()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        List<ImmutableBTreePartition> parts = Util.getAllUnfiltered(Util.cmd(cfs).build());
        Assert.assertTrue("all cap cells gone", parts.isEmpty());
    }

    void repairFlushAndDoubleCompact() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        Range<Token> fullRange = new Range<>(cfs.getPartitioner().getMinimumToken(), cfs.getPartitioner().getMinimumToken());
        cfs.updateLastSuccessfulRepair(fullRange,System.currentTimeMillis());

        Thread.sleep(1000); // the sleep hurts, but could not make test pass reliably by adjusting current millis

        cfs.forceBlockingFlush();
        cfs.forceMajorCompaction(); // first compaction trims the deleted csm element(s), leaving only the cap cell
        cfs.forceMajorCompaction(); // second compaction should remove the cap cell as the cell resolver will drop it.
    }

    @Test
    public void checkCapSurvivesCompactionWithGraceZero() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', PRIMARY KEY (pk, ck))" +
                    " WITH gc_grace_seconds = 0");
        flush();

        int pk = 1;
        int ck = 1;
        final UUID cap = CappedSortedMap.withCap(1);

        // Write three messages - cap=1 so only the last should be kept
        for (int msgIdx = 1; msgIdx <= 3; msgIdx++)
        {
            execute("UPDATE %s SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                    cap, makeMessageId(msgIdx), msgIdx, pk, ck);

            if (msgIdx == 2) // check with mix of memtables and sstables
                flush();
        }

        // Check only msgIdx 3 survives (in memtable)
        assertRows(execute("SELECT pk, ck, csm FROM %s"),
                   row(pk, ck,
                       map(
                       makeMessageId(3), 3)
                   ));

        // Compact and check again
        compact();
        assertRows(execute("SELECT pk, ck, csm FROM %s"),
                   row(pk, ck,
                       map(makeMessageId(3), 3)
                   ));

        // Flush last memtables, compact again and check
        flush();
        compact();
        assertRows(execute("SELECT pk, ck, csm FROM %s"),
                   row(pk, ck,
                       map(makeMessageId(3), 3)
                   ));

        // Mark msgIdx 3 deleted - should write tombstone on flush, expect cap cell and msgIdx3 tombstone
        execute("UPDATE %s SET csm[?] = null, csm[?] = null WHERE pk = ? AND ck = ?", cap, makeMessageId(3), pk, ck);
        flush();
        assertRows(execute("SELECT pk, ck, csm FROM %s WHERE pk = 1"));

        // Check that after two compactions all cells related to the csm have been removed.
        //
        // CellsResolvers execute before tombstone purging, so there is no way for it to know whether
        // to know if a cell is purged without replicating the logic in compaction.  Easier to wait for
        // a second compaction.
        //
        // The purger checks for local deletion time before the last repair time, so fake repairs so that
        // the tombstones are removed, and then check the cap cells are removed.
        repairFlushAndDoubleCompact();

        assertRows(execute("SELECT pk, ck, csm FROM %s WHERE pk = 1"));
        assertPartitionsEmpty();
    }


    @Test
    public void rangeTombstoneCleansUp() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', deliveries map<timeuuid,timestamp>, PRIMARY KEY (pk, ck))" +
                    " WITH gc_grace_seconds = 0");

        disableCompaction();

        final int pk = 1;
        final int ck = 1;
        final long start = 0L; // Test in the 1970s!
        final UUID cap = CappedSortedMap.withCap(3);
        for (int msgIdx = 1; msgIdx <= 3; msgIdx++)
        {
            execute("UPDATE %s SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                    cap, makeMessageId(msgIdx), msgIdx, pk, ck);

            if (msgIdx == 2) // check with mix of memtables and sstables
                flush();
        }
        flush();

        long afterWrite = Date.from(Instant.now()).getTime()*1000;

        // Simulate delivery of the first two
        Date deliverAt = Date.from(Instant.ofEpochMilli(afterWrite + 1));
        for (int msgIdx = 1; msgIdx <= 2; msgIdx++)
        {
            execute("UPDATE %s SET deliveries[?] = ? WHERE pk = ? AND ck = ?",
                    makeMessageId(msgIdx), deliverAt, pk, ck);

            if (msgIdx == 1) // check with mix of memtables and sstables
                flush();
        }

        long afterDelivery = Date.from(Instant.now()).getTime()*1000 + 1;
        assert(afterDelivery > afterWrite);

        // Simulate acknowledge of the first one
        for (int msgIdx = 1; msgIdx <= 1; msgIdx++)
        {
            UUID messageId = makeMessageId(msgIdx);
            execute("UPDATE %s SET csm[?] = null, csm[?] = null, deliveries[?] = null WHERE pk = ? AND ck = ?",
                    cap, messageId, messageId, pk, ck);
        }
        flush();

        long afterAck = Date.from(Instant.now()).getTime()*1000 + 2;
        assert(afterAck > afterDelivery);
        assertRows(execute("SELECT pk, ck, csm, deliveries FROM %s"),
                   row(1, 1,
                       map(makeMessageId(2), 2, makeMessageId(3), 3),
                       map(makeMessageId(2), deliverAt)));

        repairFlushAndDoubleCompact();

        // Delete after the write
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", afterWrite, pk, ck);
        repairFlushAndDoubleCompact();
        assertRows(execute("SELECT pk, ck, csm, deliveries FROM %s"),
                   row(1, 1,
                       null,
                       map(makeMessageId(2), deliverAt)));

        // Delete after the delivery
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", afterDelivery, pk, ck);
        repairFlushAndDoubleCompact();
        assertRows(execute("SELECT pk, ck, csm, deliveries FROM %s WHERE pk = ? AND ck = ?", pk, ck));

        // Delete after the acknowledge - everything should now be removed
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", afterAck, pk, ck);
        repairFlushAndDoubleCompact();

        assertPartitionsEmpty();
    }

    @Test
    public void adjustHotProperties() throws Throwable
    {
        int defaultCap = CappedSortedMapCellsResolver.instance.getDefaultCap();
        int minEffectiveCap = CappedSortedMapCellsResolver.instance.getMinEffectiveCap();
        int maxEffectiveCap = CappedSortedMapCellsResolver.instance.getMaxEffectiveCap();

        try
        {
            // Check the minimum effective cap can only be set in bounds
            CappedSortedMapCellsResolver.instance.setMinEffectiveCap(-1);
            assertEquals(0, CappedSortedMapCellsResolver.instance.getMinEffectiveCap());
            CappedSortedMapCellsResolver.instance.setMinEffectiveCap(CappedSortedMap.CAP_MAX + 1);
            assertEquals(CappedSortedMap.CAP_MAX, CappedSortedMapCellsResolver.instance.getMinEffectiveCap());

            // Check the Maximum effective cap can only be set in bounds
            // and that the minimum drops to be less than or equal the max
            CappedSortedMapCellsResolver.instance.setMaxEffectiveCap(CappedSortedMap.CAP_MAX + 1);
            assertEquals(CappedSortedMap.CAP_MAX, CappedSortedMapCellsResolver.instance.getMaxEffectiveCap());
            assertEquals(CappedSortedMap.CAP_MAX, CappedSortedMapCellsResolver.instance.getMinEffectiveCap());

            CappedSortedMapCellsResolver.instance.setMaxEffectiveCap(-1);
            assertEquals(0, CappedSortedMapCellsResolver.instance.getMaxEffectiveCap());
            assertEquals(0, CappedSortedMapCellsResolver.instance.getMinEffectiveCap());

            CappedSortedMapCellsResolver.instance.setMinEffectiveCap(0);
            CappedSortedMapCellsResolver.instance.setMaxEffectiveCap(CappedSortedMap.CAP_MAX);

            createTable("CREATE TABLE %s (pk int, ck int, " +
                        "csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', PRIMARY KEY (pk, ck))" +
                        " WITH gc_grace_seconds = 0");

            // Reduce the default cap to 2
            CappedSortedMapCellsResolver.instance.setDefaultCap(2);
            assertEquals(2, CappedSortedMapCellsResolver.instance.getDefaultCap());

            // Write three entries to a csm limited to two
            final int pk = 1;
            final int ck = 1;
            for (int msgIdx = 1; msgIdx <= 3; msgIdx++)
            {
                execute("UPDATE %s SET csm[?] = ? WHERE pk = ? AND ck = ?",
                        makeMessageId(msgIdx), msgIdx, pk, ck);
            }
            assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), 2,
                               makeMessageId(3), 3)));

            // How do you make execute really execute it, not cache it anywhere?

            assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), 2,
                               makeMessageId(3), 3)));


            // Lower the effective limit to 1 and check result is limited.

            flush(); // otherwise CSM served out of Memtable
            CappedSortedMapCellsResolver.instance.setMaxEffectiveCap(1);
            assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(3), 3)));

            // Raise again and see the default limit is back in place
            CappedSortedMapCellsResolver.instance.setMaxEffectiveCap(CappedSortedMap.CAP_MAX);

            assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), 2,
                               makeMessageId(3), 3)));

        }
        finally
        {
            CappedSortedMapCellsResolver.instance.setDefaultCap(defaultCap);
            CappedSortedMapCellsResolver.instance.setMinEffectiveCap(minEffectiveCap);
            CappedSortedMapCellsResolver.instance.setMaxEffectiveCap(maxEffectiveCap);
        }
    }

    @Test
    public void disableCellsResolver() throws Throwable
    {
        try
        {
            CappedSortedMapCellsResolver.instance.setCellResolverEnabled(true);
            createTable("CREATE TABLE %s (pk int, ck int, " +
                        "csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', PRIMARY KEY (pk, ck))" +
                        " WITH gc_grace_seconds = 0");

            final int pk = 1;
            final int ck = 1;
            final int cap = 2;

            // Write three entries to a csm limited to two
            for (int msgIdx = 1; msgIdx <= 3; msgIdx++)
            {
                execute("UPDATE %s SET csm[cap(?)] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                        cap, makeMessageId(msgIdx), msgIdx, pk, ck);
            }
            assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), 2, makeMessageId(3), 3)));

            CappedSortedMapCellsResolver.instance.setCellResolverEnabled(false);

            // Write three more entries to a csm limited to two
            for (int msgIdx = 4; msgIdx <= 6; msgIdx++)
            {
                execute("UPDATE %s SET csm[cap(?)] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                        cap, makeMessageId(msgIdx), msgIdx, pk, ck);
            }
            assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(2), 2,
                               makeMessageId(3), 3,
                               makeMessageId(4), 4,
                               makeMessageId(5), 5,
                               makeMessageId(6), 6)));

            // Reenable and check previously set cap respected
            CappedSortedMapCellsResolver.instance.setCellResolverEnabled(true);

            for (int msgIdx = 7; msgIdx <=7; msgIdx++)
            {
                execute("UPDATE %s SET csm[cap(?)] = null, csm[?] = ? WHERE pk = ? AND ck = ?",
                        cap, makeMessageId(msgIdx), msgIdx, pk, ck);
            }
            assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                       row(map(makeMessageId(6), 6,
                               makeMessageId(7), 7)));

        }
        finally
        {
            CappedSortedMapCellsResolver.instance.setCellResolverEnabled(true);
        }
    }

    @Test
    public void emptyPartitionExpirationNonPurged() throws Throwable
    {
        int pk = 1;
        int ck = 1;

        emptyPartitionExpirationSetup(pk, ck);
        Thread.sleep(6_000); // expire but don't purge tombstone
        assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck));
        compact(); // First compaction will expire all the TTLd cells and make the cap cell mortal again
        Assert.assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        compact(); // Second compaction will remove the now-mortal cap cell
        Assert.assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    @Test
    public void emptyPartitionExpirationPurged() throws Throwable
    {
        int pk = 1;
        int ck = 1;

        emptyPartitionExpirationSetup(pk, ck);
        Thread.sleep(11_000); // 5 seconds of TTL + 5 seconds of GC Grace
        assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck));
        compact(); // First compaction will expire all the TTLd cells and make the cap cell mortal again
        Assert.assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        compact(); // Second compaction will remove the now-mortal cap cell
        Assert.assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    public void emptyPartitionExpirationSetup(int pk, int ck) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', PRIMARY KEY (pk, ck))" +
                    " WITH gc_grace_seconds = 5");

        final UUID cap = CappedSortedMap.withCap(3);
        for (int msgIdx = 1; msgIdx <= 5; msgIdx++)
        {
            UUID messageId = makeMessageId(msgIdx);
            execute("UPDATE %s USING TTL 5 SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ? ",
                    cap, messageId, msgIdx, pk, ck);
            flush();
        }
    }

    @Test
    public void mixedExpiredAndAliveRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', " +
                    "PRIMARY KEY (pk, ck))");

        int pk = 1;
        int ck = 1;
        final UUID cap = CappedSortedMap.withCap(3);

        write(pk, ck, cap, 1, 5);
        flush();
        write(pk, ck, cap, 2, 10);
        flush();
        Thread.sleep(6_000);
        compact();

        assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                   row(map(makeMessageId(2), 2)));

        compact();
        Assert.assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    @Test
    public void mixedTtlAndAliveRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, csm 'com.apple.cie.db.marshal.CappedSortedMapType(Int32Type)', " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 5");

        int pk = 1;
        int ck = 1;
        final UUID cap = CappedSortedMap.withCap(3);

        // Write one non-expiring cell
        write(pk, ck, cap, 1, 0);
        flush();
        write(pk, ck, cap, 2, 5);
        write(pk, ck, cap, 3, 5);
        write(pk, ck, cap, 4, 5);
        flush();
        compact();

        // Non-expiring cell is kicked out by the later writes
        assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                   row(map(makeMessageId(2), 2,
                           makeMessageId(3), 3,
                           makeMessageId(4), 4)));

        // After compaction, we have one sstable, where cap cell has MAX_DELETION_TIME ldt
        Assert.assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        Thread.sleep(11_000);
        compact(); // First compaction will expire all the TTLd cells and make the cap cell mortal again
        compact(); // Second compaction will remove the now-mortal cap cell

        // All cells are now expired
        assertRows(execute("SELECT csm FROM %s WHERE pk = ? AND ck = ?", pk, ck));

        // Now, cell is compacted away
        Assert.assertEquals(0, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    public void write(int pk, int ck, UUID cap, int msgIdx, int ttl) throws Throwable
    {
        UUID messageId = makeMessageId(msgIdx);
        if (ttl > 0)
        {
            execute("UPDATE %s USING TTL ? SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ? ",
                    ttl, cap, messageId, msgIdx, pk, ck);
        }
        else
        {
            execute("UPDATE %s SET csm[?] = null, csm[?] = ? WHERE pk = ? AND ck = ? ",
                    cap, messageId, msgIdx, pk, ck);

        }
    }

}
