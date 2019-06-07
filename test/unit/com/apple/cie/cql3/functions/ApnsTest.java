/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */

package com.apple.cie.cql3.functions;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

public class ApnsTest extends CQLTester
{
    private static final long start = 0L; // Test in the 1970s!

    private TimeUUID makeMessageId(int idx)
    {
        return TimeUUID.fromUuid(UUIDs.startOf(start + idx));
    }

    @Test
    public void basicFunctionality() throws Throwable
    {
        createTable("CREATE TABLE %s ( " +
                    "    pk int, ck int, " +
                    "    events 'com.apple.cie.db.marshal.CappedSortedMapType(BytesType)', " +
                    "    deliveries map<timeuuid,frozen<tuple<tinyint,timestamp>>>, " +
                    "    PRIMARY KEY (pk, ck)) " +
                    "  WITH " +
                    "    default_time_to_live = 3024000");

        assertRows(execute(String.format("SELECT type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s' AND column_name = 'events'", KEYSPACE, currentTable())),
                   row("'com.apple.cie.db.marshal.CappedSortedMapType(org.apache.cassandra.db.marshal.BytesType)'"));

        final int pk = 1;
        final int ck = 1;
        final TimeUUID queueLen = CappedSortedMap.withCap(3);
        for (int msgIdx = 0; msgIdx <= 5; msgIdx++)
        {
            execute("UPDATE %s SET events[?] = null, events[?] = ? WHERE pk = ? AND ck = ?",
                    queueLen, makeMessageId(msgIdx), Integer.toString(msgIdx), pk, ck);
        }

        assertRows(execute("SELECT pk, ck, events FROM %s WHERE pk = 1"),
                   row(pk, ck,
                       map(makeMessageId(3), ByteBufferUtil.hexToBytes("33"),
                           makeMessageId(4), ByteBufferUtil.hexToBytes("34"),
                           makeMessageId(5), ByteBufferUtil.hexToBytes("35"))));

        // Check msdIdx 3 is the oldest deliverable
        assertRows(execute("SELECT oldestdeliverable(events, deliveries, totimestamp(now())) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedOldestDeliverableType.fromString("0:2:" + UUIDs.startOf(start + 3) + ":33:@:@")));

        assertRows(execute("SELECT koldestdeliverable(events, deliveries, totimestamp(now()), int32literal1()) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedKOldestDeliverableType.fromString("0:2:00000001000000101381b5301dd211b280808080808080800000000d0000000133ffffffffffffffff")));

        assertRows(execute("SELECT deliverable(events, deliveries, totimestamp(now())) " +
                           "FROM %s WHERE pk = 1"),
                   row(map(
                   makeMessageId(3), ByteBufferUtil.hexToBytes("0000000133ffffffffffffffff"),
                   makeMessageId(4), ByteBufferUtil.hexToBytes("0000000134ffffffffffffffff"),
                   makeMessageId(5), ByteBufferUtil.hexToBytes("0000000135ffffffffffffffff"))));

        // Mark msgIdx 3 as delivered - will be blocked for next 60s
        Date deliveryWindow = Date.from(Instant.now().plusSeconds(60));
        execute("UPDATE %s SET deliveries[?] = null, deliveries[?] = ? WHERE pk = ? AND ck = ?",
                queueLen, makeMessageId(3), tuple((byte) 20, deliveryWindow), pk, ck);

        // Check msgIdx 4 is now the oldest deliverable, 1 more deliverable, 1 undeliverable
        assertRows(execute("SELECT oldestdeliverable(events, deliveries, totimestamp(now())) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedOldestDeliverableType.fromString("1:1:" + UUIDs.startOf(start + 4) + ":34:@:@")));

        assertRows(execute("SELECT koldestdeliverable(events, deliveries, totimestamp(now()), int32literal1()) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedKOldestDeliverableType.fromString("1:1:00000001000000101381dc401dd211b280808080808080800000000d0000000134ffffffffffffffff")));

        assertRows(execute("SELECT koldestdeliverable(events, deliveries, totimestamp(now()), int32literal2()) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedKOldestDeliverableType.fromString("1:0:00000002000000101381dc401dd211b280808080808080800000000d0000000134ffffffffffffffff00000010138203501dd211b280808080808080800000000d0000000135ffffffffffffffff")));

        // And that only 4 and 5 are deliverable
        assertRows(execute("SELECT deliverable(events, deliveries, totimestamp(now())) " +
                           "FROM %s WHERE pk = 1"),
                   row(map(
                   makeMessageId(4), ByteBufferUtil.hexToBytes("0000000134ffffffffffffffff"),
                   makeMessageId(5), ByteBufferUtil.hexToBytes("0000000135ffffffffffffffff"))));


        // Mark msgIdx 4 as acknowledged */
        execute("DELETE events[?], deliveries[?] FROM %s WHERE pk = ? AND ck = ?",
                makeMessageId(4), makeMessageId(4), pk, ck);

        // Check msgIdx 5 is now the oldest deliverable
        assertRows(execute("SELECT oldestdeliverable(events, deliveries, totimestamp(now())) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedOldestDeliverableType.fromString("1:0:" + UUIDs.startOf(start + 5) + ":35:@:@")));
        assertRows(execute("SELECT koldestdeliverable(events, deliveries, totimestamp(now()), int32literal1()) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedKOldestDeliverableType.fromString("1:0:0000000100000010138203501dd211b280808080808080800000000d0000000135ffffffffffffffff")));
        assertRows(execute("SELECT koldestdeliverable(events, deliveries, totimestamp(now()), int32literal2()) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedKOldestDeliverableType.fromString("1:0:0000000100000010138203501dd211b280808080808080800000000d0000000135ffffffffffffffff")));

        // Only 5 is deliverable
        assertRows(execute("SELECT deliverable(events, deliveries, totimestamp(now())) " +
                           "FROM %s WHERE pk = 1"),
                   row(map(
                   makeMessageId(5), ByteBufferUtil.hexToBytes("0000000135ffffffffffffffff"))));

        // Mark msgIdx 5 as acknowledged */
        execute("DELETE events[?], deliveries[?] FROM %s WHERE pk = ? AND ck = ?",
                makeMessageId(5), makeMessageId(5), pk, ck);

        // Check null is returned with the queue only containing undeliverables
        assertRows(execute("SELECT oldestdeliverable(events, deliveries, totimestamp(now())) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedOldestDeliverableType.fromString("1:0:@:@:@:@")));
        assertRows(execute("SELECT koldestdeliverable(events, deliveries, totimestamp(now()), int32literal1()) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedKOldestDeliverableType.fromString("1:0:@")));
        assertRows(execute("SELECT koldestdeliverable(events, deliveries, totimestamp(now()), int32literal2()) " +
                           "FROM %s WHERE pk = 1"),
                   row(Apns.sortedKOldestDeliverableType.fromString("1:0:@")));

        // Mark msgIdx 3 as acknowledged */
        execute("DELETE events[?], deliveries[?] FROM %s WHERE pk = ? AND ck = ?",
                makeMessageId(3), makeMessageId(3), pk, ck);

        // Check no rows are returned with the queue consisting of tombstones
        assertRows(execute("SELECT oldestdeliverable(events, deliveries, totimestamp(now())) " +
                           "FROM %s WHERE pk = 1"));

        // And finally, check nothing deliverable if orphaned delivery information is left behind
        execute("UPDATE %s SET deliveries[now()] = ? WHERE pk = ? AND ck = ?",
                tuple((byte)15, deliveryWindow), pk, ck);
        assertRows(execute("SELECT oldestdeliverable(events, deliveries, totimestamp(now())) AS od " +
                   "FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                   row(tuple(0, 0, null, null, null, null)));
        assertRows(execute("SELECT koldestdeliverable(events, deliveries, totimestamp(now()), int32literal2()) AS kod " +
                           "FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                   row(tuple(0, 0, null)));
        assertRows(execute("SELECT deliverable(events, deliveries, totimestamp(now())) AS d " +
                           "FROM %s WHERE pk = ? AND ck = ?", pk, ck),
                   row((Object) null));
    }
}
