/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */

package com.apple.cie.cql3.functions;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.*;

public class CappedSortedMapTest
{
    @Test
    public void checkConstruction()
    {
        // Lowest possible queue length
        TimeUUID q0 = CappedSortedMap.withCap(0);
        assertEquals(0, CappedSortedMap.getCap(q0));

        // Highest possible queue length
        TimeUUID q16k = CappedSortedMap.withCap(32768); // should be truncated to 32677
        assertEquals(16383, CappedSortedMap.getCap(q16k));

        // Check both have largest possible timestamp
        assertEquals(1152921504606846975L, q0.uuidTimestamp());
        assertEquals(1152921504606846975L, q0.asUUID().timestamp());
        assertEquals(1152921504606846975L, q16k.uuidTimestamp());
        assertEquals(1152921504606846975L, q16k.asUUID().timestamp());

        // Check both have the correct varaiant
        assertEquals(2, q0.asUUID().variant());
        assertEquals(2, q16k.asUUID().variant());

        // Check both have the correct version
        assertEquals(1, q0.asUUID().version());
        assertEquals(1, q16k.asUUID().version());

        // Make sure the timestamp is beyond a far future date so
        // there should not be problems in the system lifetime.
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try
        {
            Date date = df.parse("2500-12-31 23:59:59");
            long epoch = date.getTime();
            TimeUUID farFutureTimeUUID = TimeUUID.fromUuid(UUIDs.endOf(epoch));
            assert(CappedSortedMap.getCap(farFutureTimeUUID) < 0);

            ByteBuffer farFutureBB = farFutureTimeUUID.toBytes();
            ByteBuffer q0BB = q0.toBytes();
            ByteBuffer q16kBB = q16k.toBytes();

            assert (TimeUUIDType.instance.compare(q0BB, farFutureBB) > 0);
            assert (TimeUUIDType.instance.compare(q16kBB, farFutureBB) > 0);
        }
        catch (ParseException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void checkRoundtripQueueBits()
    {
        for (int cap = 1; cap < 16384; cap <<= 1) {
            TimeUUID uuid = CappedSortedMap.withCap(cap);
            assertEquals(cap, CappedSortedMap.getCap(uuid));
        }
    }
}
