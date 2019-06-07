/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */

package com.apple.cie.cql3.functions;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.*;

public class CappedSortedMapTest
{
    @Test
    public void checkConstruction()
    {
        // Lowest possible queue length
        UUID q0 = CappedSortedMap.withCap(0);
        assertEquals(0, CappedSortedMap.getCap(q0));

        // Highest possible queue length
        UUID q16k = CappedSortedMap.withCap(32768); // should be truncated to 32677
        assertEquals(16383, CappedSortedMap.getCap(q16k));

        // Check both have largest possible timestamp
        assertEquals(1152921504606846975L, q0.timestamp());
        assertEquals(1152921504606846975L, q16k.timestamp());

        // Check both have the correct variant
        assertEquals(2, q0.variant());
        assertEquals(2, q16k.variant());

        // Check both have the correct version
        assertEquals(1, q0.version());
        assertEquals(1, q16k.version());

        // Make sure the timestamp is beyond a far future date so
        // there should not be problems in the system lifetime.
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try
        {
            Date date = df.parse("2500-12-31 23:59:59");
            long epoch = date.getTime();
            UUID farFutureTimeUUID = UUIDs.endOf(epoch);
            assert(CappedSortedMap.getCap(farFutureTimeUUID) < 0);

            ByteBuffer farFutureBB = ByteBufferUtil.bytes(farFutureTimeUUID);
            ByteBuffer q0BB = ByteBufferUtil.bytes(q0);
            ByteBuffer q16kBB = ByteBufferUtil.bytes(q16k);

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
            UUID uuid = CappedSortedMap.withCap(cap);
            assertEquals(cap, CappedSortedMap.getCap(uuid));
        }
    }
}
