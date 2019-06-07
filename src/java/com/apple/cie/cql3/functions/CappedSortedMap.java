/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */
package com.apple.cie.cql3.functions;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.NativeScalarFunction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CappedSortedMap
{
    public  static final int CAP_MAX = (1 << 14) - 1;
    private static final long CAP_MASK = (long) CAP_MAX;
    private static final long MSB = makeMSB();

    /* From RFC4122 section 4.1.2
     *
     * The capacity is encoded in a version 1, variant 2 UUID
     * as 14-bits in the clock sequence field, with the timestamp set to the
     * maximum permissible time.
     *
     *       0                   1                   2                   3
     *    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *   |                          time_low                             |
     *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *   |       time_mid                |         time_hi_and_version   |
     *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *   |clk_seq_hi_res |  clk_seq_low  |         node (0-1)            |
     *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *   |                         node (2-5)                            |
     *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     */
    private static long makeMSB()
    {
        long timestamp = 1152921504606846975L; // 0x0FFFFFFFFFFFFFFF
        long msb = 0L;
        msb |= (4294967295L & timestamp) << 32;           // 0x00000000FFFFFFFF (31- 0 timestamp) = TTTTTTTT00000000
        msb |= (281470681743360L & timestamp) >>> 16;     // 0x0000FFFF00000000 (47-32 timestamp) = 00000000UUUU0000
        msb |= (1152640029630136320L & timestamp) >>> 48; // 0x0FFF000000000000 (62-48 timestamp) = 0000000000000VVV
        msb |= 4096L;                                     // 0x0000000000001000 (version)
        return msb;                                       //                                      = TTTTTTTTUUUU1VVV
    }

    private static long makeLSB(int cap)
    {
        long truncCap = Math.min(cap, CAP_MAX);
        long lsb = 281474976710655L;          // node    0x0000FFFFFFFFFFFF
        lsb |= (truncCap & CAP_MASK) << 48;   // cap     0x00QQ000000000000 (14-bits of capacity)
        lsb |= -9223372036854775808L;         // variant 0x8000000000000000
        return lsb;
    }

    /**
     * Make a special cap TimeUUID for use by @{link com.apple.cie.db.marshal.CappedSortedMapType}.
     */
    static public UUID withCap(int length)
    {
        return new UUID(MSB, makeLSB(length));
    }

    static public int getCap(UUID uuid)
    {
        if (uuid.getMostSignificantBits() != MSB)
            return -1;
        long lsb = uuid.getLeastSignificantBits();
        return (int) ((lsb >> 48) & CAP_MASK);
    }

    private static final Function capFct = new NativeScalarFunction("cap", TimeUUIDType.instance, Int32Type.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            int len = Int32Type.instance.compose(parameters.get(0));
            return ByteBufferUtil.bytes(withCap(len));
        }
    };

    @SuppressWarnings("unused")
    public static Collection<Function> all()
    {
        return ImmutableList.of(capFct);
    }
}
