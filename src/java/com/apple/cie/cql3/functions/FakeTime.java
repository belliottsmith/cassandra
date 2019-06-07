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

package com.apple.cie.cql3.functions;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.NativeScalarFunction;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Custom function to supply int32 literal values for the k argument of
 * {@literal com.apple.cie.cql3.functions.Apns#koldestdeliverableFct} as it
 * is not possibly to supply them in the 3.0 series.
 */
public abstract class FakeTime
{
    static Instant fakeInstant;
    static int fakeCentiNanos;

    static
    {
        reset();
    }

    public static void reset() {
        synchronized (FakeTime.class)
        {
            Instant now = Instant.now();
            fakeInstant = now;
            fakeCentiNanos = fakeInstant.getNano() / 100;
        }
    }

    public static void set(Instant instant, int fakeCentiNanos)
    {
        synchronized (FakeTime.class)
        {
            FakeTime.fakeInstant = instant.minusNanos(instant.getNano());
            FakeTime.fakeCentiNanos = fakeCentiNanos;
        }
    }

    public static void advanceMillis(int numMillis)
    {
        synchronized (FakeTime.class)
        {
            fakeInstant = fakeInstant.plus(numMillis, ChronoUnit.MILLIS);
        }

    }

    static long fakeMillis()
    {
        synchronized (FakeTime.class)
        {
            return fakeInstant.toEpochMilli();
        }
    }

    static long fakeMicros()
    {
        return fakeMillis() * 1000L;
    }

    public static UUID fakeNow()
    {
        synchronized (FakeTime.class)
        {
            assert (fakeCentiNanos < 10_000_000);
            return UUIDGen.getTimeUUID(fakeInstant.toEpochMilli(), fakeCentiNanos++, 0l);
        }
    }

    public static Date fakeDate()
    {
        synchronized (FakeTime.class)
        {
            return Date.from(fakeInstant);
        }
    }

    public static final Function fakeNowFct = new NativeScalarFunction("fakenow", TimeUUIDType.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            return ByteBufferUtil.bytes(fakeNow());
        }
    };

    @SuppressWarnings("unused")
    public static Collection<Function> all()
    {
        return ImmutableList.of(fakeNowFct);
    }
}
