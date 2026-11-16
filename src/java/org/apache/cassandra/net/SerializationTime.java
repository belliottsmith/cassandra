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

package org.apache.cassandra.net;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * {@link ParamType} values read from the monotonic clock when the message is serialized, i.e. as close as practical to
 * when it is written to the network (rather than when it is constructed, which may precede time spent queued to be
 * sent). Like the creation time in the message header, these are encoded with a fixed width, so that a message's
 * serialized size does not depend on when it is serialized.
 *
 * Values are always deserialized as a {@link Long} (nanos), which may be (re)serialized unchanged.
 */
public final class SerializationTime
{
    /**
     * Serialized as the time of serialization, by our monotonic clock
     */
    public static final Object NOW = new Object()
    {
        @Override public String toString() { return "SerializationTime.NOW"; }
    };

    /**
     * Serialized as the nanos elapsed between {@link #sinceNanos} and serialization, by our monotonic clock
     */
    public static final class ElapsedSince
    {
        final long sinceNanos;

        public ElapsedSince(long sinceNanos)
        {
            this.sinceNanos = sinceNanos;
        }

        @Override
        public String toString()
        {
            return "SerializationTime.ElapsedSince(" + sinceNanos + ')';
        }
    }

    static final IVersionedSerializer<Object> serializer = new IVersionedSerializer<Object>()
    {
        @Override
        public void serialize(Object value, DataOutputPlus out, int version) throws IOException
        {
            long nanos;
            if (value == NOW) nanos = nanoTime();
            else if (value instanceof ElapsedSince) nanos = Math.max(0, nanoTime() - ((ElapsedSince) value).sinceNanos);
            else nanos = (Long) value;
            out.writeLong(nanos);
        }

        @Override
        public Object deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readLong();
        }

        @Override
        public long serializedSize(Object value, int version)
        {
            return TypeSizes.LONG_SIZE;
        }
    };

    private SerializationTime() {}
}
