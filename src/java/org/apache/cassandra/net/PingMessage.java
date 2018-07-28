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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.async.OutboundConnection;

import static org.apache.cassandra.net.async.OutboundConnection.Type.URGENT;
import static org.apache.cassandra.net.async.OutboundConnection.Type.LARGE_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnection.Type.SMALL_MESSAGE;

/**
 * Indicates to the recipient which {@link OutboundConnection.Type} should be used for the response.
 */
public class PingMessage
{
    public static IVersionedSerializer<PingMessage> serializer = new PingMessageSerializer();

    public static final PingMessage forSmall = new PingMessage(SMALL_MESSAGE);
    public static final PingMessage forLarge = new PingMessage(LARGE_MESSAGE);
    public static final PingMessage forUrgent = new PingMessage(URGENT);

    public final OutboundConnection.Type connectionType;

    public PingMessage(OutboundConnection.Type connectionType)
    {
        this.connectionType = connectionType;
    }

    public static class PingMessageSerializer implements IVersionedSerializer<PingMessage>
    {
        public void serialize(PingMessage t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t.connectionType.id);
        }

        public PingMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            OutboundConnection.Type connectionType = OutboundConnection.Type.fromId(in.readByte());
            switch (connectionType)
            {
                case LARGE_MESSAGE:
                    return forLarge;
                case URGENT:
                    return forUrgent;
                case SMALL_MESSAGE:
                    return forSmall;
                default:
                    throw new IllegalStateException();
            }
        }

        public long serializedSize(PingMessage t, int version)
        {
            return 1;
        }
    }
}
