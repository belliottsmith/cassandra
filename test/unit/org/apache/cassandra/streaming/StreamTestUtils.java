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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.streaming.messages.StreamMessage;

import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;

public class StreamTestUtils
{
    static StreamSession session()
    {
        StubConnectionHandler connectionHandler = new StubConnectionHandler();
        return new StreamSession(node1,
                                 node1,
                                 new StubStreamConnectionFactory(),
                                 0,
                                 false,
                                 null,
                                 PreviewKind.NONE,
                                 connectionHandler)
        {
            public void progress(Descriptor desc, ProgressInfo.Direction direction, long bytes, long total)
            {
                //no-op
            }

            public LifecycleTransaction getTransaction(UUID cfId)
            {
                return LifecycleTransaction.offline(OperationType.STREAM);
            }
        };
    }

    static class StubConnectionHandler extends ConnectionHandler
    {
        final List<StreamMessage> sentMessages = new ArrayList<>();

        StubConnectionHandler()
        {
            super(null, false);
        }

        public void sendMessage(StreamMessage message)
        {
            sentMessages.add(message);
        }

        void reset()
        {
            sentMessages.clear();
        }
    }

    static class StubStreamConnectionFactory implements StreamConnectionFactory
    {
        public Socket createConnection(InetAddress peer) throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
