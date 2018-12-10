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

            public StreamReceiveTask getReceivingTask(UUID cfId)
            {
                return new StubStreamReceiveTask(this, cfId);
            }
        };
    }

    // StreamReaderTest & StreamSessionTest, which use the StreamSessions obtained via the session()
    // method, don't actually receive streams. Instead, they either use a specially constructed StreamReader
    // or StreamSession::prepare to test that the validation of owned ranges in stream messages. Since
    // CASSANDRA-14554 though, a StreamReceiveTask is required for each cfId in order to obtain a
    // LifecycleNewTracker to synchronize access to the underlying lifecycle transaction. Because the
    // receive task will not actually run, we can safely prime it to expect 0 total files & bytes.
    static class StubStreamReceiveTask extends StreamReceiveTask
    {

        public StubStreamReceiveTask(StreamSession session, UUID cfId)
        {
            super(session, cfId, 0, 0);
        }
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
