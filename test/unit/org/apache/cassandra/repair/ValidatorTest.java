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
package org.apache.cassandra.repair;

import java.io.IOException;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.UUID;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.data.BufferDecoratedKey;
import org.apache.cassandra.db.data.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.sink.IMessageSink;
import org.apache.cassandra.sink.SinkManager;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SimpleCondition;

import static org.junit.Assert.*;

public class ValidatorTest extends SchemaLoader
{
    private final String keyspace = "Keyspace1";
    private final String columnFamily = "Standard1";
    private final IPartitioner partitioner = StorageService.getPartitioner();

    @After
    public void tearDown()
    {
        SinkManager.clear();
    }

    @Test
    public void testValidatorComplete() throws Throwable
    {
        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        final RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), keyspace, columnFamily, range);

        final SimpleCondition lock = new SimpleCondition();
        SinkManager.add(new IMessageSink()
        {
            @SuppressWarnings("unchecked")
            public MessageOut handleMessage(MessageOut message, int id, InetAddress to)
            {
                try
                {
                    if (message.verb == MessagingService.Verb.REPAIR_MESSAGE)
                    {
                        RepairMessage m = (RepairMessage) message.payload;
                        assertEquals(RepairMessage.Type.VALIDATION_COMPLETE, m.messageType);
                        assertEquals(desc, m.desc);
                        assertTrue(((ValidationComplete)m).success);
                        assertNotNull(((ValidationComplete)m).tree);
                    }
                }
                finally
                {
                    lock.signalAll();
                }
                return null;
            }

            public MessageIn handleMessage(MessageIn message, int id, InetAddress to)
            {
                return null;
            }
        });

        InetAddress remote = InetAddress.getByName("127.0.0.2");

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);

        Validator validator = new Validator(desc, remote, 0);
        validator.prepare(cfs);

        // and confirm that the tree was split
        assertTrue(validator.tree.size() > 1);

        // add a row
        Token mid = partitioner.midpoint(range.left, range.right);
        validator.add(new CompactedRowStub(new BufferDecoratedKey(mid, ByteBufferUtil.bytes("inconceivable!"))));
        validator.complete();

        // confirm that the tree was validated
        Token min = validator.tree.partitioner().getMinimumToken();
        assertNotNull(validator.tree.hash(new Range<>(min, min)));

        if (!lock.isSignaled())
            lock.await();
    }

    private static class CompactedRowStub extends AbstractCompactedRow
    {
        private CompactedRowStub(DecoratedKey key)
        {
            super(key);
        }

        public RowIndexEntry write(long currentPosition, DataOutputPlus out) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public void update(MessageDigest digest) { }

        public ColumnStats columnStats()
        {
            throw new UnsupportedOperationException();
        }

        public void close() throws IOException { }
    }

    @Test
    public void testValidatorFailed() throws Throwable
    {
        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        final RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), keyspace, columnFamily, range);

        final SimpleCondition lock = new SimpleCondition();
        SinkManager.add(new IMessageSink()
        {
            @SuppressWarnings("unchecked")
            public MessageOut handleMessage(MessageOut message, int id, InetAddress to)
            {
                try
                {
                    if (message.verb == MessagingService.Verb.REPAIR_MESSAGE)
                    {
                        RepairMessage m = (RepairMessage) message.payload;
                        assertEquals(RepairMessage.Type.VALIDATION_COMPLETE, m.messageType);
                        assertEquals(desc, m.desc);
                        assertFalse(((ValidationComplete) m).success);
                        assertNull(((ValidationComplete)m).tree);
                    }
                }
                finally
                {
                    lock.signalAll();
                }
                return null;
            }

            public MessageIn handleMessage(MessageIn message, int id, InetAddress to)
            {
                return null;
            }
        });

        InetAddress remote = InetAddress.getByName("127.0.0.2");

        Validator validator = new Validator(desc, remote, 0);
        validator.fail();

        if (!lock.isSignaled())
            lock.await();
    }
}
