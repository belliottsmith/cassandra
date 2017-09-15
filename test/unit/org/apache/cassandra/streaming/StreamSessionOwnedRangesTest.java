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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.IncomingFileMessage;
import org.apache.cassandra.streaming.messages.ReceivedMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.streaming.messages.StreamMessage.Type.*;
import static org.apache.cassandra.utils.TokenRangeTestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamSessionOwnedRangesTest
{
    private static final String TEST_NAME = "streamsession_owned_ranges_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    @BeforeClass
    public static void setupClass() throws Exception
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        StorageService.instance.initServer(0);
    }

    @Before
    public void setup() throws Exception
    {
        DatabaseDescriptor.setLogOutOfTokenRangeRequests(true);
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(true);

        StorageService.instance.getTokenMetadata().clearUnsafe();
        // All tests suppose a 2 node ring, with the other peer having the tokens 0, 200, 300
        // Initially, the local node has no tokens so when indivividual test set owned tokens or
        // pending ranges for the local node, they're always in relation to this.
        // e.g. test calls setLocalTokens(100, 300) the ring now looks like
        // peer  -> (min, 0], (100, 200], (300, 400]
        // local -> (0, 100], (200, 300], (400, max]
        //
        // Pending ranges are set in test using start/end pairs.
        // Ring is initialised:
        // peer  -> (min, max]
        // local -> (,]
        // e.g. test calls setPendingRanges(0, 100, 200, 300)
        // the pending ranges for local would be calculated as:
        // local -> (0, 100], (200, 300]
        StorageService.instance.getTokenMetadata().updateNormalTokens(Lists.newArrayList(token(0),
                                                                                         token(200),
                                                                                         token(400)),
                                                                      node1);
    }

    @Test
    public void testPrepareWithAllRequestedRangesWithinOwned() throws Exception
    {
        setLocalTokens(100);
        Collection<StreamRequest> requests = streamRequests(generateRanges(0, 10, 70, 80));

        // prepare request should succeed with or without rejection enabled
        tryPrepareExpectingSuccess(requests, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(requests, false);
    }

    @Test
    public void testPrepareWithAllRequestedRangesOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        Collection<StreamRequest> requests = streamRequests(generateRanges(-20, -10, 110, 120, 310, 320));

        tryPrepareExpectingFailure(requests);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(requests, true);
    }

    @Test
    public void testPrepareWithSomeRequestedRangesOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        Collection<StreamRequest> requests = streamRequests(generateRanges(-20, -10, 30, 40, 310, 320));

        tryPrepareExpectingFailure(requests);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(requests, true);
    }

    @Test
    public void testReceiveWithNoOwnedRanges() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(10, 20);
        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableWithRangeContained() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(10, 20);
        setLocalTokens(100);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivedTableLowestKeyBoundsExclusive() throws Exception
    {
        // verify that ranges are left exclusive
        IncomingFileMessage incoming = incomingFile(0, 10);
        setLocalTokens(100);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeWithExactMatch() throws Exception
    {
        // Because ranges are left exlusive, for the range (0, 100] the lowest permissable key is 1
        IncomingFileMessage incoming = incomingFile(1, 100);
        setLocalTokens(100);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeLessThanOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(-100, 0);
        setLocalTokens(100);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeGreaterThanOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(101, 200);
        setLocalTokens(100);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeOverlappingOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(80, 120);
        setLocalTokens(100);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableContainedBeforeMax() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(110, 120);

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableContainedAfterMin() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(-150, -140);

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableOverlappingUpward() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(-10, 10);

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableOverlappingDownward() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(90, 110);

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeContainedInFirstOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(10, 20);
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeContainedInLastOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(450, 460);
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeLessThanOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(510, 520);
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeGreaterThanOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(-20, -10);
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeOverlappingOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(80, 120);
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesAllDisjointFromReceivingTableRange() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(310, 320);
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesDisjointSpannedByReceivingTableRange() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(80, 320);
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(incoming);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivedTableRangeExactMatch() throws Exception
    {
        // bacause ranges are left exclusive, for the range (200, 300] the lowest permissable key is 201
        IncomingFileMessage incoming = incomingFile(201, 300);
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    /*****************************************************************************************
     *
     * Unlike stream requests, when receiving streams we also have to consider pending ranges
     *
     ****************************************************************************************/

    @Test
    public void testReceiveWithSinglePendingRangeReceivingTableWithRangeContained() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(10, 20);
        setPendingRanges(KEYSPACE, 0, 100);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    @Test
    public void testReceiveWithMultiplePendingRangesReceivingTableRangeContainedInFirstOwned() throws Exception
    {
        IncomingFileMessage incoming = incomingFile(10, 20);
        setPendingRanges(KEYSPACE, 0, 100, 200, 300, 400, 500);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    @Test
    public void testReceiveNormalizesOwnedAndPendingRanges() throws Exception
    {
        // Incoming file is not covered by either a single owned or pending range,
        // but it is covered by the normalized set of both
        IncomingFileMessage incoming = incomingFile(90, 110);
        setLocalTokens(100);
        setPendingRanges(KEYSPACE, 100, 120);

        tryReceiveExpectingSuccess(incoming, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(incoming, false);
    }

    private static void tryReceiveExpectingSuccess(IncomingFileMessage incoming,
                                                   boolean isOutOfRange)
    {
        StubStreamReceiveTaskFactory receiverFactory = new StubStreamReceiveTaskFactory();
        StreamSession session = session(receiverFactory);
        StubConnectionHandler handler = (StubConnectionHandler) session.handler;
        handler.reset();
        receiverFactory.reset();
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();

        // to receive the session must have been prepared with a StreamSummary so that a StreamReceiveTask
        // provided by the StubFactory is available to handle a successful stream
        session.prepareReceiving(summary(incoming));

        session.receive(incoming);

        List<StreamMessage> sent = handler.sentMessages;
        assertEquals(1, sent.size());
        assertEquals(RECEIVED, sent.get(0).type);
        ReceivedMessage received = (ReceivedMessage) sent.get(0);
        assertEquals(incoming.header.sequenceNumber, received.sequenceNumber);
        assertEquals(incoming.header.cfId, received.cfId);
        assertEquals(1, receiverFactory.sstablesReceived());
        assertEquals(startMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static void tryReceiveExpectingFailure(IncomingFileMessage incoming)
    {
        StubStreamReceiveTaskFactory receiverFactory = new StubStreamReceiveTaskFactory();
        StreamSession session = session(receiverFactory);
        StubConnectionHandler handler = (StubConnectionHandler) session.handler;
        handler.reset();
        receiverFactory.reset();
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        // to receive the session must have been prepared with a StreamSummary so that a StreamReceiveTask
        // provided by the StubFactory is available to handle a successful stream
        session.prepareReceiving(summary(incoming));

        try
        {
            session.receive(incoming);
            fail("Expected StreamReceivedOfTokenRangeException");
        }
        catch (StreamReceivedOutOfTokenRangeException e)
        {
            // expected
        }

        assertTrue(handler.sentMessages.isEmpty());
        assertEquals(0, receiverFactory.sstablesReceived());
        assertEquals(startMetricCount + 1, StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static void tryPrepareExpectingSuccess(Collection<StreamRequest> requests, boolean isOutOfRange)
    {
        StreamSession session = session();
        StubConnectionHandler handler = (StubConnectionHandler) session.handler;
        handler.reset();
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();

        session.prepare(requests, Collections.emptySet());

        List<StreamMessage> sent = handler.sentMessages;
        assertEquals(2, sent.size());
        assertEquals(PREPARE, sent.get(0).type);
        assertEquals(COMPLETE, sent.get(1).type);

        assertEquals(startMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static void tryPrepareExpectingFailure(Collection<StreamRequest> requests)
    {
        StreamSession session = session();
        StubConnectionHandler handler = (StubConnectionHandler) session.handler;
        handler.reset();
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        try
        {
            session.prepare(requests, Collections.emptySet());
            fail("Expected StreamRequestOfTokenRangeException");
        }
        catch (StreamRequestOutOfTokenRangeException e)
        {
            // expected
        }
        assertTrue(handler.sentMessages.isEmpty());
        assertEquals(startMetricCount + 1, StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static StreamSession session()
    {
        return session(new StubStreamReceiveTaskFactory());
    }

    private static StreamSession session(StubStreamReceiveTaskFactory receiverFactory)
    {
        StubConnectionHandler connectionHandler = new StubConnectionHandler();
        return new StreamSession(node1,
                                 node1,
                                 new StubStreamConnectionFactory(),
                                 0,
                                 false,
                                 UUID.randomUUID(),
                                 PreviewKind.NONE,
                                 connectionHandler,
                                 receiverFactory);
    }

    private static IncomingFileMessage incomingFile(int...tokens)
    {

        // We don't care about actually writing the sstable, we just want to inspect the tokens for the min & max keys
        // so we fake up the DecoratedKeys here to set the tokens explicitly
        Arrays.sort(tokens);
        DecoratedKey minKey = new BufferDecoratedKey(new Murmur3Partitioner.LongToken(tokens[0]),
                                                     ByteBufferUtil.bytes(tokens[0]));
        DecoratedKey maxKey = new BufferDecoratedKey(new Murmur3Partitioner.LongToken(tokens[1]),
                                                     ByteBufferUtil.bytes(tokens[1]));

        CFMetaData cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata;

        int fakeSeq = randomInt(1000);
        StubSSTableMultiWriter writer = new StubSSTableMultiWriter(cfm.cfId, filename(fakeSeq), minKey, maxKey);
        return new IncomingFileMessage(writer, fileMessageHeader(cfm, fakeSeq, tokens.length));
    }

    private static String filename(int seq)
    {
        return String.format("%s-%s-%s-%d-%s-%s",
                             KEYSPACE, TABLE,
                             BigFormat.latestVersion,
                             seq,
                             BigFormat.Type.BIG.name,
                             Component.DATA.name);
    }

    private static FileMessageHeader fileMessageHeader(CFMetaData cfm, int seq, long numKeys)
    {
        Version version = BigFormat.latestVersion;
        List<Pair<Long, Long>> fakeSections = Collections.emptyList();
        int fakeLevel = randomInt(9);
        UUID pendingRepair = UUID.randomUUID();

        return new FileMessageHeader(cfm.cfId,
                                     seq,
                                     version,
                                     SSTableFormat.Type.BIG,
                                     numKeys,
                                     fakeSections,
                                     (CompressionInfo)null,
                                     System.currentTimeMillis(),
                                     pendingRepair,
                                     fakeLevel,
                                     null);
    }

    private static StreamSummary summary(IncomingFileMessage incoming)
    {
        // number and size of files is not important for these tests
        return new StreamSummary(incoming.header.cfId, 1, 1024L);
    }

    private static Collection<StreamRequest> streamRequests(Collection<Range<Token>> ranges)
    {
        return Collections.singleton(new StreamRequest(KEYSPACE,
                                                       ranges,
                                                       Collections.singleton(TABLE)));

    }

    private static class StubStreamReceiveTaskFactory implements StreamSession.StreamReceiveTaskFactory
    {
        private final AtomicInteger received = new AtomicInteger(0);

        // task does nothing but increment the received counter
        public StreamReceiveTask newTask(StreamSession session, UUID cfId, int totalFiles, long totalSize)
        {
            return new StreamReceiveTask(session, cfId, totalFiles, totalSize)
            {
                public synchronized void received(SSTableMultiWriter sstable)
                {
                    received.incrementAndGet();
                }
            };
        }

        private void reset()
        {
            received.set(0);
        }

        private int sstablesReceived()
        {
            return received.get();
        }
    }

    private static class StubSSTableMultiWriter implements SSTableMultiWriter
    {
        private final UUID cfId;
        private final String filename;
        private final DecoratedKey minKey;
        private final DecoratedKey maxKey;

        StubSSTableMultiWriter(UUID cfId, String filename, DecoratedKey minKey, DecoratedKey maxKey)
        {
            this.cfId = cfId;
            this.filename = filename;
            this.minKey = minKey;
            this.maxKey = maxKey;
        }

        public UUID getCfId()
        {
            return cfId;
        }

        public String getFilename()
        {
            return filename;
        }

        public DecoratedKey getMinKey()
        {
            return minKey;
        }

        public DecoratedKey getMaxKey()
        {
            return maxKey;
        }

        public boolean append(UnfilteredRowIterator partition)
        {
            throw new UnsupportedOperationException();
        }

        public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
        {
            throw new UnsupportedOperationException();
        }

        public Collection<SSTableReader> finish(boolean openResult)
        {
            throw new UnsupportedOperationException();
        }

        public Collection<SSTableReader> finished()
        {
            throw new UnsupportedOperationException();
        }

        public SSTableMultiWriter setOpenResult(boolean openResult)
        {
            throw new UnsupportedOperationException();
        }

        public long getFilePointer()
        {
            throw new UnsupportedOperationException();
        }

        public Throwable commit(Throwable accumulate)
        {
            throw new UnsupportedOperationException();
        }

        public Throwable abort(Throwable accumulate)
        {
            throw new UnsupportedOperationException();
        }

        public void prepareToCommit()
        {
            throw new UnsupportedOperationException();
        }

        public void close()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class StubConnectionHandler extends ConnectionHandler
    {
        private final List<StreamMessage> sentMessages = new ArrayList<>();

        StubConnectionHandler()
        {
            super(null, false);
        }

        public void sendMessage(StreamMessage message)
        {
            sentMessages.add(message);
        }

        private void reset()
        {
            sentMessages.clear();
        }
    }

    private static class StubStreamConnectionFactory implements StreamConnectionFactory
    {
        public Socket createConnection(InetAddress peer) throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
