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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ning.compress.lzf.LZFOutputStream;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.TrackedInputStream;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.streaming.StreamTestUtils.session;
import static org.apache.cassandra.utils.TokenRangeTestUtil.broadcastAddress;
import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;
import static org.apache.cassandra.utils.TokenRangeTestUtil.randomInt;
import static org.apache.cassandra.utils.TokenRangeTestUtil.setLocalTokens;
import static org.apache.cassandra.utils.TokenRangeTestUtil.setPendingRanges;
import static org.apache.cassandra.utils.TokenRangeTestUtil.token;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamReaderTest
{
    private static final String TEST_NAME = "streamreader_test_";
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
        // All tests suppose a 2 node ring, with the other peer having the tokens 0, 200, 400
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
    public void testReceiveWithNoOwnedRanges() throws Exception
    {
        int[] tokens = {10, 20};
        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableWithRangeContained() throws Exception
    {
        int[] tokens = {10, 20};
        setLocalTokens(100);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivedTableLowestKeyBoundsExclusive() throws Exception
    {
        // verify that ranges are left exclusive
        int[] tokens = {0, 10};
        setLocalTokens(100);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeWithExactMatch() throws Exception
    {
        // Because ranges are left exlusive, for the range (0, 100] the lowest permissable key is 1
        int[] tokens = {1, 100};
        setLocalTokens(100);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeLessThanOwned() throws Exception
    {
        int[] tokens = {-100, 0};
        setLocalTokens(100);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeGreaterThanOwned() throws Exception
    {
        int[] tokens = {101, 200};
        setLocalTokens(100);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeOverlappingOwned() throws Exception
    {
        int[] tokens = {80, 120};
        setLocalTokens(100);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableContainedBeforeMax() throws Exception
    {
        int[] tokens = {110, 120};

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableContainedAfterMin() throws Exception
    {
        int[] tokens = {-150, -140};

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableOverlappingUpward() throws Exception
    {
        int[] tokens = {-10, 10};

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableOverlappingDownward() throws Exception
    {
        int[] tokens = {90, 110};

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeContainedInFirstOwned() throws Exception
    {
        int[] tokens = {10, 20};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeContainedInLastOwned() throws Exception
    {
        int[] tokens = {450, 460};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeLessThanOwned() throws Exception
    {
        int[] tokens = {510, 520};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeGreaterThanOwned() throws Exception
    {
        int[] tokens = {-20, -10};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeOverlappingOwned() throws Exception
    {
        int[] tokens = {80, 120};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesAllDisjointFromReceivingTableRange() throws Exception
    {
        int[] tokens = {310, 320};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesDisjointSpannedByReceivingTableRange() throws Exception
    {
        int[] tokens = {80, 320};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivedTableRangeExactMatch() throws Exception
    {
        // bacause ranges are left exclusive, for the range (200, 300] the lowest permissable key is 201
        int[] tokens = {201, 300};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithOwnedRangeWrappingAndReceivedFileWhollyContained() throws Exception
    {
        // peer  -> (-100, 0], (100, 200], (300, 400]
        // local -> (min, -100], (400, max]
        setLocalTokens(-100);
        int[] tokens = {-200, 500};
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithOwnedRangeWrappingAndReceivedFilePartiallyContained() throws Exception
    {
        // peer  -> (-100, 0], (100, 200], (300, 400]
        // local -> (min, -100], (400, max]
        setLocalTokens(-100);
        int[] tokens = {-200, 300};
        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithOwnedRangeWrappingAndReceivedFileNotContained() throws Exception
    {
        // peer  -> (-100, 0], (100, 200], (300, 400]
        // local -> (min, -100], (400, max]
        setLocalTokens(-100);
        int[] tokens = {0, 300};
        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    /*****************************************************************************************
     *
     * Unlike stream requests, when receiving streams we also have to consider pending ranges
     *
     ****************************************************************************************/

    @Test
    public void testReceiveWithSinglePendingRangeReceivingTableWithRangeContained() throws Exception
    {
        int[] tokens = {10, 20};
        setPendingRanges(KEYSPACE, 0, 100);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithMultiplePendingRangesReceivingTableRangeContainedInFirstOwned() throws Exception
    {
        int[] tokens = {10, 20};
        setPendingRanges(KEYSPACE, 0, 100, 200, 300, 400, 500);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveNormalizesOwnedAndPendingRanges() throws Exception
    {
        // Incoming file is not covered by either a single owned or pending range,
        // but it is covered by the normalized set of both
        int[] tokens = {90, 110};
        setLocalTokens(100);
        setPendingRanges(KEYSPACE, 100, 120);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }


    private static void tryReceiveExpectingSuccess(int[] tokens,
                                                   boolean isOutOfRange) throws IOException
    {
        StreamSession session = session();
        FileMessageHeader header = messageHeader(tokens);
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        StreamReader reader = streamReader(header, session);
        reader.read(incomingStream(tokens));
        assertEquals(isOutOfRange, StorageMetrics.totalOpsForInvalidToken.getCount() > startMetricCount);
    }

    private static void tryReceiveExpectingFailure(int[] tokens) throws IOException
    {
        StreamSession session = session();
        FileMessageHeader header = messageHeader(tokens);
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();

        try
        {
            StreamReader reader = streamReader(header, session);
            reader.read(incomingStream(tokens));
            fail("Expected StreamReceivedOfTokenRangeException");
        }
        catch (StreamReceivedOutOfTokenRangeException e)
        {
            // expected
        }
        assertTrue(StorageMetrics.totalOpsForInvalidToken.getCount() > startMetricCount);
    }

    private static ReadableByteChannel incomingStream(int...tokens) throws IOException
    {
        // fake the bytes for the stream, just keys needed as our
        // test StreamReader impl no-ops reading the actual row data
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStreamPlus out = new WrappedDataOutputStreamPlus(new LZFOutputStream(bytes));
        for (int token : tokens)
            ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes((long)token), out);
        out.flush();

        return Channels.newChannel(new ByteArrayInputStream(bytes.toByteArray()));

    }

    private static StreamReader streamReader(FileMessageHeader header, StreamSession session)
    {
        return new KeysOnlyStreamReader(header, session);
    }

    private static FileMessageHeader messageHeader(int...tokens)
    {
        CFMetaData cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata;
        Version version = BigFormat.latestVersion;
        List<Pair<Long, Long>> fakeSections = new ArrayList<>();
        // each decorated key takes up (2 + 8) bytes, so this enables the
        // StreamReader to calculate the expected number of bytes to read
        fakeSections.add(Pair.create(0L, (long)(tokens.length * 10) -1));
        int fakeLevel = randomInt(9);
        int fakeSeq = randomInt(9);
        UUID pendingRepair = UUID.randomUUID();

        return new FileMessageHeader(cfm.cfId,
                                     fakeSeq,
                                     version,
                                     SSTableFormat.Type.BIG,
                                     tokens.length,
                                     fakeSections,
                                     (CompressionInfo)null,
                                     System.currentTimeMillis(),
                                     pendingRepair,
                                     fakeLevel,
                                     null);
    }

    // Simplifies generating test data as token == key (expects key to be an encoded long)
    private static class FakeMurmur3Partitioner extends Murmur3Partitioner
    {
        public DecoratedKey decorateKey(ByteBuffer key)
        {
            return new BufferDecoratedKey(new LongToken(ByteBufferUtil.toLong(key)), key);
        }
    }

    // Stream reader which no-ops the reading/writing of the actual partition data.
    // As we only care about keys/tokens here, we don't need to generate the rest
    // of the sstable data to simulate a stream
    private static class KeysOnlyStreamReader extends StreamReader
    {
        KeysOnlyStreamReader(FileMessageHeader header, StreamSession session)
        {
            super(header, session);
        }

        protected SSTableMultiWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt, UUID pendingRepair, SSTableFormat.Type format) throws IOException
        {
            return super.createWriter(cfs, totalSize, repairedAt, pendingRepair, format);
        }

        protected StreamDeserializer getDeserializer(CFMetaData metadata,
                                                     TrackedInputStream in,
                                                     Version inputVersion,
                                                     long totalSize,
                                                     StreamSession session,
                                                     Descriptor desc) throws IOException
        {
            return new TestStreamDeserializer(metadata, in, inputVersion, getHeader(metadata), totalSize, session, desc);
        }

        private static class TestStreamDeserializer extends StreamReader.StreamDeserializer
        {

           TestStreamDeserializer(CFMetaData metadata,
                              InputStream in,
                              Version version,
                              SerializationHeader header,
                              long totalSize,
                              StreamSession session,
                              Descriptor desc) throws IOException
            {
                super(metadata.copy(new FakeMurmur3Partitioner()), in, version, header, totalSize, session, desc);
            }

            public void readPartition() throws IOException
            {
                // no-op, our dummy stream contains only decorated keys
                partitionLevelDeletion = DeletionTime.LIVE;
                iterator = EmptyIterators.unfilteredRow(metadata(), key, false);
                staticRow = Rows.EMPTY_STATIC_ROW;
            }
        }
    }
}
