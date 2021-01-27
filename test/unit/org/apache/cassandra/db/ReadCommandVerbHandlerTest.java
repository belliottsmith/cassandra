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

package org.apache.cassandra.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.utils.TokenRangeTestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadCommandVerbHandlerTest
{
    private static final String TEST_NAME = "read_command_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    private ReadCommandVerbHandler handler;
    private ColumnFamilyStore cfs;
    private long startingTotalMetricCount;
    private long startingKeyspaceMetricCount;

    @BeforeClass
    public static void init() throws Exception
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        StorageService.instance.initServer(0);
        System.setProperty(ReadCommand.OVERRIDE_DISABLED_XMAS_PATCH_PROP, "true");
    }

    @Before
    public void setup() throws Exception
    {
        DatabaseDescriptor.setLogOutOfTokenRangeRequests(true);
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(true);

        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(bytesToken(0), node1);
        StorageService.instance.getTokenMetadata().updateNormalToken(bytesToken(100), broadcastAddress);

        MessagingService.instance().clearMessageSinks();

        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        startingKeyspaceMetricCount = keyspaceMetricValue(cfs);
        startingTotalMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        handler = new ReadCommandVerbHandler();
    }

    @Test
    public void acceptReadForNaturalEndpoint() throws Exception
    {
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 50;
        ReadCommand command = command(key);
        handler.doVerb(MessageIn.create(node1,
                                        command,
                                        EMPTY_PARAMS,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId);
        getAndVerifyResponse(messageSink, messageId, false, false);
    }

    @Test
    public void rejectReadForTokenOutOfRange() throws Exception
    {
        // reject a read for a key who's token the node doesn't own the range for
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 200;
        ReadCommand command = command(key);
        handler.doVerb(MessageIn.create(node1,
                                        command,
                                        EMPTY_PARAMS,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId);
        getAndVerifyResponse(messageSink, messageId, true, true);
    }

    @Test
    public void acceptReadIfRejectionNotEnabled() throws Exception
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 200;
        ReadCommand command = command(key);
        handler.doVerb(MessageIn.create(node1,
                                        command,
                                        EMPTY_PARAMS,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId);
        getAndVerifyResponse(messageSink, messageId, true, false);
    }

    @Test
    public void rangeReadCommandBoundsAreNotChecked() throws Exception
    {
        // checking is only currently done for single partition reads, range reads will continue to
        // accept any range they are given. So for a range wholly outside the node's ownership we
        // expect the metric to remain unchanged and read command to be executed.
        // This test is added for 3.0 because the single partition & range  commands are now processed
        // by the same verb handler.
        // rdar://problem/33535104 is to extend checking to range reads
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        Range<Token> range = new Range<>(token(150), token(160));
        ReadCommand command = new StubRangeReadCommand(range, cfs.metadata);
        handler.doVerb(MessageIn.create(node1,
                                        command,
                                        EMPTY_PARAMS,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId);
        getAndVerifyResponse(messageSink, messageId, false, false);
    }

    @Test
    public void setRepairedDataTrackingFlagIfHeaderPresent()
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ReadCommand command = command(cfs.metadata);
        assertFalse(command.isTrackingRepairedStatus());
        Map<String, byte[]> params = ImmutableMap.of(ReadCommand.TRACK_REPAIRED_DATA,
                                                            MessagingService.ONE_BYTE);
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        params,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       randomInt());
        assertTrue(command.isTrackingRepairedStatus());
    }

    @Test
    public void dontSetRepairedDataTrackingFlagUnlessHeaderPresent()
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ReadCommand command = command(cfs.metadata);
        assertFalse(command.isTrackingRepairedStatus());
        Map<String, byte[]> params = ImmutableMap.of(Tracing.TRACE_HEADER,
                                                     UUIDGen.decompose(UUID.randomUUID()));
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        params,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       randomInt());
        assertFalse(command.isTrackingRepairedStatus());
    }

    @Test
    public void dontSetRepairedDataTrackingFlagIfHeadersEmpty()
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ReadCommand command = command(cfs.metadata);
        assertFalse(command.isTrackingRepairedStatus());
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        ImmutableMap.of(),
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       randomInt());
        assertFalse(command.isTrackingRepairedStatus());
    }

    @Test
    public void setResponseHeadersWithConclusiveDigest() throws Exception
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        // Commands with key >= 0 will have a conclusive digest, those with a key < 0 an inconclusive digest
        ReadCommand command = command(1);
        assertFalse(command.isTrackingRepairedStatus());

        Map<String, byte[]> params = ImmutableMap.of(ReadCommand.TRACK_REPAIRED_DATA,
                                                     MessagingService.ONE_BYTE);
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        params,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       randomInt());

        assertTrue(command.isTrackingRepairedStatus());
        MessageDelivery response = messageSink.get(10, TimeUnit.MILLISECONDS);
        assertEquals(0, ByteBufferUtil.compare((byte[]) response.message.parameters.get(ReadCommand.CONCLUSIVE_REPAIRED_DATA_DIGEST),
                                               StubReadCommand.REPAIRED_DIGEST));
        assertFalse(response.message.parameters.containsKey(ReadCommand.INCONCLUSIVE_REPAIRED_DATA_DIGEST));
    }

    @Test
    public void setResponseHeadersWithInconclusiveDigest() throws Exception
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        // Commands with key >= 0 will have a conclusive digest, those with a key < 0 an inconclusive digest
        ReadCommand command = command(-1);
        assertFalse(command.isTrackingRepairedStatus());
        Map<String, byte[]> params = ImmutableMap.of(ReadCommand.TRACK_REPAIRED_DATA,
                                                     MessagingService.ONE_BYTE);
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        params,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       randomInt());
        assertTrue(command.isTrackingRepairedStatus());
        MessageDelivery response = messageSink.get(10, TimeUnit.MILLISECONDS);
        assertEquals(0, ByteBufferUtil.compare((byte[])response.message.parameters.get(ReadCommand.INCONCLUSIVE_REPAIRED_DATA_DIGEST),
                                                StubReadCommand.REPAIRED_DIGEST));
        assertFalse(response.message.parameters.containsKey(ReadCommand.CONCLUSIVE_REPAIRED_DATA_DIGEST));
    }

    @Test
    public void dontSetResponseHeadersIfNotTrackingRepairedData() throws Exception
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        ReadCommand command = command(0);
        assertFalse(command.isTrackingRepairedStatus());
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        ImmutableMap.of(),
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       randomInt());
        assertFalse(command.isTrackingRepairedStatus());
        MessageDelivery response = messageSink.get(10, TimeUnit.MILLISECONDS);
        assertTrue(response.message.parameters.isEmpty());
    }

    private static InetAddress peer()
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{ 127, 0, 0, 9});
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void getAndVerifyResponse(ListenableFuture<MessageDelivery> messageSink,
                                      int messageId,
                                      boolean isOutOfRange,
                                      boolean expectFailure) throws InterruptedException, ExecutionException, TimeoutException
    {
        assertEquals(startingTotalMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + (isOutOfRange ? 1 : 0), keyspaceMetricValue(cfs));
        if (expectFailure)
        {
            try
            {
                MessageDelivery response = messageSink.get(10, TimeUnit.MILLISECONDS);
                fail(String.format("Didn't expect any message to be sent, but sent %s to %s in response to %s",
                                   response.message.toString(),
                                   response.to,
                                   response.id));
            }
            catch (TimeoutException e)
            {
                // expected
            }
        }
        else
        {
            MessageDelivery response = messageSink.get(10, TimeUnit.MILLISECONDS);
            assertEquals(MessagingService.Verb.REQUEST_RESPONSE, response.message.verb);
            assertEquals(broadcastAddress, response.message.from);
            assertEquals(messageId, response.id);
            assertEquals(node1, response.to);
        }
    }


    private static long keyspaceMetricValue(ColumnFamilyStore cfs)
    {
        return cfs.keyspace.metric.outOfRangeTokenReads.getCount();
    }

    private ReadCommand command(int key)
    {
        return new StubReadCommand(key, cfs.metadata);
    }

    private static ReadCommand command(CFMetaData metadata)
    {
        return new SinglePartitionReadCommand(false,
                                              0,
                                              false,
                                              metadata,
                                              FBUtilities.nowInSeconds(),
                                              ColumnFilter.all(metadata),
                                              RowFilter.NONE,
                                              DataLimits.NONE,
                                              metadata.partitioner.decorateKey(ByteBufferUtil.bytes(randomInt())),
                                              new ClusteringIndexSliceFilter(Slices.ALL, false),
                                              null);
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        private final CFMetaData cfm;
        private static final ByteBuffer REPAIRED_DIGEST = ByteBufferUtil.bytes(UUID.randomUUID());
        private final boolean isRepairedDigestConclusive;

        StubReadCommand(int key, CFMetaData cfm)
        {
            super(false,
                  0,
                  false,
                  cfm,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(cfm),
                  RowFilter.NONE,
                  DataLimits.NONE,
                  cfm.decorateKey(ByteBufferUtil.bytes(key)),
                  new ClusteringIndexSliceFilter(Slices.ALL, false),
                  null);

            this.cfm = cfm;
            isRepairedDigestConclusive = key >= 0;
        }

        public UnfilteredPartitionIterator executeLocally(ReadOrderGroup orderGroup)
        {
            return EmptyIterators.unfilteredPartition(cfm, false);
        }

        public ByteBuffer getRepairedDataDigest()
        {
            return REPAIRED_DIGEST;
        }

        public boolean isRepairedDataDigestConclusive()
        {
            return isRepairedDigestConclusive;
        }
    }

    private static class StubRangeReadCommand extends PartitionRangeReadCommand
    {
        private final CFMetaData cfm;

        StubRangeReadCommand(Range<Token> range, CFMetaData cfm)
        {
            super(false,
                    0,
                    false,
                    cfm,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(cfm),
                  RowFilter.NONE,
                  DataLimits.NONE,
                  DataRange.forTokenRange(range),
                  null);

            this.cfm = cfm;
        }

        public UnfilteredPartitionIterator executeLocally(ReadOrderGroup orderGroup)
        {
            return EmptyIterators.unfilteredPartition(cfm, false);
        }
    }
}
