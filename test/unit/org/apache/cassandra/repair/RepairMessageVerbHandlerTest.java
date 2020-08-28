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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;

import static org.apache.cassandra.utils.TokenRangeTestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RepairMessageVerbHandlerTest
{

    private static final String TEST_NAME = "repair_message_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";
    private static List<UUID> cfIds;

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        StorageService.instance.initServer(0);
        cfIds = Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata.cfId);
        ActiveRepairService.instance.consistent.local.start();
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

    /*******************************************************************************
     *
     * PrepareMessage handling tests
     *
     ******************************************************************************/

    @Test
    public void testPrepareWithAllRequestedRangesWithinOwned() throws Exception
    {
        setLocalTokens(100);
        PrepareMessage prepare = prepareMsg(generateRanges(10, 20));
        tryPrepareExpectingSuccess(prepare);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(prepare);
    }

    @Test
    public void testPrepareWithAllRequestedRangesOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        PrepareMessage prepare = prepareMsg(generateRanges(110, 120));
        tryPrepareExpectingSuccess(prepare);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(prepare);
    }

    @Test
    public void testPrepareWithSomeRequestedRangesOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        PrepareMessage prepare = prepareMsg(generateRanges(10, 20, 110, 120));
        tryPrepareExpectingSuccess(prepare);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(prepare);
    }

    @Test
    public void testValidationRequestWithRequestedRangeWithinOwned() throws Exception
    {
        setLocalTokens(100);
        ValidationRequest request = validationMsg(generateRange(10, 20));
        tryValidationExpectingSuccess(request, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryValidationExpectingSuccess(request, false);
    }

    @Test
    public void testValidationRequestWithRequestedRangeOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        ValidationRequest request = validationMsg(generateRange(110, 120));
        tryValidationExpectingFailure(request);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryValidationExpectingSuccess(request, true);
    }

    @Test
    public void testValidationRequestWithRequestedRangeOverlappingOwned() throws Exception
    {
        setLocalTokens(100);
        ValidationRequest request = validationMsg(generateRange(10, 120));
        tryValidationExpectingFailure(request);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryValidationExpectingSuccess(request, true);
    }

    private static void tryValidationExpectingFailure(ValidationRequest request) throws Exception
    {
        tryValidation(request, true, false);
    }

    private static void tryValidationExpectingSuccess(ValidationRequest request, boolean isOutOfRange) throws Exception
    {
        tryValidation(request, isOutOfRange, true);
    }

    private static void tryValidation(ValidationRequest request, boolean isOutOfRange, boolean expectSuccess) throws Exception
    {
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        MessagingService.instance().clearMessageSinks();
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        RepairMessageVerbHandler handler = new RepairMessageVerbHandler();
        int messageId = randomInt();
        MessageIn<RepairMessage> message = MessageIn.create(node1,
                                                            request,
                                                            EMPTY_PARAMS,
                                                            MessagingService.Verb.REPAIR_MESSAGE,
                                                            MessagingService.current_version);

        handler.doVerb(message, messageId);

        MessageDelivery response = messageSink.get(1, TimeUnit.SECONDS);
        assertEquals(MessagingService.Verb.REPAIR_MESSAGE, response.message.verb);
        assertEquals(broadcastAddress, response.message.from);
        assertEquals(node1, response.to);
        assertEquals(RepairMessage.Type.VALIDATION_COMPLETE, ((RepairMessage) response.message.payload).messageType);
        ValidationComplete completion = (ValidationComplete) response.message.payload;
        assertEquals(expectSuccess, completion.success());
        assertEquals(startMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static void tryPrepareExpectingSuccess(PrepareMessage prepare) throws Exception
    {
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        MessagingService.instance().clearMessageSinks();
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        RepairMessageVerbHandler handler = new RepairMessageVerbHandler();
        int messageId = randomInt();
        MessageIn<RepairMessage> message = MessageIn.create(node1,
                                                            prepare,
                                                            EMPTY_PARAMS,
                                                            MessagingService.Verb.REPAIR_MESSAGE,
                                                            MessagingService.current_version);

        handler.doVerb(message, messageId);

        MessageDelivery response = messageSink.get(100, TimeUnit.MILLISECONDS);
        assertEquals(MessagingService.Verb.INTERNAL_RESPONSE, response.message.verb);
        assertEquals(broadcastAddress, response.message.from);
        assertEquals(messageId, response.id);
        assertEquals(node1, response.to);
        assertFalse(response.message.parameters.containsKey(MessagingService.FAILURE_RESPONSE_PARAM));
        assertEquals(startMetricCount, StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static PrepareMessage prepareMsg(Collection<Range<Token>> ranges)
    {
        return new PrepareMessage(uuid(), cfIds, ranges, false, ActiveRepairService.UNREPAIRED_SSTABLE, true, PreviewKind.NONE);
    }

    private static ValidationRequest validationMsg(Range<Token> range)
    {
        UUID parentId = uuid();
        List<ColumnFamilyStore> stores = cfIds.stream()
                                                .map(Schema.instance::getColumnFamilyStoreInstance)
                                                .collect(Collectors.toList());
        ActiveRepairService.instance.registerParentRepairSession(parentId,
                                                                 node1,
                                                                 stores,
                                                                 Collections.singleton(range),
                                                                 false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                 true,
                                                                 PreviewKind.NONE);
        return new ValidationRequest(new RepairJobDesc(parentId, uuid(), KEYSPACE, TABLE, Collections.singleton(range)),
                                     randomInt());
    }
}

