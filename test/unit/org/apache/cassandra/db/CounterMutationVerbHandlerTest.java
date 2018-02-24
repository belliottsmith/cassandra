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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TokenRangeTestUtil.*;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CounterMutationVerbHandlerTest
{
    private static final String KEYSPACE = "CounterCacheTest";
    private static final String TABLE = "Counter1";

    private CounterMutationVerbHandler handler;
    private ColumnFamilyStore cfs;
    private long startingTotalMetricCount;
    private long startingKeyspaceMetricCount;

    @BeforeClass
    public static void init() throws Exception
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.counterCFMD(KEYSPACE, TABLE));
        StorageService.instance.initServer(0);
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
        cfs.truncateBlocking();
        startingKeyspaceMetricCount = keyspaceMetricValue();
        startingTotalMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();

        handler = new CounterMutationVerbHandler();
    }

    @Test
    public void acceptMutationForNaturalEndpoint() throws Exception
    {
        int messageId = randomInt();
        int value = randomInt();
        int key = 30;
        CounterMutation mutation = mutation(key, value);
        handler.doVerb(MessageIn.create(node1,
                                        mutation,
                                        EMPTY_PARAMS,
                                        MessagingService.Verb.MUTATION,
                                        MessagingService.current_version),
                       messageId);
        // unlike non-counter mutations, we can't verify the response message for a successful write.
        // acting as the leader for the mutation, we'll try to forward the writes to the other replicas
        // but this will fail as the other node isn't really there. The response message is written
        // by the callback to these messages so it will never get sent. So the best we can do is to check
        // that the mutation was actually applied locally. When a counter mutation is rejected by the verb
        // handler we *can* verify the failure response message is sent.
        verifyWrite(key, value);
        assertEquals(startingTotalMetricCount, StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount, keyspaceMetricValue());
    }

    @Test
    public void acceptMutationForPendingEndpoint() throws Exception
    {
        // remove localhost from TM and add it back as pending
        StorageService.instance.getTokenMetadata().removeEndpoint(broadcastAddress);
        Multimap<Range<Token>, InetAddress> pending = HashMultimap.create();
        pending.put(new Range<>(bytesToken(0), bytesToken(100)), broadcastAddress);
        StorageService.instance.getTokenMetadata().setPendingRangesUnsafe(KEYSPACE, pending);

        int messageId = randomInt();
        int value = randomInt();
        int key = 50;
        CounterMutation mutation = mutation(key, value);
        handler.doVerb(MessageIn.create(node1,
                                        mutation,
                                        EMPTY_PARAMS,
                                        MessagingService.Verb.MUTATION,
                                        MessagingService.current_version),
                       messageId);
        verifyWrite(key, value);
        assertEquals(startingTotalMetricCount, StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount, keyspaceMetricValue());
    }

    @Test
    public void rejectMutationForTokenOutOfRange() throws Exception
    {
        // reject a mutation for a token the node neither owns nor is pending
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int value = randomInt();
        int key = 200;
        CounterMutation mutation = mutation(key, value);
        handler.doVerb(MessageIn.create(node1,
                                        mutation,
                                        EMPTY_PARAMS,
                                        MessagingService.Verb.MUTATION,
                                        MessagingService.current_version),
                       messageId);
        verifyFailureResponse(messageSink, messageId);
        assertEquals(startingTotalMetricCount + 1, StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + 1, keyspaceMetricValue());
    }

    @Test
    public void acceptMutationIfRejectionNotEnabled() throws Exception
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        int messageId = randomInt();
        int value = randomInt();
        int key = 500;
        CounterMutation mutation = mutation(key, value);
        // the node which is the actual natural endpoint for this mutation is not a real
        // node, but if we write at CL.ANY we'll generate a hint for it and StorageProxy's
        // counterWriterPerformer will blindly apply the mutation so we can verify it locally
        handler.doVerb(MessageIn.create(node1,
                                        mutation,
                                        EMPTY_PARAMS,
                                        MessagingService.Verb.MUTATION,
                                        MessagingService.current_version),
                       messageId);

        verifyWrite(key, value);
        assertEquals(startingTotalMetricCount + 1, StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + 1, keyspaceMetricValue());
    }

    private void verifyWrite(int key, int value)
    {
        ReadCommand read = Util.cmd(cfs, bytes(key)).build();
        ColumnDefinition col = cfs.metadata.getColumnDefinition(bytes("val"));
        assertEquals((long)value, CounterContext.instance().total(Util.getOnlyRow(read).getCell(col).value()));
    }


    private static void verifyFailureResponse(ListenableFuture<MessageDelivery> messageSink, int messageId ) throws Exception
    {
        MessageDelivery response = messageSink.get(100, TimeUnit.MILLISECONDS);
        assertEquals(MessagingService.Verb.REQUEST_RESPONSE, response.message.verb);
        assertEquals(broadcastAddress, response.message.from);
        assertTrue(response.message.parameters.containsKey(MessagingService.FAILURE_RESPONSE_PARAM));
        assertEquals(messageId, response.id);
        assertEquals(node1, response.to);
    }

    private CounterMutation mutation(int key, int columnValue)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        DecoratedKey dk = cfm.decorateKey(bytes(key));
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(bytes("val"));
        ByteBuffer val = CounterContext.instance().createLocal(columnValue);
        Cell counterCell = BufferCell.live(cfs.metadata, cDef, FBUtilities.timestampMicros(), val);
        Row row = BTreeRow.singleCellRow(cfs.metadata.comparator.make("clustering_1"), counterCell);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, dk, row);
        return new CounterMutation(new Mutation(update), ConsistencyLevel.ANY);
    }

    private long keyspaceMetricValue()
    {
        return cfs.keyspace.metric.outOfRangeTokenWrites.getCount();
    }
}
