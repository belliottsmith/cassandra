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

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairSuccess;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.repair.RepairHistorySyncTask.*;

public class RepairHistorySyncTaskTest
{

    private static final InetAddressAndPort EP1;
    private static final InetAddressAndPort EP2;

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.0.0.1");
            EP2 = InetAddressAndPort.getByName("127.0.0.2");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    static Token tk(int t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    static Range<Token> range(int left, int right)
    {
        return new Range<>(tk(left), tk(right));
    }

    static Set<Range<Token>> ranges(Range<Token>... r)
    {
        return ImmutableSet.copyOf(r);
    }

    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();
        TableMetadata cfm = CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", KEYSPACE, TABLE), KEYSPACE).build();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.local(), cfm);

        DatabaseDescriptor.setChristmasPatchEnabled();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).skipRFCheckForXmasPatch();
    }

    @Before
    public void setUp() throws Exception
    {
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).clearLastSucessfulRepairUnsafe();
        SystemKeyspace.clearRepairedRanges(KEYSPACE, TABLE);
    }

    @Test
    public void noCorrectionCalculation() throws Exception
    {
        Set<Range<Token>> ranges = Sets.newHashSet(range(200, 400));
        RangeTimes referenceHistory = new RangeTimes();
        referenceHistory.add(range(100, 200), 5);
        referenceHistory.add(range(200, 300), 6);
        referenceHistory.add(range(300, 400), 7);

        RangeTimes nodeHistory = new RangeTimes();
        nodeHistory.add(range(200, 300), 6);
        nodeHistory.add(range(300, 400), 7);

        RangeTimes corrections = nodeHistory.calculateCorrections(referenceHistory, ranges);
        Assert.assertTrue(corrections.isEmpty());
    }

    @Test
    public void missingEntryCalculation() throws Exception
    {
        Set<Range<Token>> ranges = Sets.newHashSet(range(200, 400));
        RangeTimes referenceHistory = new RangeTimes();
        referenceHistory.add(range(100, 200), 5);
        referenceHistory.add(range(200, 300), 6);
        referenceHistory.add(range(300, 400), 7);

        RangeTimes nodeHistory = new RangeTimes();
        nodeHistory.add(range(200, 300), 6);

        RangeTimes expected = new RangeTimes();
        expected.add(range(300, 400), 7);

        RangeTimes corrections = nodeHistory.calculateCorrections(referenceHistory, ranges);
        Assert.assertEquals(expected, corrections);
    }

    @Test
    public void oldEntryCalculation() throws Exception
    {
        Set<Range<Token>> ranges = Sets.newHashSet(range(200, 400));
        RangeTimes referenceHistory = new RangeTimes();
        referenceHistory.add(range(100, 200), 5);
        referenceHistory.add(range(200, 300), 6);
        referenceHistory.add(range(300, 400), 7);

        RangeTimes nodeHistory = new RangeTimes();
        nodeHistory.add(range(200, 300), 6);
        nodeHistory.add(range(300, 400), 6);

        RangeTimes expected = new RangeTimes();
        expected.add(range(300, 400), 7);

        RangeTimes corrections = nodeHistory.calculateCorrections(referenceHistory, ranges);
        Assert.assertEquals(expected, corrections);
    }

    private static Message<Request> request(InetAddressAndPort endpoint, ColumnFamilyStore cfs, Set<Range<Token>> ranges)
    {
        return Message.builder(Verb.APPLE_QUERY_REPAIR_HISTORY_REQ, new Request(cfs, ranges)).from(endpoint).build();
    }

    private static Message<Response> response(InetAddressAndPort endpoint, RangeTimes times)
    {
        return Message.builder(Message.internalResponse(Verb.INTERNAL_RSP, new Response(times))).from(endpoint).build();
    }

    private static Message correctionResponse(InetAddressAndPort endpoint)
    {
        return Message.builder(Verb.INTERNAL_RSP, NoPayload.noPayload).from(endpoint).build();
    }

    static class InstrumentedVerbHandler extends VerbHandler
    {
        Map<InetAddressAndPort, Response> responses = new HashMap<>();

        @Override
        protected void sendResponse(Response response, Message<Request> request)
        {
            assert !responses.containsKey(request.from());
            responses.put(request.from(), response);
        }
    }

    @Test
    public void verbHandler() throws Exception
    {
        Range<Token> testRange = range(200, 400);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.updateLastSuccessfulRepair(testRange, 600 * 1000);

        InstrumentedVerbHandler handler = new InstrumentedVerbHandler();
        handler.doVerb(request(EP1, cfs, Sets.newHashSet(testRange)));

        Map<InetAddressAndPort, Response> expectedResponses = new HashMap<>();
        RangeTimes responseTimes = new RangeTimes();
        responseTimes.add(testRange, 600);
        expectedResponses.put(EP1, new Response(responseTimes));

        Assert.assertEquals(expectedResponses, handler.responses);
    }

    static class InstrumentedSyncTask extends RepairHistorySyncTask
    {
        public InstrumentedSyncTask(ColumnFamilyStore cfs, Map<InetAddressAndPort, Set<Range<Token>>> endpointRanges)
        {
            super(cfs, endpointRanges);
        }

        public final Map<InetAddressAndPort, Request> requestsSent = new HashMap<>();
        public volatile HistoryCallback historyCallback = null;

        @Override
        protected void sendRequest(Request request, InetAddressAndPort destination, RequestCallback callback)
        {
            if (historyCallback == null)
            {
                assert callback instanceof HistoryCallback;
                historyCallback = (HistoryCallback) callback;
            }
            else
            {
                assert callback == historyCallback;
            }

            assert !requestsSent.containsKey(destination);
            requestsSent.put(destination, request);
        }

        public final Map<InetAddressAndPort, RepairSuccess> correctionsSent = new HashMap<>();
        public volatile CorrectionCallback correctionCallback = null;

        @Override
        protected void sendCorrection(RepairSuccess correction, InetAddressAndPort destination, RequestCallback callback)
        {
            if (correctionCallback == null)
            {
                assert callback instanceof CorrectionCallback;
                correctionCallback = (CorrectionCallback) callback;
            }
            else
            {
                assert callback == correctionCallback;
            }

            assert !correctionsSent.containsKey(destination);
            correctionsSent.put(destination, correction);
        }
    }

    @Test
    public void task() throws Exception
    {
        Range<Token> testRange = range(200, 400);
        Set<Range<Token>> testRangeSet = ImmutableSet.of(testRange);

        Map<InetAddressAndPort, Set<Range<Token>>> endpointRanges = new HashMap<>();
        endpointRanges.put(EP1, testRangeSet);
        endpointRanges.put(EP2, testRangeSet);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        InstrumentedSyncTask task = new InstrumentedSyncTask(cfs, endpointRanges);

        Assert.assertFalse(task.isDone());
        Assert.assertTrue(task.requestsSent.isEmpty());
        Assert.assertNull(task.historyCallback);

        Assert.assertTrue(task.correctionsSent.isEmpty());
        Assert.assertNull(task.correctionCallback);

        // send the requests and check they were sent properly
        task.execute();
        Assert.assertFalse(task.isDone());
        Assert.assertNotNull(task.historyCallback);

        Map<InetAddressAndPort, Request> expectedRequests = new HashMap<>();
        expectedRequests.put(EP1, new Request(cfs, testRangeSet));
        expectedRequests.put(EP2, new Request(cfs, testRangeSet));
        Assert.assertEquals(expectedRequests, task.requestsSent);

        // start sending responses
        Assert.assertFalse(task.historyCallback.isDone());
        Assert.assertTrue(task.correctionsSent.isEmpty());
        RangeTimes ep1Times = new RangeTimes();
        ep1Times.add(new RangeTime(testRange, 500));

        task.historyCallback.onResponse(response(EP1, ep1Times));
        Assert.assertFalse(task.historyCallback.isDone());

        RangeTimes ep2Times = new RangeTimes();
        ep2Times.add(new RangeTime(testRange, 600));
        task.historyCallback.onResponse(response(EP2, ep2Times));
        Assert.assertTrue(task.historyCallback.isDone());

        // check that the right corrections were sent out
        Assert.assertEquals(1, task.correctionsSent.size());
        Map<InetAddressAndPort, RepairSuccess> expectedCorrections = new HashMap<>();
        expectedCorrections.put(EP1, new RepairSuccess(KEYSPACE, TABLE, testRangeSet, 600 * 1000));
        Assert.assertEquals(expectedCorrections, task.correctionsSent);

        // respond to the correction
        task.correctionCallback.onResponse(correctionResponse(EP1));
        Assert.assertTrue(task.correctionCallback.isDone());
        Assert.assertTrue(task.isDone());

    }
}
