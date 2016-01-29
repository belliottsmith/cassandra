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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairSuccess;

/**
 * Basically anti-entropy for christmas patch history data.
 *
 * The flow of operations works like this:
 *  - Send {@link Request} messages to replicas of the relevant ranges, requesting their repair history
 *  - Replicas respond with {@link RangeTimes} in {@link Response} messages
 *  - Responses are collected by {@link HistoryCallback}, which determines the highest repairedAt time for each range
 *  - If all of the replicas agree on the repair data, the history sync is complteted.
 *  - If any of the replicas reported an older time, or didn't report a time for a range, {@link RepairSuccess}
 *    messages are sent to them to make them current with the other replicas.  {@link CorrectionCallback} is used
 *    to block on their positive responses.
 */
public class RepairHistorySyncTask extends AbstractFuture<Object>
{
    private static final Logger logger = LoggerFactory.getLogger(RepairHistorySyncTask.class);

    public static final IVerbHandler verbHandler = new VerbHandler();
    public static final IVersionedSerializer requestSerializer = Request.serializer;
    public static final IVersionedSerializer responseSerializer = Response.serializer;

    private final ColumnFamilyStore cfs;
    private final Map<InetAddressAndPort, Set<Range<Token>>> endpointRanges;
    private final Set<Range<Token>> ranges;

    static class RangeTime
    {
        final Range<Token> range;
        final int time;

        public RangeTime(Range<Token> range, int time)
        {
            this.range = range;
            this.time = time;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RangeTime rangeTime = (RangeTime) o;

            if (time != rangeTime.time) return false;
            return range.equals(rangeTime.range);
        }

        public int hashCode()
        {
            int result = range.hashCode();
            result = 31 * result + time;
            return result;
        }

        public String toString()
        {
            return "{" + range + " -> " + time + '}';
        }

        static final IVersionedSerializer<RangeTime> serializer = new IVersionedSerializer<RangeTime>()
        {
            public void serialize(RangeTime rangeTime, DataOutputPlus out, int version) throws IOException
            {
                Token.serializer.serialize(rangeTime.range.left, out, version);
                Token.serializer.serialize(rangeTime.range.right, out, version);
                out.writeInt(rangeTime.time);
            }

            public RangeTime deserialize(DataInputPlus in, int version) throws IOException
            {
                return new RangeTime(new Range<>(Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version),
                                                 Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version)),
                                     in.readInt());
            }

            public long serializedSize(RangeTime rangeTime, int version)
            {
                long size = 0;
                size += Token.serializer.serializedSize(rangeTime.range.left, version);
                size += Token.serializer.serializedSize(rangeTime.range.right, version);
                size += TypeSizes.sizeof(rangeTime.time);
                return size;
            }
        };
    }

    static class RangeTimes implements Iterable<RangeTime>
    {
        final Map<Range<Token>, RangeTime> times = new HashMap<>();

        public void add(Range<Token> range, int time)
        {
            add(new RangeTime(range, time));
        }

        /**
         * adds range time we don't have one for the given range, or merge it if we do
         */
        public void add(RangeTime rangeTime)
        {
            Range<Token> range = rangeTime.range;
            if (!times.containsKey(range) || times.get(range).time < rangeTime.time)
            {
                times.put(range, rangeTime);
            }
        }

        public void addAll(RangeTimes rangeTimes)
        {
            for (RangeTime rangeTime: rangeTimes)
            {
                add(rangeTime);
            }
        }

        public boolean isEmpty()
        {
            return times.isEmpty();
        }

        public int size()
        {
            return times.size();
        }

        public Iterator<RangeTime> iterator()
        {
            return times.values().iterator();
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RangeTimes that = (RangeTimes) o;

            return times.equals(that.times);
        }

        public int hashCode()
        {
            return times.hashCode();
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            boolean first = true;
            for (RangeTime time: times.values())
            {
                if (!first)
                {
                    sb.append(", ");
                }
                sb.append(time);
                first = false;
            }
            sb.append(']');
            return sb.toString();
        }

        /**
         * Calculates the corrections needed to make this equal to reference for the given ranges
         * assumes keys are a subset of reference keys
         */
        public RangeTimes calculateCorrections(RangeTimes references, Set<Range<Token>> ranges)
        {
            RangeTimes corrections = new RangeTimes();
            for (RangeTime reference: references)
            {
                if (reference.range.intersects(ranges))
                {
                    if (!times.containsKey(reference.range) || times.get(reference.range).time < reference.time)
                    {
                        corrections.add(reference);
                    }
                }
            }
            return corrections;
        }

        static final IVersionedSerializer<RangeTimes> serializer = new IVersionedSerializer<RangeTimes>()
        {
            public void serialize(RangeTimes rangeTimes, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(rangeTimes.size());
                for (RangeTime rangeTime: rangeTimes)
                {
                    RangeTime.serializer.serialize(rangeTime, out, version);
                }
            }

            public RangeTimes deserialize(DataInputPlus in, int version) throws IOException
            {
                RangeTimes rangeTimes = new RangeTimes();
                int numRanges = in.readInt();
                for (int i=0; i<numRanges; i++)
                {
                    RangeTime rangeTime = RangeTime.serializer.deserialize(in, version);
                    Preconditions.checkState(!rangeTimes.times.containsKey(rangeTime.range));
                    rangeTimes.add(rangeTime);
                }
                return rangeTimes;
            }

            public long serializedSize(RangeTimes rangeTimes, int version)
            {
                long size = TypeSizes.sizeof(rangeTimes.size());
                for (RangeTime rangeTime: rangeTimes)
                {
                    size += RangeTime.serializer.serializedSize(rangeTime, version);
                }
                return size;
            }
        };
    }

    static class Request
    {
        public final String keyspace;
        public final String table;
        public final Set<Range<Token>> ranges;

        public Request(String keyspace, String table, Set<Range<Token>> ranges)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.ranges = ranges;
        }

        public Request(ColumnFamilyStore cfs, Set<Range<Token>> ranges)
        {
            this(cfs.keyspace.getName(), cfs.name, ranges);
        }

        Message<Request> createMessage()
        {
            return Message.builder(Verb.APPLE_QUERY_REPAIR_HISTORY_REQ, this).build();
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (!keyspace.equals(request.keyspace)) return false;
            if (!table.equals(request.table)) return false;
            return ranges.equals(request.ranges);
        }

        public int hashCode()
        {
            int result = keyspace.hashCode();
            result = 31 * result + table.hashCode();
            result = 31 * result + ranges.hashCode();
            return result;
        }

        public String toString()
        {
            return "RepairHistorySyncTask.Request{" + keyspace + '.' + table + ", " + ranges + '}';
        }

        static final IVersionedSerializer<Request> serializer = new IVersionedSerializer<Request>()
        {
            public void serialize(Request request, DataOutputPlus out, int version) throws IOException
            {
                out.writeUTF(request.keyspace);
                out.writeUTF(request.table);
                out.writeInt(request.ranges.size());
                for (Range<Token> range: request.ranges)
                {
                    AbstractBounds.tokenSerializer.serialize(range, out, version);
                }
            }

            public Request deserialize(DataInputPlus in, int version) throws IOException
            {
                String keyspace = in.readUTF();
                String table = in.readUTF();
                int size = in.readInt();
                Set<Range<Token>> ranges = new HashSet<>();
                for (int i=0; i<size; i++)
                {
                    ranges.add((Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version));
                }
                return new Request(keyspace, table, ranges);
            }

            public long serializedSize(Request request, int version)
            {
                long size = 0;
                size += TypeSizes.sizeof(request.keyspace);
                size += TypeSizes.sizeof(request.table);
                size += TypeSizes.sizeof(request.ranges.size());
                for (Range<Token> range: request.ranges)
                {
                    size += AbstractBounds.tokenSerializer.serializedSize(range, version);
                }
                return size;
            }
        };
    }

    static class Response
    {
        public final RangeTimes history;

        public Response(RangeTimes history)
        {
            this.history = history;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Response response = (Response) o;

            return history.equals(response.history);
        }

        public int hashCode()
        {
            return history.hashCode();
        }

        public String toString()
        {
            return "RepairHistorySyncTask.Response{" + history + '}';
        }

        static final IVersionedSerializer<Response> serializer = new IVersionedSerializer<Response>()
        {
            public void serialize(Response response, DataOutputPlus out, int version) throws IOException
            {
                RangeTimes.serializer.serialize(response.history, out, version);
            }

            public Response deserialize(DataInputPlus in, int version) throws IOException
            {
                return new Response(RangeTimes.serializer.deserialize(in, version));
            }

            public long serializedSize(Response response, int version)
            {
                return RangeTimes.serializer.serializedSize(response.history, version);
            }
        };
    }

    static class HistoryCallback extends AbstractFuture<Map<InetAddressAndPort, RangeTimes>> implements RequestCallback<Response>
    {
        private static final Logger syncCallbackLogger = LoggerFactory.getLogger(HistoryCallback.class);

        // guarded by synchronized(this)
        private final Map<InetAddressAndPort, RangeTimes> data = new HashMap<>();
        private final int expectedResponses;

        public HistoryCallback(int expectedResponses)
        {
            this.expectedResponses = expectedResponses;
        }

        public boolean invokeOnFailure()
        {
            return true;
        }

        public void onFailure(InetAddress from)
        {
            setException(new RuntimeException(String.format("%s failed", from)));
        }

        public synchronized void onResponse(Message<Response> msg)
        {
            syncCallbackLogger.debug("Got {} from {}", msg.payload, msg.header.from);
            data.put(msg.header.from, msg.payload.history);
            if (data.size() == expectedResponses)
            {
                set(ImmutableMap.copyOf(data));
            }
        }
    }

    static class VerbHandler implements IVerbHandler<Request>
    {
        private static final Logger verbLogger = LoggerFactory.getLogger(VerbHandler.class);

        protected void sendResponse(Response response, Message<Request> request)
        {
            MessagingService.instance().send(request.responseWith(response), request.from());
        }

        public void doVerb(Message<Request> message) throws IOException
        {
            verbLogger.debug("received {} from {}", message.payload, message.header.from);
            Request request = message.payload;
            ColumnFamilyStore cfs = Keyspace.open(request.keyspace).getColumnFamilyStore(request.table);

            RangeTimes rangeTimes = new RangeTimes();
            for (Map.Entry<Range<Token>, Integer> entry: cfs.getRepairHistoryForRanges(request.ranges).entrySet())
            {
                rangeTimes.add(entry.getKey(), entry.getValue());
            }

            sendResponse(new Response(rangeTimes), message);
        }
    }

    static class CorrectionCallback extends AbstractFuture<Object> implements RequestCallback<Object>
    {
        private static final Logger correctionCallbackLogger = LoggerFactory.getLogger(CorrectionCallback.class);

        private final int expectedResponses;
        private int responsesReceived = 0;

        public CorrectionCallback(int expectedResponses)
        {
            this.expectedResponses = expectedResponses;
        }

        public void onFailure(InetAddressAndPort from)
        {
            setException(new RuntimeException(String.format("%s failed", from)));
        }

        public boolean invokeOnFailure()
        {
            return true;
        }

        public synchronized void onResponse(Message<Object> msg)
        {
            correctionCallbackLogger.debug("Got response from {}", msg.header.from);
            responsesReceived++;
            if (responsesReceived == expectedResponses)
            {
                set(new Object());
            }
        }
    }

    public RepairHistorySyncTask(ColumnFamilyStore cfs, Map<InetAddressAndPort, Set<Range<Token>>> endpointRanges)
    {
        this.cfs = cfs;
        this.endpointRanges = endpointRanges;
        ranges = ImmutableSet.copyOf(Iterables.concat(endpointRanges.values()));
    }

    protected void sendRequest(Request request, InetAddressAndPort destination, RequestCallback callback)
    {
        MessagingService.instance().sendWithCallback(request.createMessage(), destination, callback);
    }

    protected void sendCorrection(RepairSuccess correction, InetAddressAndPort destination, RequestCallback callback)
    {
        logger.info("Sending correction {} to {}", correction, destination);
        MessagingService.instance().sendWithCallback(Message.out(Verb.APPLE_REPAIR_SUCCESS_REQ, correction), destination, callback);
    }

    private ListenableFuture<Object> processHistory(Map<InetAddressAndPort, RangeTimes> history)
    {
        RangeTimes referenceTimes = new RangeTimes();
        for (RangeTimes times: history.values())
        {
            referenceTimes.addAll(times);
        }

        int numCorrections = 0;
        Map<InetAddressAndPort, RangeTimes> corrections = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, RangeTimes> entry: history.entrySet())
        {
            RangeTimes endpointCorrections = entry.getValue().calculateCorrections(referenceTimes, endpointRanges.get(entry.getKey()));
            if (!endpointCorrections.isEmpty())
            {
                numCorrections += endpointCorrections.size();
                corrections.put(entry.getKey(), endpointCorrections);
            }
        }

        if (corrections.isEmpty())
        {
            logger.debug("Repair history already in sync for {}", this);
            return Futures.immediateFuture(new Object());
        }
        else
        {
            CorrectionCallback callback = new CorrectionCallback(numCorrections);
            for (Map.Entry<InetAddressAndPort, RangeTimes> entry: corrections.entrySet())
            {
                InetAddressAndPort endpoint = entry.getKey();
                for (RangeTime rangeTime: entry.getValue())
                {
                    RepairSuccess correction = new RepairSuccess(cfs.keyspace.getName(),
                                                                 cfs.name,
                                                                 Collections.singleton(rangeTime.range),
                                                                 TimeUnit.SECONDS.toMillis(rangeTime.time));
                    sendCorrection(correction, endpoint, callback);
                }
            }

            return callback;
        }
    }

    public void execute()
    {
        logger.debug("Syncing repair history for {}", this);

        HistoryCallback callback = new HistoryCallback(endpointRanges.size());
        for (Map.Entry<InetAddressAndPort, Set<Range<Token>>> entry: endpointRanges.entrySet())
        {
            Request request = new Request(cfs, entry.getValue());
            sendRequest(request, entry.getKey(), callback);
        }

        AsyncFunction<Map<InetAddressAndPort, RangeTimes>, Object> processFunc = this::processHistory;
        Futures.addCallback(Futures.transformAsync(callback, processFunc, MoreExecutors.directExecutor()), new FutureCallback<Object>()
        {
            public void onSuccess(@Nullable Object result)
            {
                logger.debug("Completed repair history sync for {}", RepairHistorySyncTask.this);
                RepairHistorySyncTask.this.set(result);
            }

            public void onFailure(Throwable t)
            {
                logger.error("Repair history sync failed for for {}", RepairHistorySyncTask.this, t);
                RepairHistorySyncTask.this.setException(t);
            }
        }, MoreExecutors.directExecutor());
    }

    public String toString()
    {
        Set<InetAddressAndPort> endpoints = endpointRanges.keySet();
        return "RepairHistorySyncTask{" +
               cfs.metadata.keyspace + '.' + cfs.metadata.name +
               ", endpoints=" + endpoints +
               ", ranges=" + ranges + '}';
    }
}
