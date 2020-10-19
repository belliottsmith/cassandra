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

package org.apache.cassandra.service.paxos.cleanup;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import com.google.common.util.concurrent.AbstractFuture;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.VoidSerializer;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_CLEANUP_COMPLETE;

public class PaxosCleanupComplete extends AbstractFuture<Void> implements IAsyncCallbackWithFailure<Void>, Runnable
{
    private final Set<InetAddress> waitingResponse;
    final UUID cfId;
    final Collection<Range<Token>> ranges;
    final UUID lowBound;
    final boolean skippedReplicas;

    PaxosCleanupComplete(Collection<InetAddress> endpoints, UUID cfId, Collection<Range<Token>> ranges, UUID lowBound, boolean skippedReplicas)
    {
        this.waitingResponse = new HashSet<>(endpoints);
        this.cfId = cfId;
        this.ranges = ranges;
        this.lowBound = lowBound;
        this.skippedReplicas = skippedReplicas;
    }

    public void run()
    {
        Request request = !skippedReplicas ? new Request(cfId, lowBound, ranges)
                                           : new Request(cfId, Ballot.none(), Collections.emptyList());
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_CLEANUP_COMPLETE, request, serializer);
        for (InetAddress endpoint : waitingResponse)
            MessagingService.instance().sendRRWithFailure(message, endpoint, this);
    }

    public void onFailure(InetAddress from)
    {
        setException(new PaxosCleanupException("Timed out waiting on response from " + from));
    }

    public synchronized void response(MessageIn<Void> msg)
    {
        if (isDone())
            return;

        if (!waitingResponse.remove(msg.from))
            throw new IllegalArgumentException("Received unexpected response from " + msg.from);

        if (waitingResponse.isEmpty())
            set(null);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    static class Request
    {
        final UUID cfId;
        final UUID lowBound;
        final Collection<Range<Token>> ranges;

        Request(UUID cfId, UUID lowBound, Collection<Range<Token>> ranges)
        {
            this.cfId = cfId;
            this.ranges = ranges;
            this.lowBound = lowBound;
        }
    }

    public static final IVersionedSerializer<Request> serializer = new IVersionedSerializer<Request>()
    {
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.cfId, out, version);
            UUIDSerializer.serializer.serialize(request.lowBound, out, version);
            out.writeInt(request.ranges.size());
            for (Range<Token> rt : request.ranges)
                AbstractBounds.tokenSerializer.serialize(rt, out, version);
        }

        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
            UUID lowBound = UUIDSerializer.serializer.deserialize(in, version);
            int numRanges = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>();
            for (int i = 0; i < numRanges; i++)
            {
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, MessagingService.globalPartitioner(), version);
                ranges.add(range);
            }
            return new Request(cfId, lowBound, ranges);
        }

        public long serializedSize(Request request, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(request.cfId, version);
            size += UUIDSerializer.serializer.serializedSize(request.lowBound, version);
            size += TypeSizes.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(range, version);
            return size;
        }
    };

    public static final IVerbHandler<Request> verbHandler = (message, id) -> {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(message.payload.cfId);
        cfs.onPaxosRepairComplete(message.payload.ranges, message.payload.lowBound);
        MessageOut<Void> msg = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, null, VoidSerializer.serializer);
        MessagingService.instance().sendReply(msg, id, message.from);
    };
}
