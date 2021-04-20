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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class PaxosCleanupRequest
{
    public final UUID session;
    public final UUID cfId;
    public final Collection<Range<Token>> ranges;
    public final UUID before;

    static Collection<Range<Token>> rangesOrMin(Collection<Range<Token>> ranges)
    {
        if (ranges != null && !ranges.isEmpty())
            return ranges;

        Token min = DatabaseDescriptor.getPartitioner().getMinimumToken();
        return Collections.singleton(new Range<>(min, min));
    }

    public PaxosCleanupRequest(UUID session, UUID cfId, Collection<Range<Token>> ranges, UUID before)
    {
        this.session = session;
        this.cfId = cfId;
        this.ranges = rangesOrMin(ranges);
        this.before = before;
    }

    public static final IVerbHandler<PaxosCleanupRequest> verbHandler = (message, id) -> {
        PaxosCleanupRequest request = message.payload;

        PaxosCleanupLocalCoordinator coordinator = PaxosCleanupLocalCoordinator.create(request);

        Futures.addCallback(coordinator, new FutureCallback<PaxosCleanupResponse>()
        {
            public void onSuccess(@Nullable PaxosCleanupResponse finished)
            {
                MessageOut<PaxosCleanupResponse> reply = new MessageOut<>(MessagingService.Verb.APPLE_PAXOS_CLEANUP_RESPONSE,
                                                                          finished,
                                                                          PaxosCleanupResponse.serializer);
                MessagingService.instance().sendOneWay(reply, message.from);
            }

            public void onFailure(Throwable throwable)
            {
                MessageOut<PaxosCleanupResponse> reply = new MessageOut<>(MessagingService.Verb.APPLE_PAXOS_CLEANUP_RESPONSE,
                                                                          PaxosCleanupResponse.failed(request.session, throwable.getMessage()),
                                                                          PaxosCleanupResponse.serializer);
                MessagingService.instance().sendOneWay(reply, message.from);
            }
        });

        coordinator.start();
    };

    public static final IVersionedSerializer<PaxosCleanupRequest> serializer = new IVersionedSerializer<PaxosCleanupRequest>()
    {
        public void serialize(PaxosCleanupRequest completer, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(completer.session, out, version);
            UUIDSerializer.serializer.serialize(completer.cfId, out, version);
            out.writeInt(completer.ranges.size());
            for (Range<Token> range: completer.ranges)
                AbstractBounds.tokenSerializer.serialize(range, out, version);
            UUIDSerializer.serializer.serialize(completer.before, out, version);
        }

        public PaxosCleanupRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID session = UUIDSerializer.serializer.deserialize(in, version);
            UUID cfId = UUIDSerializer.serializer.deserialize(in, version);

            int numRanges = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(numRanges);
            for (int i=0; i<numRanges; i++)
            {
                ranges.add((Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version));
            }
            UUID before = UUIDSerializer.serializer.deserialize(in, version);
            return new PaxosCleanupRequest(session, cfId, ranges, before);
        }

        public long serializedSize(PaxosCleanupRequest completer, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(completer.session, version);
            size += UUIDSerializer.serializer.serializedSize(completer.cfId, version);
            size += TypeSizes.sizeof(completer.ranges.size());
            for (Range<Token> range: completer.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(range, version);
            size += UUIDSerializer.serializer.serializedSize(completer.before, version);
            return size;
        }
    };
}
