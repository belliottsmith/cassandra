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

package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_PREPARE_REFRESH_REQ;
import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.net.MessagingService.verbStages;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

/**
 * Nodes that have promised in response to our prepare, may be missing the latestCommit, meaning we cannot be sure the
 * prior round has been committed to the necessary quorum of participants, so that it will be visible to future quorums.
 *
 * To resolve this problem, we submit the latest commit we have seen, and wait for confirmation before continuing
 * (verifying that we are still promised in the process).
 */
public class PaxosPrepareRefresh implements IAsyncCallbackWithFailure<PaxosPrepareRefresh.Response>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosPrepareRefresh.class);

    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();

    interface Callbacks
    {
        void onRefreshFailure(InetAddress from, boolean isTimeout);
        void onRefreshSuccess(UUID isSupersededBy, InetAddress from);
    }

    private final MessageOut<Request> send;
    private final Callbacks callbacks;

    public PaxosPrepareRefresh(UUID prepared, Paxos.Participants participants, Committed latestCommitted, Callbacks callbacks)
    {
        this.callbacks = callbacks;
        this.send = new MessageOut<>(APPLE_PAXOS_PREPARE_REFRESH_REQ, new Request(prepared, latestCommitted), requestSerializer)
                .permitsArtificialDelay(participants.consistencyForConsensus);
    }

    void refresh(List<InetAddress> refresh)
    {
        boolean executeOnSelf = false;
        for (int i = 0, size = refresh.size(); i < size ; ++i)
        {
            InetAddress destination = refresh.get(i);

            if (logger.isTraceEnabled())
                logger.trace("Refresh {} and Confirm {} to {}", send.payload.missingCommit, Ballot.toString(send.payload.promised, "Promise"), destination);

            if (Tracing.isTracing())
                Tracing.trace("Refresh {} and Confirm {} to {}", send.payload.missingCommit.ballot, send.payload.promised, destination);

            if (StorageProxy.canDoLocalRequest(destination))
                executeOnSelf = true;
            else
                MessagingService.instance().sendRRWithFailure(send, destination, this);
        }

        if (executeOnSelf)
            StageManager.getStage(verbStages.get(APPLE_PAXOS_PREPARE_REFRESH_REQ))
                    .execute(this::executeOnSelf);
    }

    public void onFailure(InetAddress from)
    {
        callbacks.onRefreshFailure(from, false);
    }

    public void onExpired(InetAddress from)
    {
        callbacks.onRefreshFailure(from, true);
    }

    public void response(MessageIn<Response> message)
    {
        response(message.payload, message.from);
    }

    private void executeOnSelf()
    {
        try
        {
            Response response = RequestHandler.execute(send.payload, FBUtilities.getBroadcastAddress());
            if (response == null)
                return;

            response(response, FBUtilities.getBroadcastAddress());
        }
        catch (Exception ex)
        {
            if (!(ex instanceof WriteTimeoutException))
                logger.error("Failed to apply paxos refresh-prepare locally", ex);

            onFailure(FBUtilities.getBroadcastAddress());
        }
    }

    private void response(Response response, InetAddress from)
    {
        callbacks.onRefreshSuccess(response.isSupersededBy, from);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    private static class Request
    {
        final UUID promised;
        final Committed missingCommit;

        Request(UUID promised, Committed missingCommit)
        {
            this.promised = promised;
            this.missingCommit = missingCommit;
        }
    }

    static class Response
    {
        final UUID isSupersededBy;
        Response(UUID isSupersededBy)
        {
            this.isSupersededBy = isSupersededBy;
        }
    }

    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(MessageIn<Request> message, int id)
        {
            Response response = execute(message.payload, message.from);
            if (response == null)
                Paxos.sendFailureResponse("prepare-refresh", message.from, message.payload.promised, id, message);
            else
                MessagingService.instance().sendReply(new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer)
                        .permitsArtificialDelay(message), id, message.from);
        }

        public static Response execute(Request request, InetAddress from)
        {
            Agreed commit = request.missingCommit;

            if (!Paxos.isInRangeAndShouldProcess(from, commit.update.partitionKey(), commit.update.metadata()))
                return null;

            try (PaxosState state = PaxosState.get(commit))
            {
                state.commit(commit);
                UUID latest = state.current().latestWitnessed();
                if (isAfter(latest, request.promised))
                {
                    Tracing.trace("Promise {} rescinded; latest is now {}", request.promised, latest);
                    return new Response(latest);
                }
                else
                {
                    Tracing.trace("Promise confirmed for ballot {}", request.promised);
                    return new Response(null);
                }
            }
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.promised, out, version);
            Committed.serializer.serialize(request.missingCommit, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID promise = UUIDSerializer.serializer.deserialize(in, version);
            Committed missingCommit = Committed.serializer.deserialize(in, version);
            return new Request(promise, missingCommit);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.promised, version)
                    + Committed.serializer.serializedSize(request.missingCommit, version);
        }
    }

    public static class ResponseSerializer implements IVersionedSerializer<Response>
    {
        public void serialize(Response response, DataOutputPlus out, int version) throws IOException
        {
            serializeNullable(UUIDSerializer.serializer, response.isSupersededBy, out, version);
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID isSupersededBy = deserializeNullable(UUIDSerializer.serializer, in, version);
            return new Response(isSupersededBy);
        }

        public long serializedSize(Response response, int version)
        {
            return serializedSizeNullable(UUIDSerializer.serializer, response.isSupersededBy, version);
        }
    }

}
