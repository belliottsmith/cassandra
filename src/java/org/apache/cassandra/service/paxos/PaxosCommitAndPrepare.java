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
import java.util.UUID;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_COMMIT_AND_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.service.paxos.Paxos.newBallot;
import static org.apache.cassandra.service.paxos.PaxosPrepare.responseSerializer;

public class PaxosCommitAndPrepare
{
    public static final RequestSerializer requestSerializer = new RequestSerializer();

    static PaxosPrepare commitAndPrepare(Commit commit, Paxos.Participants participants, SinglePartitionReadCommand readCommand)
    {
        UUID ballot = newBallot(commit.ballot);
        PaxosPrepare prepare = new PaxosPrepare(readCommand.partitionKey(), readCommand.metadata(), ballot,
                participants.contact.size(), participants.requiredForConsensus, null);

        Tracing.trace("Committing {}; Preparing {}", commit.ballot, ballot);
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_COMMIT_AND_PREPARE, new Request(commit, ballot, readCommand), requestSerializer);
        PaxosPrepare.start("commit-and-prepare", prepare, participants, message, RequestHandler::execute);
        return prepare;
    }


    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            Commit.serializer.serialize(request.commit, out, version);
            UUIDSerializer.serializer.serialize(request.ballot, out, version);
            ReadCommand.serializer.serialize(request.read, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            Commit commit = Commit.serializer.deserialize(in, version);
            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            SinglePartitionReadCommand readCommand = (SinglePartitionReadCommand) ReadCommand.serializer.deserialize(in, version);
            return new Request(commit, ballot, readCommand);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return Commit.serializer.serializedSize(request.commit, version)
                    + UUIDSerializer.serializer.serializedSize(request.ballot, version)
                    + ReadCommand.serializer.serializedSize(request.read, version);
        }
    }

    private static class Request extends PaxosPrepare.Request
    {
        final Commit commit;

        Request(Commit commit, UUID ballot, SinglePartitionReadCommand read)
        {
            super(ballot, read);
            this.commit = commit;
        }
    }

    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(MessageIn<Request> message, int id)
        {
            PaxosPrepare.Response response = execute(message.payload, message.from);
            if (response == null)
                Paxos.sendFailureResponse("commit-and-prepare", message.from, message.payload.ballot, id);
            else
                MessagingService.instance().sendReply(new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer), id, message.from);
        }

        private static PaxosPrepare.Response execute(Request request, InetAddress from)
        {
            Commit commit = request.commit;
            if (!Paxos.isInRangeAndShouldProcess(from, commit.update.partitionKey(), commit.update.metadata()))
                return null;

            PaxosState.commit(commit);
            return PaxosPrepare.RequestHandler.execute(request, from);
        }
    }

}
