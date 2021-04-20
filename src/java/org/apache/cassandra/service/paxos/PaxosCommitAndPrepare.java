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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
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
import static org.apache.cassandra.service.paxos.PaxosPrepare.start;

public class PaxosCommitAndPrepare
{
    public static final RequestSerializer requestSerializer = new RequestSerializer();

    static PaxosPrepare commitAndPrepare(Commit commit, Paxos.Participants participants, SinglePartitionReadCommand readCommand)
    {
        UUID ballot = newBallot(commit.ballot);
        Request request = new Request(commit, ballot, participants.consistencyForConsensus, participants.electorate, readCommand);
        PaxosPrepare prepare = new PaxosPrepare(participants, request, null);

        Tracing.trace("Committing {}; Preparing {}", commit.ballot, ballot);
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_COMMIT_AND_PREPARE, request, requestSerializer);
        start(prepare, participants, message, RequestHandler::execute);
        return prepare;
    }

    private static class Request extends PaxosPrepare.AbstractRequest<Request>
    {
        final Commit commit;

        Request(Commit commit, UUID ballot, ConsistencyLevel consistency, Paxos.Electorate electorate, SinglePartitionReadCommand read)
        {
            super(ballot, consistency, electorate, read);
            this.commit = commit;
        }

        private Request(Commit commit, UUID ballot, ConsistencyLevel consistency, Paxos.Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata)
        {
            super(ballot, consistency, electorate, partitionKey, metadata);
            this.commit = commit;
        }

        Request withoutRead()
        {
            return new Request(commit, ballot, consistency, electorate, partitionKey, metadata);
        }

        public String toString()
        {
            return "CommitAndPrepare((" + commit.ballot + ", " + commit.update + "), " + ballot + ')';
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            Commit.serializer.serialize(request.commit, out, version);
            UUIDSerializer.serializer.serialize(request.ballot, out, version);
            out.writeUnsignedVInt(request.consistency.code);
            Paxos.Electorate.serializer.serialize(request.electorate, out, version);
            out.writeBoolean(request.read != null);
            if (request.read != null)
            {
                ReadCommand.serializer.serialize(request.read, out, version);
            }
            else
            {
                CFMetaData.serializer.serialize(request.metadata, out, version);
                DecoratedKey.serializer.serialize(request.partitionKey, out, version);
            }
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            Commit commit = Commit.serializer.deserialize(in, version);
            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            ConsistencyLevel consistency = ConsistencyLevel.fromCode((int) in.readUnsignedVInt());
            Paxos.Electorate electorate = Paxos.Electorate.serializer.deserialize(in, version);
            boolean hasRead = in.readBoolean();
            if (hasRead)
            {
                SinglePartitionReadCommand readCommand = (SinglePartitionReadCommand) ReadCommand.serializer.deserialize(in, version);
                return new Request(commit, ballot, consistency, electorate, readCommand);
            }
            else
            {
                CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
                DecoratedKey partitionKey = (DecoratedKey) DecoratedKey.serializer.deserialize(in, metadata.partitioner, version);
                return new Request(commit, ballot, consistency, electorate, partitionKey, metadata);
            }
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return Commit.serializer.serializedSize(request.commit, version)
                    + UUIDSerializer.serializer.serializedSize(request.ballot, version)
                    + TypeSizes.sizeofUnsignedVInt(request.consistency.code)
                    + Paxos.Electorate.serializer.serializedSize(request.electorate, version)
                    + 1 + (request.read != null
                        ? ReadCommand.serializer.serializedSize(request.read, version)
                            : CFMetaData.serializer.serializedSize(request.metadata, version)
                            + DecoratedKey.serializer.serializedSize(request.partitionKey, version));
        }
    }

    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(MessageIn<Request> message, int id)
        {
            PaxosPrepare.Response response = execute(message.payload, message.from);
            if (response == null)
                Paxos.sendFailureResponse("CommitAndPrepare", message.from, message.payload.ballot, id);
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
