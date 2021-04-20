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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_COMMIT_AND_PREPARE_REQ;
import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;
import static org.apache.cassandra.service.paxos.Paxos.newBallot;
import static org.apache.cassandra.service.paxos.PaxosPrepare.responseSerializer;
import static org.apache.cassandra.service.paxos.PaxosPrepare.start;

public class PaxosCommitAndPrepare
{
    public static final RequestSerializer requestSerializer = new RequestSerializer();

    static PaxosPrepare commitAndPrepare(Agreed commit, Paxos.Participants participants, SinglePartitionReadCommand readCommand, boolean tryOptimisticRead)
    {
        UUID ballot = newBallot(commit.ballot, participants.consistencyForConsensus);
        Request request = new Request(commit, ballot, participants.electorate, readCommand, tryOptimisticRead);
        PaxosPrepare prepare = new PaxosPrepare(participants, request, null);

        Tracing.trace("Committing {}; Preparing {}", commit.ballot, ballot);
        MessageOut<Request> message = new MessageOut<>(APPLE_PAXOS_COMMIT_AND_PREPARE_REQ, request, requestSerializer)
                .permitsArtificialDelay(participants.consistencyForConsensus);
        start(prepare, participants, message, RequestHandler::execute);
        return prepare;
    }

    private static class Request extends PaxosPrepare.AbstractRequest<Request>
    {
        final Agreed commit;

        Request(Agreed commit, UUID ballot, Paxos.Electorate electorate, SinglePartitionReadCommand read, boolean checkProposalStability)
        {
            super(ballot, electorate, read, checkProposalStability);
            this.commit = commit;
        }

        private Request(Agreed commit, UUID ballot, Paxos.Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata, boolean checkProposalStability)
        {
            super(ballot, electorate, partitionKey, metadata, checkProposalStability);
            this.commit = commit;
        }

        Request withoutRead()
        {
            return new Request(commit, ballot, electorate, partitionKey, metadata, checkProposalStability);
        }

        public String toString()
        {
            return commit.toString("CommitAndPrepare(") + ", " + Ballot.toString(ballot) + ')';
        }
    }

    public static class RequestSerializer extends PaxosPrepare.AbstractRequestSerializer<Request, Agreed>
    {
        Request construct(Agreed param, UUID ballot, Paxos.Electorate electorate, SinglePartitionReadCommand read, boolean checkProposalStability)
        {
            return new Request(param, ballot, electorate, read, checkProposalStability);
        }

        Request construct(Agreed param, UUID ballot, Paxos.Electorate electorate, DecoratedKey partitionKey, CFMetaData metadata, boolean checkProposalStability)
        {
            return new Request(param, ballot, electorate, partitionKey, metadata, checkProposalStability);
        }

        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            Agreed.serializer.serialize(request.commit, out, version);
            super.serialize(request, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            Agreed committed = Agreed.serializer.deserialize(in, version);
            return deserialize(committed, in, version);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return Agreed.serializer.serializedSize(request.commit, version)
                    + super.serializedSize(request, version);
        }
    }

    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(MessageIn<Request> message, int id)
        {
            PaxosPrepare.Response response = execute(message.payload, message.from);
            if (response == null)
                Paxos.sendFailureResponse("CommitAndPrepare", message.from, message.payload.ballot, id, message);
            else
                MessagingService.instance().sendReply(new MessageOut<>(REQUEST_RESPONSE, response, responseSerializer)
                        .permitsArtificialDelay(message), id, message.from);
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
