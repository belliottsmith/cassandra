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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.batchlog.LegacyBatchlogMigrator;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.net.MessagingService.PERMIT_ARTIFICIAL_RESPONSE_DELAY;
import static org.apache.cassandra.net.MessagingService.ZERO_BYTE;

public class MutationVerbHandler extends AbstractMutationVerbHandler<Mutation>
{
    private void reply(Map<String, byte[]> messageInParameters, int id, InetAddress replyTo)
    {
        Tracing.trace("Enqueuing response to {}", replyTo);
        MessagingService.instance().sendReply(WriteResponse.createMessage().permitsArtificialDelay(messageInParameters), id, replyTo);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(MessageIn<Mutation> message, int id)  throws IOException
    {
        // Check if there were any forwarding headers in this message
        byte[] from = message.parameters.get(Mutation.FORWARD_FROM);
        InetAddress replyTo;
        if (from == null)
        {
            replyTo = message.from;
            byte[] forwardBytes = message.parameters.get(Mutation.FORWARD_TO);
            if (forwardBytes != null)
                forwardToLocalNodes(message.payload, message.verb, forwardBytes, message.from, message.parameters.containsKey(PERMIT_ARTIFICIAL_RESPONSE_DELAY));
        }
        else
        {
            replyTo = InetAddress.getByAddress(from);
        }

        try
        {
            processMessage(message, id, replyTo);
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }

    protected void applyMutation(int version, MessageIn<Mutation> message, int id, InetAddress replyTo)
    {
        Mutation mutation = message.payload;
        Map<String, byte[]> parameters = message.parameters;
        if (version < MessagingService.VERSION_30 && LegacyBatchlogMigrator.isLegacyBatchlogMutation(mutation))
        {
            LegacyBatchlogMigrator.handleLegacyMutation(mutation);
            reply(parameters, id, replyTo);
        }
        else
        {
            mutation.applyFuture().thenAccept(o -> reply(parameters, id, replyTo)).exceptionally(wto -> {
                failed();
                return null;
            });
        }
    }

    /**
     * Older version (< 1.0) will not send this message at all, hence we don't
     * need to check the version of the data.
     */
    private static void forwardToLocalNodes(Mutation mutation, MessagingService.Verb verb, byte[] forwardBytes, InetAddress from, boolean delayResponse) throws IOException
    {
        try (DataInputStream in = new DataInputStream(new FastByteArrayInputStream(forwardBytes)))
        {
            int size = in.readInt();

            // tell the recipients who to send their ack to
            MessageOut<Mutation> message = new MessageOut<>(verb, mutation, Mutation.serializer)
                    .withParameter(Mutation.FORWARD_FROM, from.getAddress());
            if (delayResponse)
                message = message.withParameter(PERMIT_ARTIFICIAL_RESPONSE_DELAY, ZERO_BYTE);

            // Send a message to each of the addresses on our Forward List
            for (int i = 0; i < size; i++)
            {
                InetAddress address = CompactEndpointSerializationHelper.deserialize(in);
                int id = in.readInt();
                Tracing.trace("Enqueuing forwarded write to {}", address);
                MessagingService.instance().sendOneWay(message, id, address);
            }
        }
    }
}
