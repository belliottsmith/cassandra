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

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.IVersionedSerializer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand>
{
    private static final Logger logger = LoggerFactory.getLogger( ReadCommandVerbHandler.class );
    private static final String logMessageTemplate = "Received read request from {} for token {} outside valid range";

    protected IVersionedSerializer<ReadResponse> serializer()
    {
        return ReadResponse.serializer;
    }

    public void doVerb(MessageIn<ReadCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;

        // no out of token range checking for partition range reads yet
        if (command.isSinglePartitionRead())
        {
            boolean outOfRangeTokenLogging = StorageService.instance.isOutOfTokenRangeRequestLoggingEnabled();
            boolean outOfRangeTokenRejection = StorageService.instance.isOutOfTokenRangeRequestRejectionEnabled();

            DecoratedKey key = ((SinglePartitionReadCommand)command).partitionKey();
            if ((outOfRangeTokenLogging || outOfRangeTokenRejection) && isOutOfRangeRead(command.metadata().ksName, key))
            {
                StorageMetrics.totalOpsForInvalidToken.inc();

                // Log at most 1 message per second
                if (outOfRangeTokenLogging)
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS, logMessageTemplate, message.from, key.getToken());

                if (outOfRangeTokenRejection)
                    // no need to respond, just drop the request
                    return;
            }
        }

        ReadResponse response;
        try (ReadOrderGroup opGroup = command.startOrderGroup(); UnfilteredPartitionIterator iterator = command.executeLocally(opGroup))
        {
            response = command.createResponse(iterator);
        }

        MessageOut<ReadResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, serializer());

        Tracing.trace("Enqueuing response to {}", message.from);
        MessagingService.instance().sendReply(reply, id, message.from);
    }

    private boolean isOutOfRangeRead(String keyspace, DecoratedKey key)
    {
        return ! Keyspace.open(keyspace)
                         .getReplicationStrategy()
                         .getNaturalEndpoints(key)
                         .contains(FBUtilities.getBroadcastAddress());
    }
}
