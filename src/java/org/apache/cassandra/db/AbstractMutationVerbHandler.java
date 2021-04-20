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

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NoSpamLogger;

public abstract class AbstractMutationVerbHandler<T extends IMutation> implements IVerbHandler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractMutationVerbHandler.class);
    private static final String logMessageTemplate = "Received mutation from {} for {} outside valid range for keyspace {}";

    public void processMessage(MessageIn<T> message, int id, InetAddress replyTo)
    {
        boolean outOfRangeTokenLogging = StorageService.instance.isOutOfTokenRangeRequestLoggingEnabled();
        boolean outOfRangeTokenRejection = StorageService.instance.isOutOfTokenRangeRequestRejectionEnabled();

        DecoratedKey key = message.payload.key();
        if ((outOfRangeTokenLogging || outOfRangeTokenRejection)
            && isOutOfRangeMutation(message.payload.getKeyspaceName(), key))
        {
            StorageService.instance.incOutOfRangeOperationCount();
            Keyspace.open(message.payload.getKeyspaceName()).metric.outOfRangeTokenWrites.inc();

            // Log at most 1 message per second
            if (outOfRangeTokenLogging)
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS, logMessageTemplate, replyTo, key, message.payload.getKeyspaceName());

            if (outOfRangeTokenRejection)
                sendFailureResponse(id, replyTo);
            else
                applyMutation(message.version, message.payload, id, replyTo);
        }
        else
        {
            applyMutation(message.version, message.payload, id, replyTo);
        }
    }

    abstract void applyMutation(int version, T mutation, int id, InetAddress replyTo);

    private static void sendFailureResponse(int id, InetAddress replyTo)
    {
        MessageOut responseMessage = WriteResponse.createMessage()
                                                   .withParameter(MessagingService.FAILURE_RESPONSE_PARAM,
                                                                  MessagingService.ONE_BYTE);
        MessagingService.instance().sendReply(responseMessage, id, replyTo);
    }

    private static boolean isOutOfRangeMutation(String keyspace, DecoratedKey key)
    {
        return ! StorageService.instance.isEndpointValidForWrite(keyspace, key.getToken());
    }
}
