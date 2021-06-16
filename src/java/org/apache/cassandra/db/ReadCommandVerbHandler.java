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

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.IVersionedSerializer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand>
{
    private static final Logger logger = LoggerFactory.getLogger( ReadCommandVerbHandler.class );
    private static final String logMessageTemplate = "Received read request from {} for {} outside valid range for keyspace {}";

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
        MessageParams.reset();

        // no out of token range checking for partition range reads yet
        if (command.isSinglePartitionRead())
        {
            boolean outOfRangeTokenLogging = StorageService.instance.isOutOfTokenRangeRequestLoggingEnabled();
            boolean outOfRangeTokenRejection = StorageService.instance.isOutOfTokenRangeRequestRejectionEnabled();

            DecoratedKey key = ((SinglePartitionReadCommand)command).partitionKey();
            if ((outOfRangeTokenLogging || outOfRangeTokenRejection) && isOutOfRangeRead(command.metadata().ksName, key))
            {
                StorageService.instance.incOutOfRangeOperationCount();
                Keyspace.open(command.metadata().ksName).metric.outOfRangeTokenReads.inc();

                // Log at most 1 message per second
                if (outOfRangeTokenLogging)
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS, logMessageTemplate, message.from, key, command.metadata().ksName);

                if (outOfRangeTokenRejection)
                    // no need to respond, just drop the request
                    return;
            }
        }

        if (message.parameters.containsKey(ReadCommand.TRACK_WARNINGS))
            command.trackWarnings();

        // If tracking of repaired data is requested by the coordinator (see comment below),
        // then mark that on the command before it's executed
        if (message.parameters.containsKey(ReadCommand.TRACK_REPAIRED_DATA))
            command.trackRepairedStatus();

        ReadResponse response;
        try (ReadOrderGroup opGroup = command.startOrderGroup(); UnfilteredPartitionIterator iterator = command.executeLocally(opGroup))
        {
            response = command.createResponse(iterator);
        }
        catch (AssertionError t)
        {
            throw new AssertionError(String.format("Caught an error while trying to process the command: %s", command.toCQLString()), t);
        }
        catch (TombstoneOverwhelmingException | RowIndexOversizeException e)
        {
            if (!command.isTrackingWarnings())
                throw e;

            // make sure to log as the exception is swallowed
            logger.error(e.getMessage());

            response = command.createResponse(EmptyIterators.unfilteredPartition(command.metadata(), command.isForThrift()));
            MessageOut<ReadResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, serializer());
            reply = MessageParams.addToMessage(reply);
            MessagingService.instance().sendReply(reply, id, message.from);
            return;
        }

        // From 3.0.17.x, a coordinator may request additional info about the repaired data that
        // makes up the response, namely a digest generated from the repaired data and a
        // flag indicating our level of confidence in that digest. The digest may be considered
        // inconclusive if it may have been affected by some unrepaired data during read.
        // e.g. some sstables read during this read were involved in pending but not yet
        // committed repair sessions or an unrepaired partition tombstone meant that not all
        // repaired sstables were read (but they might be on other replicas).
        MessageOut<ReadResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, serializer());
        if (command.isTrackingRepairedStatus())
        {
            String paramName = command.isRepairedDataDigestConclusive()
                               ? ReadCommand.CONCLUSIVE_REPAIRED_DATA_DIGEST
                               : ReadCommand.INCONCLUSIVE_REPAIRED_DATA_DIGEST;
            reply = reply.withParameter(paramName, ByteBufferUtil.getArray(command.getRepairedDataDigest()));
        }

        Tracing.trace("Enqueuing response to {}", message.from);
        reply = MessageParams.addToMessage(reply);
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
