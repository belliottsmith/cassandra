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

package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.UpdateRepairedRanges;

public class UpdateRepairedRangesVerbHandler implements IVerbHandler<UpdateRepairedRanges>
{
    public static final UpdateRepairedRangesVerbHandler instance = new UpdateRepairedRangesVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(UpdateRepairedRangesVerbHandler.class);

    public void doVerb(Message<UpdateRepairedRanges> message) throws IOException
    {
        logger.info("Got UpdateRepairedRanges from {}", message.header.from);
        UpdateRepairedRanges request = message.payload;
        for (Map.Entry<String, Map<Range<Token>, Integer>> cfRanges : request.perCfRanges.entrySet())
        {
            String cf = cfRanges.getKey();
            ColumnFamilyStore columnFamilyStore =  Keyspace.open(request.keyspace).getColumnFamilyStore(cf);
            for (Map.Entry<Range<Token>, Integer> repairRange : cfRanges.getValue().entrySet())
                columnFamilyStore.updateLastSuccessfulRepair(repairRange.getKey(), 1000L * repairRange.getValue());
        }

        MessagingService.instance().send(message.emptyResponse(), message.from());
    }
}
