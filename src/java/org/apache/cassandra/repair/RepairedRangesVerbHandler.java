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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairedRangesRequest;
import org.apache.cassandra.repair.messages.UpdateRepairedRanges;

public class RepairedRangesVerbHandler implements IVerbHandler<RepairedRangesRequest>
{
    public static final RepairedRangesVerbHandler instance = new RepairedRangesVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(RepairedRangesVerbHandler.class);

    public void doVerb(Message<RepairedRangesRequest> message) throws IOException
    {
        logger.info("Got a RepairedRangesRequest from {}", message.header.from);
        RepairedRangesRequest request = message.payload;
        Collection<Range<Token>> requestRanges = request.ranges;
        Map<String, Map<Range<Token>, Integer>> retMap = new HashMap<>();

        for (Map.Entry<String, Map<Range<Token>, Integer>> perCf : SystemKeyspace.getLastSuccessfulRepairsForKeyspace(request.keyspace).entrySet())
        {
            String cf = perCf.getKey();
            //Check if the CF exists then only send the ranges.
            if(!Keyspace.open(request.keyspace).columnFamilyExists(cf))
            {
                logger.info("Skipping non existent cf = " + cf);
                continue;
            }

            Map<Range<Token>, Integer> repairMap = new HashMap<>();
            for (Map.Entry<Range<Token>, Integer> entry : perCf.getValue().entrySet())
            {
                Range<Token> locallyRepairedRange = entry.getKey();
                for (Range<Token> requestRange : requestRanges)
                {
                    for (Range<Token> intersection : locallyRepairedRange.intersectionWith(requestRange))
                    {
                        Integer succeedAt = repairMap.get(intersection);
                        if (succeedAt == null || succeedAt < entry.getValue())
                            repairMap.put(intersection, entry.getValue());
                    }
                }
            }
            retMap.put(cf, repairMap);
        }

        UpdateRepairedRanges response = new UpdateRepairedRanges(request.keyspace, retMap);

        MessagingService.instance().send(message.responseWith(response), message.from());
    }
}
