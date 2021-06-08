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

import java.io.IOException;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public class ReadRepairVerbHandler extends AbstractMutationVerbHandler<Mutation>
{
    public static final ReadRepairVerbHandler instance = new ReadRepairVerbHandler();

    public void doVerb(Message<Mutation> message) throws IOException
    {
        // CIE - This method exists as python dtest relies on byte-code rewriting via Byteman, so requires this
        // class defines a "doVerb" method for some tests; once CIE and OSS become in-sync, this logic may go away
        // change: rdar://60088325 p33279387 OPS/COR Don't allow requests for out of token range operations
        super.doVerb(message);
    }

    void applyMutation(Message<Mutation> message, InetAddressAndPort respondToAddress)
    {
        message.payload.apply();
        MessagingService.instance().send(message.emptyResponse(), respondToAddress);
    }
}
