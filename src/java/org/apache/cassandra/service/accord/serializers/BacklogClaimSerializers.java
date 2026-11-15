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
package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import accord.messages.BacklogClaim;

import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class BacklogClaimSerializers
{
    public static final VersionedSerializer<BacklogClaim, Version> request = new VersionedSerializer<>()
    {
        @Override
        public void serialize(BacklogClaim msg, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(msg.txnId, out);
        }

        @Override
        public BacklogClaim deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new BacklogClaim(CommandSerializers.txnId.deserialize(in));
        }

        @Override
        public long serializedSize(BacklogClaim msg, Version version)
        {
            return CommandSerializers.txnId.serializedSize(msg.txnId);
        }
    };
}
