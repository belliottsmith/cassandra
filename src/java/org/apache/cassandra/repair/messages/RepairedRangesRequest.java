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

package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public class RepairedRangesRequest
{
    public final String keyspace;
    public final Collection<Range<Token>> ranges;

    public RepairedRangesRequest(String keyspace, Collection<Range<Token>> ranges)
    {
        this.keyspace = keyspace;
        this.ranges = ranges;
    }

    public static final IVersionedSerializer<RepairedRangesRequest> serializer = new IVersionedSerializer<RepairedRangesRequest>()
    {
        public void serialize(RepairedRangesRequest request, DataOutputPlus dos, int version) throws IOException
        {
            dos.writeUTF(request.keyspace);
            dos.writeInt(request.ranges.size());
            for (Range<Token> entry : request.ranges)
                Range.tokenSerializer.serialize(entry, dos, version);
        }

        public RepairedRangesRequest deserialize(DataInputPlus dis, int version) throws IOException
        {
            String keyspace = dis.readUTF();
            int rangeCount = dis.readInt();
            Collection<Range<Token>> ranges = new ArrayList<>(rangeCount);

            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.tokenSerializer.deserialize(dis, DatabaseDescriptor.getPartitioner(), version));

            return new RepairedRangesRequest(keyspace, ranges);
        }

        public long serializedSize(RepairedRangesRequest rrr, int version)
        {
            long size = TypeSizes.sizeof(rrr.keyspace);
            size += TypeSizes.sizeof(rrr.ranges.size());
            for (Range<Token> r : rrr.ranges)
                size += Range.tokenSerializer.serializedSize(r, version);
            return size;
        }
    };
}
