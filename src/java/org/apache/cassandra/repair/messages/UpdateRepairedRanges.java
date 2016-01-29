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
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class UpdateRepairedRanges
{
    public final String keyspace;
    public final Map<String, Map<Range<Token>, Integer>> perCfRanges;

    public UpdateRepairedRanges(String keyspace, Map<String, Map<Range<Token>, Integer>> perCFranges)
    {
        this.keyspace = keyspace;
        this.perCfRanges = perCFranges;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Map<Range<Token>, Integer>> perCfRange : perCfRanges.entrySet())
        {
            sb.append(perCfRange.getKey()).append("=").append("(");
            for (Map.Entry<Range<Token>, Integer> range : perCfRange.getValue().entrySet())
            {
                sb.append(range.getKey()).append(":").append(range.getValue()).append(",");
            }
            sb.append("),");
        }
        return sb.toString();
    }

    public static final IVersionedSerializer<UpdateRepairedRanges> serializer = new IVersionedSerializer<UpdateRepairedRanges>()
    {
        public void serialize(UpdateRepairedRanges response, DataOutputPlus dos, int version) throws IOException
        {
            dos.writeUTF(response.keyspace);
            dos.writeInt(response.perCfRanges.size());
            for (Map.Entry<String, Map<Range<Token>, Integer>> entry : response.perCfRanges.entrySet())
            {
                dos.writeUTF(entry.getKey());
                dos.writeInt(entry.getValue().size());

                for (Map.Entry<Range<Token>, Integer> repairedEntry : entry.getValue().entrySet())
                {
                    Range.tokenSerializer.serialize(repairedEntry.getKey(), dos, version);
                    dos.writeInt(repairedEntry.getValue());
                }
            }
        }

        public UpdateRepairedRanges deserialize(DataInputPlus dis, int version) throws IOException
        {
            String keyspace = dis.readUTF();
            int entryCount = dis.readInt();
            Map<String, Map<Range<Token>, Integer>> perCFranges = new HashMap<>();

            for (int i = 0; i < entryCount; i++)
            {
                String columnFamily = dis.readUTF();
                int rangeCount = dis.readInt();
                Map<Range<Token>, Integer> rangeSuccessTimes = new HashMap<>();
                for (int j = 0; j < rangeCount; j++)
                {
                    Range<Token> range = (Range<Token>) Range.tokenSerializer.deserialize(dis, DatabaseDescriptor.getPartitioner(), version);
                    int succeedAtSeconds = dis.readInt();
                    rangeSuccessTimes.put(range, succeedAtSeconds);
                }
                perCFranges.put(columnFamily, rangeSuccessTimes);
            }

            return new UpdateRepairedRanges(keyspace, perCFranges);
        }

        public long serializedSize(UpdateRepairedRanges response, int version)
        {
            long size = TypeSizes.sizeof(response.keyspace);
            size += TypeSizes.sizeof(response.perCfRanges.size());
            for (Map.Entry<String, Map<Range<Token>, Integer>> repairedRange : response.perCfRanges.entrySet())
            {
                size += TypeSizes.sizeof(repairedRange.getKey()); // cf
                size += TypeSizes.sizeof(repairedRange.getValue().size());
                for (Map.Entry<Range<Token>, Integer> r : repairedRange.getValue().entrySet())
                {
                    size += Range.tokenSerializer.serializedSize(r.getKey(), version);
                    size += TypeSizes.sizeof(r.getValue());
                }
            }
            return size;
        }
    };
}
