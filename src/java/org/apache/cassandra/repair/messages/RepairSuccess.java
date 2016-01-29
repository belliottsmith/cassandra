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
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class RepairSuccess
{
    public final String keyspace;
    public final String columnFamily;
    public final Collection<Range<Token>> ranges;
    public final long succeedAt;

    public RepairSuccess(String keyspace, String columnFamily, Collection<Range<Token>> ranges, long succeedAt)
    {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.ranges = ranges;
        this.succeedAt = succeedAt;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepairSuccess that = (RepairSuccess) o;

        if (succeedAt != that.succeedAt) return false;
        if (!keyspace.equals(that.keyspace)) return false;
        if (!columnFamily.equals(that.columnFamily)) return false;
        return ranges.equals(that.ranges);
    }

    public int hashCode()
    {
        int result = keyspace.hashCode();
        result = 31 * result + columnFamily.hashCode();
        result = 31 * result + ranges.hashCode();
        result = 31 * result + (int) (succeedAt ^ (succeedAt >>> 32));
        return result;
    }

    public String toString()
    {
        return "RepairSuccess{" + keyspace + '.' + columnFamily +
               ", ranges=" + ranges +
               ", succeedAt=" + succeedAt +
               '}';
    }
    public static final IVersionedSerializer<RepairSuccess> serializer = new IVersionedSerializer<RepairSuccess>()
    {
        public void serialize(RepairSuccess repairSuccess, DataOutputPlus dos, int version) throws IOException
        {
            dos.writeUTF(repairSuccess.keyspace);
            dos.writeUTF(repairSuccess.columnFamily);
            dos.writeInt(repairSuccess.ranges.size());
            for (Range<Token> range : repairSuccess.ranges)
                Range.tokenSerializer.serialize(range, dos, version);
            dos.writeLong(repairSuccess.succeedAt);
        }

        public RepairSuccess deserialize(DataInputPlus dis, int version) throws IOException
        {
            String keyspace = dis.readUTF();
            String column_family = dis.readUTF();
            int rangeCount = dis.readInt();
            Collection<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.tokenSerializer.deserialize(dis, DatabaseDescriptor.getPartitioner(), version));
            long succeedAt = dis.readLong();
            return new RepairSuccess(keyspace, column_family, ranges, succeedAt);
        }

        public long serializedSize(RepairSuccess sc, int version)
        {
            return TypeSizes.sizeof(sc.keyspace)
                   + TypeSizes.sizeof(sc.columnFamily)
                   + TypeSizes.sizeof(sc.ranges.size())
                   + sc.ranges.stream().collect(Collectors.summingLong(r -> Range.tokenSerializer.serializedSize(r, version)))
                   + TypeSizes.sizeof(sc.succeedAt);
        }
    };
}
