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
import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class PartitionSizeCommand
{
    public static final IVersionedSerializer<PartitionSizeCommand> serializer = new Serializer();
    public final String keyspace;
    public final String table;
    public final ByteBuffer key;

    public PartitionSizeCommand(String keyspace, String table, ByteBuffer key)
    {
        assert keyspace != null;
        assert table != null;
        assert key != null;

        this.keyspace = keyspace;
        this.table = table;
        this.key = key;
    }

    public long executeLocally()
    {
        Keyspace keyspace = Keyspace.open(this.keyspace);
        DecoratedKey dk = DatabaseDescriptor.getPartitioner().decorateKey(key);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(table);
        long size = 0;
        try (OpOrder.Group op = cfs.readOrdering.start())
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, dk));
            for (SSTableReader sstable : view.sstables)
            {
                size += sstable.getSerializedRowSize(dk);
            }
        }

        return size;
    }

    public Message<PartitionSizeCommand> getMessage()
    {
        return Message.outWithFlag(Verb.PARTITION_SIZE_REQ, this, MessageFlag.CALL_BACK_ON_FAILURE);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionSizeCommand that = (PartitionSizeCommand) o;

        return key.equals(that.key) && keyspace.equals(that.keyspace) && table.equals(that.table);
    }

    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + keyspace.hashCode();
        result = 31 * result + table.hashCode();
        return result;
    }

    private static class Serializer implements IVersionedSerializer<PartitionSizeCommand>
    {
        public void serialize(PartitionSizeCommand command, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(command.keyspace);
            out.writeUTF(command.table);
            ByteBufferUtil.writeWithShortLength(command.key, out);
        }

        public PartitionSizeCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspaceName = in.readUTF();
            String tableName = in.readUTF();
            ByteBuffer partitionKey = ByteBufferUtil.readWithShortLength(in);
            return new PartitionSizeCommand(keyspaceName, tableName, partitionKey);
        }

        public long serializedSize(PartitionSizeCommand command, int version)
        {
            return TypeSizes.sizeof(command.keyspace)
                   + TypeSizes.sizeof(command.table)
                   + ByteBufferUtil.serializedSizeWithShortLength(command.key);
        }
    }
}
