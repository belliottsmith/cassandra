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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PartitionSizeCommandTest
{
    private final static String KS = "ks";
    private final static String TBL = "tbl";

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE " + TBL + " (k INT PRIMARY KEY, v INT)", KS).build();
        SchemaLoader.createKeyspace(KS, KeyspaceParams.simple(1), table);

        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", KS, TBL));
        Schema.instance.getColumnFamilyStoreInstance(table.id).forceBlockingFlush();
    }

    @Test
    public void serialization() throws Exception
    {
        PartitionSizeCommand expected = new PartitionSizeCommand(KS, TBL, ByteBufferUtil.bytes(1));
        long size = PartitionSizeCommand.serializer.serializedSize(expected, MessagingService.current_version);

        byte[] bytes = new byte[(int) size];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        DataOutputBuffer out = new DataOutputBufferFixed(bb);
        PartitionSizeCommand.serializer.serialize(expected, out, MessagingService.current_version);
        Assert.assertEquals(size, bb.position());

        bb.rewind();
        DataInputBuffer inputBuffer = new DataInputBuffer(bb, true);
        PartitionSizeCommand actual = PartitionSizeCommand.serializer.deserialize(inputBuffer, MessagingService.current_version);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void executeLocally() throws Exception
    {
        // no key matching 0, should return 0
        PartitionSizeCommand cmd0 = new PartitionSizeCommand(KS, TBL, ByteBufferUtil.bytes(0));
        Assert.assertEquals(0, cmd0.executeLocally());

        // row with key 1 inserted in setupClass, should return non-zero
        PartitionSizeCommand cmd1 = new PartitionSizeCommand(KS, TBL, ByteBufferUtil.bytes(1));
        Assert.assertTrue(cmd1.executeLocally() > 0);
    }
}

