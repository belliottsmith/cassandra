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

package org.apache.cassandra.io.sstable;

import java.math.BigInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

public class SSTableReaderRandomPartitonerTest extends AbstractSSTableReaderPartitionerTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(RandomPartitioner.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD, 2, LongType.instance));
    }

    @Test
    public void randomPartitonerTokenTest() throws Throwable
    {
        partitionerTokenTest();
    }

    Token slightlySmallerThan(Token t)
    {
        if (t instanceof RandomPartitioner.BigIntegerToken)
        {
            RandomPartitioner.BigIntegerToken bt = (RandomPartitioner.BigIntegerToken) t;
            return new RandomPartitioner.BigIntegerToken(bt.getTokenValue().subtract(BigInteger.ONE));
        }
        throw new AssertionError(t.getClass());
    }
}
