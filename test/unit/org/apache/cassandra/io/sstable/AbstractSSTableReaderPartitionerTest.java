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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public abstract class AbstractSSTableReaderPartitionerTest
{
    public static final String KEYSPACE1 = "SSTableReaderTest";
    public static final String CF_STANDARD = "Standard1";

    public void partitionerTokenTest() throws Throwable
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);
        for (long j = 0; j < 5; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, j)
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        store.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        List<Token> tokens = new ArrayList<>(5);
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
                tokens.add(scanner.next().partitionKey().getToken());
        }
        IPartitioner partitioner = store.getPartitioner();
        // this includes tokens.get(1):
        assertTrue(sstable.containsDataIn(r(tokens.get(0), slightlySmallerThan(tokens.get(2)))));
        // min token -> the first token - 1, does not contain any data:
        assertFalse(sstable.containsDataIn(r(partitioner.getMinimumToken(), slightlySmallerThan(tokens.get(0)))));
        // min token -> first token (inclusive), contains tokens.get(0):
        assertTrue(sstable.containsDataIn(r(partitioner.getMinimumToken(), tokens.get(0))));
        // wraparound - last token -> first token - 1 -> no data
        assertFalse(sstable.containsDataIn(r(tokens.get(tokens.size() - 1), slightlySmallerThan(tokens.get(0)))));
        // wraparound - last token -> first token -> contains data
        assertTrue(sstable.containsDataIn(r(tokens.get(tokens.size() - 1), tokens.get(0))));
        // tokens.get(2) -> tokens.get(3) - 1 -> no data
        assertFalse(sstable.containsDataIn(r(tokens.get(2), slightlySmallerThan(tokens.get(3)))));
        // tokens.get(2) -> tokens.get(3) -> data
        assertTrue(sstable.containsDataIn(r(tokens.get(2), tokens.get(3))));
    }

    static Set<Range<Token>> r(Token s, Token e)
    {
        return Collections.singleton(new Range<>(s, e));
    }

    abstract Token slightlySmallerThan(Token t);
}
