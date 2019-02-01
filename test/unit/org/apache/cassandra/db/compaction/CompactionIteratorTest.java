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
package org.apache.cassandra.db.compaction;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

public class CompactionIteratorTest extends CQLTester
{
    @Test
    public void transformTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, d text, primary key (id, id2))");
        for (int i = 0; i < 10; i++)
        {
            execute("insert into %s (id, id2, d) values (1, ?, 'abc')", i);
        }
        getCurrentColumnFamilyStore().forceBlockingFlush();
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();


        try (CompactionController controller = new CompactionController(getCurrentColumnFamilyStore(), Integer.MAX_VALUE);
             ISSTableScanner scanner = sstable.getScanner();
             CompactionIterator iter = new CompactionIterator(OperationType.COMPACTION,
                                                              Collections.singletonList(scanner),
                                                              controller, FBUtilities.nowInSeconds(), null))
        {
            assertTrue(iter.hasNext());
            UnfilteredRowIterator rows = iter.next();
            assertTrue(rows.hasNext());
            assertNotNull(rows.next());

            iter.stop();
            try
            {
                // Will call Transformation#applyToRow
                rows.hasNext();
                fail("Should have thrown CompactionInterruptedException");
            }
            catch (CompactionInterruptedException e)
            {
                // ignore
            }
        }
    }

    @Test
    public void transformPartitionTest() throws Throwable
    {
        createTable("create table %s (id int, id2 int, d text, primary key (id, id2))");
        for (int i = 0; i < 10; i++)
        {
            execute("insert into %s (id, id2, d) values (1, ?, 'abc')", i);
        }
        getCurrentColumnFamilyStore().forceBlockingFlush();
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();


        try (CompactionController controller = new CompactionController(getCurrentColumnFamilyStore(), Integer.MAX_VALUE);
             ISSTableScanner scanner = sstable.getScanner();
             CompactionIterator iter = new CompactionIterator(OperationType.COMPACTION,
                                                              Collections.singletonList(scanner),
                                                              controller, FBUtilities.nowInSeconds(), null))
        {
            iter.stop();
            try
            {
                UnfilteredRowIterator rows = iter.next();
                // Will call Transformation#applyToRow
                fail("Should have thrown CompactionInterruptedException");
            }
            catch (CompactionInterruptedException e)
            {
                // ignore
            }
        }
    }
}
