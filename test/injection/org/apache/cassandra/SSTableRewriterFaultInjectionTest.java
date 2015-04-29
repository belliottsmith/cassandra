/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.FaultInjectionTestRunner;
import org.apache.cassandra.FaultInjectionTestRunner.Debug;
import org.apache.cassandra.FaultInjectionTestRunner.Faults;
import org.apache.cassandra.FaultInjectionTestRunner.Param;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.lifecycle.Transaction;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.CloseableIterator;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static org.apache.cassandra.io.sstable.SSTableRewriterTest.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(FaultInjectionTestRunner.class)
@Faults(scripts = "org/apache/cassandra/SSTableRewriter.faults")
public class SSTableRewriterFaultInjectionTest
{

    private static final String KEYSPACE = "Keyspace1";
    private static final String CF = "Standard1";

    // expected size ~10M
    private int rowCount = 1 << 10;
    private int cellCount = 10;
    private int cellSize = 1 << 10;
    private Keyspace keyspace;
    private ColumnFamilyStore cfs;
    private int inCount = -1;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, SimpleStrategy.class, KSMetaData.optsWithRF(1), SchemaLoader.standardCFMD(KEYSPACE, CF));
    }

    @Test
    public void testSafeAbort(
                        @Param(value={"1", "2"}, name="in")
                            String inCount,
                        @Param(value={"1", "2", "3"}, name="out")
                            String outCount,
                        @Param(value={"false", "true"}, name="reopen")
                            String reopen,
                        @Param(value={"0", "1"}, name = "empty")
                            String emptyWriterCount) throws Exception
    {
        if (this.inCount != parseInt(inCount))
        {
            keyspace = Keyspace.open(KEYSPACE);
            cfs = keyspace.getColumnFamilyStore(CF);
            cfs.disableAutoCompaction();
            truncate(cfs);
            validateCFS(cfs);
            for (SSTableReader reader : writeFiles(cfs, parseInt(inCount), rowCount, cellCount, cellSize))
                cfs.addSSTable(reader);
            validateCFS(cfs);
            this.inCount = parseInt(inCount);
        }
        testSafeAbort(parseInt(inCount), parseInt(outCount), parseBoolean(reopen), parseInt(emptyWriterCount));
    }

    public void testSafeAbort(int inCount, int outCount, boolean reopenIntraFile, int emptyWriterCount) throws Exception
    {
        Set<SSTableReader> compacting = cfs.getDataTracker().getView().sstables;
        assertEquals(inCount, compacting.size());
        File directory = Iterables.getFirst(compacting, null).descriptor.directory;
        validateCFS(cfs);
        int switchCount = rowCount / outCount;
        SSTableRewriter.overrideOpenInterval(reopenIntraFile ? (10 << 20) / (switchCount * 2) : 100 << 20);
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(compacting);
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             CloseableIterator<AbstractCompactedRow> iter = new CompactionIterable(OperationType.UNKNOWN, scanners.scanners, controller, SSTableFormat.Type.BIG).iterator();
             Transaction txn = cfs.getDataTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false);
        )
        {
            for (int i = 0 ; i < emptyWriterCount ; i++)
                rewriter.switchWriter(getWriter(cfs, directory));
            int count = 0;
            while (iter.hasNext())
            {
                if (count++ % switchCount == 0)
                    rewriter.switchWriter(getWriter(cfs, directory));

                rewriter.append(iter.next());
            }
            for (int i = 0 ; i < emptyWriterCount ; i++)
                rewriter.switchWriter(getWriter(cfs, directory));
            rewriter.finish();
        }
        catch (Throwable t)
        {
            assertEquals(compacting, cfs.getDataTracker().getView().sstables);
        }
        Thread.sleep(100);
        validateCFS(cfs);
    }

}
