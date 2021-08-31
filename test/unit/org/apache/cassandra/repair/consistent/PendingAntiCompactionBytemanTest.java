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

package org.apache.cassandra.repair.consistent;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(BMUnitRunner.class)
public class PendingAntiCompactionBytemanTest extends AbstractPendingAntiCompactionTest
{
    @BMRules(rules = { @BMRule(name = "Throw exception anticompaction",
                               targetClass = "Range$OrderedRangeContainmentChecker",
                               targetMethod = "contains",
                               action = "throw new org.apache.cassandra.db.compaction.CompactionInterruptedException(\"in byteman\");")} )
    @Test
    public void testExceptionAnticompaction() throws InterruptedException, NoSuchRepairSessionException
    {
        cfs.disableAutoCompaction();
        cfs2.disableAutoCompaction();
        ExecutorService es = Executors.newFixedThreadPool(1);
        makeSSTables(4, cfs, 5);
        makeSSTables(4, cfs2, 5);
        List<Range<Token>> ranges = new ArrayList<>();

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            ranges.add(new Range<>(sstable.first.getToken(), sstable.last.getToken()));
        }
        UUID prsid = prepareSession();
        try
        {
            PendingAntiCompaction pac = new PendingAntiCompaction(prsid, ranges, es, () -> false);
            pac.run().get();
            fail("PAC should throw exception when anticompaction throws exception!");
        }
        catch (ExecutionException e)
        {
            assertTrue(e.getCause() instanceof CompactionInterruptedException);
        }
        // and make sure nothing is marked compacting
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(cfs2.getTracker().getCompacting().isEmpty());
        assertEquals(4, cfs.getLiveSSTables().size());
        assertEquals(4, cfs2.getLiveSSTables().size());
    }

}
