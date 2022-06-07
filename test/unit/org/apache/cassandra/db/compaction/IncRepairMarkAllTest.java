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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IncRepairMarkAllTest extends CQLTester
{
    @Test
    public void testIncRepairMarkAll() throws Throwable
    {
        DatabaseDescriptor.setIncrementalUpdatesLastRepaired(false);
        createTable("create table %s (id int primary key) with compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}");
        for (int i = 0; i < 100; i++)
        {
            execute("insert into %s (id) values (?)", i);
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }

        ActiveRepairService.instance.setIncRepairedAtUnsafe(System.currentTimeMillis(), false, false);

        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertTrue(sstable.isRepaired());

        assertEquals(100, getCurrentColumnFamilyStore().getCompactionStrategyManager().getStrategies().get(0).get(0).getSSTables().size());
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().getStrategies().get(1).get(0).getSSTables().isEmpty());

        ActiveRepairService.instance.setIncRepairedAtUnsafe(ActiveRepairService.UNREPAIRED_SSTABLE, false, false);

        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertFalse(sstable.isRepaired());

        assertEquals(100, getCurrentColumnFamilyStore().getCompactionStrategyManager().getStrategies().get(1).get(0).getSSTables().size());
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().getStrategies().get(0).get(0).getSSTables().isEmpty());
    }

    @Test
    public void testBlockMarkOPRT() throws Throwable
    {
        DatabaseDescriptor.setIncrementalUpdatesLastRepaired(false);
        createTable("create table %s (id int primary key) with compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false, 'only_purge_repaired_tombstones': true}");
        for (int i = 0; i < 100; i++)
            execute("insert into %s (id) values (?)", i);

        getCurrentColumnFamilyStore().forceBlockingFlush();

        try
        {
            ActiveRepairService.instance.setIncRepairedAtUnsafe(System.currentTimeMillis(), false, false);
            fail("this should fail");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("only_purge_repaired_tombstones"));
        }

        ActiveRepairService.instance.setIncRepairedAtUnsafe(System.currentTimeMillis(), false, true);
    }

}
