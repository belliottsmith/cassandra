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

package org.apache.cassandra.service.paxos.cleanup;

import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PaxosTableRepairsTest
{
    private static DecoratedKey dk(int k)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(k));
    }

    private static final DecoratedKey DK1 = dk(1);
    private static final DecoratedKey DK2 = dk(2);

    private static class MockRepair implements PaxosTableRepairs.QueueableRepair
    {
        private boolean started = false;
        private final DecoratedKey key;
        private final Consumer<PaxosRepair.Result> onComplete;

        public MockRepair(DecoratedKey key, Consumer<PaxosRepair.Result> onComplete)
        {
            this.key = key;
            this.onComplete = onComplete;
        }

        @Override
        public DecoratedKey key()
        {
            return key;
        }

        @Override
        public PaxosTableRepairs.QueueableRepair start()
        {
            started = true;
            return this;
        }

        @Override
        public void cancel()
        {
            onComplete.accept(PaxosRepair.CANCELLED);
        }

        void complete()
        {
            onComplete.accept(PaxosRepair.DONE);
        }
    }

    private static class MockTableRepairs extends PaxosTableRepairs
    {
        @Override
        MockRepair createRepair(DecoratedKey key, ConsistencyLevel consistency, CFMetaData cfm, Consumer<PaxosRepair.Result> onComplete)
        {
            return new MockRepair(key, onComplete);
        }

        MockRepair startOrQueue(DecoratedKey key)
        {
            return (MockRepair) startOrQueue(key, ConsistencyLevel.SERIAL, null, r -> {});
        }
    }

    /**
     * repairs with different keys shouldn't interfere with each other
     */
    @Test
    public void testMultipleRepairs()
    {
        MockTableRepairs repairs = new MockTableRepairs();

        MockRepair repair1 = repairs.startOrQueue(DK1);
        MockRepair repair2 = repairs.startOrQueue(DK2);

        Assert.assertTrue(repair1.started);
        Assert.assertTrue(repair2.started);
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));
        Assert.assertTrue(repairs.hasActiveRepairs(DK2));

        repair1.complete();
        repair2.complete();

        // completing the repairs should have cleaned repairs map
        Assert.assertFalse(repairs.hasActiveRepairs(DK1));
        Assert.assertFalse(repairs.hasActiveRepairs(DK2));
    }

    @Test
    public void testRepairQueueing()
    {
        MockTableRepairs repairs = new MockTableRepairs();

        MockRepair repair1 = repairs.startOrQueue(DK1);
        MockRepair repair2 = repairs.startOrQueue(DK1);
        MockRepair repair3 = repairs.startOrQueue(DK1);

        Assert.assertTrue(repair1.started);
        Assert.assertFalse(repair2.started);
        Assert.assertFalse(repair3.started);
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));

        repair1.complete();
        Assert.assertTrue(repair2.started);
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));

        repair2.complete();
        Assert.assertTrue(repair3.started);
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));

        // completing the final repair should cleanup the map
        repair3.complete();
        Assert.assertFalse(repairs.hasActiveRepairs(DK1));
    }
}
