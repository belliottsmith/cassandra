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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;

public class NativeCellTest
{

    private static final NativeAllocator nativeAllocator = new NativePool(Integer.MAX_VALUE, Integer.MAX_VALUE, 1f, null).newAllocator();
    private static final OpOrder.Group group = new OpOrder().start();

    @Test
    public void testCells() throws IOException
    {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int run = 0 ; run < 1000 ; run++)
        {
            Row.Builder builder = BTreeRow.unsortedBuilder(1);
            builder.newRow(rndclustering());
            int count = rand.nextInt(1, 10);
            for (int i = 0 ; i < count ; i++)
                rndcd(builder);
            test(builder.build());
        }
    }

    private static Clustering rndclustering()
    {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        int count = rand.nextInt(1, 100);
        ByteBuffer[] values = new ByteBuffer[count];
        int size = rand.nextInt(0, 65535);
        for (int i = 0 ; i < count ; i++)
        {
            int twiceShare = 1 + (2 * size) / (count - i);
            int nextSize = Math.min(size, rand.nextInt(0, twiceShare));
            if (nextSize < 10 && rand.nextBoolean())
                continue;

            byte[] bytes = new byte[nextSize];
            rand.nextBytes(bytes);
            values[i] = ByteBuffer.wrap(bytes);
            size -= nextSize;
        }
        return new BufferClustering(values);
    }

    private static void rndcd(Row.Builder builder)
    {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        ColumnDefinition col = rndcol();
        if (!col.isComplex())
        {
            builder.addCell(rndcell(col));
        }
        else
        {
            int count = rand.nextInt(1, 100);
            for (int i = 0 ; i < count ; i++)
                builder.addCell(rndcell(col));
        }
    }

    private static ColumnDefinition rndcol()
    {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        UUID uuid = new UUID(rand.nextLong(), rand.nextLong());
        boolean isComplex = rand.nextBoolean();
        return new ColumnDefinition("", "", ColumnIdentifier.getInterned(uuid.toString(), false), isComplex ? new SetType<>(BytesType.instance, true)
                                                                                                    : BytesType.instance,
                                    -1, ColumnDefinition.Kind.REGULAR);
    }

    private static Cell rndcell(ColumnDefinition col)
    {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        long timestamp = rand.nextLong();
        int ttl = rand.nextInt();
        int localDeletionTime = rand.nextInt();
        byte[] value = new byte[rand.nextInt(0, sanesize(expdecay()))];
        rand.nextBytes(value);
        CellPath path = null;
        if (col.isComplex())
        {
            byte[] pathbytes = new byte[rand.nextInt(0, sanesize(expdecay()))];
            rand.nextBytes(value);
            path = CellPath.create(ByteBuffer.wrap(pathbytes));
        }

        return new BufferCell(col, timestamp, ttl, localDeletionTime, ByteBuffer.wrap(value), path);
    }

    private static int expdecay()
    {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        return 1 << Integer.numberOfTrailingZeros(Integer.lowestOneBit(rand.nextInt()));
    }

    private static int sanesize(int randomsize)
    {
        return Math.min(Math.max(1, randomsize), 1 << 26);
    }

    private static void test(Row row)
    {
        Row nrow = clone(row, nativeAllocator.rowBuilder(group));
        Row brow = clone(row, HeapAllocator.instance.cloningBTreeRowBuilder());
        Assert.assertEquals(row, nrow);
        Assert.assertEquals(row, brow);
    }

    private static Row clone(Row row, Row.Builder builder)
    {
        builder.newRow(row.clustering());
        if (!row.deletion().isLive())
            builder.addRowDeletion(row.deletion());
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        for (ColumnData cd : row)
        {
            if (cd instanceof Cell)
                builder.addCell((Cell) cd);
            else for (Cell cell : (ComplexColumnData) cd)
                builder.addCell(cell);
        }
        return builder.build();
    }

}
