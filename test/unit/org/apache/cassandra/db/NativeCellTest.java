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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CompoundDenseCellNameType;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.composites.SimpleSparseCellNameType;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.data.BufferCell;
import org.apache.cassandra.db.data.BufferCounterCell;
import org.apache.cassandra.db.data.BufferDeletedCell;
import org.apache.cassandra.db.data.BufferExpiringCell;
import org.apache.cassandra.db.data.Cell;
import org.apache.cassandra.db.data.NativeDataAllocator;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.composites.CellNames.compoundDense;
import static org.apache.cassandra.db.composites.CellNames.compoundSparse;
import static org.apache.cassandra.db.composites.CellNames.simpleDense;
import static org.apache.cassandra.db.composites.CellNames.simpleSparse;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class NativeCellTest
{

    private static final NativeDataAllocator nativeAllocator = new NativeDataAllocator.NativeDataPool(Integer.MAX_VALUE, Integer.MAX_VALUE, 1f, null)
                                                               .newGroup("test", new OpOrder(), new OpOrder()).newAllocator();
    private static final OpOrder.Group group = new OpOrder().start();

    static class Name
    {
        final CellName name;
        final CellNameType type;
        Name(CellName name, CellNameType type)
        {
            this.name = name;
            this.type = type;
        }
    }

    static ByteBuffer[] bytess(String ... strings)
    {
        ByteBuffer[] r = new ByteBuffer[strings.length];
        for (int i = 0 ; i < r.length ; i++)
            r[i] = bytes(strings[i]);
        return r;
    }

    final static Name[] TESTS = new Name[]
                          {
                              new Name(simpleDense(bytes("a")), new SimpleDenseCellNameType(UTF8Type.instance)),
                              new Name(simpleSparse(new ColumnIdentifier("a", true)), new SimpleSparseCellNameType(UTF8Type.instance)),
                              new Name(compoundDense(bytes("a"), bytes("b")), new CompoundDenseCellNameType(Arrays.asList(UTF8Type.instance, UTF8Type.instance))),
                              new Name(compoundSparse(bytess("b", "c"), new ColumnIdentifier("a", true), false), new CompoundSparseCellNameType(Arrays.asList(UTF8Type.instance, UTF8Type.instance))),
                              new Name(compoundSparse(bytess("b", "c"), new ColumnIdentifier("a", true), true), new CompoundSparseCellNameType(Arrays.asList(UTF8Type.instance, UTF8Type.instance)))
                          };

    private static final CFMetaData metadata = new CFMetaData("", "", ColumnFamilyType.Standard, null);
    static
    {
        try
        {
            metadata.addColumnDefinition(new ColumnDefinition(null, null, new ColumnIdentifier("a", true), UTF8Type.instance, null, null, null, null, null));
        }
        catch (ConfigurationException e)
        {
            throw new AssertionError();
        }
    }

    @Test
    public void testCells() throws IOException
    {
        Random rand = ThreadLocalRandom.current();
        for (Name test : TESTS)
        {
            byte[] bytes = new byte[16];
            rand.nextBytes(bytes);

            // test regular Cell
            Cell buf, nat;
            buf = new BufferCell(test.name, ByteBuffer.wrap(bytes), rand.nextLong());
            nat = buf.localCopy(metadata, nativeAllocator, group);
            test(test, buf, nat);

            // test DeletedCell
            buf = new BufferDeletedCell(test.name, rand.nextInt(100000), rand.nextLong());
            nat = buf.localCopy(metadata, nativeAllocator, group);
            test(test, buf, nat);

            // test ExpiringCell
            buf = new BufferExpiringCell(test.name, ByteBuffer.wrap(bytes), rand.nextLong(),  rand.nextInt(100000));
            nat = buf.localCopy(metadata, nativeAllocator, group);
            test(test, buf, nat);

            // test CounterCell
            buf = new BufferCounterCell(test.name, CounterContext.instance().createLocal(rand.nextLong()), rand.nextLong(),  rand.nextInt(100000));
            nat = buf.localCopy(metadata, nativeAllocator, group);
            test(test, buf, nat);
        }
    }

    static void test(Name test, Cell buf, Cell nat) throws IOException
    {
        Assert.assertEquals(buf, nat);
        Assert.assertEquals(nat, buf);
        Assert.assertEquals(buf, buf);

        byte[] serialized;
        try (DataOutputBuffer bufOut = new DataOutputBuffer())
        {
            test.type.columnSerializer().serialize(nat, bufOut);
            serialized = bufOut.getData();
        }

        ByteArrayInputStream bufIn = new ByteArrayInputStream(serialized, 0, serialized.length);
        Cell deserialized = test.type.columnSerializer().deserialize(new DataInputStream(bufIn));
        Assert.assertEquals(buf, deserialized);
    }

}
