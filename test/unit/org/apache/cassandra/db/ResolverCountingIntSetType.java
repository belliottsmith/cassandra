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

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;

/** Helper class for {@link WrapCellsBuilderCQLTest}
 */
public class ResolverCountingIntSetType extends SetType<Integer>
{
    static class CallCount extends EnumMap<WrapCellsBuilderCQLTest.Call, AtomicInteger>
    {
        CallCount()
        {
            super(WrapCellsBuilderCQLTest.Call.class);
            for (WrapCellsBuilderCQLTest.Call call : WrapCellsBuilderCQLTest.Call.values())
                this.put(call, new AtomicInteger());
        }
        public EnumMap<WrapCellsBuilderCQLTest.Call, Integer> get() {
            EnumMap<WrapCellsBuilderCQLTest.Call, Integer> result = new EnumMap<>(WrapCellsBuilderCQLTest.Call.class);
            this.forEach((c,v) -> result.put(c, this.get(c).get()));
            return result;
        }
    }

    public static final CallCount calls = new CallCount();

    static final ResolverCountingIntSetType instance = new ResolverCountingIntSetType();

    public Cells.Builder wrapCellsBuilder(Cells.Builder builder)
    {
        return new WrapperBuilder(builder);
    }

    private class WrapperBuilder implements Cells.Builder
    {
        private final Cells.Builder builder;

        private WrapperBuilder(Cells.Builder builder)
        {
            this.builder = builder;
        }

        public void addCell(Cell cell)
        {
            calls.get(WrapCellsBuilderCQLTest.Call.ADD).incrementAndGet();
            builder.addCell(cell);
        }

        public void endColumn()
        {
            calls.get(WrapCellsBuilderCQLTest.Call.END_COL).incrementAndGet();
            builder.endColumn();
        }
    }

    private ResolverCountingIntSetType()
    {
        super(Int32Type.instance, true);
    }

    @SuppressWarnings("unused")
    public static ResolverCountingIntSetType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        return instance;
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Custom(this);
    }
}
