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
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;


public class WrapCellsBuilderCQLTest extends CQLTester
{
    enum Call { ADD, END_COL }

    private EnumMap<Call, Integer> before;
    private EnumMap<Call, Integer> after;

    private void assertCalled(Map<Call, Integer> expectedCalls) {
        for (Call call: Call.values()) {
            int called = after.getOrDefault(call, 0) - before.getOrDefault(call, 0);
            int expected = expectedCalls.getOrDefault(call, 0);
            assertEquals(call.toString(), expected, called);
        }
    }

    protected UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        before = ResolverCountingIntSetType.calls.get();
        UntypedResultSet result = super.execute(query, values);
        after = ResolverCountingIntSetType.calls.get();
        return result;
    }

    public void flush()
    {
        before = ResolverCountingIntSetType.calls.get();
        super.flush();
        after = ResolverCountingIntSetType.calls.get();
    }

    public void compact()
    {
        before = ResolverCountingIntSetType.calls.get();
        super.compact();
        after = ResolverCountingIntSetType.calls.get();
    }

    @Test
    public void resolvesOnUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v '" +
                    ResolverCountingIntSetType.class.getCanonicalName() + "')");
        assertEmpty(execute("SELECT v FROM %s where pk = 1"));
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s(pk, v) VALUES (1, { 0 })");
        // [0] UpdateStatement -> Update Parameters -> BtreeRow$Builder
        // [0] Memtable -> addAllWithSizeDelta -> BTreeRow$Builder
        assertCalled(ImmutableMap.of(Call.ADD, 2, Call.END_COL, 2));

        // Replace the set with a new one, no merging should happen with the old
        execute("INSERT INTO %s(pk, v) VALUES (1, { 1 })");
        // [1] UpdateStatement -> Update Parameters -> BtreeRow$Builder
        // [1] Memtable -> addAllWithSizeDelta -> AtomicBTreePartition$RowUpdater Rows.merge
        // [1] Memtable -> addAllWithSizeDelta -> AtomicBTreePartition$RowUpdater build
        assertCalled(ImmutableMap.of(Call.ADD, 3, Call.END_COL, 3));

        // [2] UpdateStatement -> Update Parameters -> BtreeRow$Builder
        // [1, 2] Memtable -> addAllWithSizeDelta -> AtomicBTreePartition$RowUpdater Rows.merge
        // [1, 2] Memtable -> addAllWithSizeDelta -> AtomicBTreePartition$RowUpdater build
        execute("UPDATE %s SET v = v + { 2 } WHERE pk = 1");
        assertCalled(ImmutableMap.of(Call.ADD, 5, Call.END_COL, 3));

        // No resolution as all resolved into single Memtable BtreeRow
        assertRows(execute("SELECT v FROM %s where pk = 1"), row(set(1, 2)));

        // rdar://63285708 (CIE4 Followup - Cassandra 4.0 off-heap changes increase calls to cell resolver)
        // CIE3 called ADD zero times, CIE4 calls 2 times due to off-heap changes
        assertCalled(ImmutableMap.of(Call.ADD, 2, Call.END_COL, 1));

        // [2, 3, 4] UpdateStatement -> Update Parameters -> BtreeRow$Builder
        // [1, 2, 3, 4] Memtable -> addAllWithSizeDelta -> AtomicBTreePartition$RowUpdater Rows.merge
        // [1, 2, 3, 4] Memtable -> addAllWithSizeDelta -> AtomicBTreePartition$RowUpdater build
        execute("UPDATE %s SET v = v + { 2, 3, 4 } WHERE pk = 1");
        assertCalled(ImmutableMap.of(Call.ADD, 11, Call.END_COL, 3));

        // No resolution as all resolved into single Memtable BtreeRow
        assertRows(execute("SELECT v FROM %s where pk = 1"), row(set(1, 2, 3, 4)));
        // rdar://63285708 (CIE4 Followup - Cassandra 4.0 off-heap changes increase calls to cell resolver)
        // CIE3 called ADD zero times, CIE4 calls 4 times due to off-heap changes
        assertCalled(ImmutableMap.of(Call.ADD, 4, Call.END_COL, 1));
    }

    @Test
    public void resolvesOnCompact() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v '" +
                    ResolverCountingIntSetType.class.getCanonicalName() + "')");
        disableCompaction(KEYSPACE);
        flush(); // get system activty out of the way for looking at logs
        compact();
        logger.info("Schema changes flushed/compacted - beginning of test output");

        execute("UPDATE %s SET v = v + { 1 } WHERE pk = 1");
        // [1] UpdateStatement -> Update Parameters -> BtreeRow$Builder
        // [1] Memtable -> addAllWithSizeDelta -> BTreeRow$Builder
        assertCalled(ImmutableMap.of(Call.ADD, 2, Call.END_COL, 2));
        flush();

        // rdar://63285708 (CIE4 Followup - Cassandra 4.0 off-heap changes increase calls to cell resolver)
        // CIE3 called ADD zero times, CIE4 calls 3 times due to off-heap changes
        assertCalled(ImmutableMap.of(Call.ADD, 3, Call.END_COL, 3));

        execute("UPDATE %s SET v = v + { 2 } WHERE pk = 1");
        // [2] UpdateStatement -> Update Parameters -> BtreeRow$Builder
        // [2] Memtable -> addAllWithSizeDelta -> BTreeRow$Builder
        assertCalled(ImmutableMap.of(Call.ADD, 2, Call.END_COL, 2));
        flush();

        // rdar://63285708 (CIE4 Followup - Cassandra 4.0 off-heap changes increase calls to cell resolver)
        // CIE3 called ADD zero times, CIE4 calls 3 times due to off-heap changes
        assertCalled(ImmutableMap.of(Call.ADD, 3, Call.END_COL, 3));

        execute("UPDATE %s SET v = v + { 3 } WHERE pk = 1");
        // [3] UpdateStatement -> Update Parameters -> BtreeRow$Builder
        // [3] Memtable -> addAllWithSizeDelta -> BTreeRow$Builder
        assertCalled(ImmutableMap.of(Call.ADD, 2, Call.END_COL, 2));
        flush();
        // rdar://63285708 (CIE4 Followup - Cassandra 4.0 off-heap changes increase calls to cell resolver)
        // CIE3 called ADD zero times, CIE4 calls 3 times due to off-heap changes
        assertCalled(ImmutableMap.of(Call.ADD, 3, Call.END_COL, 3));

        compact();
        // [3] CompactionTask -> SSTableSimpleIterator -> BTreeRow.Builder.CellResolver
        // [2] CompactionTask -> SSTableSimpleIterator -> BTreeRow.Builder.CellResolver
        // [1] CompactionTask -> SSTableSimpleIterator -> BTreeRow.Builder.CellResolver
        // [1,2,3] CompactionTask -> MergeIterator -> getReduced
        assertCalled(ImmutableMap.of(Call.ADD, 6, Call.END_COL, 4));

    }
}
