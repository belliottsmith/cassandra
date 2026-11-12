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

package org.apache.cassandra.service.accord.interop;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSortedMap;

import org.agrona.collections.IntHashSet;
import org.junit.Test;

import accord.api.Data;
import accord.local.CheckedCommands;
import accord.local.Command;
import accord.local.ExecutionContext;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.Commit;
import accord.messages.PreAccept;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.SimulatedAccordCommandStore;
import org.apache.cassandra.service.accord.SimulatedAccordCommandStoreTestBase;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Interop reads must not execute while the command is PreApplied, as it may still be waiting on local dependencies.
 *
 * T is a committed read with a key dependency W (W.executeAt < T.executeAt, same key). At this replica W has
 * not applied yet. An Apply for T arrives first (e.g. from a recovery coordinator), so T becomes PreApplied
 * while still waiting on W (Commands.apply). Then the original interop coordinator's
 * AccordInteropStableThenRead for T arrives. Its ExecuteOn was (ReadyToExecute, PreApplied), so the read was
 * EXECUTEd at PreApplied without waiting for W. Intended behaviour: a read that executes for T observes every
 * write ordered before T on the key, i.e. W's value 42.
 */
public class AccordInteropReadPreAppliedTest extends SimulatedAccordCommandStoreTestBase
{
    static class RecordingStableThenRead extends AccordInteropStableThenRead
    {
        final CompletableFuture<Data> data = new CompletableFuture<>();
        volatile boolean began;
        volatile String stateAtRead;

        RecordingStableThenRead(TxnId txnId, Participants<?> scope, Commit.Kind kind, long minEpoch, Timestamp executeAt,
                                PartialTxn partialTxn, PartialDeps partialDeps, FullRoute<?> route, ReadCommand command)
        {
            super(txnId, scope, kind, minEpoch, executeAt, partialTxn, partialDeps, route, command);
            this.node = emptyNode();
            // ReadData.process(Node, long) initialises these per-delivery sets before
            // mapReduceConsume submits applyInternal to each command store; this test invokes applyInternal on the
            // single simulated command store directly, so it initialises them the same way.
            try
            {
                for (String name : new String[]{ "waitingOn", "reading" })
                {
                    Field f = accord.messages.ReadData.class.getDeclaredField(name);
                    f.setAccessible(true);
                    f.set(this, new IntHashSet());
                }
            }
            catch (ReflectiveOperationException e)
            {
                throw new AssertionError(e);
            }
        }

        @Override
        protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Participants<?> execute)
        {
            Command c = safeStore.get(txnId, StoreParticipants.empty(txnId)).current();
            began = true;
            stateAtRead = "T.saveStatus=" + c.saveStatus() + " T.waitingOn.isWaiting=" + (c.waitingOn() != null && c.waitingOn().isWaiting()) + " T.waitingOn=" + c.waitingOn();
            return super.beginRead(safeStore, executeAt, txn, execute).map(d -> { data.complete(d); return d; });
        }
    }

    @Test
    public void interopReadAtPreAppliedMustSeeEarlierDependencyWrite() throws Throwable
    {
        run(false);
    }

    /**
     * Control: same txns, but W applies first and T executes from ReadyToExecute (no Apply for T yet). The read runs
     * and returns W's value 42, which shows the harness and result extraction observe W's write when it is applied.
     */
    @Test
    public void controlReadAfterDependencyAppliedSeesWrite() throws Throwable
    {
        run(true);
    }

    private void run(boolean control) throws Throwable
    {
        AccordKeyspace.unsafeClear();
        try (SimulatedAccordCommandStore instance = new SimulatedAccordCommandStore(intTbl.id, RandomSource.wrap(new Random(42))))
        {
            // This replica owns its ranges and may serve reads for them at any executeAt (as after a completed bootstrap).
            instance.process((ExecutionContext.Empty) () -> "safeToRead", safe -> {
                safe.setSafeToRead(ImmutableSortedMap.of(Timestamp.NONE, safe.ranges().all()));
                return null;
            });

            // W: a write of value 42 to pk=1; it becomes Stable here but its Apply has not reached this replica.
            Txn w = AccordTestUtils.createTxn(AccordTestUtils.wrapInTxn("UPDATE test.tbl1 SET value = 42 WHERE pk = 1"));
            Keys wKeys = (Keys) w.keys();
            FullRoute<?> wRoute = wKeys.toRoute(wKeys.get(0).toUnseekable());
            Ranges ranges = AccordTestUtils.fullRange(w);
            Pair<TxnId, AsyncResult<PreAccept.PreAcceptOk>> wPre = instance.enqueuePreAccept(w, wRoute);
            instance.processAll();
            TxnId wId = wPre.left;
            PreAccept.PreAcceptOk wOk = getBlocking(wPre.right);
            PartialTxn wp = w.slice(ranges, true);
            PartialDeps wDeps = wOk.deps.intersecting(wRoute);
            instance.process(ExecutionContext.unsequencedReadWrite(wId, wRoute, "stable W"), safe -> {
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, wId, wRoute, wp, wId, wDeps);
                return null;
            });
            if (control)
            {
                instance.process(ExecutionContext.unsequencedReadWrite(wId, wRoute, "apply W"), safe -> {
                    CheckedCommands.apply(safe, wId, wRoute, wId, wDeps, wp, w.execute(wId, wId, new TxnData()),
                                          w.query().compute(wId, wId, w.keys(), new TxnData(), w.read(), w.update()).toPersistable());
                    return null;
                });
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!status(instance, wId, wRoute).equals(SaveStatus.Applied))
                {
                    assertTrue("control: W did not reach Applied within 10s: " + status(instance, wId, wRoute), System.nanoTime() < deadline);
                    Thread.sleep(10);
                    instance.processAll();
                }
            }

            // T: a Read-kind txn on pk=1 built as StorageProxy.readWithAccordAsync builds it (non-ephemeral); depends on W.
            long nowInSec = FBUtilities.nowInSeconds();
            SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(intTbl, nowInSec, Int32Type.instance.decompose(1));
            TableMetadatasAndKeys.KeyCollector keyCollector = new TableMetadatasAndKeys.KeyCollector(TableMetadatas.of(intTbl));
            TxnRead txnRead = TxnRead.createSerialRead(Collections.singletonList(cmd), ConsistencyLevel.QUORUM, keyCollector);
            Txn t = new Txn.InMemory(Txn.Kind.Read, txnRead.keys(), txnRead, TxnQuery.ALL, null, keyCollector.buildTablesAndKeys());
            Keys tKeys = (Keys) t.keys();
            FullRoute<?> tRoute = tKeys.toRoute(tKeys.get(0).toUnseekable());
            Pair<TxnId, AsyncResult<PreAccept.PreAcceptOk>> tPre = instance.enqueuePreAccept(t, tRoute);
            instance.processAll();
            TxnId tId = tPre.left;
            PreAccept.PreAcceptOk tOk = getBlocking(tPre.right);
            assertTrue("T's deps must contain W: " + tOk.deps, tOk.deps.contains(wId));
            assertTrue("W.executeAt < T.executeAt", wId.compareTo(tId) < 0);
            assertEquals(Txn.Kind.Read, tId.kind());
            PartialTxn tp = t.slice(ranges, true);
            PartialDeps tDeps = tOk.deps.intersecting(tRoute);
            instance.process(ExecutionContext.unsequencedReadWrite(tId, tRoute, "stable T"), safe -> {
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, tId, tRoute, tp, tId, tDeps);
                return null;
            });

            // An Apply for T reaches this replica before W applies: T is PreApplied, still waiting on W.
            if (!control) instance.process(ExecutionContext.unsequencedReadWrite(tId, tRoute, "apply T"), safe -> {
                CheckedCommands.apply(safe, tId, tRoute, tId, tDeps, tp, null,
                                      t.query().compute(tId, tId, t.keys(), new TxnData(), t.read(), t.update()).toPersistable());
                return null;
            });
            List<String> states = new ArrayList<>();
            instance.process(ExecutionContext.unsequencedReadWrite(tId, tRoute, "inspect T"), safe -> {
                Command tc = safe.get(tId, StoreParticipants.empty(tId)).current();
                states.add("T=" + tc.saveStatus() + " waitingOnKeyOfW=" + (tc.waitingOn() != null && tc.waitingOn().isWaitingOnKey()));
                return null;
            });
            instance.process(ExecutionContext.unsequencedReadWrite(wId, wRoute, "inspect W"), safe -> {
                Command wc = safe.get(wId, StoreParticipants.empty(wId)).current();
                states.add("W=" + wc.saveStatus());
                return null;
            });
            if (control)
            {
                assertEquals("control setup: " + states, "T=ReadyToExecute waitingOnKeyOfW=false", states.get(0));
                assertEquals("control setup: " + states, "W=Applied", states.get(1));
            }
            else
            {
                assertEquals("setup: T must be PreApplied waiting on W: " + states, "T=PreApplied waitingOnKeyOfW=true", states.get(0));
                assertTrue("setup: W must not be applied: " + states, !states.get(1).contains("Applied"));
            }

            // The original interop coordinator's StableThenRead for T now arrives.
            RecordingStableThenRead read = new RecordingStableThenRead(tId, tRoute, Commit.Kind.StableFastPath, tId.epoch(), tId, tp, tDeps, tRoute, cmd);
            Object reply = instance.process(ExecutionContext.unsequencedReadWrite(tId, tRoute, "interop read"), safe -> read.applyInternal(safe));
            instance.processAll();

            if (control)
                assertTrue("control: the read must execute at ReadyToExecute", read.began);
            if (!read.began)
                return; // the read waited or answered obsolete: no snapshot that misses W was served

            Data data = read.data.get(10, TimeUnit.SECONDS);
            List<Integer> values = values(data, cmd);
            if (!values.contains(42))
                fail("AccordInteropStableThenRead executed T's read while " + read.stateAtRead + " and W=" + states.get(1)
                     + " (W.executeAt < T.executeAt, same key); the read returned values " + values
                     + " and misses W's write value 42");
            assertEquals(1, values.size());
        }
    }

    private static SaveStatus status(SimulatedAccordCommandStore instance, TxnId id, FullRoute<?> route) throws Exception
    {
        List<SaveStatus> out = new ArrayList<>();
        instance.process(ExecutionContext.unsequencedReadWrite(id, route, "status"), safe -> {
            out.add(safe.get(id, StoreParticipants.empty(id)).current().saveStatus());
            return null;
        });
        return out.get(0);
    }

    @SuppressWarnings("unchecked")
    private static List<Integer> values(Data data, ReadCommand cmd) throws Exception
    {
        Field f = data.getClass().getDeclaredField("localResponses");
        f.setAccessible(true);
        List<Pair<?, ReadResponse>> responses = (List<Pair<?, ReadResponse>>) f.get(data);
        ColumnMetadata column = intTbl.getColumn(new org.apache.cassandra.cql3.ColumnIdentifier("value", true));
        List<Integer> values = new ArrayList<>();
        for (Pair<?, ReadResponse> p : responses)
        {
            try (UnfilteredPartitionIterator it = p.right.makeIterator(cmd))
            {
                while (it.hasNext())
                {
                    try (UnfilteredRowIterator rows = it.next())
                    {
                        while (rows.hasNext())
                        {
                            Unfiltered u = rows.next();
                            if (!u.isRow()) continue;
                            Cell<?> cell = ((Row) u).getCell(column);
                            if (cell != null)
                            {
                                ByteBuffer b = cell.buffer();
                                values.add(Int32Type.instance.compose(b));
                            }
                        }
                    }
                }
            }
        }
        return values;
    }
}
