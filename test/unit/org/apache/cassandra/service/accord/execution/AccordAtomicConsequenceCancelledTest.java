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

package org.apache.cassandra.service.accord.execution;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.coordinate.Coordinations;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.TestAgent;
import accord.impl.basic.InMemoryJournal;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.ExecutionContext;
import accord.local.FindKeys;
import accord.local.LoadKeys;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.local.durability.DurabilityService;
import accord.primitives.Ballot;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by LLM.
 *
 * <p>An ATOMIC consequence is marked fifo (and so cache queued) when it is preSetup, taking references to its parent's
 * entries but no positions on them, which it takes only once submitted. If its parent fails, it is cancelled before it
 * is submitted (CANCELLED_UNREGISTERED), and releases those references: this must not require that it holds positions
 * it never took, else the release fails an invariant and leaks its references.
 */
public class AccordAtomicConsequenceCancelledTest
{
    private static final TableId TABLE_ID = TableId.fromUUID(new java.util.UUID(0, 1));

    static class RecordingAgent extends TestAgent
    {
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        @Override
        public void onException(Throwable t)
        {
            failure.compareAndSet(null, t);
            super.onException(t);
        }
    }

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void atomicConsequenceOfFailedParentTest() throws InterruptedException
    {
        for (boolean withTxnId : new boolean[] { false, true })
        {
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            RecordingAgent agent = new RecordingAgent();
            AccordExecutor executor = new AccordExecutorSyncSubmit(0, RUN_WITHOUT_LOCK, 2, i -> "Sync" + i, agent);
            executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
            AccordCommandStore store = commandStore(TABLE_ID, partitioner, executor, agent);
            executor.executeDirectlyWithLock(() -> {
                executor.setCapacity(8 << 20);
                executor.setWorkingSetSize(4 << 20);
            });

            try
            {
                RoutingKey key = new TokenKey(TABLE_ID, partitioner.getToken(Int32Type.instance.decompose(0)));
                TxnId txnId = withTxnId ? TxnId.fromValues(1, 1, 0, new Id(1)) : null;
                AtomicReference<Throwable> parentFailure = new AtomicReference<>(), childFailure = new AtomicReference<>();
                CountDownLatch done = CountDownLatch.newCountDownLatch(2);
                ExecutionContext parent = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key), LoadKeys.SYNC, FindKeys.CONFLICTS, "parent");
                ExecutionContext child = atomic(ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key), LoadKeys.SYNC, FindKeys.CONFLICTS, "child"));
                store.execute(parent, (Consumer<? super SafeCommandStore>) safeStore -> {
                    store.execute(child, (Consumer<? super SafeCommandStore>) ignore -> {},
                                  (success, fail) -> { childFailure.set(fail); done.decrement(); });
                    throw new RuntimeException("deliberate failure of parent");
                }, (success, fail) -> { parentFailure.set(fail); done.decrement(); });

                assertTrue(done.await(10, TimeUnit.SECONDS));
                executor.waitForQuiescence();
                assertTrue(String.valueOf(parentFailure.get()), parentFailure.get() instanceof RuntimeException);
                assertTrue(String.valueOf(childFailure.get()), childFailure.get() instanceof CancellationException);
                if (agent.failure.get() != null)
                    throw new AssertionError("unexpected exception reported to the agent (withTxnId=" + withTxnId + ')', agent.failure.get());
                String referenced = stillReferenced(store);
                assertNull(referenced, referenced);
            }
            finally
            {
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    private static ExecutionContext atomic(ExecutionContext wrap)
    {
        return new ExecutionContext.Wrapped()
        {
            @Override public ExecutionContext wrapped() { return wrap; }
            @Override public ExecutionSequence executionSequence() { return ExecutionSequence.ATOMIC; }
        };
    }

    private static String stillReferenced(AccordCommandStore store)
    {
        try (AccordCommandStore.ExclusiveCaches caches = store.lockCaches())
        {
            for (AccordCacheEntry<?, ?, ?> entry : caches.commands())
            {
                if (entry.references() != 0)
                    return "leaked " + entry.references() + " references to " + entry;
            }
            for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
            {
                if (entry.references() != 0)
                    return "leaked " + entry.references() + " references to " + entry;
            }
        }
        return null;
    }

    private static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor, TestAgent agent)
    {
        AtomicLong clock = new AtomicLong();
        LongSupplier now = clock::incrementAndGet;
        Id nodeId = new Id(1);
        NodeCommandStoreService node = new NodeCommandStoreService()
        {
            private final ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, this::now);
            private long stamp = 0;

            @Override public AsyncExecutor someExecutor() { return null; }
            @Override public ExclusiveAsyncExecutor someExclusiveExecutor() { return null; }
            @Override public accord.api.Timeouts timeouts() { return null; }
            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }
            @Override public DurabilityService durability() { return null; }
            @Override public Id id() { return nodeId; }
            @Override public long epoch() { return 1; }
            @Override public long now() { return now.getAsLong(); }
            @Override public long uniqueNow(long atLeast) { return now.getAsLong(); }
            @Override public long elapsed(TimeUnit units) { return elapsed.applyAsLong(units); }
            @Override public TopologyManager topology() { throw new UnsupportedOperationException(); }
            @Override public Coordinations coordinations() { return new Coordinations(); }
            @Override public Scheduler scheduler() { return null; }
            @Override public long currentStamp() { return stamp; }
            @Override public void updateStamp() { ++stamp; }
            @Override public boolean isReplaying() { return false; }
            @Override public void reportLocalExecution(TxnId txnId, Route<?> route, Ballot ballot, Timestamp applyAt, Writes writes, Result result) {}
        };

        return new AccordCommandStore(0, node, agent, null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
