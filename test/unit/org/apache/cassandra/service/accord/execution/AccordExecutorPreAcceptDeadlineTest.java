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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableMap;
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
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.local.durability.DurabilityService;
import accord.messages.PreAccept;
import accord.primitives.Ballot;
import accord.primitives.FullKeyRoute;
import accord.primitives.Routable;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.execution.deadline.PreAcceptPenalty;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccordExecutorPreAcceptDeadlineTest
{
    private static final Id FAR = new Id(2), NEAR = new Id(3);
    private static final long FAR_PENALTY_MICROS = TimeUnit.MILLISECONDS.toMicros(300);
    private static final long NEAR_PENALTY_MICROS = TimeUnit.MILLISECONDS.toMicros(100);
    private static final long PROMPT_MICROS = TimeUnit.MILLISECONDS.toMicros(40);

    /** a penalty for each coordinator, by node id */
    public static class ByNodePenalty implements PreAcceptPenalty
    {
        final Map<Integer, Long> micros = new HashMap<>();
        public ByNodePenalty(Map<String, String> params)
        {
            params.forEach((k, v) -> micros.put(Integer.parseInt(k), new DurationSpec.LongMicrosecondsBound(v).to(TimeUnit.MICROSECONDS)));
        }

        @Override
        public long penaltyMicros(Node.Id local, Node.Id coordinator, TxnId txnId)
        {
            return micros.getOrDefault(coordinator.id, 0L);
        }
    }

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.getAccord().preaccept_deadline_penalty = new ParameterizedClass(ByNodePenalty.class.getName(), ImmutableMap.of("2", "300ms", "3", "100ms"));
        DatabaseDescriptor.getAccord().preaccept_deadline_max_penalty = new DurationSpec.LongMicrosecondsBound("1s");
    }

    static class Ran
    {
        final String name;
        final long atMicros;
        Ran(String name, long atMicros) { this.name = name; this.atMicros = atMicros; }
        @Override public String toString() { return name + '@' + atMicros; }
    }

    @Test
    public void contendedPreAcceptsAreDeliveredByDeadline() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey hot = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(0)));
        RoutingKey cold = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(1)));
        Condition loading = Condition.newOneTimeCondition();
        Condition release = Condition.newOneTimeCondition();

        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent());
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, key) -> {
            if (hot.equals(key))
            {
                loading.signal();
                release.awaitUninterruptibly(10, TimeUnit.SECONDS);
            }
            return null;
        }));
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        List<Ran> ran = new CopyOnWriteArrayList<>();
        CountDownLatch done = CountDownLatch.newCountDownLatch(4);
        try
        {
            long now = AccordTimeService.nowMicros();
            // A is uncontended, so runs as soon as the hot key is loaded
            TxnId a = submit(store, hot, now, FAR, "A", ran, done);
            assertTrue(loading.await(10, TimeUnit.SECONDS));
            // B, C and G find A waiting on the hot key, so are contended:
            //  - G has the lowest txnId, so runs first (at hlc + 300ms)
            //  - C has an earlier deadline than B (hlc + 100ms), but a higher txnId, so must wait for B
            TxnId b = submit(store, hot, now + 1000, FAR, "B", ran, done);
            TxnId c = submit(store, hot, now + 2000, NEAR, "C", ran, done);
            TxnId g = submit(store, hot, now - TimeUnit.MILLISECONDS.toMicros(250), FAR, "G", ran, done);
            // D is uncontended
            TxnId d = submit(store, cold, now + 3000, FAR, "D", ran, done);
            release.signal();

            assertTrue(done.await(10, TimeUnit.SECONDS));
            assertEquals(ran.toString(), 5, ran.size());
            Ran ranA = find(ran, "A"), ranB = find(ran, "B"), ranC = find(ran, "C"), ranD = find(ran, "D"), ranG = find(ran, "G");

            assertTrue(ran.toString(), ranA.atMicros < now + PROMPT_MICROS);
            assertTrue(ran.toString(), ranD.atMicros < now + PROMPT_MICROS);
            assertTrue(ran.toString(), ranG.atMicros >= g.hlc() + FAR_PENALTY_MICROS);
            assertTrue(ran.toString(), ranG.atMicros < b.hlc() + NEAR_PENALTY_MICROS);
            assertTrue(ran.toString(), ranB.atMicros >= b.hlc() + FAR_PENALTY_MICROS);
            assertTrue(ran.toString(), ranC.atMicros >= b.hlc() + FAR_PENALTY_MICROS);
            assertTrue(ran.toString(), ran.indexOf(ranA) < ran.indexOf(ranG));
            assertTrue(ran.toString(), ran.indexOf(ranG) < ran.indexOf(ranB));
            assertTrue(ran.toString(), ran.indexOf(ranB) < ran.indexOf(ranC));
        }
        finally
        {
            release.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static Ran find(List<Ran> ran, String name)
    {
        return ran.stream().filter(r -> r.name.equals(name)).findFirst().orElseThrow();
    }

    private static TxnId submit(AccordCommandStore store, RoutingKey key, long hlc, Id coordinator, String name, List<Ran> ran, CountDownLatch done)
    {
        TxnId txnId = new TxnId(1, hlc, Txn.Kind.Write, Routable.Domain.Key, coordinator);
        FullKeyRoute route = new FullKeyRoute(key, new RoutingKey[]{ key });
        PreAccept preAccept = PreAccept.SerializerSupport.create(txnId, route, 1, 1, 1, null, null, false, route);
        store.execute(preAccept, (Consumer<? super SafeCommandStore>) ignore -> ran.add(new Ran(name, AccordTimeService.nowMicros())),
                      (success, fail) -> {
                          if (fail != null) fail.printStackTrace();
                          done.decrement();
                      });
        return txnId;
    }

    private static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor)
    {
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
            @Override public long now() { return AccordTimeService.nowMicros(); }
            @Override public long uniqueNow(long atLeast) { return Math.max(atLeast + 1, now()); }
            @Override public long elapsed(TimeUnit units) { return elapsed.applyAsLong(units); }
            @Override public TopologyManager topology() { throw new UnsupportedOperationException(); }
            @Override public Coordinations coordinations() { return new Coordinations(); }
            @Override public Scheduler scheduler() { return null; }
            @Override public long currentStamp() { return stamp; }
            @Override public void updateStamp() { ++stamp; }
            @Override public boolean isReplaying() { return false; }
            @Override public void reportLocalExecution(TxnId txnId, Route<?> route, Ballot ballot, Timestamp applyAt, Writes writes, Result result) {}
        };

        return new AccordCommandStore(0, node, new TestAgent(), null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
