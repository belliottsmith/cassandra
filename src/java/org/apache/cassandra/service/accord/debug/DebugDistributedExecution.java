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

package org.apache.cassandra.service.accord.debug;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import accord.api.CoordinatorEventListener;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.ReplicaEventListener;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Tracing;
import accord.coordinate.Coordination;
import accord.coordinate.Coordination.CoordinationKind;
import accord.coordinate.ExecutePath;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.NotifySink;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Participants;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.TxnId;
import accord.messages.Message;
import accord.messages.Request;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.locator.InetAddressAndPort;

import one.profiler.Span;

/**
 * Emits async-profiler {@link Span}s for each step of a transaction's distributed execution, tagged with its
 * {@link TxnId} so that the recordings of all replicas can be joined into one timeline per transaction.
 *
 * Enabled with {@code -Daccord.debug_distributed_execution=N}: 0 (default) disables it, otherwise one transaction in
 * N is traced (1 traces all). The choice is a pure function of the TxnId, so every node traces the same transactions
 * without any coordination. Spans are recorded unconditionally (Span.end, not endIfProfiled), so they are not biased
 * towards long ones, but they exist only while a profiler recording is running.
 *
 * Every tag starts with "DX ", then a role and an event, then the TxnId; the remaining fields depend on the event:
 * <pre>
 *   DX C &lt;CoordinationKind&gt; &lt;txnId&gt;                      coordinator: one coordination phase (a span)
 *   DX E &lt;event&gt; &lt;txnId&gt; [path|durability]              coordinator: CoordinatorEventListener event (instant)
 *   DX R &lt;event&gt; &lt;txnId&gt; s&lt;store&gt; [status]             replica: ReplicaEventListener event (instant)
 *   DX M Send|Recv|Reply|Resp &lt;type&gt; &lt;txnId&gt; n&lt;peer&gt;       messages (instant); peer is the remote node
 *   DX Q|T &lt;reason&gt; &lt;txnId&gt; s&lt;store&gt;                    command store task: queued / running (spans)
 *   DX K &lt;status&gt; &lt;txnId&gt; k&lt;key&gt; s&lt;store&gt;               a transaction's CommandsForKey update for one key
 *   DX K Unblock &lt;waiter&gt; k&lt;key&gt; s&lt;store&gt; by &lt;trigger&gt;  the key no longer holds up the waiter's execution;
 *                                                       trigger is the transaction whose update released it ("-" if
 *                                                       none, e.g. a RedundantBefore update)
 *   DX K BlockedOn &lt;dep&gt; k&lt;key&gt; s&lt;store&gt; &lt;status&gt; &lt;until&gt; by &lt;trigger&gt;
 *                                                       the key cannot progress until dep reaches status
 * </pre>
 * K events are emitted for transactions sampled as above, and additionally for every transaction touching a sampled
 * key with {@code -Daccord.debug_distributed_execution_keys=K} (one key in K, by RoutingKey hash; 1 = all keys), so
 * that a sampled key's complete history is recorded and every waiter can be lined up with its trigger.
 * Messages delivered to the local node bypass the message sink, so they have no M events; the R/Q/T events of the
 * local replica are still emitted.
 */
public final class DebugDistributedExecution
{
    public static final int SAMPLE = CassandraRelevantProperties.ACCORD_DEBUG_DISTRIBUTED_EXECUTION.getInt();
    public static final int KEY_SAMPLE = CassandraRelevantProperties.ACCORD_DEBUG_DISTRIBUTED_EXECUTION_KEYS.getInt();
    public static final boolean ENABLED = SAMPLE > 0 || KEY_SAMPLE > 0;

    private static final String PREFIX = "DX ";
    // message ids of traced transactions, for tagging replies and responses with their TxnId; bounded crudely
    private static final int MAX_PENDING = 1 << 20;
    private static final Map<Long, TxnId> sent = new ConcurrentHashMap<>();
    private static final Map<String, TxnId> received = new ConcurrentHashMap<>();

    private DebugDistributedExecution() {}

    /**
     * true if the transaction is sampled and a profiler recording is running (Span.start() is 0 otherwise), so that
     * no tags are built while nothing would be recorded
     */
    public static boolean traced(@Nullable TxnId txnId)
    {
        return isSampled(txnId) && Span.start() != 0;
    }

    public static boolean isSampled(@Nullable TxnId txnId)
    {
        if (SAMPLE <= 0 || txnId == null)
            return false;
        if (SAMPLE == 1)
            return true;
        long h = (txnId.hlc() ^ ((long) txnId.node.id << 48)) * 0x9E3779B97F4A7C15L;
        return Long.remainderUnsigned(h ^ (h >>> 29), SAMPLE) == 0;
    }

    public static void instant(String tag)
    {
        long now = Span.start();
        if (now != 0)
            Span.emit(now, now, PREFIX + tag);
    }

    public static long start()
    {
        return Span.start();
    }

    public static void end(long start, String tag)
    {
        if (start != 0)
            Span.end(start, PREFIX + tag);
    }

    /*
     * Coordinator: a span per coordination phase (PreAccept, Propose, Stabilise, Execute, Persist, recovery, progress
     * log, ...), from the creation of its Tracing to Tracing.done(). Wraps whatever AccordTracing returned.
     */
    public static @Nullable Tracing phase(TxnId txnId, CoordinationKind kind, @Nullable Tracing delegate)
    {
        if (!traced(txnId))
            return delegate;
        return new PhaseSpan("C " + kind + ' ' + txnId, delegate);
    }

    private static final class PhaseSpan implements Tracing
    {
        final long start = Span.start();
        final String tag;
        final @Nullable Tracing delegate;
        boolean done;

        PhaseSpan(String tag, @Nullable Tracing delegate)
        {
            this.tag = tag;
            this.delegate = delegate;
        }

        @Override
        public void trace(CommandStore commandStore, String message)
        {
            if (delegate != null) delegate.trace(commandStore, message);
        }

        @Override
        public void trace(CommandStore commandStore, String fmt, Object... args)
        {
            // don't format messages unless someone else is tracing this transaction
            if (delegate != null) delegate.trace(commandStore, fmt, args);
        }

        @Override
        public void done()
        {
            synchronized (this)
            {
                if (done) return;
                done = true;
            }
            end(start, tag);
            if (delegate != null) delegate.done();
        }

        @Override
        public Tracing send()
        {
            return delegate == null ? null : delegate.send();
        }
    }

    /*
     * Messages: the M events. Request ids of traced transactions are remembered so that the reply sent by a replica,
     * and the response received by the coordinator, can be tagged with the TxnId too.
     */
    public static void onSend(Node.Id to, Request request, long messageId, boolean expectsReply)
    {
        TxnId txnId = request.primaryTxnId();
        if (!traced(txnId))
            return;
        if (expectsReply)
            remember(sent, messageId, txnId);
        instant("M Send " + request.type() + ' ' + txnId + " n" + to.id);
    }

    public static void onReceive(@Nullable Node.Id from, InetAddressAndPort fromEndpoint, Request request, long messageId)
    {
        TxnId txnId = request.primaryTxnId();
        if (!traced(txnId))
            return;
        remember(received, fromEndpoint.toString() + '#' + messageId, txnId);
        instant("M Recv " + request.type() + ' ' + txnId + " n" + (from == null ? "?" : from.id));
    }

    public static void onReply(Node.Id to, InetAddressAndPort toEndpoint, long messageId, @Nullable Message reply, boolean isFinal)
    {
        if (received.isEmpty())
            return;
        String key = toEndpoint.toString() + '#' + messageId;
        TxnId txnId = isFinal ? received.remove(key) : received.get(key);
        if (txnId != null)
            instant("M Reply " + (reply == null ? "failure" : reply.type()) + ' ' + txnId + " n" + to.id);
    }

    public static void onResponse(Node.Id from, long messageId, @Nullable Object reply, boolean isFinal)
    {
        if (sent.isEmpty())
            return;
        TxnId txnId = isFinal ? sent.remove(messageId) : sent.get(messageId);
        if (txnId != null)
            instant("M Resp " + (reply instanceof Message ? ((Message) reply).type() : "failure") + ' ' + txnId + " n" + from.id);
    }

    private static <K> void remember(Map<K, TxnId> map, K key, TxnId txnId)
    {
        if (map.size() >= MAX_PENDING)
            map.clear(); // requests that never got a reply (timeouts, local delivery); losing tags is acceptable here
        map.put(key, txnId);
    }

    /*
     * Keys: K events. SaferCommandsForKey reports each update of a key's CommandsForKey caused by a transaction, and
     * wraps the NotifySink through which the CommandsForKey releases (notWaiting) or blocks (waitingOn) transactions;
     * the transaction whose update is in progress on this thread is the trigger of any release.
     */
    private static final ThreadLocal<TxnId> trigger = new ThreadLocal<>();
    private static final NotifySink DEFAULT_SINK = new NotifySink.DefaultNotifySink();
    private static final NotifySink TRACING_DEFAULT_SINK = new KeyNotifySink(DEFAULT_SINK);

    public static boolean isKeySampled(@Nullable RoutingKey key)
    {
        if (KEY_SAMPLE <= 0 || key == null)
            return false;
        if (KEY_SAMPLE == 1)
            return true;
        long h = key.hashCode() * 0x9E3779B97F4A7C15L;
        return Long.remainderUnsigned(h ^ (h >>> 29), KEY_SAMPLE) == 0;
    }

    private static boolean tracedAt(RoutingKey key, @Nullable TxnId a, @Nullable TxnId b)
    {
        return (isKeySampled(key) || isSampled(a) || isSampled(b)) && Span.start() != 0;
    }

    /** @return the previous trigger, to pass to {@link #endKeyUpdate} */
    public static @Nullable TxnId beginKeyUpdate(SafeCommandStore safeStore, RoutingKey key, String event, TxnId txnId)
    {
        if (tracedAt(key, txnId, null))
            instant("K " + event + ' ' + txnId + " k" + key + " s" + safeStore.commandStore().id());
        TxnId prev = trigger.get();
        trigger.set(txnId);
        return prev;
    }

    public static void endKeyUpdate(@Nullable TxnId prev)
    {
        if (prev == null) trigger.remove();
        else trigger.set(prev);
    }

    public static NotifySink sink(@Nullable NotifySink override)
    {
        return override == null ? TRACING_DEFAULT_SINK : new KeyNotifySink(override);
    }

    private static final class KeyNotifySink implements NotifySink
    {
        final NotifySink delegate;
        KeyNotifySink(NotifySink delegate) { this.delegate = delegate; }

        @Override
        public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key, long uniqueHlc)
        {
            TxnId by = trigger.get();
            if (tracedAt(key, txnId, by))
                instant("K Unblock " + txnId + " k" + key + " s" + safeStore.commandStore().id() + " by " + (by == null ? "-" : by.toString()));
            delegate.notWaiting(safeStore, txnId, key, uniqueHlc);
        }

        @Override
        public void waitingOn(SafeCommandStore safeStore, TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
        {
            TxnId dep = txn.plainTxnId(), by = trigger.get();
            if (tracedAt(key, dep, by))
                instant("K BlockedOn " + dep + " k" + key + " s" + safeStore.commandStore().id() + ' ' + waitingOnStatus + ' ' + blockedUntil + " by " + (by == null ? "-" : by.toString()));
            delegate.waitingOn(safeStore, txn, key, waitingOnStatus, blockedUntil, notifyCfk);
        }
    }

    /*
     * Command store tasks: Q (created -> running) and T (running) spans, tagged with the task's reason and primary TxnId.
     */
    public static @Nullable String taskTag(@Nullable TxnId txnId, String reason, int commandStoreId)
    {
        return traced(txnId) ? reason + ' ' + txnId + " s" + commandStoreId : null;
    }

    /*
     * Coordinator and replica state transitions.
     */
    public static CoordinatorEventListener wrap(CoordinatorEventListener delegate)
    {
        return ENABLED ? new CoordinatorEvents(delegate) : delegate;
    }

    public static ReplicaEventListener wrap(ReplicaEventListener delegate)
    {
        return ENABLED ? new ReplicaEvents(delegate) : delegate;
    }

    private static void coordinator(String event, TxnId txnId, @Nullable Object extra)
    {
        if (traced(txnId))
            instant("E " + event + ' ' + txnId + (extra == null ? "" : " " + extra));
    }

    private static void replica(String event, SafeCommandStore safeStore, Command command, @Nullable Object extra)
    {
        TxnId txnId = command.txnId();
        if (traced(txnId))
            instant("R " + event + ' ' + txnId + " s" + safeStore.commandStore().id() + (extra == null ? "" : " " + extra));
    }

    private static final class CoordinatorEvents implements CoordinatorEventListener
    {
        final CoordinatorEventListener delegate;
        CoordinatorEvents(CoordinatorEventListener delegate) { this.delegate = delegate; }

        @Override public void onFailed(Throwable failure, TxnId txnId, Participants<?> participants, Coordination coordination)
        {
            coordinator("Failed", txnId, failure.getClass().getSimpleName());
            delegate.onFailed(failure, txnId, participants, coordination);
        }
        @Override public void onPreAccepted(TxnId txnId) { coordinator("PreAccepted", txnId, null); delegate.onPreAccepted(txnId); }
        @Override public void onAccepted(TxnId txnId, Ballot ballot, @Nullable ExecutePath path) { coordinator("Accepted", txnId, path); delegate.onAccepted(txnId, ballot, path); }
        @Override public void onExecuting(TxnId txnId, @Nullable Ballot ballot, Deps deps, @Nullable ExecutePath path)
        {
            coordinator("Executing", txnId, path + " deps=" + deps.txnIdCount());
            delegate.onExecuting(txnId, ballot, deps, path);
        }
        @Override public void onExecuted(TxnId txnId, Ballot ballot) { coordinator("Executed", txnId, null); delegate.onExecuted(txnId, ballot); }
        @Override public void onDurable(Durability durability, @Nullable Ballot ballot, TxnId txnId) { coordinator("Durable", txnId, durability); delegate.onDurable(durability, ballot, txnId); }
        @Override public void onRecoveryStarted(TxnId txnId, Ballot ballot) { coordinator("RecoveryStarted", txnId, ballot); delegate.onRecoveryStarted(txnId, ballot); }
        @Override public void onRecoveryStopped(Node node, TxnId txnId, Ballot ballot, Result success, Throwable fail)
        {
            coordinator("RecoveryStopped", txnId, fail == null ? null : fail.getClass().getSimpleName());
            delegate.onRecoveryStopped(node, txnId, ballot, success, fail);
        }
        @Override public void onInvalidated(TxnId txnId) { coordinator("Invalidated", txnId, null); delegate.onInvalidated(txnId); }
        @Override public void onRejected(TxnId txnId) { coordinator("Rejected", txnId, null); delegate.onRejected(txnId); }
        @Override public void onEpochTimeout(long epoch) { delegate.onEpochTimeout(epoch); }
        @Override public void onExhausted(@Nullable TxnId txnId) { coordinator("Exhausted", txnId, null); delegate.onExhausted(txnId); }
        @Override public void onPreempted(@Nullable TxnId txnId) { coordinator("Preempted", txnId, null); delegate.onPreempted(txnId); }
        @Override public void onTimeout(@Nullable TxnId txnId) { coordinator("Timeout", txnId, null); delegate.onTimeout(txnId); }
    }

    private static final class ReplicaEvents implements ReplicaEventListener
    {
        final ReplicaEventListener delegate;
        ReplicaEvents(ReplicaEventListener delegate) { this.delegate = delegate; }

        @Override public void onRejectPreAccept(SafeCommandStore s, Command c, Object reason) { replica("RejectPreAccept", s, c, reason); delegate.onRejectPreAccept(s, c, reason); }
        @Override public void onPreAccepted(SafeCommandStore s, Command c) { replica("PreAccepted", s, c, null); delegate.onPreAccepted(s, c); }
        @Override public void onRejectPreNotAccept(SafeCommandStore s, Command c, Object reason) { replica("RejectPreNotAccept", s, c, reason); delegate.onRejectPreNotAccept(s, c, reason); }
        @Override public void onPreNotAccepted(SafeCommandStore s, Command c) { replica("PreNotAccepted", s, c, null); delegate.onPreNotAccepted(s, c); }
        @Override public void onRejectAccept(SafeCommandStore s, Command c, Object reason) { replica("RejectAccept", s, c, reason); delegate.onRejectAccept(s, c, reason); }
        @Override public void onAccepted(SafeCommandStore s, Command c) { replica("Accepted", s, c, null); delegate.onAccepted(s, c); }
        @Override public void onRejectNotAccept(SafeCommandStore s, Command c, Object reason) { replica("RejectNotAccept", s, c, reason); delegate.onRejectNotAccept(s, c, reason); }
        @Override public void onNotAccepted(SafeCommandStore s, Command c) { replica("NotAccepted", s, c, null); delegate.onNotAccepted(s, c); }
        @Override public void onRejectCommitOrStable(SafeCommandStore s, SaveStatus commitOrStable, Command c, Object reason)
        {
            replica("RejectCommitOrStable", s, c, commitOrStable + " " + reason);
            delegate.onRejectCommitOrStable(s, commitOrStable, c, reason);
        }
        @Override public void onCommitted(SafeCommandStore s, Command c) { replica("Committed", s, c, null); delegate.onCommitted(s, c); }
        @Override public void onStable(SafeCommandStore s, Command c) { replica("Stable", s, c, null); delegate.onStable(s, c); }
        @Override public void onReadWaiting(SafeCommandStore s, Command c) { replica("ReadWaiting", s, c, c.saveStatus()); delegate.onReadWaiting(s, c); }
        @Override public void onReadStarted(SafeCommandStore s, Command c) { replica("ReadStarted", s, c, null); delegate.onReadStarted(s, c); }
        @Override public void onPreApplied(SafeCommandStore s, Command c) { replica("PreApplied", s, c, null); delegate.onPreApplied(s, c); }
        @Override public void onApplied(SafeCommandStore s, Command c) { replica("Applied", s, c, null); delegate.onApplied(s, c); }
        @Override public void onLocalExecution(Node node, TxnId txnId, Result result) { coordinator("LocalExecution", txnId, null); delegate.onLocalExecution(node, txnId, result); }
    }
}
