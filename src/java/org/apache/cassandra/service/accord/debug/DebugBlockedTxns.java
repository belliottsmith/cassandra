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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.READ_WRITE;
import static java.util.Collections.emptyList;

public class DebugBlockedTxns extends DebugTxnGraph1<DebugBlockedTxns.Txn>
{
    public static class Txn extends AbstractInfo<Txn>
    {
        public final TxnId txnId;
        public final Timestamp executeAt;
        public final SaveStatus saveStatus;
        public final RoutingKey blockedViaKey;
        public final List<TxnId> blockedBy;
        public final List<TokenKey> blockedByKey;

        public Txn(int commandStoreId, int depth, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, RoutingKey blockedViaKey, List<TxnId> blockedBy, List<TokenKey> blockedByKey)
        {
            super(commandStoreId, depth);
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.saveStatus = saveStatus;
            this.blockedViaKey = blockedViaKey;
            this.blockedBy = blockedBy;
            this.blockedByKey = blockedByKey;
        }

        public boolean isBlocked()
        {
            return !notBlocked();
        }

        public boolean notBlocked()
        {
            return blockedBy.isEmpty() && blockedByKey.isEmpty();
        }

        @Override
        public int compareTo(Txn that)
        {
            int c = Integer.compare(this.commandStoreId, that.commandStoreId);
            if (c == 0) c = Integer.compare(this.depth, that.depth);
            if (c == 0) c = this.txnId.compareTo(that.txnId);
            if (c == 0) c = this.blockedViaKeyString().compareTo(that.blockedViaKeyString());
            return c;
        }

        private String blockedViaKeyString()
        {
            return blockedViaKey == null ? "" : blockedViaKey.toString();
        }
    }

    final int maxDepth;
    final ConcurrentLinkedQueue<AsyncChain<Void>> queuedKeys = new ConcurrentLinkedQueue<>();

    public DebugBlockedTxns(IAccordService service, TxnId root, int maxDepth, Consumer<Txn> visit)
    {
        super(service, root, visit);
        this.maxDepth = maxDepth;
    }

    public static void visit(IAccordService accord, TxnId txnId, int maxDepth, long deadlineNanos, Consumer<Txn> visit) throws TimeoutException
    {
        new DebugBlockedTxns(accord, txnId, maxDepth, visit).visit(deadlineNanos);
    }

    @Override
    protected void finishRound(long deadlineNanos, List tmp) throws TimeoutException
    {
        Future<List<Void>> awaitKeys = drainToFuture(queuedKeys, (List<AsyncChain<Void>>)tmp);
        if (awaitKeys != null && !awaitKeys.awaitUntilThrowUncheckedOnInterrupt(deadlineNanos))
            throw new TimeoutException();
    }

    @Override
    protected Txn visitRootTxnSync(SafeCommandStore safeStore, Command command)
    {
        return visitTxnSync(safeStore, command, command.executeAt(), null, new HashSet<>(), 0);
    }

    private AsyncChain<Txn> visitTxnAsync(CommandStore commandStore, TxnId txnId, Timestamp rootExecuteAt, @Nullable TokenKey byKey, Set<Object> visited, int depth)
    {
        return commandStore.chain(PreLoadContext.contextFor(txnId, "Populate txn_blocked_by"), safeStore -> {
            Command command = safeStore.unsafeGetNoCleanup(txnId).current();
            if (command == null || command.saveStatus() == SaveStatus.Uninitialised)
                return null;
            return visitTxnSync(safeStore, command, rootExecuteAt, byKey, visited, depth);
        });
    }

    private Txn visitTxnSync(SafeCommandStore safeStore, Command command, Timestamp rootExecuteAt, @Nullable TokenKey byKey, Set<Object> visited, int depth)
    {
        List<TxnId> waitingOnTxnId = new ArrayList<>();
        List<TokenKey> waitingOnKey = new ArrayList<>();
        if (!command.hasBeen(Status.Applied) && command.hasBeen(Status.Stable))
        {
            // check blocking state
            Command.WaitingOn waitingOn = command.asCommitted().waitingOn();
            waitingOn.waitingOn.reverseForEach(null, null, null, null, (i1, i2, i3, i4, i) -> {
                if (i < waitingOn.txnIdCount()) waitingOnTxnId.add(waitingOn.txnId(i));
                else waitingOnKey.add((TokenKey) waitingOn.keys.get(i - waitingOn.txnIdCount()));
            });
        }

        CommandStore commandStore = safeStore.commandStore();
        if (depth < maxDepth)
        {
            for (TxnId waitingOn : waitingOnTxnId)
            {
                if (visited.add(waitingOn))
                    queued.add(visitTxnAsync(commandStore, waitingOn, rootExecuteAt, null, visited, depth + 1));
            }
            for (TokenKey key : waitingOnKey)
            {
                if (visited.add(key))
                    queuedKeys.add(visitKeysAsync(commandStore, key, rootExecuteAt, visited, depth + 1));
            }
        }

        return new Txn(commandStore.id(), depth, command.txnId(), command.executeAt(), command.saveStatus(), byKey, waitingOnTxnId, waitingOnKey);
    }

    private AsyncChain<Void> visitKeysAsync(CommandStore commandStore, TokenKey key, Timestamp rootExecuteAt, Set<Object> visited, int depth)
    {
        return commandStore.chain(PreLoadContext.contextFor(RoutingKeys.of(key.toUnseekable()), SYNC, READ_WRITE, "Populate txn_blocked_by"), safeStore -> {
            visitKeysSync(safeStore, key, rootExecuteAt, visited, depth);
        });
    }

    private void visitKeysSync(SafeCommandStore safeStore, TokenKey key, Timestamp rootExecuteAt, Set<Object> visited, int depth)
    {
        SafeCommandsForKey commandsForKey = safeStore.ifLoadedAndInitialised(key);
        TxnId blocking = commandsForKey.current().blockedOnTxnId(root, rootExecuteAt);
        CommandStore commandStore = safeStore.commandStore();
        if (blocking == null)
        {
            queued.add(AsyncChains.success(new Txn(commandStore.id(), depth, null, null, null, key, emptyList(), emptyList())));
        }
        else
        {
            if (visited.add(blocking))
                queued.add(visitTxnAsync(commandStore, blocking, rootExecuteAt, key, visited, depth));
        }
    }
}
