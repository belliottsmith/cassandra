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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.utils.concurrent.Future;

abstract class DebugTxnGraph1<T extends DebugTxnGraph1.AbstractInfo<T>>
{
    static abstract class AbstractInfo<T extends AbstractInfo<?>> implements Comparable<T>
    {
        public final int commandStoreId;
        public final int depth;

        public AbstractInfo(int commandStoreId, int depth)
        {
            this.commandStoreId = commandStoreId;
            this.depth = depth;
        }
    }

    final IAccordService service;
    final Consumer<? super T> visit;
    final TxnId root;
    final ConcurrentLinkedQueue<AsyncChain<T>> queued = new ConcurrentLinkedQueue<>();

    public DebugTxnGraph1(IAccordService service, TxnId root, Consumer<? super T> visit)
    {
        this.service = service;
        this.visit = visit;
        this.root = root;
    }

    void visit(long deadlineNanos) throws TimeoutException
    {
        CommandStores commandStores = service.node().commandStores();
        if (commandStores.count() == 0)
            return;

        int[] ids = commandStores.ids();
        List<AsyncChain<T>> chains = new ArrayList<>(ids.length);
        for (int id : ids)
            chains.add(visitRootTxnAsync(commandStores.forId(id), root));

        List<AsyncChain<T>> tmp = new ArrayList<>();
        Future<List<T>> next = AccordService.toFuture(AsyncChains.allOf(chains));
        while (next != null)
        {
            if (!next.awaitUntilThrowUncheckedOnInterrupt(deadlineNanos))
                throw new TimeoutException();

            next.rethrowIfFailed();
            List<T> process = next.getNow().stream()
                                  .filter(Objects::nonNull)
                                  .sorted(Comparator.naturalOrder())
                                  .collect(Collectors.toList());

            for (T txn : process)
                visit.accept(txn);

            finishRound(deadlineNanos, tmp);
            next = drainToFuture(queued, tmp);
        }
    }

    protected void finishRound(long deadlineNanos, List tmp) throws TimeoutException
    {
    }

    static <V> Future<List<V>> drainToFuture(Queue<AsyncChain<V>> drain, List<AsyncChain<V>> tmp)
    {
        AsyncChain<V> next;
        while (null != (next = drain.poll()))
            tmp.add(next);
        if (tmp.isEmpty())
            return null;
        Future<List<V>> result = AccordService.toFuture(AsyncChains.allOf(List.copyOf(tmp)));
        tmp.clear();
        return result;
    }

    private AsyncChain<T> visitRootTxnAsync(CommandStore commandStore, TxnId txnId)
    {
        return commandStore.chain(PreLoadContext.contextFor(txnId, "Populate txn_blocked_by"), safeStore -> {
            Command command = safeStore.unsafeGetNoCleanup(txnId).current();
            if (command == null || command.saveStatus() == SaveStatus.Uninitialised)
                return null;
            return visitRootTxnSync(safeStore, command);
        });
    }

    protected abstract T visitRootTxnSync(SafeCommandStore safeStore, Command command);
}
