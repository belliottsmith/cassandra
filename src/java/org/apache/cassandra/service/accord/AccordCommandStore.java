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

package org.apache.cassandra.service.accord;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.CommandTimeseriesHolder;
import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.DurableBefore;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.SerializerSupport;
import accord.local.SerializerSupport.MessageProvider;
import accord.messages.Message;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.Ballot;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Observable;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.service.accord.async.ExecutionOrder;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class AccordCommandStore extends CommandStore implements CacheSize
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStore.class);

    private static long getThreadId(ExecutorService executor)
    {
        try
        {
            return executor.submit(() -> Thread.currentThread().getId()).get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final long threadId;
    public final String loggingId;
    private final AccordJournal journal;
    private final ExecutorService executor;
    private final ExecutionOrder executionOrder;
    private final AccordStateCache stateCache;
    private final AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache;
    private final AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache;
    private AsyncOperation<?> currentOperation = null;
    private AccordSafeCommandStore current = null;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;
    private CommandsForRanges commandsForRanges = new CommandsForRanges();

    public AccordCommandStore(int id,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              EpochUpdateHolder epochUpdateHolder,
                              AccordJournal journal,
                              AccordStateCacheMetrics cacheMetrics)
    {
        this(id, time, agent, dataStore, progressLogFactory, epochUpdateHolder, journal, Stage.READ.executor(), Stage.MUTATION.executor(), cacheMetrics);
    }

    @VisibleForTesting
    public AccordCommandStore(int id,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              EpochUpdateHolder epochUpdateHolder,
                              AccordJournal journal,
                              ExecutorPlus loadExecutor,
                              ExecutorPlus saveExecutor,
                              AccordStateCacheMetrics cacheMetrics)
    {
        super(id, time, agent, dataStore, progressLogFactory, epochUpdateHolder);
        this.journal = journal;
        loggingId = String.format("[%s]", id);
        executor = executorFactory().sequential(CommandStore.class.getSimpleName() + '[' + id + ']');
        executionOrder = new ExecutionOrder();
        threadId = getThreadId(executor);
        stateCache = new AccordStateCache(loadExecutor, saveExecutor, 8 << 20, cacheMetrics);
        commandCache =
            stateCache.instance(TxnId.class,
                                TxnId.class,
                                AccordSafeCommand::new,
                                this::loadCommand,
                                this::saveCommand,
                                this::validateCommand,
                                AccordObjectSizes::command);
        commandsForKeyCache =
            stateCache.instance(RoutableKey.class,
                                PartitionKey.class,
                                AccordSafeCommandsForKey::new,
                                this::loadCommandsForKey,
                                this::saveCommandsForKey,
                                this::validateCommandsForKey,
                                AccordObjectSizes::commandsForKey);
        AccordKeyspace.loadCommandStoreMetadata(id, ((rejectBefore, durableBefore, redundantBefore, bootstrapBeganAt, safeToRead) -> {
            executor.submit(() -> {
                if (rejectBefore != null)
                    super.setRejectBefore(rejectBefore);
                if (durableBefore != null)
                    super.setDurableBefore(durableBefore);
                if (redundantBefore != null)
                    super.setRedundantBefore(redundantBefore);
                if (bootstrapBeganAt != null)
                    super.setBootstrapBeganAt(bootstrapBeganAt);
                if (safeToRead != null)
                    super.setSafeToRead(safeToRead);
            });
        }));
        executor.execute(() -> CommandStore.register(this));
        executor.execute(this::loadRangesToCommands);
    }

    static Factory factory(AccordJournal journal, AccordStateCacheMetrics cacheMetrics)
    {
        return (id, time, agent, dataStore, progressLogFactory, rangesForEpoch) ->
               new AccordCommandStore(id, time, agent, dataStore, progressLogFactory, rangesForEpoch, journal, cacheMetrics);
    }

    private void loadRangesToCommands()
    {
        AsyncPromise<CommandsForRanges> future = new AsyncPromise<>();
        AccordKeyspace.findAllCommandsByDomain(id, Routable.Domain.Range, ImmutableSet.of("txn_id", "status", "accepted_ballot", "execute_at"), new Observable<UntypedResultSet.Row>()
        {
            private CommandsForRanges.Builder builder = new CommandsForRanges.Builder();

            @Override
            public void onNext(UntypedResultSet.Row row) throws Exception
            {
                TxnId txnId = AccordKeyspace.deserializeTxnId(row);
                SaveStatus status = AccordKeyspace.deserializeStatus(row);
                Timestamp executeAt = AccordKeyspace.deserializeExecuteAtOrNull(row);
                Ballot accepted = AccordKeyspace.deserializeAcceptedOrNull(row);

                MessageProvider messageProvider = journal.makeMessageProvider(txnId);

                SerializerSupport.TxnAndDeps txnAndDeps = SerializerSupport.extractTxnAndDeps(status, accepted, messageProvider);
                Seekables<?, ?> keys = txnAndDeps.txn.keys();
                if (keys.domain() != Routable.Domain.Range)
                    throw new AssertionError(String.format("Txn keys are not range for %s", txnAndDeps.txn));
                Ranges ranges = (Ranges) keys;

                List<TxnId> dependsOn = txnAndDeps.deps == null ? Collections.emptyList() : txnAndDeps.deps.txnIds();
                builder.put(txnId, ranges, status, executeAt, dependsOn);
            }

            @Override
            public void onError(Throwable t)
            {
                builder = null;
                future.tryFailure(t);
            }

            @Override
            public void onCompleted()
            {
                CommandsForRanges result = this.builder.build();
                builder = null;
                future.trySuccess(result);
            }
        });
        try
        {
            commandsForRanges = future.get();
            logger.debug("Loaded {} intervals", commandsForRanges.size());
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public boolean inStore()
    {
        return Thread.currentThread().getId() == threadId;
    }

    @Override
    public void setCapacity(long bytes)
    {
        checkInStoreThread();
        stateCache.setCapacity(bytes);
    }

    @Override
    public long capacity()
    {
        return stateCache.capacity();
    }

    @Override
    public int size()
    {
        return stateCache.size();
    }

    @Override
    public long weightedSize()
    {
        return stateCache.weightedSize();
    }

    public void checkInStoreThread()
    {
        Invariants.checkState(inStore());
    }

    public void checkNotInStoreThread()
    {
        Invariants.checkState(!inStore());
    }

    public ExecutorService executor()
    {
        return executor;
    }

    public AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache()
    {
        return commandCache;
    }

    public AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache()
    {
        return commandsForKeyCache;
    }

    Command loadCommand(TxnId txnId)
    {
        return AccordKeyspace.loadCommand(this, txnId);
    }

    @Nullable
    Runnable saveCommand(Command before, Command after)
    {
        Mutation mutation = AccordKeyspace.getCommandMutation(id, before, after, nextSystemTimestampMicros());
        return null != mutation ? mutation::apply : null;
    }

    boolean validateCommand(TxnId txnId, Command evicting)
    {
        Command reloaded = AccordKeyspace.unsafeLoadCommand(this, txnId);
        return (evicting == null && reloaded == null) || (evicting != null && reloaded != null && reloaded.isEqualOrFuller(evicting));
    }

    CommandsForKey loadCommandsForKey(RoutableKey key)
    {
        return AccordKeyspace.loadCommandsForKey(this, (PartitionKey) key);
    }

    @Nullable
    private Runnable saveCommandsForKey(CommandsForKey before, CommandsForKey after)
    {
        Mutation mutation = AccordKeyspace.getCommandsForKeyMutation(id, before, after, nextSystemTimestampMicros());
        return null != mutation ? mutation::apply : null;
    }

    boolean validateCommandsForKey(RoutableKey key, CommandsForKey evicting)
    {
        CommandsForKey reloaded = AccordKeyspace.unsafeLoadCommandsForKey(this, (PartitionKey) key);
        return Objects.equals(evicting, reloaded);
    }

    @VisibleForTesting
    public AccordStateCache cache()
    {
        return stateCache;
    }

    @VisibleForTesting
    public void unsafeClearCache()
    {
        stateCache.unsafeClear();
    }

    public void setCurrentOperation(AsyncOperation<?> operation)
    {
        Invariants.checkState(currentOperation == null);
        currentOperation = operation;
    }

    public AsyncOperation<?> getContext()
    {
        Invariants.checkState(currentOperation != null);
        return currentOperation;
    }

    public void unsetCurrentOperation(AsyncOperation<?> operation)
    {
        Invariants.checkState(currentOperation == operation);
        currentOperation = null;
    }

    public long nextSystemTimestampMicros()
    {
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }
    @Override
    public <T> AsyncChain<T> submit(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return AsyncOperation.create(this, loadCtx, function);
    }

    @Override
    public <T> AsyncChain<T> submit(Callable<T> task)
    {
        return AsyncChains.ofCallable(executor, task);
    }

    public DataStore dataStore()
    {
        return store;
    }

    NodeTimeService time()
    {
        return time;
    }

    ProgressLog progressLog()
    {
        return progressLog;
    }

    public ExecutionOrder executionOrder()
    {
        return executionOrder;
    }

    @Override
    public AsyncChain<Void> execute(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        return AsyncOperation.create(this, preLoadContext, consumer);
    }

    public void executeBlocking(Runnable runnable)
    {
        try
        {
            executor.submit(runnable).get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public AccordSafeCommandStore beginOperation(PreLoadContext preLoadContext,
                                                 Map<TxnId, AccordSafeCommand> commands,
                                                 NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKeys)
    {
        Invariants.checkState(current == null);
        commands.values().forEach(AccordSafeState::preExecute);
        commandsForKeys.values().forEach(AccordSafeState::preExecute);
        current = new AccordSafeCommandStore(preLoadContext, commands, commandsForKeys, this);
        return current;
    }

    public boolean hasSafeStore()
    {
        return current != null;
    }

    public void completeOperation(AccordSafeCommandStore store)
    {
        Invariants.checkState(current == store);
        current.complete();
        current = null;
    }

    <O> O mapReduceForRange(Routables<?> keysOrRanges, Ranges slice, BiFunction<CommandTimeseriesHolder, O, O> map, O accumulate, Predicate<? super O> terminate)
    {
        keysOrRanges = keysOrRanges.slice(slice, Routables.Slice.Minimal);
        switch (keysOrRanges.domain())
        {
            case Key:
            {
                AbstractKeys<Key> keys = (AbstractKeys<Key>) keysOrRanges;
                for (CommandTimeseriesHolder summary : commandsForRanges.search(keys))
                {
                    accumulate = map.apply(summary, accumulate);
                    if (terminate.test(accumulate))
                        return accumulate;
                }
            }
            break;
            case Range:
            {
                AbstractRanges ranges = (AbstractRanges) keysOrRanges;
                for (Range range : ranges)
                {
                    CommandTimeseriesHolder summary = commandsForRanges.search(range);
                    if (summary == null)
                        continue;
                    accumulate = map.apply(summary, accumulate);
                    if (terminate.test(accumulate))
                        return accumulate;
                }
            }
            break;
            default:
                throw new AssertionError("Unknown domain: " + keysOrRanges.domain());
        }
        return accumulate;
    }

    CommandsForRanges commandsForRanges()
    {
        return commandsForRanges;
    }

    CommandsForRanges.Updater updateRanges()
    {
        return commandsForRanges.update();
    }

    public void abortCurrentOperation()
    {
        current = null;
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    protected void setRejectBefore(ReducingRangeMap<Timestamp> newRejectBefore)
    {
        super.setRejectBefore(newRejectBefore);
        // TODO (required, correctness): rework to persist via journal once available, this can lose updates in some edge cases
        AccordKeyspace.updateRejectBefore(this, newRejectBefore);
    }

    protected void setBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        super.setBootstrapBeganAt(newBootstrapBeganAt);
        // TODO (required, correctness): rework to persist via journal once available, this can lose updates in some edge cases
        AccordKeyspace.updateBootstrapBeganAt(this, newBootstrapBeganAt);
    }

    protected void setSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        super.setSafeToRead(newSafeToRead);
        // TODO (required, correctness): rework to persist via journal once available, this can lose updates in some edge cases
        AccordKeyspace.updateSafeToRead(this, newSafeToRead);
    }

    @Override
    public void setDurableBefore(DurableBefore newDurableBefore)
    {
        super.setDurableBefore(newDurableBefore);
        AccordKeyspace.updateDurableBefore(this, newDurableBefore);
    }

    @Override
    protected void setRedundantBefore(RedundantBefore newRedundantBefore)
    {
        super.setRedundantBefore(newRedundantBefore);
        // TODO (required): this needs to be synchronous, or at least needs to take effect before we rely upon it
        AccordKeyspace.updateRedundantBefore(this, newRedundantBefore);
    }

    @Override
    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        super.markShardDurable(safeStore, globalSyncId, ranges);
        commandsForRanges.prune(globalSyncId, ranges);
    }

    public NavigableMap<TxnId, Ranges> bootstrapBeganAt() { return super.bootstrapBeganAt(); }
    public NavigableMap<Timestamp, Ranges> safeToRead() { return super.safeToRead(); }

    MessageProvider makeMessageProvider(TxnId txnId)
    {
        return journal.makeMessageProvider(txnId);
    }

    @VisibleForTesting
    public void appendToJournal(Message message)
    {
        journal.appendMessageBlocking(message);
    }
}
