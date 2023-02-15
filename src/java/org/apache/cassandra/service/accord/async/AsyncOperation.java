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

package org.apache.cassandra.service.accord.async;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.RoutableKey;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordLiveCommand;
import org.apache.cassandra.service.accord.AccordLiveCommandsForKey;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;

public abstract class AsyncOperation<R> extends AsyncChains.Head<R> implements Runnable, Function<SafeCommandStore, R>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncOperation.class);

    private static class LoggingProps
    {
        private static final String COMMAND_STORE = "command_store";
        private static final String ASYNC_OPERATION = "async_op";
    }

    static class Context
    {
        final AsyncContext<TxnId, AccordLiveCommand> commands = new AsyncContext<>();
        final AsyncContext<RoutableKey, AccordLiveCommandsForKey> commandsForKeys = new AsyncContext<>();

        void getActive(AccordCommandStore commandStore)
        {
            commands.getActive(commandStore.commandCache());
            commandsForKeys.getActive(commandStore.commandsForKeyCache());
        }

        void releaseResources(AccordCommandStore commandStore)
        {
            commands.releaseResources(commandStore.commandCache());
            commandsForKeys.releaseResources(commandStore.commandsForKeyCache());
        }

        void revertChanges()
        {
            commands.revertChanges();
            commandsForKeys.revertChanges();
        }
    }

    enum State
    {
        INITIALIZED,
        SUBMITTED,
        LOADING,
        PREPARING_OPERATION,
        RUNNING,
        SAVING,
        AWAITING_SAVE,
        COMPLETING,
        FINISHED,
        FAILED
    }

    private State state = State.INITIALIZED;
    private final AccordCommandStore commandStore;
    private final PreLoadContext preLoadContext;
    private final Context context = new Context();
    private AccordSafeCommandStore safeStore;
    private final AsyncLoader loader;
    private final AsyncWriter writer;
    private R result;
    private final String loggingId;
    private BiConsumer<? super R, Throwable> callback;

    private void setLoggingIds()
    {
        MDC.put(LoggingProps.COMMAND_STORE, commandStore.loggingId);
        MDC.put(LoggingProps.ASYNC_OPERATION, loggingId);
    }

    private void clearLoggingIds()
    {
        MDC.remove(LoggingProps.COMMAND_STORE);
        MDC.remove(LoggingProps.ASYNC_OPERATION);
    }

    public AsyncOperation(AccordCommandStore commandStore, PreLoadContext preLoadContext)
    {
        this.loggingId = "0x" + Integer.toHexString(System.identityHashCode(this));
        this.commandStore = commandStore;
        this.preLoadContext = preLoadContext;
        this.loader = createAsyncLoader(commandStore, preLoadContext);
        setLoggingIds();
        this.writer = createAsyncWriter(commandStore);
        logger.trace("Created {} on {}", this, commandStore);
        clearLoggingIds();
    }

    @Override
    public String toString()
    {
        return "AsyncOperation{" + state + "}-" + loggingId;
    }

    AsyncWriter createAsyncWriter(AccordCommandStore commandStore)
    {
        return new AsyncWriter(commandStore);
    }

    AsyncLoader createAsyncLoader(AccordCommandStore commandStore, PreLoadContext preLoadContext)
    {
        return new AsyncLoader(commandStore, preLoadContext.txnIds(), toRoutableKeys(preLoadContext.keys()));
    }

    @VisibleForTesting
    State state()
    {
        return state;
    }

    @VisibleForTesting
    protected void setState(State state)
    {
        this.state = state;
    }

    private void callback(Object o, Throwable throwable)
    {
        if (throwable != null)
        {
            logger.error(String.format("Operation %s failed", this), throwable);
            fail(throwable);
        }
        else
            run();
    }

    private void finish(R result)
    {
        Invariants.checkArgument(state == State.COMPLETING, "Unexpected state %s", state);
        callback.accept(result, null);
        state = State.FINISHED;
    }

    private void fail(Throwable throwable)
    {
        Invariants.checkArgument(state != State.FINISHED && state != State.FAILED, "Unexpected state %s", state);
        switch (state)
        {
            case INITIALIZED:
            case COMPLETING:
                // nothing to cleanup, call callback
                break;
            case RUNNING:
                context.revertChanges();
            case PREPARING_OPERATION:
                commandStore.abortCurrentOperation();
            case LOADING:
                context.releaseResources(commandStore);
                break;
            case SAVING:
            case AWAITING_SAVE:
                // TODO: revert changs
                // TODO: panic?
                break;
        }
        callback.accept(null, throwable);
        state = State.FAILED;
    }

    protected void runInternal()
    {
        switch (state)
        {
            case INITIALIZED:
                state = State.LOADING;
            case LOADING:
                if (!loader.load(context, this::callback))
                    return;

                state = State.PREPARING_OPERATION;
                context.getActive(commandStore);
                safeStore = commandStore.beginOperation(preLoadContext, context.commands, context.commandsForKeys);
                state = State.RUNNING;
                result = apply(safeStore);
                commandStore.completeOperation(safeStore);

                state = State.SAVING;
            case SAVING:
            case AWAITING_SAVE:
                boolean updatesPersisted = writer.save(context, this::callback);

                if (state == State.SAVING)
                {
                    // with any updates on the way to disk, release resources so operations waiting
                    // to use these objects don't have issues with fields marked as unsaved
                    context.releaseResources(commandStore);
                    state = State.AWAITING_SAVE;
                }

                if (!updatesPersisted)
                    return;

                state = State.COMPLETING;
                finish(result);
            case FINISHED:
                break;
            default:
                throw new IllegalStateException("Unexpected state " + state);
        }
    }


    @Override
    public void run()
    {
        setLoggingIds();
        logger.trace("Running {} with state {}", this, state);
        try
        {
            commandStore.checkInStoreThread();
            commandStore.setCurrentOperation(this);
            try
            {
                runInternal();
            }
            catch (Throwable t)
            {
                logger.error(String.format("Operation %s failed", this), t);
                fail(t);
            }
            finally
            {
                commandStore.unsetCurrentOperation(this);
            }
        }
        finally
        {
            logger.trace("Exiting {}", this);
            clearLoggingIds();
        }
    }

    @Override
    public void begin(BiConsumer<? super R, Throwable> callback)
    {
        Invariants.checkArgument(this.callback == null);
        this.callback = callback;
        commandStore.executor().submit(this);
    }

    private static Iterable<RoutableKey> toRoutableKeys(Seekables<?, ?> keys)
    {
        switch (keys.domain())
        {
            default: throw new AssertionError("Unexpected domain: " + keys.domain());
            case Key:
                return (Iterable<RoutableKey>) keys;
            case Range:
                // TODO (required): implement
                throw new UnsupportedOperationException();
        }
    }

    static class ForFunction<R> extends AsyncOperation<R>
    {
        private final Function<? super SafeCommandStore, R> function;

        public ForFunction(AccordCommandStore commandStore, PreLoadContext loadCtx, Function<? super SafeCommandStore, R> function)
        {
            super(commandStore, loadCtx);
            this.function = function;
        }

        @Override
        public R apply(SafeCommandStore commandStore)
        {
            return function.apply(commandStore);
        }
    }

    public static <T> AsyncOperation<T> create(CommandStore commandStore, PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return new ForFunction<>((AccordCommandStore) commandStore, loadCtx, function);
    }

    static class ForConsumer extends AsyncOperation<Void>
    {
        private final Consumer<? super SafeCommandStore> consumer;

        public ForConsumer(AccordCommandStore commandStore, PreLoadContext loadCtx, Consumer<? super SafeCommandStore> consumer)
        {
            super(commandStore, loadCtx);
            this.consumer = consumer;
        }

        @Override
        public Void apply(SafeCommandStore commandStore)
        {
            consumer.accept(commandStore);
            return null;
        }
    }

    public static AsyncOperation<Void> create(CommandStore commandStore, PreLoadContext loadCtx, Consumer<? super SafeCommandStore> consumer)
    {
        return new ForConsumer((AccordCommandStore) commandStore, loadCtx, consumer);
    }
}
