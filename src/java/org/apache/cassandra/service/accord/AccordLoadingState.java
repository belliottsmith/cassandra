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

import java.util.concurrent.Callable;
import java.util.function.Function;

import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResults;

public abstract class AccordLoadingState<K, V>
{
    public enum LoadingState { NOT_FOUND, PENDING, LOADED, FAILED }
    private interface NonValueState {}

    private static final NonValueState NOT_FOUND = new NonValueState() {};

    private static class PendingLoad<V> extends AsyncResults.Settable<Void> implements NonValueState, Runnable
    {
        Callable<V> loadFunction;
        volatile V result;
        Throwable failure;

        public PendingLoad(Callable<V> loadFunction)
        {
            this.loadFunction = loadFunction;
        }

        @Override
        public void run()
        {
            try
            {
                result = loadFunction.call();
                trySuccess(null);
            }
            catch (Throwable t)
            {
                failure = t;
                tryFailure(t);
            }
        }
    }

    private static class FailedLoad implements NonValueState
    {
        private final Throwable cause;

        public FailedLoad(Throwable cause)
        {
            this.cause = cause;
        }
    }

    private final K key;
    private Object state = NOT_FOUND;

    public AccordLoadingState(K key)
    {
        this.key = key;
    }

    private LoadingState maybeCleanupLoad()
    {
        PendingLoad<V> load = (PendingLoad<V>) state;
        if (!load.isDone())
            return LoadingState.PENDING;

        if (load.failure != null)
        {
            state = new FailedLoad(load.failure);
            return LoadingState.FAILED;
        }
        else
        {
            state = load.result;
            return LoadingState.LOADED;
        }
    }

    private static IllegalStateException unexpectedState(LoadingState expected, LoadingState actual)
    {
        return new IllegalStateException(String.format("Unexpected state. Expected %s, was %s", expected, actual));
    }

    /**
     * Returns the current loading state. Since most calls here will be initiated by AsyncChain callbacks on
     * load completion/failure, we attempt to complete any pending states so the caller doesn't have to remember
     * to. The exception is the listen method, to prevent races where the caller found a pending load, attempts
     * to register a callback, but gets an exception because the load completed in the meantime.
     */
    private LoadingState state(boolean attemptLoadCompletion)
    {
        if (!(state instanceof NonValueState))
            return LoadingState.LOADED;

        if (state == NOT_FOUND)
            return LoadingState.NOT_FOUND;

        if (state instanceof PendingLoad)
            return attemptLoadCompletion
                   ? maybeCleanupLoad()
                   : LoadingState.PENDING;

        if (state instanceof FailedLoad)
            return LoadingState.FAILED;

        throw new IllegalStateException("Unhandled state " + state);
    }

    public LoadingState state()
    {
        return state(true);
    }

    private void checkState(LoadingState expected, boolean attemptLoadCompletion)
    {
        LoadingState actual = state(attemptLoadCompletion);
        if (actual != expected)
            throw unexpectedState(expected, actual);
    }

    public K key()
    {
        return key;
    }

    public V value()
    {
        checkState(LoadingState.LOADED, true);
        return (V) state;
    }

    public void value(V value)
    {
        checkState(LoadingState.LOADED, true);
        state = value;
    }

    public Throwable failure()
    {
        checkState(LoadingState.FAILED, true);
        return ((FailedLoad) state).cause;
    }

    /**
     * Return a runnable that will run the loadFunction in a separate thread. When the runnable
     * has completed, the state load will have either completed, or failed.
     */
    public Runnable load(Function<K, V> loadFunction)
    {
        checkState(LoadingState.NOT_FOUND, true);
        PendingLoad<V> pendingLoad = new PendingLoad<>(() -> loadFunction.apply(key));
        state = pendingLoad;
        return pendingLoad;
    }

    public AsyncChain<Void> listen()
    {
        checkState(LoadingState.PENDING, false);
        return (PendingLoad<V>) state;
    }
}
