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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.Executor;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture; // checkstyle: permit this import

import io.netty.util.concurrent.GenericFutureListener;

import static org.apache.cassandra.utils.concurrent.Awaitable.SyncAwaitable.waitUntil;

/**
 * Our synchronized {@link Future} implementation.
 *
 * Some implementation comments versus Netty's default promise:
 *  - We permit efficient initial state declaration, avoiding unnecessary CAS or lock acquisitions when mutating
 *    a Promise we are ourselves constructing (and can easily add more; only those we use have been added)
 *  - We guarantee the order of invocation of listeners (and callbacks etc, and with respect to each other)
 *  - We save some space when registering listeners, especially if there is only one listener, as we perform no
 *    extra allocations in this case.
 *  - We implement our invocation list as a concurrent stack, that is cleared on notification
 *  - We handle special values slightly differently.
 *    - We do not use a special value for null, instead using a special value to indicate the result has not been set.
 *      This means that once isSuccess() holds, the result must be a correctly typed object (modulo generics pitfalls).
 *    - All special values are also instances of FailureHolder, which simplifies a number of the logical conditions.
 */
public class SyncFuture<V> extends AbstractGeneralFuture<V>
{
    protected SyncFuture()
    {
        super();
    }

    protected SyncFuture(V immediateSuccess)
    {
        super(immediateSuccess);
    }

    protected SyncFuture(Throwable immediateFailure)
    {
        super(immediateFailure);
    }

    protected SyncFuture(FailureHolder initialState)
    {
        super(initialState);
    }

    protected SyncFuture(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        super(listener);
    }

    protected SyncFuture(FailureHolder initialState, GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        super(initialState, listener);
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transform} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public <T> Future<T> map(Function<? super V, ? extends T> mapper, Executor executor)
    {
        return map(new SyncFuture<>(), mapper, executor);
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transformAsync(ListenableFuture, AsyncFunction, Executor)} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public <T> Future<T> flatMap(Function<? super V, ? extends Future<T>> flatMapper, @Nullable Executor executor)
    {
        return flatMap(new SyncFuture<>(), flatMapper, executor);
    }

    /**
     * Shared implementation of various promise completion methods.
     * Updates the result if it is possible to do so, returning success/failure.
     *
     * If the promise is UNSET the new value will succeed;
     *          if it is UNCANCELLABLE it will succeed only if the new value is not CANCELLED
     *          otherwise it will fail, as isDone() is implied
     *
     * If the update succeeds, and the new state implies isDone(), any listeners and waiters will be notified
     */
    synchronized boolean trySet(Object v)
    {
        Object current = result;
        if (isDone(current) || (current == UNCANCELLABLE && (v == CANCELLED || v == UNCANCELLABLE)))
            return false;

        resultUpdater.lazySet(this, v);
        if (v != UNCANCELLABLE)
        {
            notifyListeners();
            notifyAll();
        }
        return true;
    }

    public synchronized boolean awaitUntil(long deadline) throws InterruptedException
    {
        if (isDone())
            return true;

        waitUntil(this, deadline);
        return isDone();
    }

    public synchronized Future<V> await() throws InterruptedException
    {
        while (!isDone())
            wait();
        return this;
    }

    /**
     * Logically append {@code newListener} to {@link #listeners}
     * (at this stage it is a stack, so we actually prepend)
     */
    synchronized void appendListener(ListenerList<V> newListener)
    {
        ListenerList.pushExclusive(listenersUpdater, this, newListener);
        if (isDone())
            notifyListeners();
    }

    private void notifyListeners()
    {
        ListenerList.notifyExclusive(listeners, this);
        listenersUpdater.lazySet(this, null);
    }
}
