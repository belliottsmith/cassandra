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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;

import io.netty.util.concurrent.GenericFutureListener;
import org.apache.cassandra.utils.concurrent.ListenerList.CallbackBiConsumerListener;
import org.apache.cassandra.utils.concurrent.ListenerList.CallbackLambdaListener;
import org.apache.cassandra.utils.concurrent.ListenerList.CallbackListener;
import org.apache.cassandra.utils.concurrent.ListenerList.CallbackListenerWithExecutor;
import org.apache.cassandra.utils.concurrent.ListenerList.GenericFutureListenerList;
import org.apache.cassandra.utils.concurrent.ListenerList.RunnableWithExecutor;
import org.apache.cassandra.utils.concurrent.ListenerList.RunnableWithNotifyExecutor;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Our default {@link Future} implementation, with all state being managed without locks (except those used by the JVM).
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
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractGeneralFuture<V> extends AbstractFuture<V> implements Future<V>
{
    volatile ListenerList<V> listeners; // either a ListenerList or GenericFutureListener (or null)
    static final AtomicReferenceFieldUpdater<AbstractGeneralFuture, ListenerList> listenersUpdater = newUpdater(AbstractGeneralFuture.class, ListenerList.class, "listeners");

    protected AbstractGeneralFuture(FailureHolder initialState)
    {
        // TODO: document visibility of constructor (i.e. must be safe published)
        resultUpdater.lazySet(this, initialState);
    }

    public AbstractGeneralFuture()
    {
        this(UNSET);
    }

    protected AbstractGeneralFuture(V immediateSuccess)
    {
        resultUpdater.lazySet(this, immediateSuccess);
    }

    protected AbstractGeneralFuture(Throwable immediateFailure)
    {
       this(new FailureHolder(immediateFailure));
    }

    protected AbstractGeneralFuture(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        this();
        listenersUpdater.lazySet(this, new GenericFutureListenerList(listener));
    }

    protected AbstractGeneralFuture(FailureHolder initialState, GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        this(initialState);
        listenersUpdater.lazySet(this, new GenericFutureListenerList(listener));
    }

    /**
     * Logically append {@code newListener} to {@link #listeners}
     * (at this stage it is a stack, so we actually prepend)
     */
    abstract void appendListener(ListenerList<V> newListener);

    /**
     * Support {@link com.google.common.util.concurrent.Futures#addCallback} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractGeneralFuture<V> addCallback(FutureCallback<? super V> callback)
    {
        appendListener(new CallbackListener<>(this, callback));
        return this;
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#addCallback} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractGeneralFuture<V> addCallback(BiConsumer<? super V, Throwable> callback)
    {
        appendListener(new CallbackBiConsumerListener<>(this, callback, null));
        return this;
    }

    @Override
    public Future<V> addCallback(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        appendListener(new CallbackBiConsumerListener<>(this, callback, executor));
        return this;
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#addCallback} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractGeneralFuture<V> addCallback(FutureCallback<? super V> callback, Executor executor)
    {
        Preconditions.checkNotNull(executor);
        appendListener(new CallbackListenerWithExecutor<>(this, callback, executor));
        return this;
    }

    /**
     * Support more fluid version of {@link com.google.common.util.concurrent.Futures#addCallback}
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractGeneralFuture<V> addCallback(Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure)
    {
        appendListener(new CallbackLambdaListener<>(this, onSuccess, onFailure, null));
        return this;
    }

    /**
     * Support more fluid version of {@link com.google.common.util.concurrent.Futures#addCallback}
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public AbstractGeneralFuture<V> addCallback(Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure, Executor executor)
    {
        appendListener(new CallbackLambdaListener<>(this, onSuccess, onFailure, executor));
        return this;
    }

    /**
     * Add a listener to be invoked once this future completes.
     * Listeners are submitted to {@link #notifyExecutor} in the order they are added (or the specified executor
     * in the case of {@link #addListener(Runnable, Executor)}.
     * if {@link #notifyExecutor} is unset, they are invoked in the order they are added.
     * The ordering holds across all variants of this method.
     */
    public Future<V> addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        appendListener(new GenericFutureListenerList(listener));
        return this;
    }

    /**
     * Add a listener to be invoked once this future completes.
     * Listeners are submitted to their {@code #executor} (or {@link #notifyExecutor}) in the order they are added;
     * if {@link #notifyExecutor} is unset, they are invoked in the order they are added.
     * The ordering holds across all variants of this method.
     */
    public void addListener(Runnable task, @Nullable Executor executor)
    {
        appendListener(new RunnableWithExecutor(task, executor));
    }

    /**
     * Add a listener to be invoked once this future completes.
     * Listeners are submitted to {@link #notifyExecutor} in the order they are added (or the specified executor
     * in the case of {@link #addListener(Runnable, Executor)}.
     * if {@link #notifyExecutor} is unset, they are invoked in the order they are added.
     * The ordering holds across all variants of this method.
     */
    public void addListener(Runnable task)
    {
        appendListener(new RunnableWithNotifyExecutor(task));
    }
}
