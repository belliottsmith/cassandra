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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import io.netty.util.concurrent.GenericFutureListener;

import static org.apache.cassandra.utils.concurrent.Awaitable.SyncAwaitable.waitUntil;

/**
 * This implementation extends {@link AbstractFuture} but imposes some restrictions on usage to gain major efficiencies:
 *  - At most one callback registration or map invocation
 *  - There will be no concurrent attempts to register a callback OR invoke {@code get} before completion.
 *  - There will be no concurrent attempts to invoke trySuccess, tryFailure or equivalent methods
 *
 * @param <V>
 */
public class MinimalFuture<V> extends AbstractFuture<V>
{
    private static final Runnable DONE = () -> {};

    private volatile Object listener;
    private static final AtomicReferenceFieldUpdater<MinimalFuture, Object> listenerUpdater = AtomicReferenceFieldUpdater.newUpdater(MinimalFuture.class, Object.class, "listener");

    // set if listener is null before we set the result
    private volatile boolean noListener; // we get this for free with compressed class pointers
    // set if a thread is waiting and needs to be signalled
    private volatile boolean hasWaiting; // we get this for free with compressed class pointers

    private static final int NO_LISTENER = 1; // only set if listener is null before we set the result
    private static final int NOTIFIED = 2; // only set if listener is null when we set the result
    private static final int WAITING = 4;
    private volatile int flags; // we get this for free with compressed class pointers
    private static final AtomicIntegerFieldUpdater<MinimalFuture> flagsUpdater = AtomicIntegerFieldUpdater.newUpdater(MinimalFuture.class, "flags");

    protected MinimalFuture()
    {
        super();
    }

    protected MinimalFuture(V immediateSuccess)
    {
        super(immediateSuccess);
    }

    protected MinimalFuture(Throwable immediateFailure)
    {
        super(immediateFailure);
    }

    protected MinimalFuture(FailureHolder initialState)
    {
        super(initialState);
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transform} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public <T> Future<T> map(Function<? super V, ? extends T> mapper, Executor executor)
    {
        return map(new MinimalFuture<>(), mapper, executor);
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transformAsync(ListenableFuture, AsyncFunction, Executor)} natively
     *
     * See {@link #addListener(GenericFutureListener)} for ordering semantics.
     */
    @Override
    public <T> Future<T> flatMap(Function<? super V, ? extends Future<T>> flatMapper, @Nullable Executor executor)
    {
        return flatMap(new MinimalFuture<>(), flatMapper, executor);
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
    boolean trySet(Object v)
    {
        Object current = result;
        if (isDone(current) || (current == UNCANCELLABLE && (v == CANCELLED || v == UNCANCELLABLE)))
            return false;

        if (v == UNCANCELLABLE)
        {
            resultUpdater.lazySet(this, UNCANCELLABLE);
            return true;
        }

        Object listener = this.listener;
        if (listener == null)
            flagsUpdater.updateAndGet(this, x -> x | NO_LISTENER);

        result = v;
        int flags = this.flags;
        if (listener != null || ((listener = this.listener) != null && canNotifyListener(flags = trySetNotified())))
        {
            // if listener was non-null before we set result OR non-null after and we atomically take control of notification
            if (listener instanceof BiConsumer)
            {
                if (v instanceof FailureHolder) ((BiConsumer) listener).accept(null, ((FailureHolder) v).cause);
                else ((BiConsumer) listener).accept(v, null);
            }
            else if (listener instanceof Runnable)
            {
                ((Runnable) listener).run();
            }
        }

        if (hasWaiting(flags))
        {
            synchronized (this)
            {
                notifyAll();
            }
        }
        return true;
    }

    private int trySetNotified()
    {
        while (true)
        {
            int flags = this.flags;
            if (!canNotifyListener(flags)) return flags;
            if (flagsUpdater.compareAndSet(this, flags, flags | NOTIFIED)) return flags;
        }
    }

    private static boolean canNotifyListener(int flags)
    {
        return (flags & NOTIFIED) == 0;
    }

    /**
     * @return true iff the producer may have missed the registration of the listener
     */
    private boolean shouldTryNotifySelf()
    {
        return (flags & NO_LISTENER) != 0;
    }

    private static boolean hasWaiting(int flags)
    {
        return (flags & WAITING) != 0;
    }

    public synchronized boolean awaitUntil(long deadline) throws InterruptedException
    {
        if (isDone())
            return true;

        flagsUpdater.updateAndGet(this, x -> x | WAITING);
        waitUntil(this, deadline);
        return isDone();
    }

    public synchronized Future<V> await() throws InterruptedException
    {
        if (isDone())
            return this;

        flagsUpdater.updateAndGet(this, x -> x | WAITING);
        while (!isDone())
            wait();

        return this;
    }

    @Override
    public void addListener(Runnable runnable, Executor executor)
    {
        addListener(() -> executor.execute(runnable));
    }

    @Override
    public void addListener(Runnable runnable)
    {
        addListenerInternal(runnable);
    }

    @Override
    public Future<V> addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super V>> listener)
    {
        addListener(() -> ListenerList.notifyListener((GenericFutureListener)listener, this));
        return this;
    }

    @Override
    public Future<V> addCallback(BiConsumer<? super V, Throwable> callback)
    {
        addListenerInternal(callback);
        return this;
    }

    @Override
    public Future<V> addCallback(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        return addCallback((success, fail)-> executor.execute(() -> callback.accept(success, fail)));
    }

    @Override
    public Future<V> addCallback(FutureCallback<? super V> callback)
    {
        return addCallback((success, fail)-> {
            if (fail != null) callback.onFailure(fail);
            else callback.onSuccess(success);
        });
    }

    @Override
    public Future<V> addCallback(FutureCallback<? super V> callback, Executor executor)
    {
        return addCallback((success, fail)-> executor.execute(() -> {
            if (fail != null) callback.onFailure(fail);
            else callback.onSuccess(success);
        }));
    }

    @Override
    public Future<V> addCallback(Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure)
    {
        return addCallback((success, fail)-> {
            if (fail != null) onFailure.accept(fail);
            else onSuccess.accept(success);
        });
    }

    @Override
    public Future<V> addCallback(Consumer<? super V> onSuccess, Consumer<? super Throwable> onFailure, Executor executor)
    {
        return addCallback((success, fail)-> executor.execute(() -> {
            if (fail != null) onFailure.accept(fail);
            else onSuccess.accept(success);
        }));
    }

    private void addListenerInternal(Object object)
    {
        Preconditions.checkState(listener == null);
        listener = object;
        Object result = this.result;
        if (isDone(result) && shouldTryNotifySelf() && canNotifyListener(trySetNotified()))
        {
            if (listener instanceof BiConsumer)
            {
                if (result instanceof FailureHolder) ((BiConsumer) listener).accept(null, ((FailureHolder) result).cause);
                else ((BiConsumer) listener).accept(result, null);
            }
            else if (listener instanceof Runnable)
            {
                ((Runnable) listener).run();
            }
        }
    }
}
