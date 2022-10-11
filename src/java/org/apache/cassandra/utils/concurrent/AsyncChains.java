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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.base.Preconditions;

public abstract class AsyncChains<V> implements AsyncChain<V>
{
    public abstract static class Catalyst<V> extends AsyncChains<V>
    {
        protected Catalyst()
        {
            super(null);
            state = this;
        }

        void begin()
        {
            begin(next());
        }
    }

    static abstract class Link<I, O> extends AsyncChains<O> implements BiConsumer<I, Throwable>
    {
        protected Link(Catalyst<?> catalyst)
        {
            super(catalyst);
        }

        @Override
        public void begin(BiConsumer<? super O, Throwable> callback)
        {
            Preconditions.checkArgument(!(callback instanceof Catalyst));
            Preconditions.checkState(state instanceof Catalyst);
            Catalyst catalyst = (Catalyst) state;
            state = callback;
            catalyst.begin();
        }
    }

    public static abstract class Map<I, O> extends Link<I, O> implements Function<I, O>
    {
        Map(Catalyst<?> catalyst)
        {
            super(catalyst);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable != null) next().accept(null, throwable);
            else next().accept(apply(i), null);
        }
    }

    static class EncapsulatedMap<I, O> extends Map<I, O>
    {
        final Function<? super I, ? extends O> map;

        EncapsulatedMap(Catalyst<?> catalyst, Function<? super I, ? extends O> map)
        {
            super(catalyst);
            this.map = map;
        }

        @Override
        public O apply(I i)
        {
            return map.apply(i);
        }
    }

    public static abstract class FlatMap<I, O> extends Link<I, O> implements Function<I, AsyncChain<O>>
    {
        FlatMap(Catalyst<?> catalyst)
        {
            super(catalyst);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable != null) next().accept(null, throwable);
            else apply(i).begin(next());
        }
    }

    static class EncapsulatedFlatMap<I, O> extends FlatMap<I, O>
    {
        final Function<? super I, ? extends AsyncChain<O>> map;

        EncapsulatedFlatMap(Catalyst<?> catalyst, Function<? super I, ? extends AsyncChain<O>> map)
        {
            super(catalyst);
            this.map = map;
        }

        @Override
        public AsyncChain<O> apply(I i)
        {
            return map.apply(i);
        }
    }

    // if extending Callback, be sure to invoke super.accept()
    static class Callback<I> extends Link<I, I>
    {
        Callback(Catalyst<?> catalyst)
        {
            super(catalyst);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            next().accept(i, throwable);
        }
    }

    static class EncapsulatedCallback<I> extends Callback<I>
    {
        final BiConsumer<? super I, Throwable> callback;

        EncapsulatedCallback(Catalyst<?> catalyst, BiConsumer<? super I, Throwable> callback)
        {
            super(catalyst);
            this.callback = callback;
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            super.accept(i, throwable);
            callback.accept(i, throwable);
        }
    }

    public static <V> AsyncChain<V> ofCallable(ExecutorService executor, Callable<V> callable)
    {
        return new Catalyst<V>()
        {
            @Override
            public void begin(BiConsumer<? super V, Throwable> next)
            {
                executor.execute(encapsulate(callable, next));
            }
        };
    }

    private static <V> Runnable encapsulate(Callable<V> callable, BiConsumer<? super V, Throwable> receiver)
    {
        return () -> {
            try
            {
                V result = callable.call();
                receiver.accept(result, null);
            }
            catch (Throwable t)
            {
                receiver.accept(null, t);
            }
        };
    }

    // either the thing we start, or the thing we do in follow-up
    Object state;
    AsyncChains(Catalyst<?> catalyst)
    {
        this.state = catalyst;
    }

    @Override
    public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
    {
        return add(EncapsulatedMap::new, mapper);
    }

    @Override
    public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
    {
        return add(EncapsulatedFlatMap::new, mapper);
    }

    @Override
    public AsyncChain<V> addCallback(BiConsumer<? super V, Throwable> callback)
    {
        return add(EncapsulatedCallback::new, callback);
    }

    // can be used by transformations that want efficiency, and can directly extend Link, FlatMap or Callback
    // (or perhaps some additional helper implementations that permit us to simply implement apply for Map and FlatMap)
    public <O> AsyncChain<O> add(Function<Catalyst<?>, AsyncChain<O>> factory)
    {
        Preconditions.checkState(state instanceof Catalyst<?>);
        Catalyst catalyst = (Catalyst) state;
        AsyncChain<O> result = factory.apply(catalyst);
        state = result;
        return result;
    }

    <T, O> AsyncChain<O> add(BiFunction<Catalyst<?>, T, AsyncChain<O>> factory, T param)
    {
        Preconditions.checkState(state instanceof Catalyst<?>);
        Catalyst catalyst = (Catalyst) state;
        AsyncChain<O> result = factory.apply(catalyst, param);
        state = result;
        return result;
    }

    BiConsumer<? super V, Throwable> next()
    {
        return (BiConsumer<? super V, Throwable>) state;
    }
}
