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

package org.apache.cassandra.distributed.api;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface IInvokableInstance
{
    public interface CallableNoExcept<O> extends Callable<O> { public O call(); }
    public interface SerializableCallable<O> extends CallableNoExcept<O>, Serializable { }
    public interface SerializableRunnable extends Runnable, Serializable {}
    public interface SerializableConsumer<O> extends Consumer<O>, Serializable {}
    public interface SerializableBiConsumer<I1, I2> extends BiConsumer<I1, I2>, Serializable {}
    public interface SerializableFunction<I, O> extends Function<I, O>, Serializable {}
    public interface SerializableBiFunction<I1, I2, O> extends BiFunction<I1, I2, O>, Serializable {}
    public interface TriFunction<I1, I2, I3, O>
    {
        O apply(I1 i1, I2 i2, I3 i3);
    }
    public interface SerializableTriFunction<I1, I2, I3, O> extends Serializable, TriFunction<I1, I2, I3, O> { }

    <O> CallableNoExcept<Future<O>> asyncCallsOnInstance(SerializableCallable<O> call);
    <O> CallableNoExcept<O> callsOnInstance(SerializableCallable<O> call);
    <O> O callOnInstance(SerializableCallable<O> call);

    CallableNoExcept<Future<?>> asyncRunsOnInstance(SerializableRunnable run);
    Runnable runsOnInstance(SerializableRunnable run);
    void runOnInstance(SerializableRunnable run);

    <I> Function<I, Future<?>> asyncAcceptsOnInstance(SerializableConsumer<I> consumer);
    <I> Consumer<I> acceptsOnInstance(SerializableConsumer<I> consumer);

    <I1, I2> BiFunction<I1, I2, Future<?>> asyncAcceptsOnInstance(SerializableBiConsumer<I1, I2> consumer);
    <I1, I2> BiConsumer<I1, I2> acceptsOnInstance(SerializableBiConsumer<I1, I2> consumer);

    <I, O> Function<I, Future<O>> asyncAppliesOnInstance(SerializableFunction<I, O> f);
    <I, O> Function<I, O> appliesOnInstance(SerializableFunction<I, O> f);

    <I1, I2, O> BiFunction<I1, I2, Future<O>> asyncAppliesOnInstance(SerializableBiFunction<I1, I2, O> f);
    <I1, I2, O> BiFunction<I1, I2, O> appliesOnInstance(SerializableBiFunction<I1, I2, O> f);

    <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> asyncAppliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f);
    <I1, I2, I3, O> TriFunction<I1, I2, I3, O> appliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f);

    // E must be a functional interface, and lambda must be implemented by a lambda function
    <E extends Serializable> E transfer(E object);

}
