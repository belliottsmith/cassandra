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

package org.apache.cassandra.distributed.impl;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;

/**
 * This version is only supported for a Cluster running the same code as the test environment, and permits
 * ergonomic cross-node behaviours, without editing the cross-version API.
 *
 * A lambda can be written tto be invoked on any or all of the nodes.
 *
 * The reason this cannot (easily) be made cross-version is that the lambda is tied to the declaring class, which will
 * not be the same in the alternate version.  Even were it not, there would likely be a runtime linkage error given
 * any code divergence.
 */
public interface IInvokableInstance extends IInstance
{
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
