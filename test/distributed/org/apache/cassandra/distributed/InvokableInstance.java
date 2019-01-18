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

package org.apache.cassandra.distributed;

import org.apache.cassandra.distributed.api.IInstanceConfig;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class InvokableInstance extends Instance implements IInvokableInstance
{
    InvokableInstance(IInstanceConfig config, ClassLoader classLoader)
    {
        super(config, classLoader);
    }

    public <O> CallableNoExcept<Future<O>> asyncCallsOnInstance(SerializableCallable<O> call) { return async(transfer(call)); }
    public <O> CallableNoExcept<O> callsOnInstance(SerializableCallable<O> call) { return sync(transfer(call)); }
    public <O> O callOnInstance(SerializableCallable<O> call) { return callsOnInstance(call).call(); }

    public CallableNoExcept<Future<?>> asyncRunsOnInstance(SerializableRunnable run) { return async(transfer(run)); }
    public Runnable runsOnInstance(SerializableRunnable run) { return sync(transfer(run)); }
    public void runOnInstance(SerializableRunnable run) { runsOnInstance(run).run(); }

    public <I> Function<I, Future<?>> asyncAcceptsOnInstance(SerializableConsumer<I> consumer) { return async(transfer(consumer)); }
    public <I> Consumer<I> acceptsOnInstance(SerializableConsumer<I> consumer) { return sync(transfer(consumer)); }

    public <I1, I2> BiFunction<I1, I2, Future<?>> asyncAcceptsOnInstance(SerializableBiConsumer<I1, I2> consumer) { return async(transfer(consumer)); }
    public <I1, I2> BiConsumer<I1, I2> acceptsOnInstance(SerializableBiConsumer<I1, I2> consumer) { return sync(transfer(consumer)); }

    public <I, O> Function<I, Future<O>> asyncAppliesOnInstance(SerializableFunction<I, O> f) { return async(transfer(f)); }
    public <I, O> Function<I, O> appliesOnInstance(SerializableFunction<I, O> f) { return sync(transfer(f)); }

    public <I1, I2, O> BiFunction<I1, I2, Future<O>> asyncAppliesOnInstance(SerializableBiFunction<I1, I2, O> f) { return async(transfer(f)); }
    public <I1, I2, O> BiFunction<I1, I2, O> appliesOnInstance(SerializableBiFunction<I1, I2, O> f) { return sync(transfer(f)); }

    public <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> asyncAppliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f) { return async(transfer(f)); }
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, O> appliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f) { return sync(transfer(f)); }

    @Override
    public <E extends Serializable> E transfer(E object)
    {
        return super.transfer(object);
    }

}
