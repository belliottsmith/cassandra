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

import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.ITestCluster;
import org.apache.cassandra.locator.InetAddressAndPort;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class DelegatingInstance implements IInstance
{
    protected volatile IInstance delegate;
    public DelegatingInstance(IInstance delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public InetAddressAndPort broadcastAddressAndPort()
    {
        return delegate.broadcastAddressAndPort();
    }

    @Override
    public Object[][] executeInternal(String query, Object... args)
    {
        return delegate.executeInternal(query, args);
    }

    @Override
    public UUID schemaVersion()
    {
        return delegate.schemaVersion();
    }

    @Override
    public void schemaChange(String query)
    {
        delegate.schemaChange(query);
    }

    @Override
    public IInstanceConfig config()
    {
        return delegate.config();
    }

    public ICoordinator coordinator()
    {
        // TODO: stash and clear coordinator on startup/shutdown?
        return delegate.coordinator();
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
    }

    @Override
    public void startup(ITestCluster cluster)
    {
        delegate.startup(cluster);
    }

    @Override
    public void receiveMessage(IMessage message)
    {
        delegate.receiveMessage(message);
    }

    @Override
    public <I> CallableNoExcept<I> callsOnInstance(SerializableCallable<I> call)
    {
        return delegate.callsOnInstance(call);
    }

    @Override
    public <I> I callOnInstance(SerializableCallable<I> call)
    {
        return delegate.callOnInstance(call);
    }

    public <I> CallableNoExcept<Future<I>> asyncCallsOnInstance(SerializableCallable<I> call)
    {
        return delegate.asyncCallsOnInstance(call);
    }

    @Override
    public CallableNoExcept<Future<?>> asyncRunsOnInstance(SerializableRunnable run)
    {
        return delegate.asyncRunsOnInstance(run);
    }

    @Override
    public Runnable runsOnInstance(SerializableRunnable run)
    {
        return delegate.runsOnInstance(run);
    }

    @Override
    public void runOnInstance(SerializableRunnable run)
    {
        delegate.runOnInstance(run);
    }

    @Override
    public <I> Function<I, Future<?>> asyncAcceptsOnInstance(SerializableConsumer<I> run)
    {
        return delegate.asyncAcceptsOnInstance(run);
    }
    @Override
    public <I> Consumer<I> acceptsOnInstance(SerializableConsumer<I> consumer)
    {
        return delegate.acceptsOnInstance(consumer);
    }

    @Override
    public <I1, I2> BiFunction<I1, I2, Future<?>> asyncAcceptsOnInstance(SerializableBiConsumer<I1, I2> consumer)
    {
        return delegate.asyncAcceptsOnInstance(consumer);
    }

    @Override
    public <I1, I2> BiConsumer<I1, I2> acceptsOnInstance(SerializableBiConsumer<I1, I2> consumer)
    {
        return delegate.acceptsOnInstance(consumer);
    }

    @Override
    public <I, O> Function<I, O> appliesOnInstance(SerializableFunction<I, O> f)
    {
        return delegate.appliesOnInstance(f);
    }

    @Override
    public <I1, I2, O> BiFunction<I1, I2, O> appliesOnInstance(SerializableBiFunction<I1, I2, O> f)
    {
        return delegate.appliesOnInstance(f);
    }

    @Override
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, O> appliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f)
    {
        return delegate.appliesOnInstance(f);
    }

    @Override
    public <I, O> Function<I, Future<O>> asyncAppliesOnInstance(SerializableFunction<I, O> f)
    {
        return delegate.asyncAppliesOnInstance(f);
    }

    @Override
    public <I1, I2, O> BiFunction<I1, I2, Future<O>> asyncAppliesOnInstance(SerializableBiFunction<I1, I2, O> f)
    {
        return delegate.asyncAppliesOnInstance(f);
    }

    @Override
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> asyncAppliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f)
    {
        return delegate.asyncAppliesOnInstance(f);
    }

    @Override
    public <E extends Serializable> E transfer(E object)
    {
        return delegate.transfer(object);
    }
}
