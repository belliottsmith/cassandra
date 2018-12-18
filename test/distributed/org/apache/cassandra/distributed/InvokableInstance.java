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

import org.apache.cassandra.distributed.api.IInvokableInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.Throwables;

public class InvokableInstance implements IInvokableInstance
{
    final ExecutorService isolatedExecutor;
    private final ClassLoader classLoader;
    private final Method deserializeOnInstance;

    InvokableInstance(String name, ClassLoader classLoader)
    {
        this.isolatedExecutor = Executors.newCachedThreadPool(new NamedThreadFactory(name, Thread.NORM_PRIORITY, classLoader, new ThreadGroup(name)));
        this.classLoader = classLoader;
        this.deserializeOnInstance = lookupDeserializeOneObject(classLoader);
    }

    void shutdown()
    {
        isolatedExecutor.shutdownNow();
    }

    <O> CallableNoExcept<Future<O>> async(CallableNoExcept<O> call) { return () -> isolatedExecutor.submit(call); }
    public <O> CallableNoExcept<Future<O>> asyncCallsOnInstance(SerializableCallable<O> call) { return async(transfer(call)); }
    <O> CallableNoExcept<O> sync(CallableNoExcept<O> call) { return () -> waitOn(async(call).call()); }
    public <O> CallableNoExcept<O> callsOnInstance(SerializableCallable<O> call) { return sync(transfer(call)); }
    public <O> O callOnInstance(SerializableCallable<O> call) { return callsOnInstance(call).call(); }

    CallableNoExcept<Future<?>> async(Runnable run) { return () -> isolatedExecutor.submit(run); }
    public CallableNoExcept<Future<?>> asyncRunsOnInstance(SerializableRunnable run) { return async(transfer(run)); }
    Runnable sync(Runnable run) { return () -> waitOn(async(run).call()); }
    public Runnable runsOnInstance(SerializableRunnable run) { return sync(transfer(run)); }
    public void runOnInstance(SerializableRunnable run) { runsOnInstance(run).run(); }

    <I> Function<I, Future<?>> async(Consumer<I> consumer) { return (a) -> isolatedExecutor.submit(() -> consumer.accept(a)); }
    public <I> Function<I, Future<?>> asyncAcceptsOnInstance(SerializableConsumer<I> consumer) { return async(transfer(consumer)); }
    <I> Consumer<I> sync(Consumer<I> consumer) { return (a) -> waitOn(async(consumer).apply(a)); }
    public <I> Consumer<I> acceptsOnInstance(SerializableConsumer<I> consumer) { return sync(transfer(consumer)); }

    <I1, I2> BiFunction<I1, I2, Future<?>> async(BiConsumer<I1, I2> consumer) { return (a, b) -> isolatedExecutor.submit(() -> consumer.accept(a, b)); }
    public <I1, I2> BiFunction<I1, I2, Future<?>> asyncAcceptsOnInstance(SerializableBiConsumer<I1, I2> consumer) { return async(transfer(consumer)); }
    <I1, I2> BiConsumer<I1, I2> sync(BiConsumer<I1, I2> consumer) { return (a, b) -> waitOn(async(consumer).apply(a, b)); }
    public <I1, I2> BiConsumer<I1, I2> acceptsOnInstance(SerializableBiConsumer<I1, I2> consumer) { return sync(transfer(consumer)); }

    <I, O> Function<I, Future<O>> async(Function<I, O> f) { return (a) -> isolatedExecutor.submit(() -> f.apply(a)); }
    public <I, O> Function<I, Future<O>> asyncAppliesOnInstance(SerializableFunction<I, O> f) { return async(transfer(f)); }
    <I, O> Function<I, O> sync(Function<I, O> f) { return (a) -> waitOn(async(f).apply(a)); }
    public <I, O> Function<I, O> appliesOnInstance(SerializableFunction<I, O> f) { return sync(transfer(f)); }

    private <I1, I2, O> BiFunction<I1, I2, Future<O>> async(BiFunction<I1, I2, O> f) { return (a, b) -> isolatedExecutor.submit(() -> f.apply(a, b)); }
    public <I1, I2, O> BiFunction<I1, I2, Future<O>> asyncAppliesOnInstance(SerializableBiFunction<I1, I2, O> f) { return async(transfer(f)); }
    private <I1, I2, O> BiFunction<I1, I2, O> sync(BiFunction<I1, I2, O> f) { return (a, b) -> waitOn(async(f).apply(a, b)); }
    public <I1, I2, O> BiFunction<I1, I2, O> appliesOnInstance(SerializableBiFunction<I1, I2, O> f) { return sync(transfer(f)); }

    private <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> async(TriFunction<I1, I2, I3, O> f) { return (a, b, c) -> isolatedExecutor.submit(() -> f.apply(a, b, c)); }
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> asyncAppliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f) { return async(transfer(f)); }
    private <I1, I2, I3, O> TriFunction<I1, I2, I3, O> sync(TriFunction<I1, I2, I3, O> f) { return (a, b, c) -> waitOn(async(f).apply(a, b, c)); }
    public <I1, I2, I3, O> TriFunction<I1, I2, I3, O> appliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f) { return sync(transfer(f)); }

    @Override
    public <E extends Serializable> E transfer(E object)
    {
        return (E) transferOneObject(object, classLoader, deserializeOnInstance);
    }

    static <E extends Serializable> E transferAdhoc(E object, ClassLoader classLoader)
    {
        return transferOneObject(object, classLoader, lookupDeserializeOneObject(classLoader));
    }

    private static <E extends Serializable> E transferOneObject(E object, ClassLoader classLoader, Method deserializeOnInstance)
    {
        byte[] bytes = serializeOneObject(object);
        try
        {
            Object onInstance = deserializeOnInstance.invoke(null, bytes);
            if (onInstance.getClass().getClassLoader() != classLoader)
                throw new IllegalStateException(onInstance + " seemingly from wrong class loader: " + onInstance.getClass().getClassLoader() + ", but expected " + classLoader);

            return (E) onInstance;
        }
        catch (IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Method lookupDeserializeOneObject(ClassLoader classLoader)
    {
        try
        {
            return classLoader.loadClass(InvokableInstance.class.getName()).getDeclaredMethod("deserializeOneObject", byte[].class);
        }
        catch (ClassNotFoundException | NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unused") // called through method invocation
    public static Object deserializeOneObject(byte[] bytes)
    {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais);)
        {
            return ois.readObject();
        }
        catch (IOException | ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static byte[] serializeOneObject(Object object)
    {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos))
        {
            oos.writeObject(object);
            oos.close();
            return baos.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <T> T waitOn(Future<T> f)
    {
        try
        {
            return f.get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            Throwables.maybeFail(e.getCause());
            throw new AssertionError();
        }
    }

}
