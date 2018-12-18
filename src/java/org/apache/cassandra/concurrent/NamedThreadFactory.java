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
package org.apache.cassandra.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;

/**
 * This class is an implementation of the <i>ThreadFactory</i> interface. This
 * is useful to give Java threads meaningful names which is useful when using
 * a tool like JConsole.
 */

public class NamedThreadFactory implements ThreadFactory
{
    public final String id;
    private final int priority;
    private final ClassLoader contextClassLoader;
    private final ThreadGroup threadGroup;
    protected final AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String id)
    {
        this(id, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String id, int priority)
    {
        this(id, priority, null, null);
    }

    public NamedThreadFactory(String id, int priority, ClassLoader contextClassLoader, ThreadGroup threadGroup)
    {
        this.id = id;
        this.priority = priority;
        this.contextClassLoader = contextClassLoader;
        this.threadGroup = threadGroup;
    }

    public Thread newThread(Runnable runnable)
    {
        String name = id + ':' + n.getAndIncrement();
        Thread thread = new  FastThreadLocalThread(threadGroup, threadLocalDeallocator(runnable), name);
        thread.setPriority(priority);
        thread.setDaemon(true);
        if (contextClassLoader != null)
            thread.setContextClassLoader(contextClassLoader);
        return thread;
    }

    /**
     * Ensures that {@link FastThreadLocal#remove() FastThreadLocal.remove()} is called when the {@link Runnable#run()}
     * method of the given {@link Runnable} instance completes to ensure cleanup of {@link FastThreadLocal} instances.
     * This is especially important for direct byte buffers allocated locally for a thread.
     */
    public static Runnable threadLocalDeallocator(Runnable r)
    {
        return () ->
        {
            try {
                r.run();
            } finally {
                FastThreadLocal.removeAll();
            }
        };
    }
}
