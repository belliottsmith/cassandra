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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class JMXEnabledSingleThreadExecutor extends JMXEnabledThreadPoolExecutor
{
    public JMXEnabledSingleThreadExecutor(String threadPoolName, String jmxPath)
    {
        super(1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new SingleThreadFactory(threadPoolName), jmxPath);
    }

    @Override
    public void setCoreThreads(int number)
    {
        throw new UnsupportedOperationException("Cannot change core pool size for single threaded executor.");
    }

    @Override
    public void setMaximumThreads(int number)
    {
        throw new UnsupportedOperationException("Cannot change max threads for single threaded executor.");
    }

    public boolean isExecutedBy(Thread otherThread)
    {
        return ((SingleThreadFactory) getThreadFactory()).isSameThread(otherThread);
    }

    static class SingleThreadFactory extends NamedThreadFactory
    {
        private volatile Thread thread = null;

        SingleThreadFactory(String id)
        {
            super(id);
        }

        @Override
        public Thread newThread(Runnable r)
        {
            // Ideally it won't be called more than once unless keepAliveTime is configured to shorter value.
            this.thread = super.newThread(r);
            return this.thread;
        }

        boolean isSameThread(Thread otherThread)
        {
            return this.thread == otherThread;
        }
    }
}
