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
package org.apache.cassandra.utils;

import java.lang.management.ThreadMXBean;

/**
 * This class simplifies 2 JMX calls to make it easier to work with for operations: java.lang:type=Threading's getAllThreadIds()
 * and getThreadAllocatedBytes.  When running under HotSpot, the Threading MBean exposes thread allocations, which could
 * be fetched from Manager and exposed to Hubble, but operations felt it would be easier if this logic existed in Cassandra
 * directly.
 *
 * The core logic is {@link #getAllocatedBytes()}, all other methods are compliments to that method.
 *
 * @see rdar://91176476 (Add a JMX metric to show global allocations) #2851
 */
public class Threading implements ThreadingMBean
{
    private final com.sun.management.ThreadMXBean bean;

    public Threading(com.sun.management.ThreadMXBean bean)
    {
        this.bean = bean;
    }

    public static Threading create(ThreadMXBean bean)
    {
        if (!(bean instanceof com.sun.management.ThreadMXBean))
            return null;
        return new Threading((com.sun.management.ThreadMXBean) bean);
    }

    public boolean isThreadAllocatedMemorySupported()
    {
        return bean.isThreadAllocatedMemorySupported();
    }

    @Override
    public boolean isThreadAllocatedMemoryEnabled()
    {
        return bean.isThreadAllocatedMemoryEnabled();
    }

    @Override
    public void setThreadAllocatedMemoryEnabled(boolean enable)
    {
        bean.setThreadAllocatedMemoryEnabled(enable);
    }

    @Override
    public long[] getThreadAllocatedBytes()
    {
        return bean.getThreadAllocatedBytes(bean.getAllThreadIds());
    }

    @Override
    public long getAllocatedBytes()
    {
        long value = 0;
        for (long v : getThreadAllocatedBytes())
        {
            if (v > 0) // -1 == "missing", but safter to only increment IFF v is positive
                value += v;
        }
        return Math.max(0, value); // in case overflow
    }
}
