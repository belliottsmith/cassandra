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

package org.apache.cassandra.auth;


import java.util.function.BooleanSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageService;

public class CacheRefresher<K, V> implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(CacheRefresher.class);

    private final String name;
    private final LoadingCache<K, V> cache;
    private final Object deleteSentinel;
    private final BooleanSupplier skipCondition;

    private CacheRefresher(String name, LoadingCache<K, V> cache, Object deleteSentinel, BooleanSupplier skipCondition)
    {
        this.name = name;
        this.cache = cache;
        this.deleteSentinel = deleteSentinel;
        this.skipCondition = skipCondition;
    }

    public void run()
    {
        try
        {
            logger.debug("Refreshing {} cache", name);
            for (K key : cache.asMap().keySet()) 
            {
                if (skipCondition.getAsBoolean())
                {
                    logger.debug("Skipping {} cache refresh", name);
                    return;
                }

                cache.refresh(key);
                if (deleteSentinel != null && cache.getIfPresent(key) == deleteSentinel) 
                {
                    cache.invalidate(key);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Unexpected exception refreshing {} cache", name, e);
        }
    }

    @VisibleForTesting
    public static <K, V> CacheRefresher<K, V> create(String name, LoadingCache<K, V> cache, Object deleteSentinel, BooleanSupplier skipCondition)
    {
        logger.info("Creating CacheRefresher for {}", name);
        return new CacheRefresher<>(name, cache, deleteSentinel, skipCondition);
    }

    public static <K, V> CacheRefresher<K, V> create(String name, LoadingCache<K, V> cache, Object deleteSentinel)
    {
        // by default we skip cache refreshes if the node has been decommed
        return create(name, cache, deleteSentinel, StorageService.instance::isDecommissioned);
    }

    public static <K, V> CacheRefresher<K, V> create(String name, LoadingCache<K, V> cache)
    {
        return create(name, cache, null);
    }
}
