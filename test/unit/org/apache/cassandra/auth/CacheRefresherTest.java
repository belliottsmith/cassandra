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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.junit.Assert;
import org.junit.Test;

public class CacheRefresherTest
{
    @Test
    public void refresh() throws Exception
    {
        Map<String, String> src = new HashMap<>();
        CacheLoader<String, String> loader = new CacheLoader<String, String>()
        {
            public String load(String key) throws Exception
            {
                return src.get(key);
            }
        };
        LoadingCache<String, String> cache = CacheBuilder.newBuilder().build(loader);

        AtomicBoolean skipRefresh = new AtomicBoolean(false);
        BooleanSupplier skipCondition = skipRefresh::get;

        CacheRefresher<String, String> refresher = CacheRefresher.create("test", cache, null, skipCondition);
        src.put("some", "thing");
        Assert.assertEquals("thing", cache.get("some"));

        // cache should still have old value...
        src.put("some", "one");
        Assert.assertEquals("thing", cache.get("some"));

        // ... but refresher should update it
        refresher.run();
        Assert.assertEquals("one", cache.get("some"));

        // if the skip condition returns true, don't refresh
        skipRefresh.set(true);
        src.put("some", "body");
        refresher.run();
        Assert.assertEquals("one", cache.get("some"));
        // change the skip condition back to false and refresh
        skipRefresh.set(false);
        refresher.run();
        Assert.assertEquals("body", cache.get("some"));
    }
}