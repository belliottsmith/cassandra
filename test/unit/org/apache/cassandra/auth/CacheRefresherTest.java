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
        CacheRefresher<String, String> refresher = CacheRefresher.create("test", cache);

        src.put("some", "thing");
        Assert.assertEquals("thing", cache.get("some"));

        // cache should still have old value...
        src.put("some", "one");
        Assert.assertEquals("thing", cache.get("some"));

        // ... but refresher should update it
        refresher.run();
        Assert.assertEquals("one", cache.get("some"));
    }
}