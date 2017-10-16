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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class RolesCacheTest
{

    private static final RoleResource testResource = RoleResource.role("test_role");
    private static final Set<Role> testRoles = Collections.singleton(new Role("test_role", false, false,
                                                                       Collections.emptyMap(),
                                                                       Collections.emptySet()));

    @Before
    public void setup()
    {
        RolesCache.unregisterMBean();
    }

    @After
    public void tearDown()
    {
        System.clearProperty("cassandra.roles_cache.warming.max_retries");
        System.clearProperty("cassandra.roles_cache.warming.retry_interval_ms");
    }

    @Test
    public void warmCacheUsingEntryProvider() throws Exception
    {
        AtomicBoolean provided = new AtomicBoolean(false);
        Cacheable<RoleResource, Set<Role>> entryProvider = new Cacheable<RoleResource, Set<Role>>()
        {
            public Map<RoleResource, Set<Role>> getInitialEntriesForCache()
            {
                provided.set(true);
                return Collections.singletonMap(testResource, testRoles);
            }
        };
        RolesCache cache = new RolesCache(new RoleTestUtils.LocalExecutionRoleManager(), true);
        cache.warm(entryProvider);
        assertEquals(1, cache.size());
        assertTrue(provided.get());
    }

    @Test
    public void warmCacheIsSafeIfCachingIsDisabled() throws Exception
    {
        Cacheable<RoleResource, Set<Role>> entryProvider = new Cacheable<RoleResource, Set<Role>>()
        {
            public Map<RoleResource, Set<Role>> getInitialEntriesForCache()
            {
                return Collections.singletonMap(testResource, testRoles);
            }
        };

        RolesCache cache = new RolesCache(new RoleTestUtils.LocalExecutionRoleManager(), false);
        cache.warm(entryProvider);
        assertEquals(0, cache.size());
    }

    @Test
    public void providerSuppliesMoreEntriesThanCapacity() throws Exception
    {
        final int maxEntries = DatabaseDescriptor.getRolesCacheMaxEntries();
        Cacheable<RoleResource, Set<Role>> entryProvider = new Cacheable<RoleResource, Set<Role>>()
        {
            public Map<RoleResource, Set<Role>> getInitialEntriesForCache()
            {
                Map<RoleResource, Set<Role>> entries = new HashMap<>();
                for (int i = 0; i < maxEntries * 2; i++)
                {
                    entries.put(RoleResource.role("test_role_" + i), testRoles);
                }
                return entries;
            }
        };

        RolesCache cache = new RolesCache(new RoleTestUtils.LocalExecutionRoleManager(), true);
        cache.warm(entryProvider);
        assertEquals(maxEntries, cache.size());
    }

    @Test
    public void handleProviderErrorDuringWarming() throws Exception
    {
        System.setProperty("cassandra.roles_cache.warming.max_retries", "3");
        System.setProperty("cassandra.roles_cache.warming.retry_interval_ms", "0");
        final AtomicInteger attempts = new AtomicInteger(0);

        Cacheable<RoleResource, Set<Role>> entryProvider = new Cacheable<RoleResource, Set<Role>>()
        {
            public Map<RoleResource, Set<Role>> getInitialEntriesForCache()
            {
                if (attempts.incrementAndGet() < 3)
                    throw new RuntimeException("BOOM");

                return Collections.singletonMap(testResource, testRoles);
            }
        };

        RolesCache cache = new RolesCache(new RoleTestUtils.LocalExecutionRoleManager(), true);
        cache.warm(entryProvider);
        assertEquals(testRoles, cache.getRoles(testResource));
        // we should have made 3 attempts to get the initial entries
        assertEquals(3, attempts.get());
    }
}
