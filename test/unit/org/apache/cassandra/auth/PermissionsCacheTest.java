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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.auth.RoleTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PermissionsCacheTest
{
    private final AuthenticatedUser testUser = new AuthenticatedUser("user");
    private final IResource testResource = DataResource.table("test_ks", "test_table");
    private final Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> singleTestPermission =
        Collections.singletonMap(Pair.create(testUser, testResource), Permission.ALL);

    private int restoreValidity;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.setDaemonInitialized();
        StorageService.instance.initServer(0);
    }

    @Before
    public void setup()
    {
        PermissionsCache.unregisterMBean();

        ColumnFamilyStore.getIfExists(AuthKeyspace.NAME, AuthKeyspace.ROLES).truncateBlocking();
        ColumnFamilyStore.getIfExists(AuthKeyspace.NAME, AuthKeyspace.ROLE_MEMBERS).truncateBlocking();
        ColumnFamilyStore.getIfExists(AuthKeyspace.NAME, AuthKeyspace.ROLE_PERMISSIONS).truncateBlocking();
        Pair<String, String> legacyPermissionsKsAndCfs = Pair.create(AuthKeyspace.NAME, "permissions");
        if (Schema.instance.hasCF(legacyPermissionsKsAndCfs))
        {
            Schema.instance.dropTable(legacyPermissionsKsAndCfs.left, legacyPermissionsKsAndCfs.right);
        }

        // Initialize the role manager, this is necessary because CassandraAuthorizer checks
        // for superuser status early in the authz process.
        IRoleManager roleManager = new RoleTestUtils.LocalExecutionRoleManager();
        roleManager.setup();
        Roles.initRolesCache(new RolesCache(roleManager, true));

        for (RoleResource role : ALL_ROLES)
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, role, new RoleOptions());

        // grab the current setting for permissions validity to restore when a test modifies it
        restoreValidity = DatabaseDescriptor.getPermissionsValidity();
    }

    @After
    public void tearDown()
    {
        System.clearProperty("cassandra.permissions_cache.warming.max_retries");
        System.clearProperty("cassandra.permissions_cache.warming.retry_interval_ms");

        DatabaseDescriptor.setPermissionsValidity(restoreValidity);
    }

    @Test
    public void warmCacheUsingEntryProvider() throws Exception
    {
        AtomicBoolean provided = new AtomicBoolean(false);
        Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>> entryProvider = new Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
        {
            public Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> getInitialEntriesForCache()
            {
                provided.set(true);
                return singleTestPermission;
            }
        };
        PermissionsCache cache = new PermissionsCache(new CassandraAuthorizer());
        cache.warm(entryProvider);
        assertEquals(1, cache.size());
        assertTrue(provided.get());
    }

    @Test
    public void warmCacheIsSafeIfCachingIsDisabled() throws Exception
    {
        Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>> entryProvider = new Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
        {
            public Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> getInitialEntriesForCache()
            {
                return singleTestPermission;
            }
        };

        DatabaseDescriptor.setPermissionsValidity(0); // Set validity to 0 to disable cache
        PermissionsCache cache = new PermissionsCache(new CassandraAuthorizer());
        cache.warm(entryProvider);
        assertEquals(0, cache.size());
        assertEquals(Permission.NONE, cache.getPermissions(testUser, testResource));
        assertEquals(0, cache.size()); // Verify cache is still disabled and empty
    }

    @Test
    public void providerSuppliesMoreEntriesThanCapacity() throws Exception
    {
        final int maxEntries = DatabaseDescriptor.getPermissionsCacheMaxEntries();
        Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>> entryProvider = new Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
        {
            public Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> getInitialEntriesForCache()
            {
                Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> entries = new HashMap<>();
                for (int i = 0; i < maxEntries * 2; i++)
                {
                    entries.put(Pair.create(new AuthenticatedUser("test_user " + i), testResource), Permission.ALL);
                }
                return entries;
            }
        };

        PermissionsCache cache = new PermissionsCache(new CassandraAuthorizer());
        cache.warm(entryProvider);
        assertEquals(maxEntries, cache.size());
    }

    @Test
    public void handleProviderErrorDuringWarming() throws Exception
    {
        System.setProperty("cassandra.permissions_cache.warming.max_retries", "3");
        System.setProperty("cassandra.permissionss_cache.warming.retry_interval_ms", "0");
        final AtomicInteger attempts = new AtomicInteger(0);

        Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>> entryProvider = new Cacheable<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
        {
            public Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> getInitialEntriesForCache()
            {
                if (attempts.incrementAndGet() < 3)
                    throw new RuntimeException("BOOM");

                return singleTestPermission;
            }
        };

        PermissionsCache cache = new PermissionsCache(new CassandraAuthorizer());
        cache.warm(entryProvider);
        assertEquals(Permission.ALL, cache.getPermissions(testUser, testResource));
        // we should have made 3 attempts to get the initial entries
        assertEquals(3, attempts.get());
    }


    @Test
    public void warmCacheLoadsAllEntries() throws Exception
    {
        IResource table1 = Resources.fromName("data/ks1/t1");
        IResource table2 = Resources.fromName("data/ks2/t2");

        // setup a hierarchy of roles. ROLE_B is granted LOGIN privs, ROLE_B_1 and ROLE_B_2.
        // ROLE_C is granted LOGIN along with ROLE_C_1 & ROLE_C_2
        IRoleManager roleManager = new LocalExecutionRoleManager();
        roleManager.setup();

        RoleOptions withLogin = new RoleOptions();
        withLogin.setOption(IRoleManager.Option.LOGIN, Boolean.TRUE);
        roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, ROLE_B, withLogin);
        roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, ROLE_C, withLogin);
        grantRolesTo(roleManager, ROLE_B, ROLE_B_1, ROLE_B_2);
        grantRolesTo(roleManager, ROLE_C, ROLE_C_1, ROLE_C_2);

        // Granted on ks1.t1: B1 -> {SELECT, MODIFY}, B2 -> {AUTHORIZE}, so B -> {SELECT, MODIFY, AUTHORIZE}
        QueryProcessor.process(String.format("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                                             "VALUES ('%s','%s', {'SELECT','MODIFY'})",
                                             ROLE_B_1.getName(),
                                             table1.getName()),
                               ConsistencyLevel.ONE);
        QueryProcessor.process(String.format("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                                             "VALUES ('%s','%s', {'AUTHORIZE'})",
                                             ROLE_B_2.getName(),
                                             table1.getName()),
                               ConsistencyLevel.ONE);

        // Granted on ks2.t2: C1 -> {SELECT, MODIFY}, C2 -> {AUTHORIZE}, so C -> {SELECT, MODIFY, AUTHORIZE}
        QueryProcessor.process(String.format("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                                             "VALUES ('%s','%s', {'SELECT','MODIFY'})",
                                             ROLE_C_1.getName(),
                                             table2.getName()),
                               ConsistencyLevel.ONE);
        QueryProcessor.process(String.format("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                                             "VALUES ('%s','%s', {'AUTHORIZE'})",
                                             ROLE_C_2.getName(),
                                             table2.getName()),
                               ConsistencyLevel.ONE);

        CassandraAuthorizer authorizer = new CassandraAuthorizer();
        Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> cacheEntries = authorizer.getInitialEntriesForCache();

        // only ROLE_B and ROLE_C have LOGIN privs, so we only they should be in the cached
        assertEquals(2, cacheEntries.size());
        assertEquals(EnumSet.of(Permission.SELECT, Permission.MODIFY, Permission.AUTHORIZE),
                     cacheEntries.get(Pair.create(new AuthenticatedUser(ROLE_B.getRoleName()), table1)));
        assertEquals(EnumSet.of(Permission.SELECT, Permission.MODIFY, Permission.AUTHORIZE),
                     cacheEntries.get(Pair.create(new AuthenticatedUser(ROLE_C.getRoleName()), table2)));
    }

    @Test
    public void warmCacheWithEmptyTable() throws Exception
    {
        CassandraAuthorizer authorizer = new CassandraAuthorizer();
        Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> cacheEntries = authorizer.getInitialEntriesForCache();
        assertTrue(cacheEntries.isEmpty());
    }

    @Test
    public void warmCacheFromLegacyTable() throws Exception
    {
        IResource table1 = Resources.fromName("data/ks1/t1");
        IResource table2 = Resources.fromName("data/ks2/t2");
        AuthenticatedUser user1 = new AuthenticatedUser("user1");
        AuthenticatedUser user2 = new AuthenticatedUser("user2");
        // recreate the legacy schema and insert a couple of sets of permissions
        QueryProcessor.process("CREATE TABLE system_auth.permissions ( " +
                               "username text, " +
                               "resource text, " +
                               "permissions set<text>," +
                               "PRIMARY KEY(username, resource))",
                               ConsistencyLevel.ONE);

        QueryProcessor.process(String.format("INSERT INTO system_auth.permissions (username, resource, permissions) " +
                                             "VALUES ('%s', '%s', {'SELECT', 'MODIFY'})",
                                             user1.getName(),
                                             table1.getName()),
                               ConsistencyLevel.ONE);
        QueryProcessor.process(String.format("INSERT INTO system_auth.permissions (username, resource, permissions) " +
                                             "VALUES ('%s', '%s', {'AUTHORIZE'})",
                                             user2.getName(),
                                             table2.getName()),
                               ConsistencyLevel.ONE);

        // no permissions present in the new permissions table
        UntypedResultSet perms = QueryProcessor.process("SELECT * FROM system_auth.role_permissions", ConsistencyLevel.ONE);
        assertTrue(perms.isEmpty());

        CassandraAuthorizer authorizer = new CassandraAuthorizer();
        Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> cacheEntries = authorizer.getInitialEntriesForCache();
        assertEquals(2, cacheEntries.size());
        assertEquals(EnumSet.of(Permission.SELECT, Permission.MODIFY), cacheEntries.get(Pair.create(user1, table1)));
        assertEquals(EnumSet.of(Permission.AUTHORIZE), cacheEntries.get(Pair.create(user2, table2)));
    }
}
