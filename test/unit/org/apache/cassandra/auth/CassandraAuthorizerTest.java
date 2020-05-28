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

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.auth.AuthTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraAuthorizerTest
{
    @BeforeClass
    public static void setupClass()
    {
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setAuthorizer(new StubAuthorizer());
        SchemaLoader.loadSchema();
        StorageService.instance.initServer(0);
    }

    @Before
    public void setup()
    {
        ColumnFamilyStore.getIfExists(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES).truncateBlocking();
        ColumnFamilyStore.getIfExists(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_MEMBERS).truncateBlocking();
        ColumnFamilyStore.getIfExists(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS).truncateBlocking();
    }

    @Test
    public void testBulkLoadingForAuthCache()
    {
        IResource table1 = Resources.fromName("data/ks1/t1");
        IResource table2 = Resources.fromName("data/ks2/t2");

        // setup a hierarchy of roles. ROLE_B is granted LOGIN privs, ROLE_B_1 and ROLE_B_2.
        // ROLE_C is granted LOGIN along with ROLE_C_1 & ROLE_C_2
        IRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        roleManager.setup();
        Roles.initRolesCache(roleManager, () -> true);

        for (RoleResource role : ALL_ROLES)
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, role, new RoleOptions());

        RoleOptions withLogin = new RoleOptions();
        withLogin.setOption(IRoleManager.Option.LOGIN, Boolean.TRUE);
        roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, ROLE_B, withLogin);
        roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, ROLE_C, withLogin);
        grantRolesTo(roleManager, ROLE_B, ROLE_B_1, ROLE_B_2);
        grantRolesTo(roleManager, ROLE_C, ROLE_C_1, ROLE_C_2);

        CassandraAuthorizer authorizer = new CassandraAuthorizer();
        // Granted on ks1.t1: B1 -> {SELECT, MODIFY}, B2 -> {AUTHORIZE}, so B -> {SELECT, MODIFY, AUTHORIZE}
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.SELECT, Permission.MODIFY), table1, ROLE_B_1);
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.AUTHORIZE), table1, ROLE_B_2);

        // Granted on ks2.t2: C1 -> {SELECT, MODIFY}, C2 -> {AUTHORIZE}, so C -> {SELECT, MODIFY, AUTHORIZE}
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.SELECT, Permission.MODIFY), table2, ROLE_C_1);
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.AUTHORIZE), table2, ROLE_C_2);

        Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> cacheEntries = authorizer.bulkLoader().get();

        // only ROLE_B and ROLE_C have LOGIN privs, so only they should be in the cached
        assertEquals(2, cacheEntries.size());
        assertEquals(EnumSet.of(Permission.SELECT, Permission.MODIFY, Permission.AUTHORIZE),
                     cacheEntries.get(Pair.create(new AuthenticatedUser(ROLE_B.getRoleName()), table1)));
        assertEquals(EnumSet.of(Permission.SELECT, Permission.MODIFY, Permission.AUTHORIZE),
                     cacheEntries.get(Pair.create(new AuthenticatedUser(ROLE_C.getRoleName()), table2)));
    }

    @Test
    public void testBulkLoadingForAuthCachWithEmptyTable()
    {
        IRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        Roles.initRolesCache(roleManager, () -> true);

        CassandraAuthorizer authorizer = new CassandraAuthorizer();
        Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> cacheEntries = authorizer.bulkLoader().get();
        assertTrue(cacheEntries.isEmpty());
    }
}
