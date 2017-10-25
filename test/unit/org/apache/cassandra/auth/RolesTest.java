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

import java.util.Set;

import com.google.common.collect.Iterables;
import org.apache.cassandra.service.StorageService;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.auth.RoleTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RolesTest
{

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer(0);
        IRoleManager roleManager = new RoleTestUtils.LocalExecutionRoleManager();
        roleManager.setup();
        Roles.initRolesCache(new RolesCache(roleManager, true));
        for (RoleResource role : ALL_ROLES)
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, role, new RoleOptions());
        grantRolesTo(roleManager, ROLE_A, ROLE_B, ROLE_C);
    }

    @Test
    public void superuserStatusIsCached()
    {
        boolean hasSuper = Roles.hasSuperuserStatus(ROLE_A);
        long count = getReadCount();

        assertEquals(hasSuper, Roles.hasSuperuserStatus(ROLE_A));
        assertEquals(count, getReadCount());
    }

    @Test
    public void loginPrivilegeIsCached()
    {
        boolean canLogin = Roles.canLogin(ROLE_A);
        long count = getReadCount();

        assertEquals(canLogin, Roles.canLogin(ROLE_A));
        assertEquals(count, getReadCount());
    }

    @Test
    public void grantedRoleIterableIsCached()
    {
        Iterable<Role> granted = Roles.getGrantedRoles(ROLE_A);
        long count = getReadCount();

        assertTrue(Iterables.elementsEqual(granted, Roles.getGrantedRoles(ROLE_A)));
        assertEquals(count, getReadCount());
    }

    @Test
    public void grantedRoleSetIsCached()
    {
        Set<RoleResource> granted = Roles.getRoles(ROLE_A);
        long count = getReadCount();

        assertEquals(granted, Roles.getRoles(ROLE_A));
        assertEquals(count, getReadCount());
    }
}
