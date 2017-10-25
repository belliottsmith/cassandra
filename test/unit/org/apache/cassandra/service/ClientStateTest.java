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

package org.apache.cassandra.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ClientStateTest
{
    @BeforeClass
    public static void beforeClass()
    {
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
        DatabaseDescriptor.daemonInitialization();
    }
    @AfterClass
    public static void afterClass()
    {
        System.clearProperty("org.apache.cassandra.disable_mbean_registration");
    }

    @Test
    public void permissionsCheckStartsAtHeadOfResourceChain() throws Exception {
        // verify that when performing a permissions check, we start from the
        // root IResource in the applicable hierarchy and proceed to the more
        // granular resources until we find the required permission (or until
        // we reach the end of the resource chain). This is because our typical
        // usage is to grant blanket permissions on the root resources to users
        // and so we save lookups, cache misses and cache space by traversing in
        // this order. e.g. for DataResources, we typically grant perms on the
        // 'data' resource, so when looking up a users perms on a specific table
        // it makes sense to follow: data -> keyspace -> table

        final AtomicInteger getPermissionsRequestCount = new AtomicInteger(0);
        final IResource rootResource = DataResource.root();
        final IResource tableResource = DataResource.table("test_ks", "test_table");
        final AuthenticatedUser testUser = new AuthenticatedUser("test_user")
        {
            public Set<Permission> getPermissions(IResource resource)
            {
                getPermissionsRequestCount.incrementAndGet();
                if (resource.equals(rootResource))
                    return Permission.ALL;

                fail(String.format("Permissions requested for unexpected resource %s", resource));
                // need a return to make the compiler happy
                return null;
            }
        };

        // set up the role cache so that we can associate our test user with the client
        // state (ClientState::login requires that the user's primary role has login privilege
        Supplier<Map<RoleResource, Set<Role>>> bulkLoader =
            () -> Collections.singletonMap(testUser.getPrimaryRole(),
                                           Collections.singleton(new Role(testUser.getName(),
                                                                          false,
                                                                          true,
                                                                          new HashMap<>(),
                                                                          new HashSet<>())));
        RolesCache cache = new RolesCache(new AuthTestUtils.LocalCassandraRoleManager(bulkLoader), () -> true);
        cache.warm();
        assertEquals(1, cache.getEstimatedSize());
        Roles.initRolesCache(cache);

        // finally, need to configure CassandraAuthorizer so we don't shortcircuit out of the authz process
        DatabaseDescriptor.setAuthorizer(new CassandraAuthorizer());

        // check permissions on the table, which should check for the root resource first
        // & return successfully without needing to proceed further
        ClientState state = ClientState.forInternalCalls();
        state.login(testUser);
        state.ensurePermission(Permission.SELECT, tableResource);
        assertEquals(1, getPermissionsRequestCount.get());
    }
}
