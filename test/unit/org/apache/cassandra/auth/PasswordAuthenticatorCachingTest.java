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

import java.util.Map;

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
import org.mindrot.jbcrypt.BCrypt;

import static org.apache.cassandra.auth.RoleTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;



public class PasswordAuthenticatorCachingTest
{
    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.setDaemonInitialized();
        StorageService.instance.initServer(0);
    }

    @Before
    public void setup() throws Exception
    {
        ColumnFamilyStore.getIfExists(AuthKeyspace.NAME, AuthKeyspace.ROLES).truncateBlocking();
        ColumnFamilyStore.getIfExists(AuthKeyspace.NAME, AuthKeyspace.ROLE_MEMBERS).truncateBlocking();
        Pair<String, String> credentialsKsAndCfs = Pair.create(AuthKeyspace.NAME, "credentials");
        if (Schema.instance.hasCF(credentialsKsAndCfs))
        {
            Schema.instance.dropTable(credentialsKsAndCfs.left, credentialsKsAndCfs.right);
        }
    }

    @Test
    public void warmCacheLoadsAllEntries() throws Exception
    {
        IRoleManager roleManager = new RoleTestUtils.LocalExecutionRoleManager();
        roleManager.setup();
        for (RoleResource r : ALL_ROLES)
        {
            RoleOptions options = new RoleOptions();
            options.setOption(IRoleManager.Option.PASSWORD, "hash_for_" + r.getRoleName());
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, r, options);
        }

        PasswordAuthenticator authenticator = new PasswordAuthenticator();
        Map<String, String> cacheEntries = authenticator.getInitialEntriesForCache();

        assertEquals(ALL_ROLES.length, cacheEntries.size());
        cacheEntries.forEach((username, hash) -> assertTrue(BCrypt.checkpw("hash_for_" + username, hash)));
    }

    @Test
    public void warmCacheWithEmptyTable() throws Exception
    {
        PasswordAuthenticator authenticator = new PasswordAuthenticator();
        Map<String, String> cacheEntries = authenticator.getInitialEntriesForCache();
        assertTrue(cacheEntries.isEmpty());
    }

    @Test
    public void warmCacheFromLegacyTable() throws Exception
    {
        // recreate the legacy schema and insert a couple of users' credentials
        QueryProcessor.process("CREATE TABLE system_auth.credentials ( " +
                               "username text, " +
                               "salted_hash text, " +
                               "PRIMARY KEY(username))",
                               ConsistencyLevel.ONE);

        QueryProcessor.process("INSERT INTO system_auth.credentials (username, salted_hash) VALUES ('user1', 'fake_hash_1')",
                               ConsistencyLevel.ONE);
        QueryProcessor.process("INSERT INTO system_auth.credentials (username, salted_hash) VALUES ('user2', 'fake_hash_2')",
                               ConsistencyLevel.ONE);

        // no roles present in the new roles table
        UntypedResultSet roles = QueryProcessor.process("SELECT * FROM system_auth.roles", ConsistencyLevel.ONE);
        assertTrue(roles.isEmpty());

        PasswordAuthenticator authenticator = new PasswordAuthenticator();
        Map<String, String> cacheEntries = authenticator.getInitialEntriesForCache();
        assertEquals(2, cacheEntries.size());
        assertEquals("fake_hash_1", cacheEntries.get("user1"));
        assertEquals("fake_hash_2", cacheEntries.get("user2"));
    }

}
