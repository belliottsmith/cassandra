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


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.CIEInternalKeyspace;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyspaceQuotaTest extends CQLTester
{
    @BeforeClass
    public static void before() throws ConfigurationException
    {
        DatabaseDescriptor.setKeyspaceQuotasEnabled(true);
        DatabaseDescriptor.setKeyspaceQuotaRefreshTimeInSec(1);

        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
        StorageService.instance.getTokenMetadata().updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), FBUtilities.getBroadcastAddressAndPort());

        SchemaTestUtil.announceNewKeyspace(CIEInternalKeyspace.metadata());
        KeyspaceQuota.scheduleQuotaCheck();
    }

    @AfterClass
    public static void after()
    {
        StorageService.instance.getTokenMetadata().clearUnsafe();
    }

    @Test
    public void rejectTest() throws Throwable
    {
        assertFalse(Keyspace.open(keyspace()).disabledForWrites);
        createTable("CREATE TABLE %s (id int primary key, v int)");
        execute("INSERT INTO %s (id, v) VALUES (1,1)");
        flush();
        Keyspace ks = Keyspace.open(keyspace());
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO cie_internal.ks_quota (keyspace_name, max_ks_size_mb) VALUES ('%s', 0)", keyspace()));
        while (!ks.disabledForWrites)
            Thread.sleep(100);
        QueryProcessor.executeOnceInternal(String.format("DELETE FROM cie_internal.ks_quota WHERE keyspace_name = '%s'", keyspace()));
        while (ks.disabledForWrites)
            Thread.sleep(100);
        assertFalse(Keyspace.open(keyspace()).disabledForWrites);
    }

    @Test
    public void defaultQuotaTest() throws Throwable
    {
        assertFalse(Keyspace.open(keyspace()).disabledForWrites);
        createTable("CREATE TABLE %s (id int primary key, v int)");
        execute("INSERT INTO %s (id, v) VALUES (1,1)");
        flush();
        Keyspace ks = Keyspace.open(keyspace());
        DatabaseDescriptor.setDefaultKeyspaceQuotaBytes(0);
        while (!ks.disabledForWrites)
            Thread.sleep(100);
        // can't use execute(...) here because it doesn't call checkAccess
        boolean gotException = false;
        try
        {
            QueryProcessor.process(String.format("insert into %s.%s (id, v) values (2,2)", keyspace(), currentTable()), ConsistencyLevel.ONE);
        }
        catch (Throwable t)
        {
            gotException = t.getMessage().toLowerCase().contains("quota");
        }
        assertTrue(gotException);
        DatabaseDescriptor.setDefaultKeyspaceQuotaBytes(-1);
        while (ks.disabledForWrites)
            Thread.sleep(100);
        QueryProcessor.process(String.format("insert into %s.%s (id, v) values (2,2)", keyspace(), currentTable()), ConsistencyLevel.ONE);

    }
}
