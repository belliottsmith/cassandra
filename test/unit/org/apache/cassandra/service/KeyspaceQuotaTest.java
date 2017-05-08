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

import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyspaceQuotaTest extends CQLTester
{
    @Before
    public void before() throws ConfigurationException, UnknownHostException
    {
        DatabaseDescriptor.setKeyspaceQuotasEnabled(true);
        DatabaseDescriptor.setKeyspaceQuotaRefreshTimeInSec(1);

        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
        StorageService.instance.getTokenMetadata().updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), FBUtilities.getBroadcastAddress());
        MigrationManager.announceNewKeyspace(CIEInternalKeyspace.metadata(), false);
        KeyspaceQuota.scheduleQuotaCheck();
    }

    @Test
    public void rejectTest() throws Throwable
    {
        assertFalse(Keyspace.open(keyspace()).disabledForWrites);
        createTable("CREATE TABLE %s (id int primary key, v int)");
        execute("INSERT INTO %s (id, v) VALUES (1,1)");
        flush();
        QueryProcessor.executeOnceInternal(String.format("INSERT INTO cie_internal.ks_quota (keyspace_name, max_ks_size_mb) VALUES ('%s', 0)", keyspace()));
        Thread.sleep(2000);
        assertTrue(Keyspace.open(keyspace()).disabledForWrites);
        QueryProcessor.executeOnceInternal(String.format("DELETE FROM cie_internal.ks_quota WHERE keyspace_name = '%s'", keyspace()));
        Thread.sleep(2000);
        assertFalse(Keyspace.open(keyspace()).disabledForWrites);
    }
}
