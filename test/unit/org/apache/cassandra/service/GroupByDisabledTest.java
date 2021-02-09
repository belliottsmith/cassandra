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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_GROUP_BY;

public class GroupByDisabledTest extends CQLTester
{
    static String originalGroupBySetting = null;

    @BeforeClass
    public static void beforeClass()
    {
        originalGroupBySetting = ALLOW_GROUP_BY.getString();
        disablePreparedReuseForTest(); // as the result changes depending on test system properties
    }
    @AfterClass
    public static void afterClass()
    {
        if (originalGroupBySetting == null)
            System.clearProperty(ALLOW_GROUP_BY.getKey());
        else
            System.setProperty(ALLOW_GROUP_BY.getKey(), originalGroupBySetting);
    }

    private void assertGroupByDisabled()
    {
        Assert.assertEquals(false, ALLOW_GROUP_BY.getBoolean());
        Throwable thrown = null;
        try
        {
            execute("SELECT * FROM system_schema.keyspaces GROUP BY keyspace_name");
        }
        catch (Throwable t)
        {
            thrown = t;
        }
        Assert.assertNotNull("Should throw an IllegalRequestException", thrown);
        Assert.assertEquals(InvalidRequestException.class, thrown.getClass());
        Assert.assertEquals("GROUP BY is disabled for ACI Cassandra 4.0. Please contact aci-cassandra@group.apple.com to discuss.",
                            thrown.getMessage());
    }

    @Test
    public void checkDisabledByDefault()
    {
        System.clearProperty(ALLOW_GROUP_BY.getKey());
        assertGroupByDisabled();
    }

    @Test
    public void checkExplicitlyDisabled()
    {
        System.setProperty(ALLOW_GROUP_BY.getKey(), "false");
        assertGroupByDisabled();
    }

    @Test
    public void checkCanGroupByIfEnabled() throws Throwable
    {
        System.setProperty(ALLOW_GROUP_BY.getKey(), "true");
        Assert.assertEquals(true, ALLOW_GROUP_BY.getBoolean());
        UntypedResultSet resultSet = execute("SELECT * FROM system_schema.keyspaces GROUP BY keyspace_name");
        Assert.assertFalse(resultSet.isEmpty());
    }
}
