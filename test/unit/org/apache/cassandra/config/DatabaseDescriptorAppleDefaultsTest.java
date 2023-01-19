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

package org.apache.cassandra.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatabaseDescriptorAppleDefaultsTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void ipExclusionsTest() throws UnknownHostException
    {
        // A selection of IPs from the list
        List<String> testExcludedIPs = Arrays.asList("10.110.60.1", "10.110.60.62", "2a01:b740:5fa::1", "17.217.24.83");

        for (String testExcludedIP : testExcludedIPs)
        {
            assertTrue(DatabaseDescriptor.getInternodeErrorReportingExclusions().contains(InetAddress.getByName(testExcludedIP)));
            assertTrue(DatabaseDescriptor.getClientErrorReportingExclusions().contains(InetAddress.getByName(testExcludedIP)));
        }

        assertFalse(DatabaseDescriptor.getInternodeErrorReportingExclusions().contains(InetAddress.getLocalHost()));
        assertFalse(DatabaseDescriptor.getClientErrorReportingExclusions().contains(InetAddress.getLocalHost()));
    }
}
