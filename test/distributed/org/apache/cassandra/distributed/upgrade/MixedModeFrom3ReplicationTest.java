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

package org.apache.cassandra.distributed.upgrade;

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.shared.Versions;

public class MixedModeFrom3ReplicationTest extends MixedModeReplicationTestBase
{
    @BeforeClass
    public static void setup()
    {
        // Since the tests bypass networking in favor of mocks, this makes it so when nodes 1/2 go down then back up
        // node 3 may not detect this before it tries to setup default roles (see org.apache.cassandra.auth.CassandraRoleManager.setup),
        // so attempts to query and node1/2 fail due to having an empty ring state
        System.setProperty("cassandra.superuser_setup_delay_ms", Long.toString(TimeUnit.MINUTES.toMillis(10)));
    }

    @Test
    public void testSimpleStrategy30to3X() throws Throwable
    {
        testSimpleStrategy(v30, v3X);
    }

    @Test
    public void testSimpleStrategy() throws Throwable
    {
        testSimpleStrategy(v30);
    }
}
