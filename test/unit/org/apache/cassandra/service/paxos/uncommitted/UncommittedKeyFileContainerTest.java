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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;

public class UncommittedKeyFileContainerTest
{
    private static final UUID CFID = UUID.randomUUID();

    private File directory = null;

    @Before
    public void setUp()
    {
        if (directory != null)
            FileUtils.deleteRecursive(directory);

        directory = Files.createTempDir();
    }

    /**
     * attempting to get a flush writer should fail if there's another in progress flush
     */
    @Test
    public void concurrentFlushFailure() throws IOException
    {
        UncommittedKeyFileContainer state = new UncommittedKeyFileContainer(CFID, directory, null);

        UncommittedKeyFileContainer.FlushWriter writer = state.createFlushWriter();

        try
        {
            state.createFlushWriter();
            Assert.fail("shouldn't be able to open another writer until the existing one has been closed");
        }
        catch (IllegalStateException e)
        {
            // expected
        }

        writer.finish();

        // this should succeed since the previous writer was closed
        writer = state.createFlushWriter();
        writer.finish();
    }
}
