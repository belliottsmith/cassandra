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

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.cassandra.io.util.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;

import static org.apache.cassandra.io.sstable.format.big.BigFormat.CIE_MIN_SSTABLE_VERSION_PROPERTY;
import static org.apache.cassandra.service.StartupChecks.SKIP_MIN_SSTABLE_VERSION_CHECK_PROPERTY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MinSSTableVersionCheckTest
{
    private static final String KEYSPACE = "test_ks";
    private static final String TABLE    = "test_table";

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Before
    @After
    public void setup()
    {
        System.clearProperty(SKIP_MIN_SSTABLE_VERSION_CHECK_PROPERTY);
    }

    @Test
    public void skipCheckIfDisabled() throws Exception
    {
        // no exceptions thrown because the check is skipped
        writeFile(BigFormat.instance.getVersion("ma"));
        System.setProperty(SKIP_MIN_SSTABLE_VERSION_CHECK_PROPERTY, "true");
        execute();
    }

    @Test
    public void checkEnabledButOnlySupportedSSTables() throws Exception
    {
        writeFile(BigFormat.instance.getVersion("mf"));
        execute();
    }

    @Test
    public void defaultAllowMFFiles() throws Exception
    {
        writeFile(BigFormat.instance.getVersion("mf"));
        execute();
    }

    @Test
    public void defaultRejectEarlierThanMFFiles() throws Exception
    {
        writeFile(BigFormat.instance.getVersion("me"));
        executeAndFail();
    }

    @Test
    public void failWithBothInvalidAndUnsupportedErrorMessage() throws Exception
    {
        writeFile(BigFormat.instance.getVersion("ma")); // unsupported
        writeFile(BigFormat.instance.getVersion("ja")); // invalid
        executeAndFail(startupException ->
                       {
                           String error = startupException.getMessage();
                           assertTrue("Should contain error for invalid version", error.contains("Detected unreadable sstables "));
                           assertTrue("Should contain error for unsupported version", error.contains("Detected readable but unsupported sstables "));
                           assertTrue("Should contain suggestion for unsupported version",
                                      error.contains(String.format("set the allowed min version using -D%s=version or skip the check altogether with -D%s=true",
                                                                   CIE_MIN_SSTABLE_VERSION_PROPERTY,
                                                                   SKIP_MIN_SSTABLE_VERSION_CHECK_PROPERTY)));
                       });
    }

    private void execute() throws StartupException
    {
        Config config = new Config();
        config.data_file_directories = new String[] { tempDir.getRoot().getAbsolutePath() };
        config.commitlog_directory = tempDir.getRoot().getAbsolutePath() + "/commitlogs";
        config.saved_caches_directory = tempDir.getRoot().getAbsolutePath() + "/saved_caches";
        config.hints_directory = tempDir.getRoot().getAbsolutePath() + "/hints";
        DatabaseDescriptor.setConfig(config);
        StartupChecks.checkSSTablesFormat.execute();
    }

    private void executeAndFail(Consumer<StartupException> check)
    {
        try
        {
            execute();
            fail("Expected an exception but none thrown");
        }
        catch (StartupException e)
        {
            check.accept(e);
        }
    }

    private void executeAndFail()
    {
        executeAndFail(e -> assertTrue(e.getMessage().startsWith("Detected readable but unsupported sstables ")));
    }

    private File writeFile(Version version) throws IOException
    {
        File dir = new File(tempDir.getRoot());
        String filename = new Descriptor(version, dir, KEYSPACE, TABLE, 1, SSTableFormat.Type.BIG).filenameFor(Component.DATA);
        File file = new File(filename);
        assertTrue(file.createFileIfNotExists());
        assertTrue(file.isReadable());
        return file;
    }
}
