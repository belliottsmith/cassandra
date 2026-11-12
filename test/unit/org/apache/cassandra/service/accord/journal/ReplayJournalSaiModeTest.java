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

package org.apache.cassandra.service.accord.journal;

import java.nio.file.Files;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.CommandStore;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.AccordConfig.JournalConfig.ReplayMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordKeyspace;

import static org.apache.cassandra.config.AccordConfig.JournalConfig.ReplayMode.ALL;
import static org.apache.cassandra.config.AccordConfig.JournalConfig.ReplayMode.NON_DURABLE;
import static org.apache.cassandra.config.AccordConfig.JournalConfig.ReplayMode.PART_NON_DURABLE;
import static org.apache.cassandra.config.AccordConfig.RangeIndexMode.in_memory;
import static org.apache.cassandra.config.AccordConfig.RangeIndexMode.journal_sai;

/**
 * With journal_sai, replay rebuilds the in-memory index of uncompacted journal segments only from the commands it
 * replays, so config validation, and replay itself (which may be requested at runtime), refuse modes that skip commands.
 */
public class ReplayJournalSaiModeTest
{
    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setAccordTransactionsEnabled(true);
        // must be set before AccordKeyspace defines the journal table, which then carries the index
        DatabaseDescriptor.getAccord().range_index_mode = journal_sai;
        DatabaseDescriptor.getAccord().journal.replay = ALL; // as required by journal_sai, else startup replay fails
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
    }

    @Test
    public void configValidationRefusesReplayModesThatSkipCommands()
    {
        for (ReplayMode mode : ReplayMode.values())
        {
            boolean skipsCommands = mode == PART_NON_DURABLE || mode == NON_DURABLE;
            AccordConfig.validateReplayMode(in_memory, mode);
            try
            {
                AccordConfig.validateReplayMode(journal_sai, mode);
                Assert.assertFalse("expected " + mode + " to be refused with journal_sai", skipsCommands);
            }
            catch (ConfigurationException e)
            {
                Assert.assertTrue("expected " + mode + " to be permitted with journal_sai: " + e.getMessage(), skipsCommands);
            }
        }
    }

    @Test
    public void replayRefusesReplayModesThatSkipCommands() throws Throwable
    {
        File dir = new File(Files.createTempDirectory("replay-journal-sai"));
        dir.deleteRecursiveOnExit();
        AccordJournal journal = new AccordJournal(new TestParams(), dir, Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL));
        journal.start(null);
        try
        {
            Assert.assertNotNull("precondition: journal_sai range search", journal.rangeSearch);
            try
            {
                Replay.replay(journal, PART_NON_DURABLE, new CommandStore[0], null);
                Assert.fail("expected PART_NON_DURABLE replay to be refused with journal_sai");
            }
            catch (IllegalArgumentException expected)
            {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("journal_sai"));
            }
            Assert.assertTrue(Replay.replay(journal, ALL, new CommandStore[0], null));
        }
        finally
        {
            journal.stop();
        }
    }
}
