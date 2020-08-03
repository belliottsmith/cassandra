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
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.*;

public class UncommittedKeyFileTest
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

    private UncommittedKeyFile mergeWithFile(UncommittedKeyFile file, List<PaxosKeyState> toAdd) throws IOException
    {
        UncommittedKeyFile.MergeWriter writer = new UncommittedKeyFile.MergeWriter(CFID, directory, file);
        for (PaxosKeyState commit : toAdd)
            writer.mergeAndAppend(commit);
        return writer.finish();
    }

    private static void assertIteratorContents(Iterable<PaxosKeyState> expected, CloseableIterator<PaxosKeyState> iterator)
    {
        try (CloseableIterator<PaxosKeyState> iter = iterator)
        {
            Assert.assertEquals(Lists.newArrayList(expected), Lists.newArrayList(iter));
        }
    }

    private static void assertFileContents(UncommittedKeyFile file, int generation, List<PaxosKeyState> states)
    {
        Assert.assertEquals(generation, file.generation);
        assertIteratorContents(states, file.iterator(ALL_RANGES));
    }

    /**
     * Test various merge scenarios
     */
    @Test
    public void testMergeWriter() throws IOException
    {
        UUID[] ballots = createBallots(5);

        UncommittedKeyFile file1 = mergeWithFile(null, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                                          new PaxosKeyState(CFID, dk(5), ballots[1], false),
                                                          new PaxosKeyState(CFID, dk(7), ballots[1], false),
                                                          new PaxosKeyState(CFID, dk(9), ballots[1], false)));

        assertFileContents(file1, 0, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                        new PaxosKeyState(CFID, dk(5), ballots[1], false),
                                        new PaxosKeyState(CFID, dk(7), ballots[1], false),
                                        new PaxosKeyState(CFID, dk(9), ballots[1], false)));

        // add a commit from the past for key 3, update key 5, and commit key 7
        UncommittedKeyFile file2 = mergeWithFile(file1, kl(new PaxosKeyState(CFID, dk(3), ballots[0], true),
                                                           new PaxosKeyState(CFID, dk(5), ballots[2], false),
                                                           new PaxosKeyState(CFID, dk(7), ballots[2], true)));

        // key 7 should be gone because committed keys aren't written out
        assertFileContents(file2, 1, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                        new PaxosKeyState(CFID, dk(5), ballots[2], false),
                                        new PaxosKeyState(CFID, dk(9), ballots[1], false)));

        // add a new key and update an adjacent one
        UncommittedKeyFile file3 = mergeWithFile(file2, kl(new PaxosKeyState(CFID, dk(4), ballots[3], false),
                                                           new PaxosKeyState(CFID, dk(5), ballots[3], false)));
        assertFileContents(file3, 2, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                        new PaxosKeyState(CFID, dk(4), ballots[3], false),
                                        new PaxosKeyState(CFID, dk(5), ballots[3], false),
                                        new PaxosKeyState(CFID, dk(9), ballots[1], false)));

        // add 2 new keys
        UncommittedKeyFile file4 = mergeWithFile(file3, kl(new PaxosKeyState(CFID, dk(6), ballots[4], false),
                                                           new PaxosKeyState(CFID, dk(7), ballots[4], false)));
        assertFileContents(file4, 3, kl(new PaxosKeyState(CFID, dk(3), ballots[1], false),
                                        new PaxosKeyState(CFID, dk(4), ballots[3], false),
                                        new PaxosKeyState(CFID, dk(5), ballots[3], false),
                                        new PaxosKeyState(CFID, dk(6), ballots[4], false),
                                        new PaxosKeyState(CFID, dk(7), ballots[4], false),
                                        new PaxosKeyState(CFID, dk(9), ballots[1], false)));
    }

    @Test
    public void committedOpsArentWritten() throws Exception
    {
        UUID[] ballots = createBallots(2);

        UncommittedKeyFile file1 = mergeWithFile(null, kl(new PaxosKeyState(CFID, dk(1), ballots[0], false)));
        assertFileContents(file1, 0, kl(new PaxosKeyState(CFID, dk(1), ballots[0], false)));

        UncommittedKeyFile file2 = mergeWithFile(file1, kl(new PaxosKeyState(CFID, dk(1), ballots[1], true)));
        assertFileContents(file2, 1, kl());
    }

    @Test
    public void noopFlushesDontCreateNewFile() throws Exception
    {
        UUID[] ballots = createBallots(2);

        UncommittedKeyFile file1 = mergeWithFile(null, kl(new PaxosKeyState(CFID, dk(1), ballots[0], false)));
        assertFileContents(file1, 0, kl(new PaxosKeyState(CFID, dk(1), ballots[0], false)));

        UncommittedKeyFile file2 = mergeWithFile(file1, kl());
        Assert.assertSame(file1, file2);
        assertFileContents(file2, 0, kl(new PaxosKeyState(CFID, dk(1), ballots[0], false)));
    }

    /**
     * TableFile.cleanupAndLoadMostRecent should remove all paxos files except the most recent
     */
    @Test
    public void testCleanup() throws Exception
    {
        UUID[] ballots = createBallots(5);

        List<File> expectedDeletions = new ArrayList<>();
        UncommittedKeyFile lastFile = null;
        for (int i=0; i<ballots.length; i++)
        {
            if (lastFile != null)
                expectedDeletions.add(lastFile.getFile());

            lastFile = mergeWithFile(lastFile, kl(new PaxosKeyState(CFID, dk(i), ballots[i], false)));
        }

        // nothing should have been deleted yet
        for (File file : expectedDeletions)
            Assert.assertTrue(file.exists());

        UncommittedKeyFile loadedFile = UncommittedKeyFile.cleanupAndLoadMostRecent(directory, CFID);

        for (File file : expectedDeletions)
            Assert.assertFalse(file.exists());

        Assert.assertTrue(lastFile.getFile().exists());
        Assert.assertEquals(lastFile.getFile(), loadedFile.getFile());
    }

    @Test
    public void testIterator() throws Exception
    {
        UUID[] ballots = createBallots(10);
        List<PaxosKeyState> expected = new ArrayList<>(ballots.length);

        for (int i=0; i<ballots.length; i++)
        {
            UUID ballot = ballots[i];
            DecoratedKey dk = dk(i);
            expected.add(new PaxosKeyState(CFID, dk, ballot, false));
        }

        UncommittedKeyFile file = mergeWithFile(null, expected);

        assertIteratorContents(expected.subList(0, 5), file.iterator(Collections.singleton(r(null, tk(4)))));
        assertIteratorContents(expected.subList(3, 7), file.iterator(Collections.singleton(r(2, 6))));
        assertIteratorContents(expected.subList(8, 10), file.iterator(Collections.singleton(r(tk(7), null))));
        assertIteratorContents(Iterables.concat(expected.subList(1, 5), expected.subList(6, 9)),
                               file.iterator(Lists.newArrayList(r(0, 4), r(5, 8))));


    }

    /**
     * backing file shouldn't be deleted until all of the readers have closed, but
     * it should stop returning iterators as soon as it's been marked for delete
     */
    @Test
    public void testDeletion() throws Exception
    {
        UncommittedKeyFile file = mergeWithFile(null, kl(new PaxosKeyState(CFID, dk(1), createBallots(1)[0], false)));
        File backingFile = file.getFile();
        Assert.assertTrue(backingFile.exists());

        CloseableIterator<PaxosKeyState> iter1 = file.iterator(ALL_RANGES);
        Assert.assertNotNull(iter1);
        CloseableIterator<PaxosKeyState> iter2 = file.iterator(ALL_RANGES);
        Assert.assertNotNull(iter2);

        file.markDeleted();

        // iterators should stop being returned as soon as a file is marked for delete
        Assert.assertNull(file.iterator(ALL_RANGES));

        // but it shouldn't be deleted until open iterators have been closed
        Assert.assertTrue(backingFile.exists());

        iter1.close();
        Assert.assertTrue(backingFile.exists());

        iter2.close();
        Assert.assertFalse(backingFile.exists());
    }
}
