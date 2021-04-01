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
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.*;

public class PaxosUncommittedTrackerTest
{
    private File directory = null;
    private PaxosUncommittedTracker tracker;
    private PaxosMockUpdateSupplier updates;
    private UncommittedKeyFileContainer state;

    @Before
    public void setUp()
    {
        if (directory != null)
            FileUtils.deleteRecursive(directory);

        directory = Files.createTempDir();

        tracker = new PaxosUncommittedTracker(directory);
        updates = new PaxosMockUpdateSupplier();
        tracker.setUpdateSupplier(updates);
        state = tracker.getOrCreateTableState(CFID);
    }

    private static List<UncommittedPaxosKey> uncommittedList(PaxosUncommittedTracker tracker, Collection<Range<Token>> ranges)
    {
        try (CloseableIterator<UncommittedPaxosKey> iterator = tracker.uncommittedKeyIterator(CFID, ranges))
        {
            return Lists.newArrayList(iterator);
        }
    }

    private static List<UncommittedPaxosKey> uncommittedList(PaxosUncommittedTracker tracker, Range<Token> range)
    {
        return uncommittedList(tracker, Collections.singleton(range));
    }

    private static List<UncommittedPaxosKey> uncommittedList(PaxosUncommittedTracker tracker)
    {
        return uncommittedList(tracker, FULL_RANGE);
    }

    @Test
    public void inmemory() throws Exception
    {
        Assert.assertNull(state.getCurrentFile());
        int size = 5;
        List<PaxosKeyState> expected = new ArrayList<>(size);
        int key = 0;
        for (UUID ballot : createBallots(size))
        {
            DecoratedKey dk = dk(key++);
            updates.inProgress(CFID, dk, ballot);
            expected.add(new PaxosKeyState(CFID, dk, ballot, false));
        }

        Assert.assertEquals(expected, uncommittedList(tracker));
        Assert.assertNull(state.getCurrentFile());
    }

    @Test
    @Ignore // until all of 3.0.19.61-hotfix has been merged in to 3.0.24
    public void onDisk() throws Exception
    {
        Assert.assertNull(state.getCurrentFile());
        int size = 5;
        List<PaxosKeyState> expected = new ArrayList<>(size);
        int key = 0;
        for (UUID ballot : createBallots(size))
        {
            DecoratedKey dk = dk(key++);
            updates.inProgress(CFID, dk, ballot);
            expected.add(new PaxosKeyState(CFID, dk, ballot, false));
        }
        tracker.flushUpdates();

        Assert.assertEquals(expected, uncommittedList(tracker));

        Assert.assertNotNull(state.getCurrentFile());
        Assert.assertEquals(expected, kl(state.getCurrentFile().iterator(Collections.singleton(FULL_RANGE))));
    }

    @Test
    @Ignore // until all of 3.0.19.61-hotfix has been merged in to 3.0.24
    public void mixed() throws Exception
    {
        Assert.assertNull(state.getCurrentFile());
        int size = 10;
        PaxosKeyState[] expectedArr = new PaxosKeyState[size];
        List<PaxosKeyState> inMemory = new ArrayList<>(size / 2);
        List<PaxosKeyState> onDisk = new ArrayList<>(size / 2);
        UUID[] ballots = createBallots(size);

        for (int i=0; i<size; i+=2)
        {
            UUID ballot = ballots[i];
            DecoratedKey dk = dk(i);
            updates.inProgress(CFID, dk, ballot);
            PaxosKeyState ballotState = new PaxosKeyState(CFID, dk, ballot, false);;
            onDisk.add(ballotState);
            expectedArr[i] = ballotState;
        }

        tracker.flushUpdates();

        for (int i=1; i<size; i+=2)
        {
            UUID ballot = ballots[i];
            DecoratedKey dk = dk(i);
            updates.inProgress(CFID, dk, ballot);
            PaxosKeyState ballotState = new PaxosKeyState(CFID, dk, ballot, false);;
            inMemory.add(ballotState);
            expectedArr[i] = ballotState;
        }

        List<PaxosKeyState> expected = kl(expectedArr);

        Assert.assertEquals(expected, uncommittedList(tracker));

        Assert.assertNotNull(state.getCurrentFile());
        Assert.assertEquals(onDisk, kl(state.getCurrentFile().iterator(Collections.singleton(FULL_RANGE))));
    }

    @Test
    public void committed() throws Exception
    {
        UncommittedKeyFileContainer state = new UncommittedKeyFileContainer(CFID, directory, null);
        Assert.assertNull(state.getCurrentFile());
        UUID ballot = createBallots(1)[0];

        DecoratedKey dk = dk(1);
        updates.inProgress(CFID, dk, ballot);

        Assert.assertEquals(kl(new PaxosKeyState(CFID, dk, ballot, false)), uncommittedList(tracker));

        updates.committed(CFID, dk, ballot);
        Assert.assertTrue(uncommittedList(tracker).isEmpty());
    }

    /**
     * Test that commits don't resolve in progress transactions with more recent ballots
     */
    @Test
    public void pastCommit()
    {
        UUID[] ballots = createBallots(2);
        DecoratedKey dk = dk(1);
        Assert.assertTrue(ballots[1].timestamp() > ballots[0].timestamp());

        updates.inProgress(CFID, dk, ballots[1]);
        updates.committed(CFID, dk, ballots[0]);

        Assert.assertEquals(kl(new PaxosKeyState(CFID, dk, ballots[1], false)), uncommittedList(tracker));
    }

    @Test
    public void tokenRange() throws Exception
    {
        Assert.assertNull(state.getCurrentFile());
        int size = 10;
        PaxosKeyState[] expectedArr = new PaxosKeyState[size];
        UUID[] ballots = createBallots(size);

        for (int i=0; i<size; i+=2)
        {
            UUID ballot = ballots[i];
            DecoratedKey dk = dk(i);
            updates.inProgress(CFID, dk, ballot);
            PaxosKeyState ballotState = new PaxosKeyState(CFID, dk, ballot, false);;
            expectedArr[i] = ballotState;
        }

        tracker.flushUpdates();

        for (int i=1; i<size; i+=2)
        {
            UUID ballot = ballots[i];
            DecoratedKey dk = dk(i);
            updates.inProgress(CFID, dk, ballot);
            PaxosKeyState ballotState = new PaxosKeyState(CFID, dk, ballot, false);;
            expectedArr[i] = ballotState;
        }

        List<PaxosKeyState> expected = kl(expectedArr);

        Assert.assertEquals(expected.subList(0, 5), uncommittedList(tracker, r(null, tk(4))));
        Assert.assertEquals(expected.subList(3, 7), uncommittedList(tracker, r(2, 6)));
        Assert.assertEquals(expected.subList(8, 10), uncommittedList(tracker, r(tk(7), null)));
        Assert.assertEquals(Lists.newArrayList(Iterables.concat(expected.subList(1, 5), expected.subList(6, 9))),
                                             uncommittedList(tracker, Lists.newArrayList(r(0, 4), r(5, 8))));
    }
}
