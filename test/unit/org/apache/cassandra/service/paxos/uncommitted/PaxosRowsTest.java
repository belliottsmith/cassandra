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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.*;


public class PaxosRowsTest
{
    protected static String ks;
    protected static final String tbl = "tbl";
    protected static CFMetaData cfm;
    protected static UUID cfId;

    static Row paxosRowFor(DecoratedKey key)
    {
        return PaxosUncommittedTests.paxosRowFor(cfm.cfId, key);
    }

    private static Commit commitFor(UUID ballot, DecoratedKey key)
    {
        return PaxosUncommittedTests.commitFor(cfm, ballot, key);
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SystemKeyspace.finishStartup();

        ks = "coordinatorsessiontest";
        cfm = CFMetaData.compile("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", ks);
        cfId = cfm.cfId;
    }

    @Before
    public void setUp() throws Exception
    {
        PAXOS_CFS.truncateBlocking();
    }

    @Test
    public void testRowInterpretation()
    {
        DecoratedKey key = dk(5);
        UUID[] ballots = createBallots(3);

        SystemKeyspace.savePaxosPromise(key, cfm, ballots[0]);
        Assert.assertNull(PaxosRows.getCommitState(key, paxosRowFor(key), null));

        SystemKeyspace.savePaxosProposal(commitFor(ballots[1], key));
        Assert.assertEquals(new PaxosKeyState(cfId, key, ballots[1], false), PaxosRows.getCommitState(key, paxosRowFor(key), null));
        Assert.assertEquals(new PaxosKeyState(cfId, key, ballots[1], false), PaxosRows.getCommitState(key, paxosRowFor(key), null));

        // test cfid filter mismatch
        Assert.assertNull(PaxosRows.getCommitState(key, paxosRowFor(key), UUID.randomUUID()));

        SystemKeyspace.savePaxosCommit(commitFor(ballots[2], key));
        Assert.assertEquals(new PaxosKeyState(cfId, key, ballots[2], true), PaxosRows.getCommitState(key, paxosRowFor(key), null));
    }

    @Test
    public void testIterator()
    {
        UUID[] ballots = createBallots(10);
        List<PaxosKeyState> expected = new ArrayList<>(ballots.length);
        for (int i=0; i<ballots.length; i++)
        {
            UUID ballot = ballots[i];
            DecoratedKey key = dk(i);

            if (i%2 == 0)
            {
                SystemKeyspace.savePaxosProposal(commitFor(ballot, key));
                expected.add(new PaxosKeyState(cfId, key, ballot, false));
            }
            else
            {
                SystemKeyspace.savePaxosCommit(commitFor(ballot, key));
                expected.add(new PaxosKeyState(cfId, key, ballot, true));
            }
        }

        PartitionRangeReadCommand command = PartitionRangeReadCommand.allDataRead(PAXOS_CFM, FBUtilities.nowInSeconds());
        try (ReadOrderGroup opGroup = command.startOrderGroup();
             UnfilteredPartitionIterator partitions = command.executeLocally(opGroup);
             CloseableIterator<PaxosKeyState> iterator = PaxosRows.toIterator(partitions, cfm.cfId, true))
        {
            Assert.assertEquals(expected, Lists.newArrayList(iterator));
        }
    }
}
