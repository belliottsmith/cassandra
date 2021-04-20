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

package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupRequest;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupResponse;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupSession;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_CLEANUP_START_PREPARE;
import static org.apache.cassandra.net.MessagingService.Verb.APPLE_PAXOS_CLEANUP_REQUEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepairJobTest extends SchemaLoader
{
    private static final long TEST_TIMEOUT_S = 10;
    private static final long THREAD_TIMEOUT_MILLIS = 100;
    private static final IPartitioner PARTITIONER = ByteOrderedPartitioner.instance;
    private static final IPartitioner MURMUR3_PARTITIONER = Murmur3Partitioner.instance;
    private static final String KEYSPACE = "RepairJobTest";
    private static final String CF = "Standard1";
    private static final Object messageLock = new Object();

    private static final List<Range<Token>> fullRange = Collections.singletonList(new Range<>(MURMUR3_PARTITIONER.getMinimumToken(),
                                                                                              MURMUR3_PARTITIONER.getRandomToken()));

    private static final Range<Token> RANGE_1 = range(0, 1);
    private static final Range<Token> RANGE_2 = range(2, 3);
    private static final Range<Token> RANGE_3 = range(4, 5);
    private static final RepairJobDesc JOB_DESC = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), KEYSPACE, CF, Collections.emptyList());
    private static InetAddress addr1;
    private static InetAddress addr2;
    private static InetAddress addr3;
    private static InetAddress addr4;
    private RepairSession session;
    private RepairJob job;
    private RepairJobDesc sessionJobDesc;

    // So that threads actually get recycled and we can have accurate memory accounting while testing
    // memory retention from CASSANDRA-14096
    private static class MeasureableRepairSession extends RepairSession
    {
        public MeasureableRepairSession(UUID parentRepairSession, UUID id, Collection<Range<Token>> ranges,
                                        String keyspace, RepairParallelism parallelismDegree, boolean allReplicas,
                                        Set<InetAddress> endpoints, boolean isIncremental, boolean pullRepair,
                                        boolean force, PreviewKind previewKind, boolean repairPaxos, boolean paxosOnly, boolean optimiseStreams,
                                        String... cfnames)
        {
            super(parentRepairSession, id, ranges, keyspace, parallelismDegree, allReplicas,
                  endpoints, isIncremental, pullRepair, force, previewKind, repairPaxos, paxosOnly, optimiseStreams, cfnames);
        }

        protected DebuggableThreadPoolExecutor createExecutor()
        {
            DebuggableThreadPoolExecutor executor = super.createExecutor();
            executor.setKeepAliveTime(THREAD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return executor;
        }
    }

    @BeforeClass
    public static void setupClass() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
        addr1 = InetAddress.getByName("127.0.0.1");
        addr2 = InetAddress.getByName("127.0.0.2");
        addr3 = InetAddress.getByName("127.0.0.3");
        addr4 = InetAddress.getByName("127.0.0.4");
    }

    @Before
    public void setup()
    {
        Set<InetAddress> neighbors = new HashSet<>(Arrays.asList(addr2, addr3));

        UUID parentRepairSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepairSession, FBUtilities.getBroadcastAddress(),
                                                                 Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF)), fullRange, false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE, false, PreviewKind.NONE);

        this.session = new MeasureableRepairSession(parentRepairSession, UUIDGen.getTimeUUID(), fullRange,
                                                    KEYSPACE, RepairParallelism.SEQUENTIAL, true, neighbors,
                                                    false, false, false, PreviewKind.NONE, true, false, false, CF);

        this.job = new RepairJob(session, CF, false, PreviewKind.NONE, false);
        this.sessionJobDesc = new RepairJobDesc(session.parentRepairSession, session.getId(),
                                                session.keyspace, CF, session.getRanges());

        DatabaseDescriptor.setBroadcastAddress(addr1);
    }

    @After
    public void reset()
    {
        ActiveRepairService.instance.terminateSessions();
        MessagingService.instance().clearMessageSinks();
    }

    /**
     * Ensure we can do an end to end repair of consistent data and get the messages we expect
     */
    @Test
    public void testEndToEndNoDifferences() throws Exception
    {
        Map<InetAddress, MerkleTrees> mockTrees = new HashMap<>();
        mockTrees.put(FBUtilities.getBroadcastAddress(), createInitialTree(false));
        mockTrees.put(addr2, createInitialTree(false));
        mockTrees.put(addr3, createInitialTree(false));

        List<MessageOut> observedMessages = new ArrayList<>();
        interceptRepairMessages(mockTrees, observedMessages);

        job.run();

        Thread.sleep(1000);
        RepairResult result = job.get(TEST_TIMEOUT_S, TimeUnit.SECONDS);

        assertEquals(3, result.stats.size());

        // RepairJob should send out SNAPSHOTS -> VALIDATIONS -> done
        List<RepairMessage.Type> expectedTypes = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            expectedTypes.add(RepairMessage.Type.SNAPSHOT);
        for (int i = 0; i < 3; i++)
            expectedTypes.add(RepairMessage.Type.VALIDATION_REQUEST);

        assertEquals(expectedTypes, observedMessages.stream()
                                                    .map(k -> ((RepairMessage) k.payload).messageType)
                                                    .collect(Collectors.toList()));
    }

    /**
     * Regression test for CASSANDRA-14096. We should not retain memory in the RepairSession once the
     * ValidationTask -> SyncTask transform is done.
     */
    @Test
    public void testNoTreesRetainedAfterDifference() throws Throwable
    {
        Map<InetAddress, MerkleTrees> mockTrees = new HashMap<>();
        mockTrees.put(FBUtilities.getBroadcastAddress(), createInitialTree(false));
        mockTrees.put(addr2, createInitialTree(true));
        mockTrees.put(addr3, createInitialTree(false));

        List<MessageOut> observedMessages = new ArrayList<>();
        interceptRepairMessages(mockTrees, observedMessages);

        List<TreeResponse> mockTreeResponses = mockTrees.entrySet().stream()
                                                        .map(e -> new TreeResponse(e.getKey(), e.getValue()))
                                                        .collect(Collectors.toList());

        long singleTreeSize = ObjectSizes.measureDeep(mockTrees.get(addr2));

        // Use a different local address so we get all RemoteSyncs (as LocalSyncs try to reach out over the network).
        List<SyncTask> syncTasks =  RepairJob.createStandardSyncTasks(sessionJobDesc, mockTreeResponses,
                                                                      addr4, // local
                                                                      session.isIncremental,
                                                                      session.pullRepair,
                                                                      session.previewKind);

        // SyncTasks themselves should not contain significant memory
        assertTrue(ObjectSizes.measureDeep(syncTasks) < 0.8 * singleTreeSize);

        ListenableFuture<List<SyncStat>> syncResults = job.executeTasks(syncTasks);

        // The session can retain memory in the contained executor until the threads expire, so we wait for the threads
        // that ran the Tree -> SyncTask conversions to die and release the memory
        int millisUntilFreed;
        for (millisUntilFreed = 0; millisUntilFreed < TEST_TIMEOUT_S * 1000; millisUntilFreed += THREAD_TIMEOUT_MILLIS)
        {
            // The measured size of the syncingTasks, and result of the computation should be much smaller
            if (ObjectSizes.measureDeep(session) < 0.8 * singleTreeSize)
                break;
            TimeUnit.MILLISECONDS.sleep(THREAD_TIMEOUT_MILLIS);
        }

        assertTrue(millisUntilFreed < TEST_TIMEOUT_S * 1000);

        List<SyncStat> results = syncResults.get(TEST_TIMEOUT_S, TimeUnit.SECONDS);

        assertTrue(ObjectSizes.measureDeep(results) < 0.8 * singleTreeSize);

        assertEquals(3, results.size());
        // Should be two RemoteSyncTasks with ranges and one empty one
        assertExpectedDifferences(new ArrayList<>(session.getSyncingTasks().values()), 1, 1, 0);

        int numDifferent = 0;
        for (SyncStat stat : results)
        {
            if (stat.nodes.endpoint1.equals(addr2) || stat.nodes.endpoint2.equals(addr2))
            {
                assertEquals(1, stat.numberOfDifferences);
                numDifferent++;
            }
        }
        assertEquals(2, numDifferent);
    }

    private void assertExpectedDifferences(Collection<CompletableRemoteSyncTask> tasks, Integer ... differences)
    {
        List<Integer> expectedDifferences = new ArrayList<>(Arrays.asList(differences));
        List<Integer> observedDifferences = tasks.stream()
                                                 .map(t -> (int) ((SyncTask)t).getCurrentStat().numberOfDifferences)
                                                 .collect(Collectors.toList());
        assertEquals(expectedDifferences.size(), observedDifferences.size());
        assertTrue(expectedDifferences.containsAll(observedDifferences));
    }

    private MerkleTrees createInitialTree(boolean invalidate)
    {
        MerkleTrees tree = new MerkleTrees(MURMUR3_PARTITIONER);
        tree.addMerkleTrees((int) Math.pow(2, 15), fullRange);
        tree.init();

        if (invalidate)
        {
            // change a range in one of the trees
            Token token = MURMUR3_PARTITIONER.midpoint(fullRange.get(0).left, fullRange.get(0).right);
            tree.invalidate(token);
            tree.get(token).hash("non-empty hash!".getBytes());
        }

        return tree;
    }

    private void interceptRepairMessages(Map<InetAddress, MerkleTrees> mockTrees,
                                         List<MessageOut> messageCapture)
    {
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                if (message.verb == APPLE_PAXOS_CLEANUP_START_PREPARE)
                {
                    MessageIn<?> messageIn = MessageIn.create(to, Paxos.newBallot(null, ConsistencyLevel.SERIAL),
                                                              Collections.emptyMap(),
                                                              MessagingService.Verb.REQUEST_RESPONSE,
                                                              MessagingService.current_version);
                    MessagingService.instance().receive(messageIn, id, System.currentTimeMillis(), false);
                    return false;
                }

                if (message.verb == APPLE_PAXOS_CLEANUP_REQUEST)
                {
                    PaxosCleanupRequest request = (PaxosCleanupRequest) message.payload;
                    PaxosCleanupSession.finishSession(to, new PaxosCleanupResponse(request.session, true, null));
                    return false;
                }

                if (message == null || !(message.payload instanceof RepairMessage))
                    return false;

                // So different Thread's messages don't overwrite each other.
                synchronized (messageLock)
                {
                    messageCapture.add(message);
                }

                RepairMessage rm = (RepairMessage) message.payload;
                switch (rm.messageType)
                {
                    case SNAPSHOT:
                        MessageIn<?> messageIn = MessageIn.create(to, null,
                                                                  Collections.emptyMap(),
                                                                  MessagingService.Verb.REQUEST_RESPONSE,
                                                                  MessagingService.current_version);
                        MessagingService.instance().receive(messageIn, id, System.currentTimeMillis(), false);
                        break;
                    case VALIDATION_REQUEST:
                        session.validationComplete(sessionJobDesc, to, mockTrees.get(to));
                        break;
                    case SYNC_REQUEST:
                        SyncRequest syncRequest = (SyncRequest) rm;
                        SessionSummary summary = new SessionSummary(syncRequest.src, syncRequest.dst, Collections.emptyList(), Collections.emptyList());
                        session.syncComplete(sessionJobDesc, new NodePair(syncRequest.src, syncRequest.dst), true, Collections.singletonList(summary));
                        break;
                    default:
                        break;
                }
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return message.verb == MessagingService.Verb.REQUEST_RESPONSE;
            }
        });
    }

    @Test
    public void testCreateStandardSyncTasks()
    {
        testCreateStandardSyncTasks(false);
    }

    @Test
    public void testCreateStandardSyncTasksPullRepair()
    {
        testCreateStandardSyncTasks(true);
    }

    public static void testCreateStandardSyncTasks(boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"),
                                                         treeResponse(addr2, RANGE_1, "different", RANGE_2, "same", RANGE_3, "different"),
                                                         treeResponse(addr3, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"));

        Map<Pair<InetAddress, InetAddress>, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));
        assertEquals(3, tasks.size());

        SyncTask task = tasks.get(pair(addr1, addr2));
        assertTrue(task.isLocal());
        assertTrue(((LocalSyncTask)task).requestRanges);
        assertEquals(!pullRepair, ((LocalSyncTask)task).transferRanges);
        assertEqualsNoOrder(task.rangesToSync, Arrays.asList(RANGE_1, RANGE_3));

        task = tasks.get(pair(addr2, addr3));
        assertTrue(task instanceof RemoteSyncTask);
        assertFalse(task.isLocal());
        assertEqualsNoOrder(task.rangesToSync, Arrays.asList(RANGE_1, RANGE_3));

        assertTrue(tasks.get(pair(addr1, addr3)).rangesToSync.isEmpty());
    }

    @Test
    public void testOptimisedCreateStandardSyncTasksAllDifferent()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one", RANGE_3, "one"),
                                                         treeResponse(addr2, RANGE_1, "two", RANGE_2, "two", RANGE_3, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "three", RANGE_3, "three"));

        Map<Pair<InetAddress, InetAddress>, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(JOB_DESC,
                                                                                            treeResponses,
                                                                                            addr1, // local
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        for ( Pair<InetAddress, InetAddress> pair : Arrays.asList(pair(addr1, addr2),
                                                                  pair(addr1, addr3),
                                                                  pair(addr2, addr1),
                                                                  pair(addr2, addr3),
                                                                  pair(addr3, addr1),
                                                                  pair(addr3, addr2)))
        {
            assertEqualsNoOrder(tasks.get(pair).rangesToSync, Arrays.asList(RANGE_1, RANGE_2, RANGE_3));
        }
    }

    @Test
    public void testCreateStandardSyncTasksAllDifferent()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one", RANGE_3, "one"),
                                                         treeResponse(addr2, RANGE_1, "two", RANGE_2, "two", RANGE_3, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "three", RANGE_3, "three"));

        Map<Pair<InetAddress, InetAddress>, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    false,
                                                                                    true,
                                                                                    PreviewKind.ALL));

        assertEquals(3, tasks.size());
        assertTrue(tasks.get(pair(addr1, addr2)).isLocal());
        assertEqualsNoOrder(tasks.get(pair(addr1, addr2)).rangesToSync, Arrays.asList(RANGE_1, RANGE_2, RANGE_3));

        assertFalse(tasks.get(pair(addr2, addr3)).isLocal());
        assertEqualsNoOrder(tasks.get(pair(addr2, addr3)).rangesToSync, Arrays.asList(RANGE_1, RANGE_2, RANGE_3));

        assertTrue(tasks.get(pair(addr1, addr3)).isLocal());
        assertEqualsNoOrder(tasks.get(pair(addr1, addr3)).rangesToSync, Arrays.asList(RANGE_1, RANGE_2, RANGE_3));
    }


    @Test
    public void testOptimisedCreateStandardSyncTasks()
    {
        /*
        addr1 will stream range1 from addr3
                          range2 from addr2 or addr3
        addr2 will stream range1 from addr3
                          range2 from addr1
        addr3 will stream range1 from addr1 or addr2
                          range2 from addr1
         */

        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one"),
                                                         treeResponse(addr2, RANGE_1, "one", RANGE_2, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "two"));

        Map<Pair<InetAddress, InetAddress>, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(JOB_DESC,
                                                                                            treeResponses,
                                                                                            addr4, // local
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        assertTrue(tasks.values().stream().allMatch(t -> t instanceof AsymmetricRemoteSyncTask));

        // addr1 streams range1 from addr3:
        assertTrue(tasks.get(pair(addr1, addr3)).rangesToSync.contains(RANGE_1));
        // addr1 can get range2 from either addr2 or addr3 but not from both
        assertStreamRangeFromEither(tasks, RANGE_2, addr1, addr2, addr3);

        // addr2 streams range1 from addr3
        assertTrue(tasks.get(pair(addr2, addr3)).rangesToSync.contains(RANGE_1));
        // addr2 streams range2 from addr1
        assertTrue(tasks.get(pair(addr2, addr1)).rangesToSync.contains(RANGE_2));
        // addr3 can get range1 from either addr1 or addr2 but not from both
        assertStreamRangeFromEither(tasks, RANGE_1, addr3, addr2, addr1);
        // addr3 streams range2 from addr1
        assertTrue(tasks.get(pair(addr3, addr1)).rangesToSync.contains(RANGE_2));
    }

    private static TreeResponse treeResponse(InetAddress addr, Object... rangesAndHashes)
    {
        MerkleTrees trees = new MerkleTrees(PARTITIONER);
        for (int i = 0; i < rangesAndHashes.length; i += 2)
        {
            Range<Token> range = (Range<Token>) rangesAndHashes[i];
            String hash = (String) rangesAndHashes[i + 1];
            MerkleTree tree = trees.addMerkleTree(2, MerkleTree.RECOMMENDED_DEPTH, range);
            tree.get(range.left).hash(hash.getBytes());
        }

        return new TreeResponse(addr, trees);
    }

    private static Token tk(int i)
    {
        return PARTITIONER.getToken(ByteBufferUtil.bytes(i));
    }

    private static Range<Token> range(int from, int to)
    {
        return new Range<>(tk(from), tk(to));
    }

    public static Map< Pair<InetAddress, InetAddress>, SyncTask> toMap(List<SyncTask> tasks)
    {
        ImmutableMap.Builder< Pair<InetAddress, InetAddress>, SyncTask> map = ImmutableMap.builder();
        tasks.forEach(t -> map.put(Pair.create(t.firstEndpoint, t.secondEndpoint), t));
        return map.build();
    }
    private static Pair<InetAddress, InetAddress> pair(InetAddress node1, InetAddress node2)
    {
        return Pair.create(node1, node2);
    }

    // Asserts that ranges are streamed from one of the nodes but not from the both
    public static void assertStreamRangeFromEither(Map<Pair<InetAddress, InetAddress>, SyncTask> tasks, Range<Token> range,
                                                   InetAddress target, InetAddress either, InetAddress or)
    {
        SyncTask task1 = tasks.get(pair(target, either));
        SyncTask task2 = tasks.get(pair(target, or));

        boolean foundRange = false;
        if (task1 != null && task1.rangesToSync.contains(range))
        {
            foundRange = true;
            assertDoesntStreamRangeFrom(range, task2);
        }
        else if (task2 != null && task2.rangesToSync.contains(range))
        {
            foundRange = true;
            assertDoesntStreamRangeFrom(range, task1);
        }
        assertTrue(foundRange);
    }

    public static void assertDoesntStreamRangeFrom(Range<Token> range, SyncTask task)
    {
        if (task == null)
            return; // Doesn't stream anything

        assertFalse(task.rangesToSync.contains(range));
    }

    private static void assertEqualsNoOrder(List<?> a, List<?> b)
    {
        assertEquals(a.size(), b.size());
        assertEquals(new HashSet<>(a), new HashSet<>(b));
    }
}
