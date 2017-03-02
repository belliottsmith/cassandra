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
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.consistent.CoordinatorSessions;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ActiveRepairService is the starting point for manual "active" repairs.
 *
 * Each user triggered repair will correspond to one or multiple repair session,
 * one for each token range to repair. On repair session might repair multiple
 * column families. For each of those column families, the repair session will
 * request merkle trees for each replica of the range being repaired, diff those
 * trees upon receiving them, schedule the streaming ofthe parts to repair (based on
 * the tree diffs) and wait for all those operation. See RepairSession for more
 * details.
 *
 * The creation of a repair session is done through the submitRepairSession that
 * returns a future on the completion of that session.
 */
public class ActiveRepairService implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener, ActiveRepairServiceMBean
{
    /**
     * @deprecated this statuses are from the previous JMX notification service,
     * which will be deprecated on 4.0. For statuses of the new notification
     * service, see {@link org.apache.cassandra.streaming.StreamEvent.ProgressEvent}
     */
    @Deprecated
    public static enum Status
    {
        STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED
    }

    public class ConsistentSessions
    {
        public final LocalSessions local = new LocalSessions();
        public final CoordinatorSessions coordinated = new CoordinatorSessions();
    }

    public final ConsistentSessions consistent = new ConsistentSessions();

    private boolean registeredForEndpointChanges = false;

    public static CassandraVersion SUPPORTS_GLOBAL_PREPARE_FLAG_VERSION = new CassandraVersion("2.2.1");

    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService(FailureDetector.instance, Gossiper.instance);

    public static final long UNREPAIRED_SSTABLE = 0;
    public static final UUID NO_PENDING_REPAIR = null;

    /**
     * A map of active coordinator session.
     */
    private final ConcurrentMap<UUID, RepairSession> sessions = new ConcurrentHashMap<>();

    private final ConcurrentMap<UUID, ParentRepairSession> parentRepairSessions = new ConcurrentHashMap<>();

    private final IFailureDetector failureDetector;
    private final Gossiper gossiper;

    public ActiveRepairService(IFailureDetector failureDetector, Gossiper gossiper)
    {
        this.failureDetector = failureDetector;
        this.gossiper = gossiper;

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void start()
    {
        consistent.local.start();
        ScheduledExecutors.optionalTasks.scheduleAtFixedRate(consistent.local::cleanup, 0,
                                                             LocalSessions.CLEANUP_INTERVAL,
                                                             TimeUnit.SECONDS);
    }

    @Override
    public List<Map<String, String>> getSessions(boolean all)
    {
        return consistent.local.sessionInfo(all);
    }

    @Override
    public void failSession(String session, boolean force)
    {
        UUID sessionID = UUID.fromString(session);
        consistent.local.cancelSession(sessionID, force);
    }

    /**
     * Requests repairs for the given keyspace and column families.
     *
     * @return Future for asynchronous call or null if there is no need to repair
     */
    public RepairSession submitRepairSession(UUID parentRepairSession,
                                             Collection<Range<Token>> range,
                                             String keyspace,
                                             RepairParallelism parallelismDegree,
                                             boolean allReplicas,
                                             Set<InetAddress> endpoints,
                                             long repairedAt,
                                             boolean isConsistent,
                                             boolean pullRepair,
                                             boolean force,
                                             ListeningExecutorService executor,
                                             String... cfnames)

    {
        if (endpoints.isEmpty())
            return null;
        if (cfnames.length == 0)
            return null;

        final RepairSession session = new RepairSession(parentRepairSession, UUIDGen.getTimeUUID(), range, keyspace, parallelismDegree, allReplicas, endpoints, repairedAt, isConsistent, pullRepair, force, cfnames);

        sessions.put(session.getId(), session);
        // register listeners
        gossiper.register(session);
        failureDetector.registerFailureDetectionEventListener(session);

        // unregister listeners at completion
        session.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                failureDetector.unregisterFailureDetectionEventListener(session);
                gossiper.unregister(session);
                sessions.remove(session.getId());
            }
        }, MoreExecutors.sameThreadExecutor());
        session.start(executor);
        return session;
    }

    public synchronized void terminateSessions()
    {
        Throwable cause = new IOException("Terminate session is called");
        for (RepairSession session : sessions.values())
        {
            session.forceShutdown(cause);
        }
        parentRepairSessions.clear();
    }

    /**
     * Return all of the neighbors with whom we share the provided range.
     *
     * @param keyspaceName keyspace to repair
     * @param keyspaceLocalRanges local-range for given keyspaceName
     * @param toRepair token to repair
     *
     * @return neighbors with whom we share the provided range
     */
    public static Set<InetAddress> getAllNeighbors(String keyspaceName, Collection<Range<Token>> keyspaceLocalRanges,
                                                Range<Token> toRepair)
    {
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : keyspaceLocalRanges)
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException("Requested range intersects a local range but is not fully contained in one; this would lead to imprecise repair");
            }
        }
        if (rangeSuperSet == null || !replicaSets.containsKey(rangeSuperSet))
            return Collections.emptySet();

        Set<InetAddress> neighbors = new HashSet<>(replicaSets.get(rangeSuperSet));
        neighbors.remove(FBUtilities.getBroadcastAddress());
        return neighbors;
    }

    public static Set<InetAddress> filterNeighbors(Set<InetAddress> neighbors,
                                                   Range<Token> toRepair,
                                                   Collection<String> dataCenters,
                                                   Collection<String> hosts)
    {
        if (dataCenters != null && !dataCenters.isEmpty())
        {
            TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> dcEndpoints = Sets.newHashSet();
            Multimap<String,InetAddress> dcEndpointsMap = topology.getDatacenterEndpoints();
            for (String dc : dataCenters)
            {
                Collection<InetAddress> c = dcEndpointsMap.get(dc);
                if (c != null)
                   dcEndpoints.addAll(c);
            }
            return Sets.intersection(neighbors, dcEndpoints);
        }
        else if (hosts != null && !hosts.isEmpty())
        {
            Set<InetAddress> specifiedHost = new HashSet<>();
            for (final String host : hosts)
            {
                try
                {
                    final InetAddress endpoint = InetAddress.getByName(host.trim());
                    if (endpoint.equals(FBUtilities.getBroadcastAddress()) || neighbors.contains(endpoint))
                        specifiedHost.add(endpoint);
                }
                catch (UnknownHostException e)
                {
                    throw new IllegalArgumentException("Unknown host specified " + host, e);
                }
            }

            if (!specifiedHost.contains(FBUtilities.getBroadcastAddress()))
                throw new IllegalArgumentException("The current host must be part of the repair");

            if (specifiedHost.size() <= 1)
            {
                String msg = "Specified hosts %s do not share range %s needed for repair. Either restrict repair ranges " +
                             "with -st/-et options, or specify one of the neighbors that share this range with " +
                             "this node: %s.";
                throw new IllegalArgumentException(String.format(msg, hosts, toRepair, neighbors));
            }

            specifiedHost.remove(FBUtilities.getBroadcastAddress());
            return specifiedHost;

        }

        return neighbors;
    }

    /**
     * Check that the intended repair doesn't overlap with any other repairs currently in progress
     */
    public synchronized void validateIntendedRepair(String keyspace, RepairOption options)
    {
        String[] columnFamilies = options.getColumnFamilies().toArray(new String[options.getColumnFamilies().size()]);
        Set<UUID> cfids = new HashSet<>();
        try
        {
            for (ColumnFamilyStore cfs : StorageService.instance.getValidColumnFamilies(false, false, keyspace, columnFamilies))
            {
                cfids.add(cfs.metadata.cfId);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        for (ParentRepairSession prs : parentRepairSessions.values())
        {
            if (!Sets.intersection(cfids, prs.getCfIds()).isEmpty())
            {
                for (Range<Token> ir : options.getRanges())
                {
                    for (Range<Token> pr : prs.getRanges())
                    {
                        if (ir.intersects(pr))
                        {
                            throw new IllegalArgumentException(String.format("Intended repair job {} overlaps with existing repair session {}", options, prs));
                        }
                    }
                }

            }
        }
    }

    public void prepareForRepair(UUID parentRepairSession, InetAddress coordinator, Set<InetAddress> endpoints, RepairOption options, List<ColumnFamilyStore> columnFamilyStores)
    {
        // we only want repairedAt for incremental repairs, for non incremental repairs, UNREPAIRED_SSTABLE will preserve repairedAt on streamed sstables
        long repairedAt = options.isIncremental() ? System.currentTimeMillis() : ActiveRepairService.UNREPAIRED_SSTABLE;
        registerParentRepairSession(parentRepairSession, coordinator, columnFamilyStores, options.getRanges(), options.isIncremental(), repairedAt, options.isGlobal());
        final CountDownLatch prepareLatch = new CountDownLatch(endpoints.size());
        final AtomicBoolean status = new AtomicBoolean(true);
        final Set<String> failedNodes = Collections.synchronizedSet(new HashSet<String>());
        IAsyncCallbackWithFailure callback = new IAsyncCallbackWithFailure()
        {
            public void response(MessageIn msg)
            {
                prepareLatch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }

            public void onFailure(InetAddress from)
            {
                status.set(false);
                failedNodes.add(from.getHostAddress());
                prepareLatch.countDown();
            }
        };

        List<UUID> cfIds = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores)
            cfIds.add(cfs.metadata.cfId);

        for (InetAddress neighbour : endpoints)
        {
            if (FailureDetector.instance.isAlive(neighbour))
            {
                PrepareMessage message = new PrepareMessage(parentRepairSession, cfIds, options.getRanges(), options.isIncremental(), repairedAt, options.isGlobal());
                MessageOut<RepairMessage> msg = message.createMessage();
                MessagingService.instance().sendRR(msg, neighbour, callback, DatabaseDescriptor.getRpcTimeout(), true);
            }
            else
            {
                if (!options.isForcedRepair())
                {
                    status.set(false);
                    failedNodes.add(neighbour.getHostAddress());
                }
                prepareLatch.countDown();
            }
        }
        try
        {
            prepareLatch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            removeParentRepairSession(parentRepairSession);
            throw new RuntimeException("Did not get replies from all endpoints. List of failed endpoint(s): " + failedNodes.toString(), e);
        }

        if (!status.get())
        {
            removeParentRepairSession(parentRepairSession);
            throw new RuntimeException("Did not get positive replies from all endpoints. List of failed endpoint(s): " + failedNodes.toString());
        }
    }

    public void registerParentRepairSession(UUID parentRepairSession, InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal)
    {
        assert isIncremental || repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE;
        if (!registeredForEndpointChanges)
        {
            Gossiper.instance.register(this);
            FailureDetector.instance.registerFailureDetectionEventListener(this);
            registeredForEndpointChanges = true;
        }

        parentRepairSessions.put(parentRepairSession, new ParentRepairSession(coordinator, columnFamilyStores, ranges, isIncremental, repairedAt, isGlobal));
    }

    public ParentRepairSession getParentRepairSession(UUID parentSessionId)
    {
        ParentRepairSession session = parentRepairSessions.get(parentSessionId);
        // this can happen if a node thinks that the coordinator was down, but that coordinator got back before noticing
        // that it was down itself.
        if (session == null)
            throw new RuntimeException("Parent repair session with id = " + parentSessionId + " has failed.");

        return session;
    }

    /**
     * called when the repair session is done - either failed or anticompaction has completed
     *
     * clears out any snapshots created by this repair
     *
     * @param parentSessionId
     * @return
     */
    public synchronized ParentRepairSession removeParentRepairSession(UUID parentSessionId)
    {
        for (ColumnFamilyStore cfs : getParentRepairSession(parentSessionId).columnFamilyStores.values())
        {
            if (cfs.snapshotExists(parentSessionId.toString()))
                cfs.clearSnapshot(parentSessionId.toString());
        }
        return parentRepairSessions.remove(parentSessionId);
    }

    public void handleMessage(InetAddress endpoint, RepairMessage message)
    {
        RepairJobDesc desc = message.desc;
        RepairSession session = sessions.get(desc.sessionId);
        if (session == null)
            return;
        switch (message.messageType)
        {
            case VALIDATION_COMPLETE:
                ValidationComplete validation = (ValidationComplete) message;
                session.validationComplete(desc, endpoint, validation.trees);
                break;
            case SYNC_COMPLETE:
                // one of replica is synced.
                SyncComplete sync = (SyncComplete) message;
                session.syncComplete(desc, sync.nodes, sync.success);
                break;
            default:
                break;
        }
    }

    /**
     * We keep a ParentRepairSession around for the duration of the entire repair, for example, on a 256 token vnode rf=3 cluster
     * we would have 768 RepairSession but only one ParentRepairSession. We use the PRS to avoid anticompacting the sstables
     * 768 times, instead we take all repaired ranges at the end of the repair and anticompact once.
     */
    public static class ParentRepairSession
    {
        private final Map<UUID, ColumnFamilyStore> columnFamilyStores = new HashMap<>();
        private final Collection<Range<Token>> ranges;
        public final boolean isIncremental;
        public final boolean isGlobal;
        public final long repairedAt;
        public final InetAddress coordinator;

        public ParentRepairSession(InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal)
        {
            this.coordinator = coordinator;
            for (ColumnFamilyStore cfs : columnFamilyStores)
            {
                this.columnFamilyStores.put(cfs.metadata.cfId, cfs);
            }
            this.ranges = ranges;
            this.repairedAt = repairedAt;
            this.isIncremental = isIncremental;
            this.isGlobal = isGlobal;
        }

        public synchronized void maybeSnapshot(UUID cfId, UUID parentSessionId)
        {
            String snapshotName = parentSessionId.toString();
            if (!columnFamilyStores.get(cfId).snapshotExists(snapshotName))
            {
                columnFamilyStores.get(cfId).snapshot(snapshotName, new Predicate<SSTableReader>()
                {
                    public boolean apply(SSTableReader sstable)
                    {
                        return sstable != null &&
                               (!isIncremental || !sstable.isRepaired()) &&
                               !(sstable.metadata.isIndex()) && // exclude SSTables from 2i
                               new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges);
                    }
                }, true);
            }
        }

        public long getRepairedAt()
        {
            if (isGlobal)
                return repairedAt;
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        }

        public Collection<ColumnFamilyStore> getColumnFamilyStores()
        {
            return ImmutableSet.<ColumnFamilyStore>builder().addAll(columnFamilyStores.values()).build();
        }

        public Set<UUID> getCfIds()
        {
            return ImmutableSet.copyOf(Iterables.transform(getColumnFamilyStores(), cfs -> cfs.metadata.cfId));
        }

        public Collection<Range<Token>> getRanges()
        {
            return ImmutableSet.copyOf(ranges);
        }

        @Override
        public String toString()
        {
            return "ParentRepairSession{" +
                    "columnFamilyStores=" + columnFamilyStores +
                    ", ranges=" + ranges +
                    ", repairedAt=" + repairedAt +
                    '}';
        }
    }

    /*
    If the coordinator node dies we should remove the parent repair session from the other nodes.
    This uses the same notifications as we get in RepairSession
     */
    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    /**
     * Something has happened to a remote node - if that node is a coordinator, we mark the parent repair session id as failed.
     *
     * The fail marker is kept in the map for 24h to make sure that if the coordinator does not agree
     * that the repair failed, we need to fail the entire repair session
     *
     * @param ep  endpoint to be convicted
     * @param phi the value of phi with with ep was convicted
     */
    public void convict(InetAddress ep, double phi)
    {
        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold() || parentRepairSessions.isEmpty())
            return;

        Set<UUID> toRemove = new HashSet<>();

        for (Map.Entry<UUID, ParentRepairSession> repairSessionEntry : parentRepairSessions.entrySet())
        {
            if (repairSessionEntry.getValue().coordinator.equals(ep))
            {
                toRemove.add(repairSessionEntry.getKey());
            }
        }

        if (!toRemove.isEmpty())
        {
            logger.debug("Removing {} in parent repair sessions", toRemove);
            for (UUID id : toRemove)
                removeParentRepairSession(id);
        }
    }

    static class RepairSuccessSerializer implements IVersionedSerializer<RepairSuccess>
    {
        public void serialize(RepairSuccess repairSuccess, DataOutputPlus dos, int version) throws IOException
        {
            dos.writeUTF(repairSuccess.keyspace);
            dos.writeUTF(repairSuccess.columnFamily);
            dos.writeInt(repairSuccess.ranges.size());
            for (Range<Token> range : repairSuccess.ranges)
                Range.tokenSerializer.serialize(range, dos, version);
            dos.writeLong(repairSuccess.succeedAt);
        }

        public RepairSuccess deserialize(DataInputPlus dis, int version) throws IOException
        {
            String keyspace = dis.readUTF();
            String column_family = dis.readUTF();
            int rangeCount = dis.readInt();
            Collection<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.tokenSerializer.deserialize(dis, DatabaseDescriptor.getPartitioner(), version));
            long succeedAt = dis.readLong();
            return new RepairSuccess(keyspace, column_family, ranges, succeedAt);
        }

        public long serializedSize(RepairSuccess sc, int version)
        {
            return TypeSizes.sizeof(sc.keyspace)
                   + TypeSizes.sizeof(sc.columnFamily)
                   + TypeSizes.sizeof(sc.ranges.size())
                   + sc.ranges.stream().collect(Collectors.summingLong(r -> Range.tokenSerializer.serializedSize(r, version)))
                   + TypeSizes.sizeof(sc.succeedAt);
        }
    }

    public static class RepairSuccess
    {
        public static final RepairSuccessSerializer serializer = new RepairSuccessSerializer();

        public final String keyspace;
        public final String columnFamily;
        public final Collection<Range<Token>> ranges;
        public final long succeedAt;

        public RepairSuccess(String keyspace, String columnFamily, Collection<Range<Token>> ranges, long succeedAt)
        {
            this.keyspace = keyspace;
            this.columnFamily = columnFamily;
            this.ranges = ranges;
            this.succeedAt = succeedAt;
        }

        public MessageOut createMessage()
        {
            return new MessageOut<RepairSuccess>(MessagingService.Verb.APPLE_REPAIR_SUCCESS, this, serializer);
        }
    }

        public static class RepairSuccessVerbHandler implements IVerbHandler<RepairSuccess>
        {
            public void doVerb(MessageIn<RepairSuccess> message, int id)
            {
                RepairSuccess success = message.payload;
                logger.info("Received repair success for {}/{}, {} at {}", new Object[]{success.keyspace, success.columnFamily, success.ranges, success.succeedAt});
                ColumnFamilyStore cfs = Keyspace.open(success.keyspace).getColumnFamilyStore(success.columnFamily);
                for (Range<Token> range : success.ranges)
                    cfs.updateLastSuccessfulRepair(range, success.succeedAt);
                MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
            }
        }
}
