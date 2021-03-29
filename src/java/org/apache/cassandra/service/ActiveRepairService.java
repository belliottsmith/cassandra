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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;

import org.apache.cassandra.config.RepairedDataExclusion;
import org.apache.cassandra.config.RepairedDataTrackingExclusions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
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
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanup;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.repair.consistent.CoordinatorSessions;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Pair;
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

    public enum ParentRepairStatus
    {
        IN_PROGRESS, COMPLETED, FAILED
    }

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

    private static final Set<RepairSuccessListener> repairSuccessListeners = new CopyOnWriteArraySet<>();

    private static class RepairCommandExecutorHandle
    {
        private final static ThreadPoolExecutor repairCommandExecutor;
        static
        {
            Config.RepairCommandPoolFullStrategy strategy = DatabaseDescriptor.getRepairCommandPoolFullStrategy();
            int corePoolSize = 1;
            BlockingQueue<Runnable> queue;
            if (strategy == Config.RepairCommandPoolFullStrategy.reject)
            {
                // new threads will be created on demand up to max pool
                // size so we can leave corePoolSize at 1 to start with
                queue = new SynchronousQueue<>();
            }
            else
            {
                // tasks will be queued if at least corePoolSize threads
                // are running, so initialize it to the desired size
                corePoolSize = DatabaseDescriptor.getRepairCommandPoolSize();
                queue = new LinkedBlockingQueue<>();
            }

            repairCommandExecutor = new JMXEnabledThreadPoolExecutor(corePoolSize,
                                                                     DatabaseDescriptor.getRepairCommandPoolSize(),
                                                                     1, TimeUnit.HOURS,
                                                                     queue,
                                                                     new NamedThreadFactory("Repair-Task"),
                                                                     "internal",
                                                                     new ThreadPoolExecutor.AbortPolicy());
            // allow idle core threads to be terminated
            repairCommandExecutor.allowCoreThreadTimeOut(true);
            RepairMetrics.init();
        }
    }

    public static ThreadPoolExecutor repairCommandExecutor()
    {
        return RepairCommandExecutorHandle.repairCommandExecutor;
    }

    private final IFailureDetector failureDetector;
    private final Gossiper gossiper;
    private final Cache<Integer, Pair<ParentRepairStatus, List<String>>> repairStatusByCmd;

    public ActiveRepairService(IFailureDetector failureDetector, Gossiper gossiper)
    {
        this.failureDetector = failureDetector;
        this.gossiper = gossiper;
        this.repairStatusByCmd = CacheBuilder.newBuilder()
                                             .expireAfterWrite(
                                             Long.getLong("cassandra.parent_repair_status_expiry_seconds",
                                                          TimeUnit.SECONDS.convert(1, TimeUnit.DAYS)), TimeUnit.SECONDS).build();

        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    public void start()
    {
        consistent.local.start();
        ScheduledExecutors.optionalTasks.scheduleAtFixedRate(consistent.local::cleanup, 0,
                                                             LocalSessions.CLEANUP_INTERVAL,
                                                             TimeUnit.SECONDS);
    }

    public void stop()
    {
        consistent.local.stop();
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

    public boolean getDebugValidationPreviewEnabled()
    {
        return DatabaseDescriptor.getDebugValidationPreviewEnabled();
    }

    public void setDebugValidationPreviewEnabled(boolean enabled)
    {
        DatabaseDescriptor.setDebugValidationPreviewEnabled(enabled);
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
                                             boolean isIncremental,
                                             boolean pullRepair,
                                             boolean force,
                                             PreviewKind previewKind,
                                             boolean optimiseStreams,
                                             boolean repairPaxos,
                                             boolean paxosOnly,
                                             ListeningExecutorService executor,
                                             String... cfnames)

    {
        if (repairPaxos && previewKind != PreviewKind.NONE)
            throw new IllegalArgumentException("cannot repair paxos in a preview repair");

        if (endpoints.isEmpty())
            return null;

        if (previewKind.isPreview() && DatabaseDescriptor.getRepairedDataTrackingExclusionsEnabled())
            cfnames = filterTablesForRepairedDataTrackingExclusions(DatabaseDescriptor.getRepairedDataTrackingExclusions(),
                                                                    previewKind,
                                                                    parentRepairSession,
                                                                    keyspace,
                                                                    cfnames);

        if (cfnames.length == 0)
            return null;

        final RepairSession session = new RepairSession(parentRepairSession, UUIDGen.getTimeUUID(), range, keyspace, parallelismDegree, allReplicas, endpoints, isIncremental, pullRepair, force, previewKind, repairPaxos, paxosOnly, optimiseStreams, cfnames);

        sessions.put(session.getId(), session);
        // register listeners
        registerOnFdAndGossip(session);

        if (session.previewKind == PreviewKind.REPAIRED)
        {
            LocalSessions.registerListener(session);
            repairSuccessListeners.add(session);
        }

        // remove session at completion
        session.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                sessions.remove(session.getId());
                LocalSessions.unregisterListener(session);
                repairSuccessListeners.remove(session);
            }
        }, MoreExecutors.sameThreadExecutor());
        session.start(executor);
        return session;
    }

    @VisibleForTesting
    public static String[] filterTablesForRepairedDataTrackingExclusions(RepairedDataTrackingExclusions exclusions,
                                                                         PreviewKind previewKind,
                                                                         UUID parentRepairSession,
                                                                         String keyspace,
                                                                         String[] cfnames)
    {
        List<String> filteredTables = new ArrayList<>(cfnames.length);
        for (String table : cfnames)
        {
            if (exclusions.getExclusion(keyspace, table) != RepairedDataExclusion.ALL)
                filteredTables.add(table);
            else
                logger.info("[{}] Skipping preview repair for {}.{} - it is excluded by repaired_data_tracking_exclusions",
                            previewKind.logPrefix(parentRepairSession), keyspace, table);
        }
        if (cfnames.length != filteredTables.size())
            cfnames = filteredTables.toArray(new String[0]);
        return cfnames;
    }

    public boolean getUseOffheapMerkleTrees()
    {
        return DatabaseDescriptor.useOffheapMerkleTrees();
    }

    public void setUseOffheapMerkleTrees(boolean value)
    {
        DatabaseDescriptor.useOffheapMerkleTrees(value);
    }

    private <T extends AbstractFuture &
               IEndpointStateChangeSubscriber &
               IFailureDetectionEventListener> void registerOnFdAndGossip(final T task)
    {
        gossiper.register(task);
        failureDetector.registerFailureDetectionEventListener(task);

        // unregister listeners at completion
        task.addListener(new Runnable()
        {
            /**
             * When repair finished, do clean up
             */
            public void run()
            {
                failureDetector.unregisterFailureDetectionEventListener(task);
                gossiper.unregister(task);
            }
        }, MoreExecutors.sameThreadExecutor());
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

    public void recordRepairStatus(int cmd, ParentRepairStatus parentRepairStatus, List<String> messages)
    {
        repairStatusByCmd.put(cmd, Pair.create(parentRepairStatus, messages));
    }


    @VisibleForTesting
    public Pair<ParentRepairStatus, List<String>> getRepairStatus(Integer cmd)
    {
        return repairStatusByCmd.getIfPresent(cmd);
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
                throw new IllegalArgumentException(String.format("Requested range %s intersects a local range (%s) " +
                                                                 "but is not fully contained in one; this would lead to " +
                                                                 "imprecise repair. keyspace: %s", toRepair.toString(),
                                                                 range.toString(), keyspaceName));
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
     * we only want to set repairedAt for incremental repairs including all replicas for a token range. For non-global
     * incremental repairs, forced incremental repairs, and full repairs, the UNREPAIRED_SSTABLE value will prevent
     * sstables from being promoted to repaired or preserve the repairedAt/pendingRepair values, respectively.
     */
    static long getRepairedAt(RepairOption options, boolean force)
    {
        // we only want to set repairedAt for incremental repairs including all replicas for a token range. For non-global incremental repairs, full repairs, the UNREPAIRED_SSTABLE value will prevent
        // sstables from being promoted to repaired or preserve the repairedAt/pendingRepair values, respectively. For forced repairs, repairedAt time is only set to UNREPAIRED_SSTABLE if we actually
        // end up skipping replicas
        if (options.isIncremental() && options.isGlobal() && ! force)
        {
            return Clock.instance.currentTimeMillis();
        }
        else
        {
            return  ActiveRepairService.UNREPAIRED_SSTABLE;
        }
    }

    public static boolean verifyCompactionsPendingThreshold(UUID parentRepairSession, PreviewKind previewKind)
    {
        // Snapshot values so failure message is consistent with decision
        int pendingCompactions = CompactionManager.instance.getPendingTasks();
        int pendingThreshold = ActiveRepairService.instance.getRepairPendingCompactionRejectThreshold();
        if (pendingCompactions > pendingThreshold)
        {
            logger.error("[{}] Rejecting incoming repair, pending compactions ({}) above threshold ({})",
                          previewKind.logPrefix(parentRepairSession), pendingCompactions, pendingThreshold);
            return false;
        }
        return true;
    }

    public void prepareForRepair(UUID parentRepairSession, InetAddress coordinator, Set<InetAddress> endpoints, RepairOption options, boolean isForcedRepair, List<ColumnFamilyStore> columnFamilyStores)
    {
        // we only want repairedAt for incremental repairs, for non incremental repairs, UNREPAIRED_SSTABLE will preserve repairedAt on streamed sstables
        if (!verifyCompactionsPendingThreshold(parentRepairSession, options.getPreviewKind()))
            failRepair(parentRepairSession, "Rejecting incoming repair, pending compactions above threshold"); // failRepair throws exception

        long repairedAt = getRepairedAt(options, isForcedRepair);
        registerParentRepairSession(parentRepairSession, coordinator, columnFamilyStores, options.getRanges(), options.isIncremental(), repairedAt, options.isGlobal(), options.getPreviewKind());
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
                PrepareMessage message = new PrepareMessage(parentRepairSession, cfIds, options.getRanges(), options.isIncremental(), repairedAt, options.isGlobal(), options.getPreviewKind());
                MessageOut<RepairMessage> msg = message.createMessage();
                MessagingService.instance().sendRR(msg, neighbour, callback, DatabaseDescriptor.getRpcTimeout(), true);
            }
            else
            {
                // we pre-filter the endpoints we want to repair for forced incremental repairs. So if any of the
                // remaining ones go down, we still want to fail so we don't create repair sessions that can't complete
                if (isForcedRepair && !options.isIncremental())
                {
                    prepareLatch.countDown();
                }
                else
                {
                    // bailout early to avoid potentially waiting for a long time.
                    failRepair(parentRepairSession, "Endpoint not alive: " + neighbour);
                }
            }
        }

        try
        {
            if (!prepareLatch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS)) {
                failRepair(parentRepairSession, "Did not get replies from all endpoints.");
            }
        }
        catch (InterruptedException e)
        {
            failRepair(parentRepairSession, "Interrupted while waiting for prepare repair response.");
        }

        if (!status.get())
        {
            failRepair(parentRepairSession, "Got negative replies from endpoints " + failedNodes);
        }
    }

    private void failRepair(UUID parentRepairSession, String errorMsg) {
        removeParentRepairSession(parentRepairSession);
        throw new RuntimeException(errorMsg);
    }

    public synchronized void registerParentRepairSession(UUID parentRepairSession, InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal, PreviewKind previewKind)
    {
        assert isIncremental || repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE;
        if (!registeredForEndpointChanges)
        {
            Gossiper.instance.register(this);
            FailureDetector.instance.registerFailureDetectionEventListener(this);
            registeredForEndpointChanges = true;
        }

        if (!parentRepairSessions.containsKey(parentRepairSession))
        {
            parentRepairSessions.put(parentRepairSession, new ParentRepairSession(coordinator, columnFamilyStores, ranges, isIncremental, repairedAt, isGlobal, previewKind));
        }
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
        String snapshotName = parentSessionId.toString();
        ParentRepairSession session = parentRepairSessions.remove(parentSessionId);
        if (session == null)
            return null;
        for (ColumnFamilyStore cfs : session.columnFamilyStores.values())
        {
            if (cfs.snapshotExists(snapshotName))
                cfs.clearSnapshot(snapshotName);
        }
        return session;
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
                session.syncComplete(desc, sync.nodes, sync.success, sync.summaries);
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
        public final PreviewKind previewKind;

        public ParentRepairSession(InetAddress coordinator, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean isIncremental, long repairedAt, boolean isGlobal, PreviewKind previewKind)
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
            this.previewKind = previewKind;
        }

        public boolean isPreview()
        {
            return previewKind != PreviewKind.NONE;
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

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RepairSuccess that = (RepairSuccess) o;

            if (succeedAt != that.succeedAt) return false;
            if (!keyspace.equals(that.keyspace)) return false;
            if (!columnFamily.equals(that.columnFamily)) return false;
            return ranges.equals(that.ranges);
        }

        public int hashCode()
        {
            int result = keyspace.hashCode();
            result = 31 * result + columnFamily.hashCode();
            result = 31 * result + ranges.hashCode();
            result = 31 * result + (int) (succeedAt ^ (succeedAt >>> 32));
            return result;
        }

        public String toString()
        {
            return "RepairSuccess{" + keyspace + '.' + columnFamily +
                   ", ranges=" + ranges +
                   ", succeedAt=" + succeedAt +
                   '}';
        }
    }

    public static class RepairSuccessVerbHandler implements IVerbHandler<RepairSuccess>
    {
        public void doVerb(MessageIn<RepairSuccess> message, int id)
        {
            RepairSuccess success = message.payload;
            logger.info("Received repair success for {}/{}, {} at {}", new Object[]{success.keyspace, success.columnFamily, success.ranges, success.succeedAt});

            ColumnFamilyStore cfs = Keyspace.open(success.keyspace).getColumnFamilyStore(success.columnFamily);

            // we need to stop preview repair sessions when we receive a repair success since that could make us use
            // different xmas-patch-repair times when validating on different replicas
            for (RepairSuccessListener listener : repairSuccessListeners)
                listener.onRepairSuccess(success);

            CompactionManager.instance.stopConflictingPreviewValidations(cfs.metadata, success.ranges, "RepairSuccess received");

            for (Range<Token> range : success.ranges)
                cfs.updateLastSuccessfulRepair(range, success.succeedAt);
            MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
        }
    }

    public interface RepairSuccessListener
    {
        void onRepairSuccess(RepairSuccess message);
    }

    public void abortRepairsInvolvingTable(UUID tableId)
    {
        Set<UUID> parentSessionsToRemove = new HashSet<>();
        for (Map.Entry<UUID, ParentRepairSession> repairSessionEntry : parentRepairSessions.entrySet())
        {
            if (repairSessionEntry.getValue().columnFamilyStores.containsKey(tableId))
            {
                parentSessionsToRemove.add(repairSessionEntry.getKey());
            }
        }
        logger.info("Stopping parent sessions {} due to truncation of tableId={}", parentSessionsToRemove, tableId);
        parentSessionsToRemove.forEach(this::removeParentRepairSession);
    }

    public int getRepairPendingCompactionRejectThreshold()
    {
        return DatabaseDescriptor.getRepairPendingCompactionRejectThreshold();
    }

    public void setRepairPendingCompactionRejectThreshold(int value)
    {
        DatabaseDescriptor.setRepairPendingCompactionRejectThreshold(value);
    }

    public ListenableFuture<?> repairPaxosForTopologyChange(String ksName, Collection<Range<Token>> ranges, String reason)
    {
        if (ranges.isEmpty())
            return Futures.immediateFuture(null);
        List<CFMetaData> cfms = Lists.newArrayList(Schema.instance.getKSMetaData(ksName).tables);
        List<ListenableFuture<Void>> futures = new ArrayList<>(ranges.size() * cfms.size());
        Keyspace keyspace = Keyspace.open(ksName);
        for (Range<Token> range: ranges)
        {
            for (CFMetaData cfm : cfms)
            {
                Set<InetAddress> endpoints = Sets.newHashSet(StorageService.instance.getLiveNaturalEndpoints(keyspace.getReplicationStrategy(), range.right));
                if (!PaxosRepair.hasSufficientLiveNodesForTopologyChange(keyspace, range, endpoints))
                {
                    Set<InetAddress> downEndpoints = Sets.newHashSet(StorageService.instance.getNaturalEndpoints(ksName, range.right));
                    downEndpoints.removeAll(endpoints);

                    throw new RuntimeException(String.format("Insufficient live nodes to repair paxos for %s in %s for %s.\n" +
                                                             "There must be enough live nodes to satisfy EACH_QUORUM, but the following nodes are down: %s\n" +
                                                             "This check can be skipped by setting either the yaml property skip_paxos_repair_on_topology_change or " +
                                                             "the system property cassandra.skip_paxos_repair_on_topology_change to false. The jmx property " +
                                                             "StorageService.SkipPaxosRepairOnTopologyChange can also be set to false to temporarily disable without " +
                                                             "restarting the node\n" +
                                                             "Individual keyspaces can be skipped with the yaml property skip_paxos_repair_on_topology_change_keyspaces, the" +
                                                             "system property cassandra.skip_paxos_repair_on_topology_change_keyspaces, or temporarily with the jmx" +
                                                             "property StorageService.SkipPaxosRepairOnTopologyChangeKeyspaces\n" +
                                                             "Skipping this check can lead to paxos correctness issues",
                                                             range, ksName, reason, downEndpoints));
                }
                endpoints.addAll(StorageService.instance.getTokenMetadata().pendingEndpointsFor(range.right, ksName));
                ListenableFuture<Void> future = PaxosCleanup.cleanup(endpoints, cfm.cfId, Collections.singleton(range), false, repairCommandExecutor());
                futures.add(future);
            }
        }

        return Futures.allAsList(futures);
    }

    public int getPaxosRepairParallelism()
    {
        return DatabaseDescriptor.getPaxosRepairParallelism();
    }

    public void setPaxosRepairParallelism(int v)
    {
        DatabaseDescriptor.setPaxosRepairParallelism(v);
    }
}
