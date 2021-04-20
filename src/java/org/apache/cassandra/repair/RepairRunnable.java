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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.consistent.SyncStatSummary;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.consistent.CoordinatorSession;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.RepairedDataVerifier;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

public class RepairRunnable extends WrappedRunnable implements ProgressEventNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(RepairRunnable.class);

    private final StorageService storageService;
    private final int cmd;
    private final RepairOption options;
    private final String keyspace;

    private final String tag;
    private final AtomicInteger progress = new AtomicInteger();
    private final int totalProgress;

    private final List<ProgressListener> listeners = new ArrayList<>();

    public RepairRunnable(StorageService storageService, int cmd, RepairOption options, String keyspace)
    {
        this.storageService = storageService;
        this.cmd = cmd;
        this.options = options;
        this.keyspace = keyspace;

        this.tag = "repair:" + cmd;
        // get valid column families, calculate neighbors, validation, prepare for repair + number of ranges to repair
        this.totalProgress = 4 + options.getRanges().size();
    }

    @Override
    public void addProgressListener(ProgressListener listener)
    {
        listeners.add(listener);
    }

    @Override
    public void removeProgressListener(ProgressListener listener)
    {
        listeners.remove(listener);
    }


    protected void fireProgressEvent(ProgressEvent event)
    {
        for (ProgressListener listener : listeners)
        {
            listener.progress(tag, event);
        }
    }

    protected void fireErrorAndComplete(int progressCount, int totalProgress, String message)
    {
        String errorMessage = String.format("Repair command #%d failed with error %s", cmd, message);
        fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, progressCount, totalProgress, errorMessage));
        String completionMessage = String.format("Repair command #%d finished with error", cmd);
        fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, progressCount, totalProgress, completionMessage));
        recordFailure(errorMessage, completionMessage);
    }

    @VisibleForTesting
    static class CommonRange
    {
        public final Set<InetAddress> endpoints;
        public final Collection<Range<Token>> ranges;

        public CommonRange(Set<InetAddress> endpoints, Collection<Range<Token>> ranges)
        {
            Preconditions.checkArgument(endpoints != null && !endpoints.isEmpty(), "Endpoints can not be empty");
            Preconditions.checkArgument(ranges != null && !ranges.isEmpty(), "ranges can not be empty");
            this.endpoints = endpoints;
            this.ranges = ranges;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CommonRange that = (CommonRange) o;

            if (!endpoints.equals(that.endpoints)) return false;
            return ranges.equals(that.ranges);
        }

        public int hashCode()
        {
            int result = endpoints.hashCode();
            result = 31 * result + ranges.hashCode();
            return result;
        }

        public String toString()
        {
            return "CommonRange{" +
                   "endpoints=" + endpoints +
                   ", ranges=" + ranges +
                   '}';
        }
    }

    protected static Map<InetAddress, Set<Range<Token>>> collectEndpointRanges(List<CommonRange> commonRanges)
    {
        Map<InetAddress, Set<Range<Token>>> endpointRanges = new HashMap<>();
        for (CommonRange commonRange: commonRanges)
        {
            for (InetAddress endpoint: commonRange.endpoints)
            {
                if (!endpointRanges.containsKey(endpoint))
                {
                    endpointRanges.put(endpoint, new HashSet<>());
                }
                endpointRanges.get(endpoint).addAll(commonRange.ranges);
            }
        }
        return new HashMap<>(endpointRanges);
    }

    protected void syncRepairHistory(Iterable<ColumnFamilyStore> tables, List<CommonRange> commonRanges)
    {
        Map<InetAddress, Set<Range<Token>>> endpointRanges = collectEndpointRanges(commonRanges);

        // commonRanges only contains endpoint -> ranges for nodes that aren't this node (the coordinator).
        // Here, we gather all ranges and add endpoint -> range for this node since we want to include the
        // local repair history in the sync (since we probably have the most up to date data locally)
        Set<Range<Token>> allRanges = new HashSet<>();
        for (Set<Range<Token>> ranges: endpointRanges.values())
        {
            allRanges.addAll(ranges);
        }
        endpointRanges.put(FBUtilities.getBroadcastAddress(), allRanges);

        List<ListenableFuture<Object>> futures = new ArrayList<>();

        logger.info("Syncing repair history for [{}] on {}", tablesToString(tables), endpointRanges);
        for (ColumnFamilyStore cfs : tables)
        {
            RepairHistorySyncTask syncTask = new RepairHistorySyncTask(cfs, ImmutableMap.copyOf(endpointRanges));
            syncTask.execute();
            futures.add(syncTask);
        }

        try
        {
            Futures.successfulAsList(futures).get(DatabaseDescriptor.getRepairHistorySyncTimeoutSeconds(), TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String tablesToString(Iterable<ColumnFamilyStore> tables)
    {
        StringBuilder sb = new StringBuilder();
        for (ColumnFamilyStore table : tables)
        {
            sb.append(String.format("%s.%s, ", table.keyspace.getName(), table.name));
        }
        return sb.toString();
    }

    protected void runMayThrow() throws Exception
    {
        ActiveRepairService.instance.recordRepairStatus(cmd, ActiveRepairService.ParentRepairStatus.IN_PROGRESS, ImmutableList.of());
        final TraceState traceState;
        final UUID parentSession = UUIDGen.getTimeUUID();

        String[] columnFamilies = options.getColumnFamilies().toArray(new String[options.getColumnFamilies().size()]);
        Iterable<ColumnFamilyStore> validColumnFamilies;
        try
        {
            validColumnFamilies = storageService.getValidColumnFamilies(false, false, keyspace, columnFamilies);
            progress.incrementAndGet();
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Repair {} failed:", parentSession, e);
            fireErrorAndComplete(progress.get(), totalProgress, e.getMessage());
            return;
        }

        if (Iterables.isEmpty(validColumnFamilies))
        {
            String message = String.format("%s Empty keyspace, skipping repair: %s", parentSession, keyspace);
            logger.info(message);
            fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, 0, 0, message));
            return;
        }

        final long startTime = System.currentTimeMillis();
        String message = String.format("Starting repair command #%d (%s), repairing keyspace %s with %s", cmd, parentSession, keyspace,
                                       options);
        logger.info(message);
        if (options.isTraced())
        {
            StringBuilder cfsb = new StringBuilder();
            for (ColumnFamilyStore cfs : validColumnFamilies)
                cfsb.append(", ").append(cfs.keyspace.getName()).append(".").append(cfs.name);

            UUID sessionId = Tracing.instance.newSession(Tracing.TraceType.REPAIR);
            traceState = Tracing.instance.begin("repair", ImmutableMap.of("keyspace", keyspace, "columnFamilies",
                                                                          cfsb.substring(2)));
            message = message + " tracing with " + sessionId;
            fireProgressEvent(new ProgressEvent(ProgressEventType.START, 0, 100, message));
            Tracing.traceRepair(message);
            traceState.enableActivityNotification(tag);
            for (ProgressListener listener : listeners)
                traceState.addProgressListener(listener);
            Thread queryThread = createQueryThread(cmd, sessionId);
            queryThread.setName("RepairTracePolling");
            queryThread.start();
        }
        else
        {
            fireProgressEvent(new ProgressEvent(ProgressEventType.START, 0, 100, message));
            traceState = null;
        }

        Set<InetAddress> allNeighbors = new HashSet<>();
        List<CommonRange> commonRanges = new ArrayList<>();

        //pre-calculate output of getLocalRanges and pass it to getNeighbors to increase performance and prevent
        //calculation multiple times
        Collection<Range<Token>> keyspaceLocalRanges = storageService.getLocalRanges(keyspace);
        final Map<Set<InetAddress>, Boolean> allReplicaMap = new HashMap<>();
        try
        {
            for (Range<Token> range : options.getRanges())
            {
                Set<InetAddress> unfilteredNeighbors = ActiveRepairService.getAllNeighbors(keyspace, keyspaceLocalRanges, range);
                Set<InetAddress> neighbors = ActiveRepairService.filterNeighbors(unfilteredNeighbors, range,
                                                                              options.getDataCenters(),
                                                                              options.getHosts());
                if (neighbors.isEmpty())
                {
                    if (options.ignoreUnreplicatedKeyspaces())
                    {
                        logger.info("{} Found no neighbors for range {} for {} - ignoring since repairing with --ignore-unreplicated-keyspaces", parentSession, range, keyspace);
                        continue;
                    }
                    else
                    {
                        String errorMessage = String.format("Nothing to repair for %s in %s - aborting", range, keyspace);
                        logger.error("Repair {} {}", parentSession, errorMessage);
                        fireErrorAndComplete(progress.get(), totalProgress, errorMessage);
                        return;
                    }
                }

                boolean allReplicas = unfilteredNeighbors.equals(neighbors);
                if (!allReplicas && allReplicaMap.containsKey(neighbors))
                    allReplicaMap.put(neighbors, false);
                else if (!allReplicaMap.containsKey(neighbors))
                    allReplicaMap.put(neighbors, allReplicas);
                addRangeToNeighbors(commonRanges, range, neighbors);
                allNeighbors.addAll(neighbors);
            }

            progress.incrementAndGet();
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Repair {} failed:", parentSession, e);
            fireErrorAndComplete(progress.get(), totalProgress, e.getMessage());
            return;
        }

        if (options.ignoreUnreplicatedKeyspaces() && allNeighbors.isEmpty())
        {
            String ignoreUnreplicatedMessage = String.format("Nothing to repair for %s in %s - unreplicated keyspace is ignored since repair was called with --ignore-unreplicated-keyspaces",
                                                             options.getRanges(),
                                                             keyspace);

            logger.info("Repair {} {}", parentSession, ignoreUnreplicatedMessage);
            fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE,
                                                    progress.get(),
                                                    totalProgress,
                                                    ignoreUnreplicatedMessage));
            ActiveRepairService.instance.recordRepairStatus(cmd, ActiveRepairService.ParentRepairStatus.COMPLETED,
                                                            ImmutableList.of(ignoreUnreplicatedMessage));
            return;
        }

        // Validate columnfamilies
        List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>();
        try
        {
            Iterables.addAll(columnFamilyStores, validColumnFamilies);
            progress.incrementAndGet();
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Repair {} failed:", parentSession, e);
            fireErrorAndComplete(progress.get(), totalProgress, e.getMessage());
            return;
        }

        String[] cfnames = new String[columnFamilyStores.size()];
        for (int i = 0; i < columnFamilyStores.size(); i++)
        {
            cfnames[i] = columnFamilyStores.get(i).name;
        }

        if (!options.isPreview())
        {
            SystemDistributedKeyspace.startParentRepair(parentSession, keyspace, cfnames, options.getRanges());
        }

        boolean force = options.isForcedRepair();

        if (force && options.isIncremental())
        {
            Set<InetAddress> actualNeighbors = Sets.newHashSet(Iterables.filter(allNeighbors, FailureDetector.instance::isAlive));
            force = !allNeighbors.equals(actualNeighbors);
            allNeighbors = actualNeighbors;
        }

        try (Timer.Context ctx = Keyspace.open(keyspace).metric.repairPrepareTime.time())
        {
            ActiveRepairService.instance.prepareForRepair(parentSession, FBUtilities.getBroadcastAddress(), allNeighbors, options, force, columnFamilyStores);
            progress.incrementAndGet();
        }
        catch (Throwable t)
        {
            logger.error("Repair {} failed:", parentSession, t);
            if (!options.isPreview())
            {
                SystemDistributedKeyspace.failParentRepair(parentSession, t);
            }
            fireErrorAndComplete(progress.get(), totalProgress, t.getMessage());
            return;
        }

        if (options.isGlobal() && !options.isForcedRepair())
        {
            syncRepairHistory(columnFamilyStores, commonRanges);
        }

        if (options.isPreview())
        {
            previewRepair(parentSession, startTime, allReplicaMap, commonRanges, cfnames);
        }
        else if (options.isIncremental())
        {
            incrementalRepair(parentSession, startTime, force, traceState, allNeighbors, allReplicaMap, commonRanges, cfnames);
        }
        else
        {
            normalRepair(parentSession, startTime, traceState, allReplicaMap, commonRanges, cfnames);
        }
    }

    protected void unhandledException(Exception e)
    {
        fireErrorAndComplete(progress.get(), totalProgress, e.getMessage());
    }

    private void normalRepair(UUID parentSession,
                              long startTime,
                              TraceState traceState,
                              Map<Set<InetAddress>, Boolean> allReplicaMap,
                              List<CommonRange> commonRanges,
                              String... cfnames)
    {

        // Set up RepairJob executor for this repair command.
        ListeningExecutorService executor = createExecutor();

        final ListenableFuture<List<RepairSessionResult>> allSessions = submitRepairSessions(parentSession, false, executor, allReplicaMap, commonRanges, cfnames);

        // After all repair sessions completes(successful or not),
        // run anticompaction if necessary and send finish notice back to client
        final Collection<Range<Token>> successfulRanges = new ArrayList<>();
        final AtomicBoolean hasFailure = new AtomicBoolean();
        ListenableFuture repairResult = Futures.transform(allSessions, new AsyncFunction<List<RepairSessionResult>, Object>()
        {
            @SuppressWarnings("unchecked")
            public ListenableFuture apply(List<RepairSessionResult> results) throws Exception
            {
                // filter out null(=failed) results and get successful ranges
                for (RepairSessionResult sessionResult : results)
                {
                    logger.debug("Repair result: {}", results);
                    if (sessionResult != null)
                    {
                        // don't record successful repair if we had to skip ranges
                        if (!sessionResult.skippedReplicas)
                        {
                            successfulRanges.addAll(sessionResult.ranges);
                        }
                    }
                    else
                    {
                        hasFailure.compareAndSet(false, true);
                    }
                }
                return Futures.immediateFuture(null);
            }
        });
        Futures.addCallback(repairResult, new RepairCompleteCallback(parentSession, successfulRanges, startTime, traceState, hasFailure, executor));
    }

    /**
     * removes dead nodes from common ranges, and exludes ranges left without any participants
     */
    @VisibleForTesting
    static List<CommonRange> filterCommonRanges(List<CommonRange> commonRanges, Set<InetAddress> liveEndpoints, boolean force)
    {
        if (!force)
        {
            return commonRanges;
        }
        else
        {
            List<CommonRange> filtered = new ArrayList<>(commonRanges.size());

            for (CommonRange commonRange: commonRanges)
            {
                Set<InetAddress> endpoints = ImmutableSet.copyOf(Iterables.filter(commonRange.endpoints, liveEndpoints::contains));

                // this node is implicitly a participant in this repair, so a single endpoint is ok here
                if (!endpoints.isEmpty())
                {
                    filtered.add(new CommonRange(endpoints, commonRange.ranges));
                }
            }
            Preconditions.checkState(!filtered.isEmpty(), "Not enough live endpoints for a repair");
            return filtered;
        }
    }

    private void incrementalRepair(UUID parentSession,
                                   long startTime,
                                   boolean forceRepair,
                                   TraceState traceState,
                                   Set<InetAddress> allNeighbors,
                                   Map<Set<InetAddress>, Boolean> allReplicaMap,
                                   List<CommonRange> commonRanges,
                                   String... cfnames)
    {
        // the local node also needs to be included in the set of participants, since coordinator sessions aren't persisted
        Set<InetAddress> allParticipants = ImmutableSet.<InetAddress>builder()
                                           .addAll(allNeighbors)
                                           .add(FBUtilities.getBroadcastAddress())
                                           .build();

        List<CommonRange> allRanges = filterCommonRanges(commonRanges, allParticipants, forceRepair);

        CoordinatorSession coordinatorSession = ActiveRepairService.instance.consistent.coordinated.registerSession(parentSession, allParticipants, forceRepair);
        ListeningExecutorService executor = createExecutor();
        AtomicBoolean hasFailure = new AtomicBoolean(false);
        ListenableFuture repairResult = coordinatorSession.execute(() -> submitRepairSessions(parentSession, true, executor, allReplicaMap, allRanges, cfnames),
                                                                   hasFailure);
        Collection<Range<Token>> ranges = new HashSet<>();
        for (Collection<Range<Token>> range : Iterables.transform(allRanges, cr -> cr.ranges))
        {
            ranges.addAll(range);
        }
        Futures.addCallback(repairResult, new RepairCompleteCallback(parentSession, ranges, startTime, traceState, hasFailure, executor));
    }

    private void previewRepair(UUID parentSession,
                               long startTime,
                               Map<Set<InetAddress>, Boolean> allReplicaMap,
                               List<CommonRange> commonRanges,
                               String... cfnames)
    {

        logger.debug("Starting preview repair for {}", parentSession);
        // Set up RepairJob executor for this repair command.
        ListeningExecutorService executor = createExecutor();

        final ListenableFuture<List<RepairSessionResult>> allSessions = submitRepairSessions(parentSession, false, executor, allReplicaMap, commonRanges, cfnames);

        Futures.addCallback(allSessions, new FutureCallback<List<RepairSessionResult>>()
        {
            public void onSuccess(List<RepairSessionResult> results)
            {
                try
                {
                    PreviewKind previewKind = options.getPreviewKind();
                    assert previewKind != PreviewKind.NONE;
                    SyncStatSummary summary = new SyncStatSummary(true);
                    summary.consumeSessionResults(results);

                    final String message;
                    if (summary.isEmpty())
                    {
                        message = (previewKind == PreviewKind.REPAIRED ? "Repaired data is in sync" : "Previewed data was in sync") + " for " + parentSession;
                        logger.info(message);
                        fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, progress.get(), totalProgress, message));

                        // no need to keep the merkle trees around if everything was ok
                        if (DatabaseDescriptor.getDebugValidationPreviewEnabled() && previewKind == PreviewKind.REPAIRED)
                        {
                            FileUtils.deleteRecursive(RepairSession.merkleTreeDir(parentSession));
                        }
                        RepairMetrics.previewFailures.inc();
                    }
                    else
                    {
                        boolean isRepaired = previewKind == PreviewKind.REPAIRED;
                        String christmasPatchWarning = isRepaired ? summary.getChristmasPatchDisabledWarning() : "";
                        message = (isRepaired ? "Repaired data is inconsistent" : "Preview complete")
                                  + " for " + parentSession + christmasPatchWarning + '\n' + summary.toString(isRepaired);
                        logger.info(message);
                        if (previewKind == PreviewKind.REPAIRED)
                            maybeSnapshotReplicas(parentSession, keyspace, results);
                        fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, progress.get(), totalProgress, message));
                    }

                    String successMessage = "Repair preview completed successfully";
                    fireProgressEvent(new ProgressEvent(ProgressEventType.SUCCESS, progress.get(), totalProgress,
                                                        successMessage));
                    String completionMessage = complete();

                    ActiveRepairService.instance.recordRepairStatus(cmd, ActiveRepairService.ParentRepairStatus.COMPLETED,
                                                           ImmutableList.of(message, successMessage, completionMessage));
                }
                catch (Throwable t)
                {
                    logger.error("Error completing preview repair", t);
                    onFailure(t);
                }
            }

            public void onFailure(Throwable t)
            {
                fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, progress.get(), totalProgress, t.getMessage()));
                logger.error("Error completing preview repair", t);
                String completionMessage = complete();
                recordFailure(t.getMessage(), completionMessage);
            }

            private String complete()
            {
                logger.debug("Preview repair {} completed", parentSession);

                String duration = DurationFormatUtils.formatDurationWords(System.currentTimeMillis() - startTime,
                                                                          true, true);
                String message = String.format("Repair preview #%d finished in %s", cmd, duration);
                fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, progress.get(), totalProgress, message));
                executor.shutdownNow();
                return message;
            }
        });
    }

    private void maybeSnapshotReplicas(UUID parentSession, String keyspace, List<RepairSessionResult> results)
    {
        if (!DatabaseDescriptor.snapshotOnRepairedDataMismatch())
            return;

        try
        {
            Set<String> mismatchingTables = new HashSet<>();
            Set<InetAddress> nodes = new HashSet<>();
            for (RepairSessionResult sessionResult : results)
            {
                for (RepairResult repairResult : emptyIfNull(sessionResult.repairJobResults))
                {
                    for (SyncStat stat : emptyIfNull(repairResult.stats))
                    {
                        if (stat.numberOfDifferences > 0)
                            mismatchingTables.add(repairResult.desc.columnFamily);
                        // snapshot all replicas, even if they don't have any differences
                        nodes.add(stat.nodes.endpoint1);
                        nodes.add(stat.nodes.endpoint2);
                    }
                }
            }

            String snapshotName = DiagnosticSnapshotService.getSnapshotName(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX);
            for (String table : mismatchingTables)
            {
                // we can shortcut the snapshot existence locally since the repair coordinator is always a replica (unlike in the read case)
                if (!Keyspace.open(keyspace).getColumnFamilyStore(table).snapshotExists(snapshotName))
                {
                    logger.info("{} Snapshotting {}.{} for preview repair mismatch with tag {} on instances {}",
                                options.getPreviewKind().logPrefix(parentSession),
                                keyspace, table, snapshotName, nodes);
                    DiagnosticSnapshotService.repairedDataMismatch(Keyspace.open(keyspace).getColumnFamilyStore(table).metadata, nodes);
                }
                else
                {
                    logger.info("{} Not snapshotting {}.{} - snapshot {} exists",
                                options.getPreviewKind().logPrefix(parentSession),
                                keyspace, table, snapshotName);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("{} Failed snapshotting replicas", options.getPreviewKind().logPrefix(parentSession), e);
        }
    }

    private static <T> Iterable<T> emptyIfNull(Iterable<T> iter)
    {
        if (iter == null)
            return Collections.emptyList();
        return iter;
    }

    private ListenableFuture<List<RepairSessionResult>> submitRepairSessions(UUID parentSession,
                                                                             boolean isIncremental,
                                                                             ListeningExecutorService executor,
                                                                             Map<Set<InetAddress>, Boolean> allReplicaMap,
                                                                             List<CommonRange> commonRanges,
                                                                             String... cfnames)
    {
        List<ListenableFuture<RepairSessionResult>> futures = new ArrayList<>(options.getRanges().size());

        // we do endpoint filtering at the start of an incremental repair,
        // so repair sessions shouldn't also be checking liveness
        boolean force = options.isForcedRepair() && !isIncremental;
        for (CommonRange cr : commonRanges)
        {
            logger.info("Starting RepairSession for {}", cr);
            RepairSession session = ActiveRepairService.instance.submitRepairSession(parentSession,
                                                                                     cr.ranges,
                                                                                     keyspace,
                                                                                     options.getParallelism(),
                                                                                     allReplicaMap.getOrDefault(cr.endpoints, false),
                                                                                     cr.endpoints,
                                                                                     isIncremental,
                                                                                     options.isPullRepair(),
                                                                                     force,
                                                                                     options.getPreviewKind(),
                                                                                     options.optimiseStreams(),
                                                                                     executor,
                                                                                     cfnames);
            if (session == null)
                continue;
            Futures.addCallback(session, new RepairSessionCallback(session));
            futures.add(session);
        }
        return Futures.successfulAsList(futures);
    }

    private ListeningExecutorService createExecutor()
    {
        return MoreExecutors.listeningDecorator(new JMXEnabledThreadPoolExecutor(options.getJobThreads(),
                                                                                      Integer.MAX_VALUE,
                                                                                      TimeUnit.SECONDS,
                                                                                      new LinkedBlockingQueue<>(),
                                                                                      new NamedThreadFactory("Repair#" + cmd),
                                                                                      "internal"));
    }

    private class RepairSessionCallback implements FutureCallback<RepairSessionResult>
    {
        private final RepairSession session;

        public RepairSessionCallback(RepairSession session)
        {
            this.session = session;
        }

        public void onSuccess(RepairSessionResult result)
        {
            /**
             * If the success message below is modified, it must also be updated on
             * {@link org.apache.cassandra.utils.progress.jmx.LegacyJMXProgressSupport}
             * for backward-compatibility support.
             */
            String message = String.format("Repair session %s for range %s finished", session.getId(),
                                           session.getRanges().toString());
            logger.info(message);
            fireProgressEvent(new ProgressEvent(ProgressEventType.PROGRESS,
                                                progress.incrementAndGet(),
                                                totalProgress,
                                                message));
        }

        public void onFailure(Throwable t)
        {
            /**
             * If the failure message below is modified, it must also be updated on
             * {@link org.apache.cassandra.utils.progress.jmx.LegacyJMXProgressSupport}
             * for backward-compatibility support.
             */
            String message = String.format("Repair session %s for range %s failed with error %s",
                                           session.getId(), session.getRanges().toString(), t.getMessage());
            logger.error(message, t);
            fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR,
                                                progress.incrementAndGet(),
                                                totalProgress,
                                                message));
        }
    }

    private class RepairCompleteCallback implements FutureCallback<Object>
    {
        final UUID parentSession;
        final Collection<Range<Token>> successfulRanges;
        final long startTime;
        final TraceState traceState;
        final AtomicBoolean hasFailure;
        final ExecutorService executor;

        public RepairCompleteCallback(UUID parentSession,
                                      Collection<Range<Token>> successfulRanges,
                                      long startTime,
                                      TraceState traceState,
                                      AtomicBoolean hasFailure,
                                      ExecutorService executor)
        {
            this.parentSession = parentSession;
            this.successfulRanges = successfulRanges;
            this.startTime = startTime;
            this.traceState = traceState;
            this.hasFailure = hasFailure;
            this.executor = executor;
        }

        public void onSuccess(Object result)
        {
            if (!options.isPreview())
            {
                SystemDistributedKeyspace.successfulParentRepair(parentSession, successfulRanges);
            }
            final String message;
            if (hasFailure.get())
            {
                message = "Some repair failed";
                fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, progress.get(), totalProgress,
                                                    message));
            }
            else
            {
                message = "Repair completed successfully";
                fireProgressEvent(new ProgressEvent(ProgressEventType.SUCCESS, progress.get(), totalProgress,
                                                    message));
            }
            String completionMessage = repairComplete();
            if (hasFailure.get())
            {
                recordFailure(message, completionMessage);
            }
            else
            {
                ActiveRepairService.instance.recordRepairStatus(cmd, ActiveRepairService.ParentRepairStatus.COMPLETED,
                                                       ImmutableList.of(message, completionMessage));
            }
        }

        public void onFailure(Throwable t)
        {
            fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, progress.get(), totalProgress, t.getMessage()));
            if (!options.isPreview())
            {
                SystemDistributedKeyspace.failParentRepair(parentSession, t);
            }
            String completionMessage = repairComplete();
            recordFailure(t.getMessage(), completionMessage);
        }

        private String repairComplete()
        {
            ActiveRepairService.instance.removeParentRepairSession(parentSession);
            long durationMillis = System.currentTimeMillis() - startTime;
            String duration = DurationFormatUtils.formatDurationWords(durationMillis, true, true);
            String message = String.format("Repair command #%d finished in %s", cmd, duration);
            fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, progress.get(), totalProgress, message));
            logger.info(options.getPreviewKind().logPrefix(parentSession) + message);
            if (options.isTraced() && traceState != null)
            {
                for (ProgressListener listener : listeners)
                    traceState.removeProgressListener(listener);
                // Because DebuggableThreadPoolExecutor#afterExecute and this callback
                // run in a nondeterministic order (within the same thread), the
                // TraceState may have been nulled out at this point. The TraceState
                // should be traceState, so just set it without bothering to check if it
                // actually was nulled out.
                Tracing.instance.set(traceState);
                Tracing.traceRepair(message);
                Tracing.instance.stopSession();
            }
            executor.shutdownNow();
            Keyspace.open(keyspace).metric.repairTime.update(durationMillis, TimeUnit.MILLISECONDS);
            return message;
        }
    }

    private void recordFailure(String failureMessage, String completionMessage)
    {
        // Note we rely on the first message being the reason for the failure
        // when inspecting this state from RepairRunner.queryForCompletedRepair
        String failure = failureMessage == null ? "unknown failure" : failureMessage;
        String completion = completionMessage == null ? "unknown completion" : completionMessage;

        ActiveRepairService.instance.recordRepairStatus(cmd, ActiveRepairService.ParentRepairStatus.FAILED,
                                               ImmutableList.of(failure, completion));
    }

    private void addRangeToNeighbors(List<CommonRange> neighborRangeList, Range<Token> range, Set<InetAddress> neighbors)
    {
        for (int i = 0; i < neighborRangeList.size(); i++)
        {
            CommonRange cr = neighborRangeList.get(i);

            if (cr.endpoints.containsAll(neighbors))
            {
                cr.ranges.add(range);
                return;
            }
        }

        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(range);
        neighborRangeList.add(new CommonRange(neighbors, ranges));
    }

    private Thread createQueryThread(final int cmd, final UUID sessionId)
    {
        return new Thread(NamedThreadFactory.threadLocalDeallocator(new WrappedRunnable()
        {
            // Query events within a time interval that overlaps the last by one second. Ignore duplicates. Ignore local traces.
            // Wake up upon local trace activity. Query when notified of trace activity with a timeout that doubles every two timeouts.
            public void runMayThrow() throws Exception
            {
                TraceState state = Tracing.instance.get(sessionId);
                if (state == null)
                    throw new Exception("no tracestate");

                String format = "select event_id, source, activity from %s.%s where session_id = ? and event_id > ? and event_id < ?;";
                String query = String.format(format, TraceKeyspace.NAME, TraceKeyspace.EVENTS);
                SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query).prepare(ClientState.forInternalCalls()).statement;

                ByteBuffer sessionIdBytes = ByteBufferUtil.bytes(sessionId);
                InetAddress source = FBUtilities.getBroadcastAddress();

                HashSet<UUID>[] seen = new HashSet[] { new HashSet<>(), new HashSet<>() };
                int si = 0;
                UUID uuid;

                long tlast = System.currentTimeMillis(), tcur;

                TraceState.Status status;
                long minWaitMillis = 125;
                long maxWaitMillis = 1000 * 1024L;
                long timeout = minWaitMillis;
                boolean shouldDouble = false;

                while ((status = state.waitActivity(timeout)) != TraceState.Status.STOPPED)
                {
                    if (status == TraceState.Status.IDLE)
                    {
                        timeout = shouldDouble ? Math.min(timeout * 2, maxWaitMillis) : timeout;
                        shouldDouble = !shouldDouble;
                    }
                    else
                    {
                        timeout = minWaitMillis;
                        shouldDouble = false;
                    }
                    ByteBuffer tminBytes = ByteBufferUtil.bytes(UUIDGen.minTimeUUID(tlast - 1000));
                    ByteBuffer tmaxBytes = ByteBufferUtil.bytes(UUIDGen.maxTimeUUID(tcur = System.currentTimeMillis()));
                    QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Lists.newArrayList(sessionIdBytes,
                                                                                                                  tminBytes,
                                                                                                                  tmaxBytes));
                    ResultMessage.Rows rows = statement.execute(QueryState.forInternalCalls(), options);
                    UntypedResultSet result = UntypedResultSet.create(rows.result);

                    for (UntypedResultSet.Row r : result)
                    {
                        if (source.equals(r.getInetAddress("source")))
                            continue;
                        if ((uuid = r.getUUID("event_id")).timestamp() > (tcur - 1000) * 10000)
                            seen[si].add(uuid);
                        if (seen[si == 0 ? 1 : 0].contains(uuid))
                            continue;
                        String message = String.format("%s: %s", r.getInetAddress("source"), r.getString("activity"));
                        fireProgressEvent(
                        new ProgressEvent(ProgressEventType.NOTIFICATION, 0, 0, message));
                    }
                    tlast = tcur;

                    si = si == 0 ? 1 : 0;
                    seen[si].clear();
                }
            }
        }));
    }
}
