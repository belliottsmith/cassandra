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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.reads.RepairedDataTracker;
import org.apache.cassandra.service.reads.RepairedDataVerifier;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.db.IMutation.MAX_MUTATION_SIZE;

public class DataResolver extends ResponseResolver
{
    private static final boolean DROP_OVERSIZED_READ_REPAIR_MUTATIONS =
        Boolean.getBoolean("cassandra.drop_oversized_readrepair_mutations");

    @VisibleForTesting
    final Queue<ReadRepairHandler> repairResults = new ConcurrentLinkedQueue<>();

    private final boolean enforceStrictLiveness;

    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount)
    {
        super(keyspace, command, consistency, maxResponseCount);
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
    }

    public PartitionIterator getData()
    {
        ReadResponse response = responses.iterator().next().payload;
        return UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }

    public void compareResponses()
    {
        // We need to fully consume the results to trigger read repairs if appropriate
        try (PartitionIterator iterator = resolve())
        {
            PartitionIterators.consume(iterator);
        }
    }

    private void repairPartition(Map<InetAddress, Mutation> mutations, ConsistencyLevel.ResponseTracker blockFor, List<InetAddress> initial, List<InetAddress> additional)
    {
        ReadRepairHandler handler = new ReadRepairHandler(consistency, mutations, blockFor, initial, additional);
        handler.sendInitialRepairs();
        repairResults.add(handler);
    }

    public void maybeSendAdditionalRepairs()
    {
        for (ReadRepairHandler repairHandler: repairResults)
        {
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().cfId);
            repairHandler.maybeSendAdditionalRepairs(cfs.additionalWriteLatencyNanos, TimeUnit.NANOSECONDS);
        }
    }

    public void awaitRepairs()
    {
        ReadRepairHandler timeOut = null;
        for (ReadRepairHandler repairHandler: repairResults)
            if (!repairHandler.awaitRepairs(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
                timeOut = repairHandler;
        if (timeOut != null)
        {
            // We got all responses, but timed out while repairing
            int blockFor = timeOut.blockFor();
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, Math.min(blockFor - 1, blockFor - timeOut.waitingOn()), blockFor, true);
        }
    }

    public PartitionIterator resolve()
    {
        return resolve(()->{});
    }

    public PartitionIterator resolve(Runnable onShortRead)
    {
        // If requested, inspect each response for a digest of the replica's repaired data set
        RepairedDataTracker repairedDataTracker = command.isTrackingRepairedStatus()
                                                  ? new RepairedDataTracker(getRepairedDataVerifier(command))
                                                  : null;

        if (!needsReplicaFilteringProtection())
        {
            ResolveContext context = new ResolveContext(responses.size());
            return resolveWithReadRepair(context,
                                         () -> new RepairMergeListener(context.sources, repairedDataTracker),
                                         i -> shortReadProtectedResponse(i, context, onShortRead),
                                         UnaryOperator.identity(),
                                         repairedDataTracker);
        }

        return resolveWithReplicaFilteringProtection(repairedDataTracker, onShortRead);
    }

    private boolean needsReplicaFilteringProtection()
    {
        if (command.rowFilter().isEmpty() || !DatabaseDescriptor.isReplicaFilteringProtectionEnabled())
            return false;

        IndexMetadata indexMetadata = command.indexMetadata();

        if (indexMetadata == null || !indexMetadata.isCustom())
        {
            return true;
        }

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(command.metadata().cfId);

        assert cfs != null;

        Index index = command.getIndex(cfs);

        assert index != null;

        return index.supportsReplicaFilteringProtection(command.rowFilter());
    }

    private class ResolveContext
    {
        private final InetAddress[] sources;
        private final DataLimits.Counter mergedResultCounter;

        private ResolveContext(int count)
        {
            assert count <= responses.size();
            this.sources = new InetAddress[count];
            for (int i = 0; i < count; i++)
                sources[i] = responses.get(i).from;
            this.mergedResultCounter = command.limits().newCounter(command.nowInSec(),
                                                                   true,
                                                                   command.selectsFullPartition(),
                                                                   enforceStrictLiveness);
        }

        private boolean needShortReadProtection()
        {
            // If we have only one result, there is no read repair to do and we can't get short reads
            // Also, so-called "short reads" stems from nodes returning only a subset of the results they have for a
            // partition due to the limit, but that subset not being enough post-reconciliation. So if we don't have limit,
            // don't bother protecting against short reads.
            return sources.length > 1 && !command.limits().isUnlimited();
        }
    }

    @FunctionalInterface
    private interface MergeListenerProvider
    {
        UnfilteredPartitionIterators.MergeListener get();
    }

    @FunctionalInterface
    private interface ResponseProvider
    {
        UnfilteredPartitionIterator getResponse(int i);
    }

    private UnfilteredPartitionIterator shortReadProtectedResponse(int i, ResolveContext context, Runnable onShortRead)
    {
        UnfilteredPartitionIterator originalResponse = responses.get(i).payload.makeIterator(command);

        return context.needShortReadProtection()
               ? extendWithShortReadProtection(originalResponse, context, i, onShortRead)
               : originalResponse;
    }

    private PartitionIterator resolveWithReadRepair(ResolveContext context,
                                                    MergeListenerProvider mergeListenerProvider,
                                                    ResponseProvider responseProvider,
                                                    UnaryOperator<PartitionIterator> preCountFilter,
                                                    RepairedDataTracker repairedDataTracker)
    {
        return resolveInternal(context, mergeListenerProvider, responseProvider, preCountFilter, repairedDataTracker);
    }

    private PartitionIterator resolveWithReplicaFilteringProtection(RepairedDataTracker repairedDataTracker, Runnable onShortRead)
    {
        // Protecting against inconsistent replica filtering (some replica returning a row that is outdated but that
        // wouldn't be removed by normal reconciliation because up-to-date replica have filtered the up-to-date version
        // of that row) involves 3 main elements:
        //   1) We combine short-read protection and a merge listener that identifies potentially "out-of-date"
        //      rows to create an iterator that is guaranteed to produce enough valid row results to satisfy the query 
        //      limit if enough actually exist. A row is considered out-of-date if its merged from is non-empty and we 
        //      receive not response from at least one replica. In this case, it is possible that filtering at the
        //      "silent" replica has produced a more up-to-date result.
        //   2) This iterator is passed to the standard resolution process with read-repair, but is first wrapped in a 
        //      response provider that lazily "completes" potentially out-of-date rows by directly querying them on the
        //      replicas that were previously silent. As this iterator is consumed, it caches valid data for potentially
        //      out-of-date rows, and this cached data is merged with the fetched data as rows are requested. If there
        //      is no replica divergence, only rows in the partition being evalutated will be cached (then released
        //      when the partition is consumed).
        //   3) After a "complete" row is materialized, it must pass the row filter supplied by the original query 
        //      before it counts against the limit.

        // We could get more responses while this method runs, which is ok (we're happy to ignore any response not here
        // at the beginning of this method), so grab the response count once and use that through the method.
        int count = responses.size();
        // We need separate contexts, as each context has his own counter
        ResolveContext firstPhaseContext = new ResolveContext(count);
        ResolveContext secondPhaseContext = new ResolveContext(count);

        ReplicaFilteringProtection rfp = new ReplicaFilteringProtection(keyspace,
                                                                        command,
                                                                        consistency,
                                                                        firstPhaseContext.sources,
                                                                        DatabaseDescriptor.getCachedReplicaRowsWarnThreshold(),
                                                                        DatabaseDescriptor.getCachedReplicaRowsFailThreshold());

        PartitionIterator firstPhasePartitions = resolveInternal(firstPhaseContext,
                                                                 rfp::mergeController,
                                                                 i -> shortReadProtectedResponse(i, firstPhaseContext, onShortRead),
                                                                 UnaryOperator.identity(),
                                                                 repairedDataTracker);

        // Note that we do not pass a repaired data tracker here, as the second phase does not return digests.
        PartitionIterator completedPartitions = resolveWithReadRepair(secondPhaseContext,
                                                                      () -> new RepairMergeListener(secondPhaseContext.sources, repairedDataTracker),
                                                                      i -> rfp.queryProtectedPartitions(firstPhasePartitions, i),
                                                                      results -> command.rowFilter().filter(results, command.metadata(), command.nowInSec()),
                                                                      null);
        
        // Ensure that the RFP instance has a chance to record metrics when the iterator closes.
        return PartitionIterators.doOnClose(completedPartitions, firstPhasePartitions::close);
    }

    private PartitionIterator resolveInternal(ResolveContext context,
                                              MergeListenerProvider mergeListenerProvider,
                                              ResponseProvider responseProvider,
                                              UnaryOperator<PartitionIterator> preCountFilter,
                                              RepairedDataTracker repairedDataTracker)
    {
        int count = context.sources.length;
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(count);

        for (int i = 0; i < count; i++)
        {
            iters.add(responseProvider.getResponse(i));

            // The repaired data digest may be marked conclusive or inconclusive depending on whether any
            // of the repaired sstables on the replica were involved in pending, but locally committed
            // repair sessions at the time of the query. A response message may contain either a conclusive
            // or inconclusive digest, but not both.
            if (repairedDataTracker != null)
            {
                MessageIn<ReadResponse> msg = responses.get(i);

                if (msg.parameters.containsKey(ReadCommand.CONCLUSIVE_REPAIRED_DATA_DIGEST))
                {
                    repairedDataTracker.recordDigest(msg.from,
                                                     ByteBuffer.wrap(msg.parameters.get(ReadCommand.CONCLUSIVE_REPAIRED_DATA_DIGEST)),
                                                     true);
                }
                else if (msg.parameters.containsKey(ReadCommand.INCONCLUSIVE_REPAIRED_DATA_DIGEST))
                {
                    repairedDataTracker.recordDigest(msg.from,
                                                     ByteBuffer.wrap(msg.parameters.get(ReadCommand.INCONCLUSIVE_REPAIRED_DATA_DIGEST)),
                                                     false);
                }
            }
        }

        /*
         * Even though every response, individually, will honor the limit, it is possible that we will, after the merge,
         * have more rows than the client requested. To make sure that we still conform to the original limit,
         * we apply a top-level post-reconciliation counter to the merged partition iterator.
         *
         * Short read protection logic (ShortReadRowsProtection.moreContents()) relies on this counter to be applied
         * to the current partition to work. For this reason we have to apply the counter transformation before
         * empty partition discard logic kicks in - for it will eagerly consume the iterator.
         *
         * That's why the order here is: 1) merge; 2) filter rows; 3) count; 4) discard empty partitions
         *
         * See CASSANDRA-13747 for more details.
         */

        // If we have only one result, there is no read repair to do and we can't make a comparison between repaired data sets.
        UnfilteredPartitionIterator merged = iters.size() == 1
                                             ? iters.get(0)
                                             : UnfilteredPartitionIterators.merge(iters, command.nowInSec(), mergeListenerProvider.get());
        Filter filter = new Filter(command.nowInSec(), command.metadata().enforceStrictLiveness());
        FilteredPartitions filtered = FilteredPartitions.filter(merged, filter);
        PartitionIterator counted = Transformation.apply(preCountFilter.apply(filtered), context.mergedResultCounter);

        return command.isForThrift()
               ? counted
               : Transformation.apply(counted, new EmptyPartitionsDiscarder());
    }

    protected RepairedDataVerifier getRepairedDataVerifier(ReadCommand command)
    {
        return RepairedDataVerifier.verifier(command);
    }

    private class RepairMergeListener implements UnfilteredPartitionIterators.MergeListener
    {
        private final InetAddress[] sources;
        private final RepairedDataTracker repairedDataTracker;

        private RepairMergeListener(InetAddress[] sources, RepairedDataTracker repairedDataTracker)
        {
            this.sources = sources;
            this.repairedDataTracker = repairedDataTracker;
        }

        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
        {
            return new MergeListener(partitionKey, columns(versions), isReversed(versions));
        }

        private PartitionColumns columns(List<UnfilteredRowIterator> versions)
        {
            Columns statics = Columns.NONE;
            Columns regulars = Columns.NONE;
            for (UnfilteredRowIterator iter : versions)
            {
                if (iter == null)
                    continue;

                PartitionColumns cols = iter.columns();
                statics = statics.mergeTo(cols.statics);
                regulars = regulars.mergeTo(cols.regulars);
            }
            return new PartitionColumns(statics, regulars);
        }

        private boolean isReversed(List<UnfilteredRowIterator> versions)
        {
            for (UnfilteredRowIterator iter : versions)
            {
                if (iter == null)
                    continue;

                // Everything will be in the same order
                return iter.isReverseOrder();
            }

            assert false : "Expected at least one iterator";
            return false;
        }

        public void close()
        {
            if (repairedDataTracker != null)
                repairedDataTracker.verify();
        }

        private class MergeListener implements UnfilteredRowIterators.MergeListener
        {
            private final DecoratedKey partitionKey;
            private final PartitionColumns columns;
            private final boolean isReversed;
            // the updates we will send to each source we read from (or null if none)
            // plus one tail entry for any repair we will send to non-participants of the read phase
            private PartitionUpdate[] repairs;
            // bits, one for each source, indicating if we intend to build a mutation in response (before knowing if one is warranted)
            private BitSet shouldWriteBackTo;
            // true iff writing to node that did not participate in read phase; implies repairs.length == 1 + sources.length
            private boolean shouldBuildFullDiff;
            // the index in sources and repairs of the response from a given address, if any
            private Map<InetAddress, Integer> sourceIdxLookup;
            // those nodes we will initially send any mutations to
            private List<InetAddress> initialRecipients;
            // those nodes we haven't written to, but could if we had to speculate
            private List<InetAddress> additionalRecipients;
            private ConsistencyLevel.ResponseTracker blockFor;

            private final Row.Builder[] currentRows = new Row.Builder[sources.length];
            private final RowDiffListener diffListener;

            // The partition level deletion for the merge row.
            private DeletionTime partitionLevelDeletion;
            // When merged has a currently open marker, its time. null otherwise.
            private DeletionTime mergedDeletionTime;
            // For each source, the time of the current deletion as known by the source.
            private final DeletionTime[] sourceDeletionTime = new DeletionTime[sources.length];
            // For each source, record if there is an open range to send as repair, and from where.
            private final Slice.Bound[] markerToRepair = new Slice.Bound[sources.length];

            private MergeListener(DecoratedKey partitionKey, PartitionColumns columns, boolean isReversed)
            {
                this.partitionKey = partitionKey;
                this.columns = columns;
                this.isReversed = isReversed;

                this.diffListener = new RowDiffListener()
                {
                    public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
                    {
                        if (merged != null && !merged.equals(original))
                            currentRow(i, clustering).addPrimaryKeyLivenessInfo(merged);
                    }

                    public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original)
                    {
                        if (merged != null && !merged.equals(original))
                            currentRow(i, clustering).addRowDeletion(merged);
                    }

                    public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
                    {
                        if (merged != null && !merged.equals(original))
                            currentRow(i, clustering).addComplexDeletion(column, merged);
                    }

                    public void onCell(int i, Clustering clustering, Cell merged, Cell original)
                    {
                        if (merged != null && !merged.equals(original))
                            currentRow(i, clustering).addCell(merged);
                    }
                };
            }

            private void initialise()
            {
                sourceIdxLookup = Maps.newHashMapWithExpectedSize(sources.length);
                for (int i = 0 ; i < sources.length ; ++i)
                    sourceIdxLookup.put(sources[i], i);

                Token token = partitionKey.getToken();
                AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();

                // grab the natural endpoints, since they could have changed
                // (we ignore the possibility they changed during the read portion of read-repair)
                List<InetAddress> candidates = StorageProxy.getLiveSortedEndpoints(rs, token);
                Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(token, keyspace.getName());
                for (InetAddress endpoint : pending)
                    if (FailureDetector.instance.isAlive(endpoint))
                        candidates.add(endpoint);

                blockFor = consistency.trackWrite(rs, candidates, pending);
                int blockForCount = blockFor.blockFor();

                // sort the nodes we contacted originally to the front
                candidates.sort(Comparator.comparingInt(e ->
                    sourceIdxLookup.get(e) != null ? -1 : 0
                ));

                switch (consistency)
                {
                    case EACH_QUORUM:
                        if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                        {
                            initialRecipients = ConsistencyLevel.filterForEachQuorum(keyspace, candidates, ReadRepairDecision.NONE);
                            candidates.removeAll(new HashSet<>(initialRecipients));
                            additionalRecipients = candidates;
                            break;
                        }
                    case LOCAL_QUORUM:
                    case LOCAL_ONE:
                    case LOCAL_SERIAL:
                        if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy)
                            candidates.removeIf(inet -> !ConsistencyLevel.isLocal(inet));

                    default:
                        initialRecipients = candidates.subList(0, blockForCount);
                        additionalRecipients = candidates.subList(blockForCount, candidates.size());
                        break;
                }

                shouldWriteBackTo = new BitSet(sources.length);
                for (InetAddress e : initialRecipients)
                {
                    Integer idx = sourceIdxLookup.get(e);
                    if (idx != null)
                        shouldWriteBackTo.set(idx);
                }

                shouldBuildFullDiff = shouldWriteBackTo.cardinality() < initialRecipients.size();
                repairs = new PartitionUpdate[sources.length + (shouldBuildFullDiff ? 1 : 0)];
            }

            private void applyToPartition(int i, Consumer<PartitionUpdate> apply)
            {
                if (repairs == null)
                    initialise();

                if (shouldWriteBackTo.get(i))
                {
                    if (repairs[i] == null)
                        repairs[i] = new PartitionUpdate(command.metadata(), partitionKey, columns, 1);
                    apply.accept(repairs[i]);
                }

                if (shouldBuildFullDiff)
                {
                    if (repairs[repairs.length - 1] == null)
                        repairs[repairs.length - 1] = new PartitionUpdate(command.metadata(), partitionKey, columns, 1);
                    apply.accept(repairs[repairs.length - 1]);
                }
            }

            /**
             * The partition level deletion with with which source {@code i} is currently repaired, or
             * {@code DeletionTime.LIVE} if the source is not repaired on the partition level deletion (meaning it was
             * up to date on it). The output* of this method is only valid after the call to
             * {@link #onMergedPartitionLevelDeletion}.
             */
            private DeletionTime partitionLevelRepairDeletion(int i)
            {
                if (repairs == null)
                    initialise();

                return repairs[i] == null ? DeletionTime.LIVE : repairs[i].partitionLevelDeletion();
            }

            private Row.Builder currentRow(int i, Clustering clustering)
            {
                if (currentRows[i] == null)
                {
                    currentRows[i] = BTreeRow.sortedBuilder();
                    currentRows[i].newRow(clustering);
                }
                return currentRows[i];
            }

            public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
            {
                this.partitionLevelDeletion = mergedDeletion;
                for (int i = 0; i < versions.length; i++)
                {
                    if (mergedDeletion.supersedes(versions[i]))
                        applyToPartition(i, p -> p.addPartitionDeletion(mergedDeletion));
                }
            }

            public Row onMergedRows(Row merged, Row[] versions)
            {
                // If a row was shadowed post merged, it must be by a partition level or range tombstone, and we handle
                // those case directly in their respective methods (in other words, it would be inefficient to send a row
                // deletion as repair when we know we've already send a partition level or range tombstone that covers it).
                if (merged.isEmpty())
                    return merged;

                Rows.diff(diffListener, merged, versions);
                for (int i = 0; i < currentRows.length; i++)
                {
                    if (currentRows[i] != null)
                    {
                        Row row = currentRows[i].build();
                        applyToPartition(i, p -> p.add(row));
                    }
                }
                Arrays.fill(currentRows, null);

                return merged;
            }

            private DeletionTime currentDeletion()
            {
                return mergedDeletionTime == null ? partitionLevelDeletion : mergedDeletionTime;
            }

            public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
            {
                try
                {
                    // The code for merging range tombstones is a tad complex and we had the assertions there triggered
                    // unexpectedly in a few occasions (CASSANDRA-13237, CASSANDRA-13719). It's hard to get insights
                    // when that happen without more context that what the assertion errors give us however, hence the
                    // catch here that basically gather as much as context as reasonable.
                    internalOnMergedRangeTombstoneMarkers(merged, versions);
                }
                catch (AssertionError e)
                {
                    // The following can be pretty verbose, but it's really only triggered if a bug happen, so we'd
                    // rather get more info to debug than not.
                    CFMetaData table = command.metadata();
                    String details = String.format("Error merging RTs on %s.%s: command=%s, reversed=%b, merged=%s, versions=%s, sources={%s}, responses:%n %s",
                                                   table.ksName, table.cfName,
                                                   command.toCQLString(),
                                                   isReversed,
                                                   merged == null ? "null" : merged.toString(table),
                                                   '[' + Joiner.on(", ").join(Iterables.transform(Arrays.asList(versions), rt -> rt == null ? "null" : rt.toString(table))) + ']',
                                                   Arrays.toString(sources),
                                                   makeResponsesDebugString());
                    throw new AssertionError(details, e);
                }
            }

            private String makeResponsesDebugString()
            {
                return Joiner.on(",\n")
                             .join(Iterables.transform(getMessages(), m -> m.from + " => " + m.payload.toDebugString(command, partitionKey)));
            }

            private void internalOnMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
            {
                // The current deletion as of dealing with this marker.
                DeletionTime currentDeletion = currentDeletion();

                for (int i = 0; i < versions.length; i++)
                {
                    // we are not collecting a mutation for this version/source, skip;
                    if (shouldWriteBackTo != null && !shouldWriteBackTo.get(i))
                        continue;
                    RangeTombstoneMarker marker = versions[i];

                    // Update what the source now thinks is the current deletion
                    if (marker != null)
                        sourceDeletionTime[i] = marker.isOpen(isReversed) ? marker.openDeletionTime(isReversed) : null;

                    // If merged == null, some of the source is opening or closing a marker
                    if (merged == null)
                    {
                        // but if it's not this source, move to the next one
                        if (marker == null)
                            continue;

                        // We have a close and/or open marker for a source, with nothing corresponding in merged.
                        // Because merged is a superset, this implies that we have a current deletion (being it due to an
                        // early opening in merged or a partition level deletion) and that this deletion will still be
                        // active after that point. Further whatever deletion was open or is open by this marker on the
                        // source, that deletion cannot supersedes the current one.
                        //
                        // But while the marker deletion (before and/or after this point) cannot supersede the current
                        // deletion, we want to know if it's equal to it (both before and after), because in that case
                        // the source is up to date and we don't want to include it into repair.
                        //
                        // So in practice we have 2 possible cases:
                        //  1) the source was up-to-date on deletion up to that point: then it won't be from that point
                        //     on unless it's a boundary and the new opened deletion time is also equal to the current
                        //     deletion (note that this implies the boundary has the same closing and opening deletion
                        //     time, which should generally not happen, but can due to legacy reading code not avoiding
                        //     this for a while, see CASSANDRA-13237).
                        //  2) the source wasn't up-to-date on deletion up to that point and it may now be (if it isn't
                        //     we just have nothing to do for that marker).
                        assert !currentDeletion.isLive() : currentDeletion.toString();

                        // Is the source up to date on deletion? It's up to date if it doesn't have an open RT repair
                        // nor an "active" partition level deletion (where "active" means that it's greater or equal
                        // to the current deletion: if the source has a repaired partition deletion lower than the
                        // current deletion, this means the current deletion is due to a previously open range tombstone,
                        // and if the source isn't currently repaired for that RT, then it means it's up to date on it).
                        DeletionTime partitionRepairDeletion = partitionLevelRepairDeletion(i);
                        if (markerToRepair[i] == null && currentDeletion.supersedes(partitionRepairDeletion))
                        {
                            /*
                             * Since there is an ongoing merged deletion, the only way we don't have an open repair for
                             * this source is that it had a range open with the same deletion as current marker,
                             * and the marker is closing it.
                             */
                            assert marker.isClose(isReversed) && currentDeletion.equals(marker.closeDeletionTime(isReversed))
                                 : String.format("currentDeletion=%s, marker=%s", currentDeletion, marker.toString(command.metadata()));

                            // and so unless it's a boundary whose opening deletion time is still equal to the current
                            // deletion (see comment above for why this can actually happen), we have to repair the source
                            // from that point on.
                            if (!(marker.isOpen(isReversed) && currentDeletion.equals(marker.openDeletionTime(isReversed))))
                                markerToRepair[i] = marker.closeBound(isReversed).invert();
                        }
                        // In case 2) above, we only have something to do if the source is up-to-date after that point
                        // (which, since the source isn't up-to-date before that point, means we're opening a new deletion
                        // that is equal to the current one).
                        else
                        {
                            if (markerToRepair[i] == null)
                            {
                                // Only way we can have no open RT repair is that partition deletion that has the same timestamp
                                // as the deletion and same local deletion time. In such case, since partition deletion covers
                                // an entire partition, we do not include it into repair.
                                assert currentDeletion.localDeletionTime() == partitionRepairDeletion.localDeletionTime();
                            }
                            else if (marker.isOpen(isReversed) && currentDeletion.equals(marker.openDeletionTime(isReversed)))
                            {
                                closeOpenMarker(i, marker.openBound(isReversed).invert());
                            }
                        }
                    }
                    else
                    {
                        // We have a change of current deletion in merged (potentially to/from no deletion at all).

                        if (merged.isClose(isReversed))
                        {
                            // We're closing the merged range. If we've marked the source as needing to be repaired for
                            // that range, close and add it to the repair to be sent.
                            if (markerToRepair[i] != null)
                                closeOpenMarker(i, merged.closeBound(isReversed));

                        }

                        if (merged.isOpen(isReversed))
                        {
                            // If we're opening a new merged range (or just switching deletion), then unless the source
                            // is up to date on that deletion (note that we've updated what the source deleteion is
                            // above), we'll have to sent the range to the source.
                            DeletionTime newDeletion = merged.openDeletionTime(isReversed);
                            DeletionTime sourceDeletion = sourceDeletionTime[i];
                            if (!newDeletion.equals(sourceDeletion))
                                markerToRepair[i] = merged.openBound(isReversed);
                        }
                    }
                }

                if (merged != null)
                    mergedDeletionTime = merged.isOpen(isReversed) ? merged.openDeletionTime(isReversed) : null;
            }

            private void closeOpenMarker(int i, Slice.Bound close)
            {
                Slice.Bound open = markerToRepair[i];
                RangeTombstone rt = new RangeTombstone(Slice.make(isReversed ? close : open, isReversed ? open : close), currentDeletion());
                applyToPartition(i, p -> p.add(rt));
                markerToRepair[i] = null;
            }

            public void close()
            {
                if (repairs == null)
                    return;

                boolean hasRepairs = false;
                for (int i = 0 ; !hasRepairs && i < repairs.length ; ++i)
                    hasRepairs = repairs[i] != null;
                if (!hasRepairs)
                    return;

                PartitionUpdate fullDiffRepair = shouldBuildFullDiff ? repairs[repairs.length - 1] : null;
                HashMap<InetAddress, Mutation> mutations = Maps.newHashMapWithExpectedSize(initialRecipients.size());
                for (InetAddress endpoint : initialRecipients)
                {
                    Integer idx = sourceIdxLookup.get(endpoint);
                    PartitionUpdate repair = idx == null ? fullDiffRepair : repairs[idx];

                    Mutation mutation = createRepairMutation(repair, consistency, endpoint, false);
                    if (mutation == null)
                        continue;

                    mutations.put(endpoint, mutation);
                }

                repairPartition(mutations, blockFor, initialRecipients, additionalRecipients);
            }
        }
    }

    /**
     * Create a read repair mutation from the given update, if the mutation is not larger than the maximum
     * mutation size, otherwise return null. Or, if we're configured to be strict, throw an exception.
     */
    public static Mutation createRepairMutation(PartitionUpdate update, ConsistencyLevel consistency, InetAddress destination, boolean suppressException)
    {
        if (update == null)
            return null;

        Mutation mutation = new Mutation(update);
        int messagingVersion = MessagingService.instance().getVersion(destination);

        try
        {
            mutation.validateSize( messagingVersion, 0);
            return mutation;
        }
        catch (MutationExceededMaxSizeException e)
        {
            DecoratedKey key = mutation.key();
            CFMetaData cfm = update.metadata();
            Keyspace keyspace = Keyspace.open(cfm.ksName);
            if (DROP_OVERSIZED_READ_REPAIR_MUTATIONS)
            {
                logger.debug("Encountered an oversized ({}/{}) read repair mutation for table {}.{}, key {}, node {}",
                             e.mutationSize,
                             MAX_MUTATION_SIZE,
                             cfm.ksName,
                             cfm.cfName,
                             cfm.getKeyValidator().getString(key.getKey()),
                             destination);
            }
            else
            {
                logger.warn("Encountered an oversized ({}/{}) read repair mutation for table {}.{}, key {}, node {}",
                            e.mutationSize,
                            MAX_MUTATION_SIZE,
                            cfm.ksName,
                            cfm.cfName,
                            cfm.getKeyValidator().getString(key.getKey()),
                            destination);

                if (!suppressException)
                {
                    int blockFor = consistency.blockFor(keyspace);
                    Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
                    throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
                }
            }
            return null;
        }
    }

    @SuppressWarnings("resource")
    private UnfilteredPartitionIterator extendWithShortReadProtection(UnfilteredPartitionIterator partitions,
                                                                      ResolveContext context,
                                                                      int i,
                                                                      Runnable onShortRead)
    {
        DataLimits.Counter singleResultCounter =
            command.limits().newCounter(command.nowInSec(), false, command.selectsFullPartition(), enforceStrictLiveness).onlyCount();

        // The pre-fetch callback used here makes the initial round of responses for this replica collectable.
        ShortReadPartitionsProtection protection = new ShortReadPartitionsProtection(context.sources[i],
                                                                                     () -> responses.clearUnsafe(i),
                                                                                     singleResultCounter,
                                                                                     context.mergedResultCounter,
                                                                                     onShortRead);

        /*
         * The order of extention and transformations is important here. Extending with more partitions has to happen
         * first due to the way BaseIterator.hasMoreContents() works: only transformations applied after extension will
         * be called on the first partition of the extended iterator.
         *
         * Additionally, we want singleResultCounter to be applied after SRPP, so that its applyToPartition() method will
         * be called last, after the extension done by SRRP.applyToPartition() call. That way we preserve the same order
         * when it comes to calling SRRP.moreContents() and applyToRow() callbacks.
         *
         * See ShortReadPartitionsProtection.applyToPartition() for more details.
         */

        // extend with moreContents() only if it's a range read command with no partition key specified
        if (!command.isLimitedToOnePartition())
            partitions = MorePartitions.extend(partitions, protection);     // register SRPP.moreContents()

        partitions = Transformation.apply(partitions, protection);          // register SRPP.applyToPartition()
        partitions = Transformation.apply(partitions, singleResultCounter); // register the per-source counter

        return partitions;
    }

    /*
     * We have a potential short read if the result from a given node contains the requested number of rows
     * (i.e. it has stopped returning results due to the limit), but some of them haven't
     * made it into the final post-reconciliation result due to other nodes' row, range, and/or partition tombstones.
     *
     * If that is the case, then that node may have more rows that we should fetch, as otherwise we could
     * ultimately return fewer rows than required. Also, those additional rows may contain tombstones which
     * which we also need to fetch as they may shadow rows or partitions from other replicas' results, which we would
     * otherwise return incorrectly.
     */
    private class ShortReadPartitionsProtection extends Transformation<UnfilteredRowIterator> implements MorePartitions<UnfilteredPartitionIterator>
    {
        private final InetAddress source;
        private final Runnable preFetchCallback; // called immediately before fetching more contents

        private final DataLimits.Counter singleResultCounter; // unmerged per-source counter
        private final DataLimits.Counter mergedResultCounter; // merged end-result counter
        private final Runnable onShortRead;
        private DecoratedKey lastPartitionKey; // key of the last observed partition

        private boolean partitionsFetched; // whether we've seen any new partitions since iteration start or last moreContents() call

        private ShortReadPartitionsProtection(InetAddress source, 
                                              Runnable preFetchCallback, 
                                              DataLimits.Counter singleResultCounter, 
                                              DataLimits.Counter mergedResultCounter, 
                                              Runnable onShortRead)
        {
            this.source = source;
            this.preFetchCallback = preFetchCallback;
            this.singleResultCounter = singleResultCounter;
            this.mergedResultCounter = mergedResultCounter;
            this.onShortRead = onShortRead;
        }

        @Override
        public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            partitionsFetched = true;

            lastPartitionKey = partition.partitionKey();

            /*
             * Extend for moreContents() then apply protection to track lastClustering by applyToRow().
             *
             * If we don't apply the transformation *after* extending the partition with MoreRows,
             * applyToRow() method of protection will not be called on the first row of the new extension iterator.
             */
            ShortReadRowsProtection protection = new ShortReadRowsProtection(partition.metadata(), partition.partitionKey());
            return Transformation.apply(MoreRows.extend(partition, protection), protection);
        }

        /*
         * We only get here once all the rows and partitions in this iterator have been iterated over, and so
         * if the node had returned the requested number of rows but we still get here, then some results were
         * skipped during reconciliation.
         */
        public UnfilteredPartitionIterator moreContents()
        {
            // never try to request additional partitions from replicas if our reconciled partitions are already filled to the limit
            assert !mergedResultCounter.isDone();

            // we do not apply short read protection when we have no limits at all
            assert !command.limits().isUnlimited();

            /*
             * If this is a single partition read command or an (indexed) partition range read command with
             * a partition key specified, then we can't and shouldn't try fetch more partitions.
             */
            assert !command.isLimitedToOnePartition();

            /*
             * If the returned result doesn't have enough rows/partitions to satisfy even the original limit, don't ask for more.
             *
             * Can only take the short cut if there is no per partition limit set. Otherwise it's possible to hit false
             * positives due to some rows being uncounted for in certain scenarios (see CASSANDRA-13911).
             */
            if (!singleResultCounter.isDone() && command.limits().perPartitionCount() == DataLimits.NO_LIMIT)
                return null;

            /*
             * Either we had an empty iterator as the initial response, or our moreContents() call got us an empty iterator.
             * There is no point to ask the replica for more rows - it has no more in the requested range.
             */
            if (!partitionsFetched)
                return null;
            partitionsFetched = false;

            /*
             * We are going to fetch one partition at a time for thrift and potentially more for CQL.
             * The row limit will either be set to the per partition limit - if the command has no total row limit set, or
             * the total # of rows remaining - if it has some. If we don't grab enough rows in some of the partitions,
             * then future ShortReadRowsProtection.moreContents() calls will fetch the missing ones.
             */
            int toQuery = command.limits().count() != DataLimits.NO_LIMIT
                        ? command.limits().count() - mergedResultCounter.counted()
                        : command.limits().perPartitionCount();

            ColumnFamilyStore.metricsFor(command.metadata().cfId).shortReadProtectionRequests.mark();
            Tracing.trace("Requesting {} extra rows from {} for short read protection", toQuery, source);

            // If we've arrived here, all responses have been consumed, and we're about to request more.
            preFetchCallback.run();
            
            PartitionRangeReadCommand cmd = makeFetchAdditionalPartitionReadCommand(toQuery);
            return executeReadCommand(cmd);
        }

        private PartitionRangeReadCommand makeFetchAdditionalPartitionReadCommand(int toQuery)
        {
            PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;

            DataLimits newLimits = cmd.limits().forShortReadRetry(toQuery);

            AbstractBounds<PartitionPosition> bounds = cmd.dataRange().keyRange();
            AbstractBounds<PartitionPosition> newBounds = bounds.inclusiveRight()
                                                        ? new Range<>(lastPartitionKey, bounds.right)
                                                        : new ExcludingBounds<>(lastPartitionKey, bounds.right);
            DataRange newDataRange = cmd.dataRange().forSubRange(newBounds);

            return cmd.withUpdatedLimitsAndDataRange(newLimits, newDataRange);
        }

        private class ShortReadRowsProtection extends Transformation<UnfilteredRowIterator> implements MoreRows<UnfilteredRowIterator>
        {
            private final CFMetaData metadata;
            private final DecoratedKey partitionKey;

            private Clustering lastClustering; // clustering of the last observed row

            private int lastCounted = 0; // last seen recorded # before attempting to fetch more rows
            private int lastFetched = 0; // # rows returned by last attempt to get more (or by the original read command)
            private int lastQueried = 0; // # extra rows requested from the replica last time

            private ShortReadRowsProtection(CFMetaData metadata, DecoratedKey partitionKey)
            {
                this.metadata = metadata;
                this.partitionKey = partitionKey;
            }

            @Override
            public Row applyToRow(Row row)
            {
                lastClustering = row.clustering();
                return row;
            }

            /*
             * We only get here once all the rows in this iterator have been iterated over, and so if the node
             * had returned the requested number of rows but we still get here, then some results were skipped
             * during reconciliation.
             */
            public UnfilteredRowIterator moreContents()
            {
                // never try to request additional rows from replicas if our reconciled partition is already filled to the limit
                assert !mergedResultCounter.isDoneForPartition();

                // we do not apply short read protection when we have no limits at all
                assert !command.limits().isUnlimited();

                /*
                 * If the returned partition doesn't have enough rows to satisfy even the original limit, don't ask for more.
                 *
                 * Can only take the short cut if there is no per partition limit set. Otherwise it's possible to hit false
                 * positives due to some rows being uncounted for in certain scenarios (see CASSANDRA-13911).
                 */
                if (!singleResultCounter.isDoneForPartition() && command.limits().perPartitionCount() == DataLimits.NO_LIMIT)
                    return null;

                /*
                 * If the replica has no live rows in the partition, don't try to fetch more.
                 *
                 * Note that the previous branch [if (!singleResultCounter.isDoneForPartition()) return null] doesn't
                 * always cover this scenario:
                 * isDoneForPartition() is defined as [isDone() || rowInCurrentPartition >= perPartitionLimit],
                 * and will return true if isDone() returns true, even if there are 0 rows counted in the current partition.
                 *
                 * This can happen with a range read if after 1+ rounds of short read protection requests we managed to fetch
                 * enough extra rows for other partitions to satisfy the singleResultCounter's total row limit, but only
                 * have tombstones in the current partition.
                 *
                 * One other way we can hit this condition is when the partition only has a live static row and no regular
                 * rows. In that scenario the counter will remain at 0 until the partition is closed - which happens after
                 * the moreContents() call.
                 */
                if (singleResultCounter.countedInCurrentPartition() == 0)
                    return null;

                /*
                 * This is a table with no clustering columns, and has at most one row per partition - with EMPTY clustering.
                 * We already have the row, so there is no point in asking for more from the partition.
                 */
                if (Clustering.EMPTY == lastClustering)
                    return null;

                lastFetched = singleResultCounter.countedInCurrentPartition() - lastCounted;
                lastCounted = singleResultCounter.countedInCurrentPartition();

                // getting back fewer rows than we asked for means the partition on the replica has been fully consumed
                if (lastQueried > 0 && lastFetched < lastQueried)
                    return null;
                /*
                 * At this point we know that:
                 *     1. the replica returned [repeatedly?] as many rows as we asked for and potentially has more
                 *        rows in the partition
                 *     2. at least one of those returned rows was shadowed by a tombstone returned from another
                 *        replica
                 *     3. we haven't satisfied the client's limits yet, and should attempt to query for more rows to
                 *        avoid a short read
                 *
                 * In the ideal scenario, we would get exactly min(a, b) or fewer rows from the next request, where a and b
                 * are defined as follows:
                 *     [a] limits.count() - mergedResultCounter.counted()
                 *     [b] limits.perPartitionCount() - mergedResultCounter.countedInCurrentPartition()
                 *
                 * It would be naive to query for exactly that many rows, as it's possible and not unlikely
                 * that some of the returned rows would also be shadowed by tombstones from other hosts.
                 *
                 * Note: we don't know, nor do we care, how many rows from the replica made it into the reconciled result;
                 * we can only tell how many in total we queried for, and that [0, mrc.countedInCurrentPartition()) made it.
                 *
                 * In general, our goal should be to minimise the number of extra requests - *not* to minimise the number
                 * of rows fetched: there is a high transactional cost for every individual request, but a relatively low
                 * marginal cost for each extra row requested.
                 *
                 * As such it's better to overfetch than to underfetch extra rows from a host; but at the same
                 * time we want to respect paging limits and not blow up spectacularly.
                 *
                 * Note: it's ok to retrieve more rows that necessary since singleResultCounter is not stopping and only
                 * counts.
                 *
                 * With that in mind, we'll just request the minimum of (count(), perPartitionCount()) limits.
                 *
                 * See CASSANDRA-13794 for more details.
                 */
                lastQueried = Math.min(command.limits().count(), command.limits().perPartitionCount());

                ColumnFamilyStore.metricsFor(metadata.cfId).shortReadProtectionRequests.mark();
                Tracing.trace("Requesting {} extra rows from {} for short read protection", lastQueried, source);
                onShortRead.run();

                SinglePartitionReadCommand cmd = makeFetchAdditionalRowsReadCommand(lastQueried);
                return UnfilteredPartitionIterators.getOnlyElement(executeReadCommand(cmd), cmd);
            }

            private SinglePartitionReadCommand makeFetchAdditionalRowsReadCommand(int toQuery)
            {
                ClusteringIndexFilter filter = command.clusteringIndexFilter(partitionKey);
                if (null != lastClustering)
                    filter = filter.forPaging(metadata.comparator, lastClustering, false);

                return SinglePartitionReadCommand.create(command.isForThrift(),
                                                         command.metadata(),
                                                         command.nowInSec(),
                                                         command.columnFilter(),
                                                         command.rowFilter(),
                                                         command.limits().forShortReadRetry(toQuery),
                                                         partitionKey,
                                                         filter,
                                                         command.indexMetadata());
            }
        }

        private UnfilteredPartitionIterator executeReadCommand(ReadCommand cmd)
        {
            DataResolver resolver = new DataResolver(keyspace, cmd, ConsistencyLevel.ONE, 1);
            ReadCallback handler = new ReadCallback(resolver, ConsistencyLevel.ONE, cmd, Collections.singletonList(source));

            if (StorageProxy.canDoLocalRequest(source))
                StageManager.getStage(Stage.READ).maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(cmd, handler));
            else
                MessagingService.instance().sendRRWithFailure(cmd.createMessage(MessagingService.current_version), source, handler);

            // We don't call handler.get() because we want to preserve tombstones since we're still in the middle of merging node results.
            handler.awaitResults();
            assert resolver.responses.size() == 1;
            return resolver.responses.get(0).payload.makeIterator(command);
        }
    }
}
