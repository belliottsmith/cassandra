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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.transform.DuplicateRowChecker;
import org.apache.cassandra.exceptions.IndexSizeAbortException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.TombstoneAbortException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.db.ReadCommand.INDEX_SIZE_ABORT;
import static org.apache.cassandra.db.ReadCommand.INDEX_SIZE_WARNING;
import static org.apache.cassandra.db.ReadCommand.TOMBSTONE_ABORT;
import static org.apache.cassandra.db.ReadCommand.TOMBSTONE_WARNING;

public class ReadCallback implements IAsyncCallbackWithFailure<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );
    private class WarningCounter
    {
        // the highest number of tombstones reported by a node's warning
        final AtomicInteger tombstoneWarnings = new AtomicInteger();
        final AtomicInteger maxTombstoneWarningCount = new AtomicInteger();
        // the highest number of tombstones reported by a node's rejection. This should be the same as
        // our configured limit, but including to aid in diagnosing misconfigurations
        final AtomicInteger tombstoneAborts = new AtomicInteger();
        final AtomicInteger maxTombstoneAbortsCount = new AtomicInteger();

        // the largest size index we've seen a warning for
        final AtomicInteger indexSizeWarnings = new AtomicInteger();
        final AtomicLong maxIndexWarningSize = new AtomicLong();
        // the largest size index a read has been aborted over
        final AtomicInteger indexSizeAborts = new AtomicInteger();
        final AtomicLong maxIndexAbortSize = new AtomicLong();

        // TODO: take message as arg and return boolean for 'had warning' etc
        void addTombstoneWarning(InetAddress from, byte[] bytes)
        {
            if (!waitingFor(from)) return;
            tombstoneWarnings.incrementAndGet();
            maxTombstoneWarningCount.accumulateAndGet(ByteArrayUtil.getInt(bytes), Math::max);
        }

        void addTombstoneAbort(InetAddress from, byte[] bytes)
        {
            if (!waitingFor(from)) return;
            tombstoneAborts.incrementAndGet();
            maxTombstoneAbortsCount.accumulateAndGet(ByteArrayUtil.getInt(bytes), Math::max);
        }

        void addIndexSizeWarning(InetAddress from, byte[] bytes)
        {
            if (!waitingFor(from)) return;
            indexSizeWarnings.incrementAndGet();
            maxIndexWarningSize.accumulateAndGet(ByteArrayUtil.getLong(bytes), Math::max);
        }

        void addIndexSizeAbort(InetAddress from, byte[] bytes)
        {
            if (!waitingFor(from)) return;
            indexSizeAborts.incrementAndGet();
            maxIndexAbortSize.accumulateAndGet(ByteArrayUtil.getLong(bytes), Math::max);
        }
    }

    public final ResponseResolver resolver;
    final SimpleCondition condition = new SimpleCondition();
    private final long start;
    final int blockfor;
    final List<InetAddress> endpoints;
    private final ReadCommand command;
    private final ConsistencyLevel consistencyLevel;
    private static final AtomicIntegerFieldUpdater<ReadCallback> recievedUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "received");
    private volatile int received = 0;
    private static final AtomicIntegerFieldUpdater<ReadCallback> failuresUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "failures");
    private volatile int failures = 0;

    private volatile WarningCounter warningCounter;
    private static final AtomicReferenceFieldUpdater<ReadCallback, WarningCounter> warningsUpdater
    = AtomicReferenceFieldUpdater.newUpdater(ReadCallback.class, WarningCounter.class, "warningCounter");
    private final Keyspace keyspace; // TODO push this into ConsistencyLevel?

    /**
     * Constructor when response count has to be calculated and blocked for.
     */
    public ReadCallback(ResponseResolver resolver, ConsistencyLevel consistencyLevel, ReadCommand command, List<InetAddress> filteredEndpoints)
    {
        this(resolver,
             consistencyLevel,
             consistencyLevel.blockFor(Keyspace.open(command.metadata().ksName)),
             command,
             Keyspace.open(command.metadata().ksName),
             filteredEndpoints);
    }

    public ReadCallback(ResponseResolver resolver, ConsistencyLevel consistencyLevel, int blockfor, ReadCommand command, Keyspace keyspace, List<InetAddress> endpoints)
    {
        this.command = command;
        this.keyspace = keyspace;
        this.blockfor = blockfor;
        this.consistencyLevel = consistencyLevel;
        this.resolver = resolver;
        this.start = System.nanoTime();
        this.endpoints = endpoints;
        // we don't support read repair (or rapid read protection) for range scans yet (CASSANDRA-6897)
        assert !(command instanceof PartitionRangeReadCommand) || blockfor >= endpoints.size();

        if (logger.isTraceEnabled())
            logger.trace(String.format("Blockfor is %s; setting up requests to %s", blockfor, StringUtils.join(this.endpoints, ",")));
    }

    public boolean await(long timePastStart, TimeUnit unit)
    {
        long time = unit.toNanos(timePastStart) - (System.nanoTime() - start);
        try
        {
            return condition.await(time, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }

    @VisibleForTesting
    public static String tombstoneAbortMessage(int nodes, int tombstones, String cql)
    {
        return String.format("%s nodes scanned over %s tombstones and aborted the query %s (see tombstone_failure_threshold)", nodes, tombstones, cql);
    }

    @VisibleForTesting
    public static String tombstoneWarnMessage(int nodes, int tombstones, String cql)
    {
        return String.format("%s nodes scanned up to %s tombstones and issued tombstone warnings for query %s  (see tombstone_warn_threshold)", nodes, tombstones, cql);
    }

    @VisibleForTesting
    public static String indexSizeAbortMessage(int nodes, long maxIndexSize, String cql)
    {
        return String.format("%s nodes encountered estimated materialized row index sizes of up to %sb and aborted the query %s", nodes, maxIndexSize, cql);
    }

    @VisibleForTesting
    public static String indexSizeWarnMessage(int nodes, long maxIndexSize, String cql)
    {
        return String.format("%s nodes encountered materialized row index sizes of up to %sb and issued index size warnings for query %s", nodes, maxIndexSize, cql);
    }

    private ColumnFamilyStore cfs()
    {
        return Schema.instance.getColumnFamilyStoreInstance(command.metadata().cfId);
    }

    public void awaitResults() throws ReadFailureException, ReadTimeoutException
    {
        boolean signaled = await(command.getTimeout(), TimeUnit.MILLISECONDS);
        boolean failed = blockfor + failures > endpoints.size();

        WarningCounter warnings = warningCounter;
        if (warnings != null)
        {
            if (warnings.tombstoneAborts.get() > 0)
            {
                String msg = tombstoneAbortMessage(warnings.tombstoneAborts.get(), warnings.maxTombstoneAbortsCount.get(), command.toCQLString());
                ClientWarn.instance.warn(msg + " with " + command.loggableTokens());
                logger.warn("{} with query {}", msg, command.toCQLString());
                cfs().metric.clientTombstoneAborts.mark();
            }

            if (warnings.tombstoneWarnings.get() > 0)
            {
                String msg = tombstoneWarnMessage(warnings.tombstoneWarnings.get(), warnings.maxTombstoneWarningCount.get(), command.toCQLString());
                ClientWarn.instance.warn(msg + " with " + command.loggableTokens());
                logger.warn("{} with query {}", msg, command.toCQLString());
                cfs().metric.clientTombstoneWarnings.mark();
            }

            if (warnings.indexSizeAborts.get() > 0)
            {
                String msg = indexSizeAbortMessage(warnings.indexSizeAborts.get(), warnings.maxIndexAbortSize.get(), command.toCQLString());
                ClientWarn.instance.warn(msg + " with " + command.loggableTokens());
                logger.warn("{} with query {}", msg, command.toCQLString());
                cfs().metric.clientIndexSizeAborts.mark();
            }

            if (warnings.indexSizeWarnings.get() > 0)
            {
                String msg = indexSizeWarnMessage(warnings.indexSizeWarnings.get(), warnings.maxIndexWarningSize.get(), command.toCQLString());
                ClientWarn.instance.warn(msg + " with " + command.loggableTokens());
                logger.warn("{} with query {}", msg, command.toCQLString());
                cfs().metric.clientIndexSizeWarnings.mark();
            }
        }

        if (signaled && !failed)
            return;

        if (Tracing.isTracing())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            Tracing.trace("{}; received {} of {} responses{}", (failed ? "Failed" : "Timed out"), received, blockfor, gotData);
        }
        else if (logger.isDebugEnabled())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            logger.debug("{}; received {} of {} responses{}", (failed ? "Failed" : "Timed out"), received, blockfor, gotData);
        }

        if (warnings != null && warnings.tombstoneAborts.get() > 0)
            throw new TombstoneAbortException(warnings.tombstoneAborts.get(), warnings.maxTombstoneAbortsCount.get(), command.toCQLString(), resolver.isDataPresent(), consistencyLevel, received, failures, blockfor);

        if (warnings != null && warnings.indexSizeAborts.get() > 0)
            throw new IndexSizeAbortException(warnings.indexSizeAborts.get(), warnings.maxIndexAbortSize.get(), command.toCQLString(), resolver.isDataPresent(), consistencyLevel, received, failures, blockfor);

        // Same as for writes, see AbstractWriteResponseHandler
        throw failed
            ? new ReadFailureException(consistencyLevel, received, failures, blockfor, resolver.isDataPresent())
            : new ReadTimeoutException(consistencyLevel, received, blockfor, resolver.isDataPresent());
    }

    public PartitionIterator get() throws ReadFailureException, ReadTimeoutException, DigestMismatchException
    {
        awaitResults();

        PartitionIterator result = blockfor == 1 ? resolver.getData() : resolver.resolve();
        if (logger.isTraceEnabled())
            logger.trace("Read: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        return DuplicateRowChecker.duringRead(result, endpoints);
    }

    public int blockFor()
    {
        return blockfor;
    }

    private WarningCounter getWarningCounter()
    {
        WarningCounter current;
        do {

            current = warningCounter;
            if (current != null)
                return current;

            current = new WarningCounter();
        } while (!warningsUpdater.compareAndSet(this, null, current));
        return current;
    }

    public void response(MessageIn<ReadResponse> message)
    {
        if (message.parameters.containsKey(TOMBSTONE_ABORT))
        {
            getWarningCounter().addTombstoneAbort(message.from, message.parameters.get(TOMBSTONE_ABORT));
            onFailure(message.from);
            return;
        }
        else if (message.parameters.containsKey(TOMBSTONE_WARNING))
        {
            getWarningCounter().addTombstoneWarning(message.from, message.parameters.get(TOMBSTONE_WARNING));
        }

        if (message.parameters.containsKey(INDEX_SIZE_ABORT))
        {
            getWarningCounter().addIndexSizeAbort(message.from, message.parameters.get(INDEX_SIZE_ABORT));
            onFailure(message.from);
            return;
        }
        else if (message.parameters.containsKey(INDEX_SIZE_WARNING))
        {
            getWarningCounter().addIndexSizeWarning(message.from, message.parameters.get(INDEX_SIZE_WARNING));
        }

        resolver.preprocess(message);
        int n = waitingFor(message.from)
              ? recievedUpdater.incrementAndGet(this)
              : received;

        /*
         * Ensure that data is present and the response accumulator has properly published the
         * responses it has received. This may result in not signaling immediately when we receive
         * the minimum number of required results, but it guarantees at least the minimum will
         * be accessible when we do signal. (see rdar://77320313)
         */
        if (n >= blockfor && resolver.responses.size() >= blockfor && resolver.isDataPresent())
        {
            condition.signalAll();
            // kick off a background digest comparison if this is a result that (may have) arrived after
            // the original resolve that get() kicks off as soon as the condition is signaled
            if (blockfor < endpoints.size() && n == endpoints.size())
            {
                TraceState traceState = Tracing.instance.get();
                if (traceState != null)
                    traceState.trace("Initiating read-repair");
                StageManager.getStage(Stage.READ_REPAIR).execute(new AsyncRepairRunner(traceState));
            }
        }
    }

    /**
     * @return true if the message counts towards the blockfor threshold
     */
    private boolean waitingFor(InetAddress from)
    {
        return !consistencyLevel.isDatacenterLocal() || DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from));
    }

    /**
     * @return the current number of received responses
     */
    public int getReceivedCount()
    {
        return received;
    }

    public void response(ReadResponse result)
    {
        MessageIn<ReadResponse> message = MessageIn.create(FBUtilities.getBroadcastAddress(),
                                                           result,
                                                           MessageParams.getParams(),
                                                           MessagingService.Verb.INTERNAL_RESPONSE,
                                                           MessagingService.current_version);
        response(message);
    }

    public void assureSufficientLiveNodes(AbstractReplicationStrategy replicationStrategy) throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(replicationStrategy, endpoints);
    }

    public boolean isLatencyForSnitch()
    {
        return true;
    }

    private class AsyncRepairRunner implements Runnable
    {
        private final TraceState traceState;

        public AsyncRepairRunner(TraceState traceState)
        {
            this.traceState = traceState;
        }

        public void run()
        {
            // If the resolver is a DigestResolver, we need to do a full data read if there is a mismatch.
            // Otherwise, resolve will send the repairs directly if needs be (and in that case we should never
            // get a digest mismatch).
            try
            {
                resolver.compareResponses();
            }
            catch (DigestMismatchException e)
            {
                assert resolver instanceof DigestResolver;

                if (traceState != null)
                    traceState.trace("Digest mismatch: {}", e.toString());
                if (logger.isDebugEnabled())
                    logger.debug("Digest mismatch:", e);
                
                ReadRepairMetrics.repairedBackground.mark();
                // if enabled, request additional info about repaired data from replicas
                if (DatabaseDescriptor.getRepairedDataTrackingForPartitionReadsEnabled())
                    command.trackRepairedStatus();

                final DataResolver repairResolver = new DataResolver(keyspace, command, consistencyLevel, endpoints.size());
                AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

                for (InetAddress endpoint : endpoints)
                {
                    MessageOut<ReadCommand> message = command.createMessage(MessagingService.instance().getVersion(endpoint));
                    MessagingService.instance().sendRR(message, endpoint, repairHandler);
                }
            }
        }
    }

    @Override
    public void onFailure(InetAddress from)
    {
        int n = waitingFor(from)
              ? failuresUpdater.incrementAndGet(this)
              : failures;

        if (blockfor + n > endpoints.size())
            condition.signalAll();
    }
}
