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
package org.apache.cassandra.dht;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.EndpointsByRange;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairedRangesRequest;
import org.apache.cassandra.repair.messages.UpdateRepairedRanges;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifierSupport;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.net.MessagingService;

public class BootStrapper extends ProgressEventNotifierSupport
{
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);

    /* endpoint that needs to be bootstrapped */
    protected final InetAddressAndPort address;
    /* token of the node being bootstrapped. */
    protected final Collection<Token> tokens;
    protected final TokenMetadata tokenMetadata;

    public BootStrapper(InetAddressAndPort address, Collection<Token> tokens, TokenMetadata tmd)
    {
        assert address != null;
        assert tokens != null && !tokens.isEmpty();

        this.address = address;
        this.tokens = tokens;
        this.tokenMetadata = tmd;
    }

    public ListenableFuture<StreamState> bootstrap(StreamStateStore stateStore, boolean useStrictConsistency)
    {
        logger.trace("Beginning bootstrap process");

        RangeStreamer streamer = new RangeStreamer(tokenMetadata,
                                                   tokens,
                                                   address,
                                                   StreamOperation.BOOTSTRAP,
                                                   useStrictConsistency,
                                                   DatabaseDescriptor.getEndpointSnitch(),
                                                   stateStore,
                                                   true,
                                                   DatabaseDescriptor.getStreamingConnectionsPerHost());
        final List<String> nonLocalStrategyKeyspaces = Schema.instance.getNonLocalStrategyKeyspaces();
        if (nonLocalStrategyKeyspaces.isEmpty())
            logger.debug("Schema does not contain any non-local keyspaces to stream on bootstrap");
        for (String keyspaceName : nonLocalStrategyKeyspaces)
        {
            AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
            streamer.addRanges(keyspaceName, strategy.getPendingAddressRanges(tokenMetadata, tokens, address));
        }

        StreamResultFuture bootstrapStreamResult = streamer.fetchAsync();
        bootstrapStreamResult.addEventListener(new StreamEventHandler()
        {
            private final AtomicInteger receivedFiles = new AtomicInteger();
            private final AtomicInteger totalFilesToReceive = new AtomicInteger();

            @Override
            public void handleStreamEvent(StreamEvent event)
            {
                switch (event.eventType)
                {
                    case STREAM_PREPARED:
                        StreamEvent.SessionPreparedEvent prepared = (StreamEvent.SessionPreparedEvent) event;
                        int currentTotal = totalFilesToReceive.addAndGet((int) prepared.session.getTotalFilesToReceive());
                        ProgressEvent prepareProgress = new ProgressEvent(ProgressEventType.PROGRESS, receivedFiles.get(), currentTotal, "prepare with " + prepared.session.peer + " complete");
                        fireProgressEvent("bootstrap", prepareProgress);
                        break;

                    case FILE_PROGRESS:
                        StreamEvent.ProgressEvent progress = (StreamEvent.ProgressEvent) event;
                        if (progress.progress.isCompleted())
                        {
                            int received = receivedFiles.incrementAndGet();
                            ProgressEvent currentProgress = new ProgressEvent(ProgressEventType.PROGRESS, received, totalFilesToReceive.get(), "received file " + progress.progress.fileName);
                            fireProgressEvent("bootstrap", currentProgress);
                        }
                        break;

                    case STREAM_COMPLETE:
                        StreamEvent.SessionCompleteEvent completeEvent = (StreamEvent.SessionCompleteEvent) event;
                        ProgressEvent completeProgress = new ProgressEvent(ProgressEventType.PROGRESS, receivedFiles.get(), totalFilesToReceive.get(), "session with " + completeEvent.peer + " complete");
                        fireProgressEvent("bootstrap", completeProgress);
                        break;
                }
            }

            @Override
            public void onSuccess(StreamState streamState)
            {
                ProgressEventType type;
                String message;

                if (streamState.hasFailedSession())
                {
                    type = ProgressEventType.ERROR;
                    message = "Some bootstrap stream failed";
                }
                else
                {
                    type = ProgressEventType.SUCCESS;
                    message = "Bootstrap streaming success";
                }
                ProgressEvent currentProgress = new ProgressEvent(type, receivedFiles.get(), totalFilesToReceive.get(), message);
                fireProgressEvent("bootstrap", currentProgress);
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                ProgressEvent currentProgress = new ProgressEvent(ProgressEventType.ERROR, receivedFiles.get(), totalFilesToReceive.get(), throwable.getMessage());
                fireProgressEvent("bootstrap", currentProgress);
            }
        });

        // Run `fetchRepairedRanges` on the completion of bootstrapStreamResult.
        // If bootstrapStreamResult is failed, the function is skipped.
        Function<StreamState, StreamState> repairedRangesFetcher = streamState ->
        {
            streamer.fetchRepairedRanges(Integer.MAX_VALUE);
            return streamState;
        };
        return Futures.transform(bootstrapStreamResult, repairedRangesFetcher, MoreExecutors.directExecutor());
    }

    public static volatile boolean stopFetchingRepairRanges = false;
    /**
     * General idea is that after we bootstrap the new node, we ask the nodes we were streaming ranges from for
     * the data in their repair_history system cf.
     *
     * We essentially ask for "give me the repair info for the intersection of the ranges we just bootstrapped"
     *
     * so, in a 3 node cluster, A, B, C, we replace C with D, and bootstrap range x->y from A, we then ask A for the repair info
     * that intersects range x->y, meaning that if A had repair info about range x -> y2 (where y2 > y), we will only
     * store repair info on D for x -> y
     *
     * @param bootstrappedRanges
     */
    public static void fetchRepairedRanges(Map<String, Multimap<InetAddressAndPort, RangeStreamer.FetchReplica>> bootstrappedRanges, int retriesAllowed)
    {
        //We dont want to keep it remain disabled while C* is running. This can cause move/decom to not get newer ranges.
        stopFetchingRepairRanges = false;
        for (String table : bootstrappedRanges.keySet())
        {
            Multimap<InetAddressAndPort, RangeStreamer.FetchReplica> tableRange = bootstrappedRanges.get(table);
            tableRange.forEach((InetAddressAndPort endPointToFetch, RangeStreamer.FetchReplica fetchReplica) ->
            {
                boolean failed = true;
                int retries = 0;
                while(failed && retries <= retriesAllowed && !stopFetchingRepairRanges)
                {
                    retries++;

                    failed = !fetchRepairedRanges(table, fetchReplica.remote.range(), endPointToFetch);

                    //Try to get ranges from other endpoints
                    if(failed)
                    {
                        failed = !fetchRepairedRanges(table, fetchReplica);
                    }
                }

                if(failed)
                {
                    logger.error("Failed to fetch repaired ranges from {} for keyspace {}. Giving up. ", endPointToFetch, table);
                }
            });
        }
    }

    private static boolean fetchRepairedRanges(String table, RangeStreamer.FetchReplica fetchReplica)
    {
        AbstractReplicationStrategy strat = Keyspace.open(table).getReplicationStrategy();
        EndpointsByRange endpointsByRange = strat.getRangeAddresses();

        //Get Endpoints to fetch this from
        Range<Token> tokenRange = fetchReplica.remote.range();
        InetAddressAndPort address = fetchReplica.remote.endpoint();
        if(FBUtilities.getLocalAddressAndPort().equals(address))
            return true;
        if(FailureDetector.instance.isAlive(address))
        {
            fetchRepairedRanges(table, fetchReplica.remote.range(), address);
            return true;
        }
        else
        {
            logger.error("Could not get repaired ranges for {}", tokenRange);
            return false;
        }
    }


    private static boolean fetchRepairedRanges(String table, Range<Token> tokens, InetAddressAndPort endPointToFetch)
    {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        int timeoutInMin = 1;
        logger.info("Fetching repaired ranges ({}) from {} for keyspace {}", tokens, endPointToFetch, table);

        MessagingService.instance().sendWithCallback(Message.out(Verb.APPLE_REPAIRED_RANGES_REQ, new RepairedRangesRequest(table, Collections.singleton(tokens))), endPointToFetch, new RequestCallback<UpdateRepairedRanges>()
        {
            public void onResponse(Message<UpdateRepairedRanges> msg)
            {
                for (Map.Entry<String, Map<Range<Token>, Integer>> cfRanges : msg.payload.perCfRanges.entrySet())
                {
                    ColumnFamilyStore cf = Keyspace.open(msg.payload.keyspace).getColumnFamilyStore(cfRanges.getKey());
                    for (Map.Entry<Range<Token>, Integer> repairRange : cfRanges.getValue().entrySet())
                    {
                        cf.updateLastSuccessfulRepair(repairRange.getKey(), 1000L * repairRange.getValue());
                    }
                }
                result.complete(true);
            }
        });
        try
        {
            return result.get(timeoutInMin, TimeUnit.MINUTES);
        }
        catch (TimeoutException | ExecutionException | InterruptedException e)
        {
            logger.error("Could not fetch repaired ranges from {} for keyspace {}", endPointToFetch, table);
            return false;
        }
    }


    /**
     * if initialtoken was specified, use that (split on comma).
     * otherwise, if allocationKeyspace is specified use the token allocation algorithm to generate suitable tokens
     * else choose num_tokens tokens at random
     */
    public static Collection<Token> getBootstrapTokens(final TokenMetadata metadata, InetAddressAndPort address, long schemaWaitDelay) throws ConfigurationException
    {
        String allocationKeyspace = DatabaseDescriptor.getAllocateTokensForKeyspace();
        Integer allocationLocalRf = DatabaseDescriptor.getAllocateTokensForLocalRf();
        Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
        if (initialTokens.size() > 0 && allocationKeyspace != null)
            logger.warn("manually specified tokens override automatic allocation");

        // if user specified tokens, use those
        if (initialTokens.size() > 0)
        {
            Collection<Token> tokens = getSpecifiedTokens(metadata, initialTokens);
            BootstrapDiagnostics.useSpecifiedTokens(address, allocationKeyspace, tokens, DatabaseDescriptor.getNumTokens());
            return tokens;
        }

        int numTokens = DatabaseDescriptor.getNumTokens();
        if (numTokens < 1)
            throw new ConfigurationException("num_tokens must be >= 1");

        if (allocationKeyspace != null)
            return allocateTokens(metadata, address, allocationKeyspace, numTokens, schemaWaitDelay);

        if (allocationLocalRf != null)
            return allocateTokens(metadata, address, allocationLocalRf, numTokens, schemaWaitDelay);

        if (numTokens == 1)
            logger.warn("Picking random token for a single vnode.  You should probably add more vnodes and/or use the automatic token allocation mechanism.");

        Collection<Token> tokens = getRandomTokens(metadata, numTokens);
        BootstrapDiagnostics.useRandomTokens(address, metadata, numTokens, tokens);
        return tokens;
    }

    private static Collection<Token> getSpecifiedTokens(final TokenMetadata metadata,
                                                        Collection<String> initialTokens)
    {
        logger.info("tokens manually specified as {}",  initialTokens);
        List<Token> tokens = new ArrayList<>(initialTokens.size());
        for (String tokenString : initialTokens)
        {
            Token token = metadata.partitioner.getTokenFactory().fromString(tokenString);
            if (metadata.getEndpoint(token) != null)
                throw new ConfigurationException("Bootstrapping to existing token " + tokenString + " is not allowed (decommission/removenode the old node first).");
            tokens.add(token);
        }
        return tokens;
    }

    static Collection<Token> allocateTokens(final TokenMetadata metadata,
                                            InetAddressAndPort address,
                                            String allocationKeyspace,
                                            int numTokens,
                                            long schemaWaitDelay)
    {
        StorageService.instance.waitForSchema(schemaWaitDelay);
        if (!FBUtilities.getBroadcastAddressAndPort().equals(InetAddressAndPort.getLoopbackAddress()))
            Gossiper.waitToSettle();

        Keyspace ks = Keyspace.open(allocationKeyspace);
        if (ks == null)
            throw new ConfigurationException("Problem opening token allocation keyspace " + allocationKeyspace);
        AbstractReplicationStrategy rs = ks.getReplicationStrategy();

        Collection<Token> tokens = TokenAllocation.allocateTokens(metadata, rs, address, numTokens);
        BootstrapDiagnostics.tokensAllocated(address, metadata, allocationKeyspace, numTokens, tokens);
        return tokens;
    }


    static Collection<Token> allocateTokens(final TokenMetadata metadata,
                                            InetAddressAndPort address,
                                            int rf,
                                            int numTokens,
                                            long schemaWaitDelay)
    {
        StorageService.instance.waitForSchema(schemaWaitDelay);
        if (!FBUtilities.getBroadcastAddressAndPort().equals(InetAddressAndPort.getLoopbackAddress()))
            Gossiper.waitToSettle();

        Collection<Token> tokens = TokenAllocation.allocateTokens(metadata, rf, address, numTokens);
        BootstrapDiagnostics.tokensAllocated(address, metadata, rf, numTokens, tokens);
        return tokens;
    }

    public static Collection<Token> getRandomTokens(TokenMetadata metadata, int numTokens)
    {
        Set<Token> tokens = new HashSet<>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = metadata.partitioner.getRandomToken();
            if (metadata.getEndpoint(token) == null)
                tokens.add(token);
        }

        logger.info("Generated random tokens. tokens are {}", tokens);
        return tokens;
    }
}
