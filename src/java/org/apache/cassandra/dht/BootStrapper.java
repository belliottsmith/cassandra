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

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Multimap;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifierSupport;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class BootStrapper extends ProgressEventNotifierSupport
{
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);

    /* endpoint that needs to be bootstrapped */
    protected final InetAddress address;
    /* token of the node being bootstrapped. */
    protected final Collection<Token> tokens;
    protected final TokenMetadata tokenMetadata;

    public BootStrapper(InetAddress address, Collection<Token> tokens, TokenMetadata tmd)
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
                                                   "Bootstrap",
                                                   useStrictConsistency,
                                                   DatabaseDescriptor.getEndpointSnitch(),
                                                   stateStore);
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));
        streamer.addSourceFilter(new RangeStreamer.ExcludeLocalNodeFilter());

        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
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
            if (DatabaseDescriptor.enableChristmasPatch())
            {
                fetchRepairedRanges(streamer.toFetch(), Integer.MAX_VALUE);
            }
            return streamState;
        };

        return Futures.transform(bootstrapStreamResult, repairedRangesFetcher);
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
    public static void fetchRepairedRanges(Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> bootstrappedRanges, int retriesAllowed)
    {
        //We dont want to keep it remain disabled while C* is running. This can cuase move/decom to not get newer ranges.
        stopFetchingRepairRanges = false;
        for (String table : bootstrappedRanges.keySet())
        {
            for (Map.Entry<InetAddress, Collection<Range<Token>>> tokens : bootstrappedRanges.get(table))
            {
                boolean failed = true;
                int retries = 0;
                while(failed && retries <= retriesAllowed && !stopFetchingRepairRanges)
                {
                    InetAddress endPointToFetch = tokens.getKey();
                    retries++;

                    failed = !fetchRepairedRanges(table, tokens.getValue(), endPointToFetch);

                    //Try to get ranges from other endpoints
                    if(failed)
                    {
                        failed = !fetchRepairedRanges(table, tokens.getValue());
                    }
                }

                if(failed)
                {
                    logger.error("Failed to fetch repaired ranges from {} for keyspace {}. Giving up. ", tokens.getKey(), table);
                }
            }
        }
    }

    private static boolean fetchRepairedRanges(String table, Collection<Range<Token>> tokens)
    {
        AbstractReplicationStrategy strat = Keyspace.open(table).getReplicationStrategy();
        Multimap<Range<Token>, InetAddress> rangeAddresses = strat.getRangeAddresses();
        //Get Endpoints to fetch this from
        outer: for(Range<Token> tokenRange: tokens)
        {
            for (Range<Token> currentRange : rangeAddresses.keySet())
            {
                if (currentRange.contains(tokenRange))
                {
                    logger.info("Existing range {} contains {} - trying to fetch from one of {}", currentRange, tokenRange, rangeAddresses.get(currentRange));
                    for(InetAddress address : rangeAddresses.get(currentRange))
                    {
                        if(FBUtilities.getLocalAddress().equals(address))
                            continue;
                        if(FailureDetector.instance.isAlive(address) && fetchRepairedRanges(table, Collections.singleton(tokenRange), address))
                        {
                            continue outer;
                        }
                    }
                }
            }

            logger.error("Could not get repaired ranges for {}", tokenRange);
            return false;
        }

        return true;
    }


    private static boolean fetchRepairedRanges(String table, Collection<Range<Token>> tokens, InetAddress endPointToFetch)
    {
        int timeoutInMin = 1;
        logger.info("Fetching repaired ranges ({}) from {} for keyspace {}", tokens, endPointToFetch, table);
        AsyncOneResponse<UpdateRepairedRanges> response = MessagingService.instance().sendRR(new RepairedRangesRequest(table, tokens).createMessage(), endPointToFetch, TimeUnit.MILLISECONDS.convert(timeoutInMin, TimeUnit.MINUTES));
        try
        {
            UpdateRepairedRanges payload = response.get(timeoutInMin, TimeUnit.MINUTES);
            for (Map.Entry<String, Map<Range<Token>, Integer>> cfRanges : payload.perCfRanges.entrySet())
            {
                ColumnFamilyStore cf = Keyspace.open(payload.keyspace).getColumnFamilyStore(cfRanges.getKey());
                for (Map.Entry<Range<Token>, Integer> repairRange : cfRanges.getValue().entrySet())
                {
                    cf.updateLastSuccessfulRepair(repairRange.getKey(), 1000L * repairRange.getValue());
                }
            }
        }
        catch (TimeoutException e)
        {
            logger.error("Could not fetch repaired ranges from {} for keyspace {}", endPointToFetch, table);
            return false;
        }
        return true;
    }


    /**
     * if initialtoken was specified, use that (split on comma).
     * otherwise, if allocationKeyspace is specified use the token allocation algorithm to generate suitable tokens
     * else choose num_tokens tokens at random
     */
    public static Collection<Token> getBootstrapTokens(final TokenMetadata metadata, InetAddress address) throws ConfigurationException
    {
        String allocationKeyspace = DatabaseDescriptor.getAllocateTokensForKeyspace();
        Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
        if (initialTokens.size() > 0 && allocationKeyspace != null)
            logger.warn("manually specified tokens override automatic allocation");

        // if user specified tokens, use those
        if (initialTokens.size() > 0)
            return getSpecifiedTokens(metadata, initialTokens);

        int numTokens = DatabaseDescriptor.getNumTokens();
        if (numTokens < 1)
            throw new ConfigurationException("num_tokens must be >= 1");

        if (allocationKeyspace != null)
            return allocateTokens(metadata, address, allocationKeyspace, numTokens);

        if (numTokens == 1)
            logger.warn("Picking random token for a single vnode.  You should probably add more vnodes and/or use the automatic token allocation mechanism.");

        return getRandomTokens(metadata, numTokens);
    }

    private static Collection<Token> getSpecifiedTokens(final TokenMetadata metadata,
                                                        Collection<String> initialTokens)
    {
        logger.trace("tokens manually specified as {}",  initialTokens);
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
                                            InetAddress address,
                                            String allocationKeyspace,
                                            int numTokens)
    {
        Keyspace ks = Keyspace.open(allocationKeyspace);
        if (ks == null)
            throw new ConfigurationException("Problem opening token allocation keyspace " + allocationKeyspace);
        AbstractReplicationStrategy rs = ks.getReplicationStrategy();

        return TokenAllocation.allocateTokens(metadata, rs, address, numTokens);
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
        return tokens;
    }

    public static class StringSerializer implements IVersionedSerializer<String>
    {
        public static final StringSerializer instance = new StringSerializer();

        public void serialize(String s, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String s, int version)
        {
            return TypeSizes.sizeof(s);
        }
    }

    public static class RepairedRangesRequest
    {
        public static final RepairedRangesRequestSerializer serializer = new RepairedRangesRequestSerializer();

        public final String keyspace;
        public final Collection<Range<Token>> ranges;

        public RepairedRangesRequest(String keyspace, Collection<Range<Token>> ranges)
        {
            this.keyspace = keyspace;
            this.ranges = ranges;
        }

        public MessageOut<RepairedRangesRequest> createMessage()
        {
            return new MessageOut<>(MessagingService.Verb.APPLE_REPAIRED_RANGES, this, serializer); // STREAM_INITIATE_DONE was used in 2.1
        }
    }

    public static class UpdateRepairedRanges
    {
        public static final RepairedRangesResponseSerializer serializer = new RepairedRangesResponseSerializer();

        public final String keyspace;
        public final Map<String, Map<Range<Token>, Integer>> perCfRanges;


        public UpdateRepairedRanges(String keyspace, Map<String, Map<Range<Token>, Integer>> perCFranges)
        {
            this.keyspace = keyspace;
            this.perCfRanges = perCFranges;
        }

        public MessageOut<UpdateRepairedRanges> createMessage(boolean request)
        {
            return new MessageOut<>(request? MessagingService.Verb.APPLE_UPDATE_REPAIRED_RANGES : MessagingService.Verb.INTERNAL_RESPONSE, this, serializer); // BINARY was used in 2.1
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Map<Range<Token>, Integer>> perCfRange : perCfRanges.entrySet())
            {
                sb.append(perCfRange.getKey()).append("=").append("(");
                for (Map.Entry<Range<Token>, Integer> range : perCfRange.getValue().entrySet())
                {
                    sb.append(range.getKey()).append(":").append(range.getValue()).append(",");
                }
                sb.append("),");
            }
            return sb.toString();
        }
    }

    static class RepairedRangesRequestSerializer implements IVersionedSerializer<RepairedRangesRequest>
    {
        public void serialize(RepairedRangesRequest request, DataOutputPlus dos, int version) throws IOException
        {
            dos.writeUTF(request.keyspace);
            dos.writeInt(request.ranges.size());
            for (Range<Token> entry : request.ranges)
                Range.tokenSerializer.serialize(entry, dos, version);
        }

        public RepairedRangesRequest deserialize(DataInputPlus dis, int version) throws IOException
        {
            String keyspace = dis.readUTF();
            int rangeCount = dis.readInt();
            Collection<Range<Token>> ranges = new ArrayList<>(rangeCount);

            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.tokenSerializer.deserialize(dis, DatabaseDescriptor.getPartitioner(), version));

            return new RepairedRangesRequest(keyspace, ranges);
        }

        public long serializedSize(RepairedRangesRequest rrr, int version)
        {
            long size = TypeSizes.sizeof(rrr.keyspace);
            size += TypeSizes.sizeof(rrr.ranges.size());
            for (Range<Token> r : rrr.ranges)
                size += Range.tokenSerializer.serializedSize(r, version);
            return size;
        }
    }

    static class RepairedRangesResponseSerializer implements IVersionedSerializer<UpdateRepairedRanges>
    {
        public void serialize(UpdateRepairedRanges response, DataOutputPlus dos, int version) throws IOException
        {
            dos.writeUTF(response.keyspace);
            dos.writeInt(response.perCfRanges.size());
            for (Map.Entry<String, Map<Range<Token>, Integer>> entry : response.perCfRanges.entrySet())
            {
                dos.writeUTF(entry.getKey());
                dos.writeInt(entry.getValue().size());

                for (Map.Entry<Range<Token>, Integer> repairedEntry : entry.getValue().entrySet())
                {
                    Range.tokenSerializer.serialize(repairedEntry.getKey(), dos, version);
                    dos.writeInt(repairedEntry.getValue());
                }
            }
        }

        public UpdateRepairedRanges deserialize(DataInputPlus dis, int version) throws IOException
        {
            String keyspace = dis.readUTF();
            int entryCount = dis.readInt();
            Map<String, Map<Range<Token>, Integer>> perCFranges = new HashMap<>();

            for (int i = 0; i < entryCount; i++)
            {
                String columnFamily = dis.readUTF();
                int rangeCount = dis.readInt();
                Map<Range<Token>, Integer> rangeSuccessTimes = new HashMap<>();
                for (int j = 0; j < rangeCount; j++)
                {
                    Range<Token> range = (Range<Token>) Range.tokenSerializer.deserialize(dis, DatabaseDescriptor.getPartitioner(), version);
                    int succeedAtSeconds = dis.readInt();
                    rangeSuccessTimes.put(range, succeedAtSeconds);
                }
                perCFranges.put(columnFamily, rangeSuccessTimes);
            }

            return new UpdateRepairedRanges(keyspace, perCFranges);
        }

        public long serializedSize(UpdateRepairedRanges response, int version)
        {
            long size = TypeSizes.sizeof(response.keyspace);
            size += TypeSizes.sizeof(response.perCfRanges.size());
            for (Map.Entry<String, Map<Range<Token>, Integer>> repairedRange : response.perCfRanges.entrySet())
            {
                size += TypeSizes.sizeof(repairedRange.getKey()); // cf
                size += TypeSizes.sizeof(repairedRange.getValue().size());
                for (Map.Entry<Range<Token>, Integer> r : repairedRange.getValue().entrySet())
                {
                    size += Range.tokenSerializer.serializedSize(r.getKey(), version);
                    size += TypeSizes.sizeof(r.getValue());
                }
            }
            return size;
        }
    }

    public static class RepairedRangesHandler implements IVerbHandler<RepairedRangesRequest>
    {
        public void doVerb(MessageIn<RepairedRangesRequest> message, int id)
        {
            logger.info("Got a RepairedRangesRequest from {}", message.from);
            RepairedRangesRequest request = message.payload;
            Collection<Range<Token>> requestRanges = request.ranges;
            Map<String, Map<Range<Token>, Integer>> retMap = new HashMap<>();

            for (Map.Entry<String, Map<Range<Token>, Integer>> perCf : SystemKeyspace.getLastSuccessfulRepairsForKeyspace(request.keyspace).entrySet())
            {
                String cf = perCf.getKey();
                //Check if the CF exists then only send the ranges.
                if(!Keyspace.open(request.keyspace).columnFamilyExists(cf))
                {
                    logger.info("Skipping non existent cf = " + cf);
                    continue;
                }

                Map<Range<Token>, Integer> repairMap = new HashMap<>();
                for (Map.Entry<Range<Token>, Integer> entry : perCf.getValue().entrySet())
                {
                    Range<Token> locallyRepairedRange = entry.getKey();
                    for (Range<Token> requestRange : requestRanges)
                    {
                        for (Range<Token> intersection : locallyRepairedRange.intersectionWith(requestRange))
                        {
                            Integer succeedAt = repairMap.get(intersection);
                            if (succeedAt == null || succeedAt < entry.getValue())
                                repairMap.put(intersection, entry.getValue());
                        }
                    }
                }
                retMap.put(cf, repairMap);
            }
            UpdateRepairedRanges response = new UpdateRepairedRanges(request.keyspace, retMap);

            MessagingService.instance().sendReply(response.createMessage(false), id, message.from);
        }
    }

}
