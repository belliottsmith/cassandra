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
package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.topology.Topology;
import accord.utils.RandomSource;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.AccordSpec.QueueShardModel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.metrics.CacheSizeMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordExecutor.InfiniteLoopAccordExecutor;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.config.AccordSpec.QueueShardModel.THREAD_PER_SHARD;
import static org.apache.cassandra.service.accord.AccordExecutor.LockLoopAccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.apache.cassandra.service.accord.AccordExecutor.LockLoopAccordExecutor.Mode.RUN_WITH_LOCK;

public class AccordCommandStores extends CommandStores implements CacheSize
{
    public static final String ACCORD_STATE_CACHE = "AccordStateCache";

    private final CacheSizeMetrics cacheSizeMetrics;
    private final AccordExecutor[] executors;
    private long cacheSize;

    AccordCommandStores(NodeCommandStoreService node, Agent agent, DataStore store, RandomSource random,
                        ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenerFactory,
                        AccordJournal journal, AccordExecutor[] executors)
    {
        super(node, agent, store, random, shardDistributor, progressLogFactory, listenerFactory,
              AccordCommandStore.factory(journal, id -> executors[id % executors.length]));
        setCapacity(DatabaseDescriptor.getAccordCacheSizeInMiB() << 20);
        this.executors = executors;
        this.cacheSizeMetrics = new CacheSizeMetrics(ACCORD_STATE_CACHE, this);
    }

    static Factory factory(AccordJournal journal)
    {
        return (time, agent, store, random, shardDistributor, progressLogFactory, listenerFactory) -> {
            AccordExecutor[] executors = new AccordExecutor[DatabaseDescriptor.getAccordQueueShardCount()];
            for (int id = 0; id < executors.length; id++)
            {
                AccordStateCacheMetrics metrics = new AccordStateCacheMetrics(ACCORD_STATE_CACHE);
                QueueShardModel shardModel = DatabaseDescriptor.getAccordQueueShardModel();
                String baseName = CommandStore.class.getSimpleName() + '[' + id;
                int threads = Math.max(DatabaseDescriptor.getAccordConcurrentOps() / DatabaseDescriptor.getAccordQueueShardCount(), 1);
                switch (shardModel)
                {
                    case THREAD_PER_SHARD:
                    case THREAD_PER_SHARD_SYNC_QUEUE:
                        executors[id] = new InfiniteLoopAccordExecutor(shardModel == THREAD_PER_SHARD ? RUN_WITHOUT_LOCK : RUN_WITH_LOCK, baseName + ']', metrics, Stage.READ.executor(), Stage.MUTATION.executor(), Stage.READ.executor(), agent);
                        break;
                    case THREAD_POOL_PER_SHARD:
                        executors[id] = new InfiniteLoopAccordExecutor(RUN_WITHOUT_LOCK, threads, num -> baseName + ',' + num + ']', metrics, exec -> exec::submit, exec -> exec::submit, exec -> exec::submit, agent);
                        break;
                    case THREAD_POOL_PER_SHARD_EXCLUDES_IO:
                        executors[id] = new InfiniteLoopAccordExecutor(RUN_WITHOUT_LOCK, threads, num -> baseName + ',' + num + ']', metrics, Stage.READ.executor(), Stage.MUTATION.executor(), Stage.READ.executor(), agent);
                        break;
                }
            }

            return new AccordCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, listenerFactory, journal, executors);
        };
    }

    @Override
    protected boolean shouldBootstrap(Node node, Topology previous, Topology updated, Range range)
    {
        if (!super.shouldBootstrap(node, previous, updated, range))
            return false;
        // we see new ranges when a new keyspace is added, so avoid bootstrap in these cases
        return contains(previous, ((AccordRoutingKey)  range.start()).table());
    }

    private static boolean contains(Topology previous, TableId searchTable)
    {
        for (Range range : previous.ranges())
        {
            TableId table = ((AccordRoutingKey)  range.start()).table();
            if (table.equals(searchTable))
                return true;
        }
        return false;
    }

    public synchronized void setCapacity(long bytes)
    {
        cacheSize = bytes;
        refreshCacheSizes();
    }

    @Override
    public long capacity()
    {
        return cacheSize;
    }

    @Override
    public int size()
    {
        int size = 0;
        for (AccordExecutor executor : executors)
            size += executor.size();
        return size;
    }

    @Override
    public long weightedSize()
    {
        long size = 0;
        for (AccordExecutor executor : executors)
            size += executor.weightedSize();
        return size;
    }

    synchronized void refreshCacheSizes()
    {
        if (count() == 0)
            return;
        long perExecutor = cacheSize / executors.length;
        // TODO (low priority, safety): we might transiently breach our limit if we increase one store before decreasing another
        for (AccordExecutor executor : executors)
            executor.execute(() -> executor.setCapacity(perExecutor));
    }

    public void waitForQuiescense()
    {
        boolean hadPending;
        try
        {
            do
            {
                hadPending = false;
                List<Future<?>> futures = new ArrayList<>();
                for (AccordExecutor executor : this.executors)
                {
                    if (!hadPending && executor.hasTasks())
                        hadPending = true;
                    futures.add(executor.submit(() -> {}));
                }
                for (Future<?> future : futures)
                    future.get();
                futures.clear();
            }
            while (hadPending);
        }
        catch (ExecutionException e)
        {
            throw new IllegalStateException("Should have never been thrown", e);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        for (AccordExecutor executor : executors)
        {
            executor.shutdown();
            try
            {
                executor.awaitTermination(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
        //TODO shutdown isn't useful by itself, we need a way to "wait" as well.  Should be AutoCloseable or offer awaitTermination as well (think Shutdownable interface)
    }
}
