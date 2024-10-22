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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.IntrusiveLinkedList;
import accord.utils.Invariants;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.metrics.CacheAccessMetrics;
import org.apache.cassandra.service.accord.AccordCachingState.Status;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.utils.Invariants.checkState;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.LOADED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.SAVING;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between the two object types.
 * </p>
 * Supports dynamic object sizes. After each acquire/free cycle, the cacheable objects size is recomputed to
 * account for data added/removed during txn processing if it's modified flag is set
 */
public class AccordStateCache extends IntrusiveLinkedList<AccordCachingState<?,?>> implements CacheSize
{
    private static final Logger logger = LoggerFactory.getLogger(AccordStateCache.class);

    // Debug mode to verify that loading from journal + system tables results in
    // functionally identical (or superceding) command to the one we've just evicted.
    private static boolean VALIDATE_LOAD_ON_EVICT = false;

    @VisibleForTesting
    public static void validateLoadOnEvict(boolean value)
    {
        VALIDATE_LOAD_ON_EVICT = value;
    }

    static class Stats
    {
        long queries;
        long hits;
        long misses;
    }

    public static final class ImmutableStats
    {
        public final long queries;
        public final long hits;
        public final long misses;
        
        public ImmutableStats(Stats stats)
        {
            queries = stats.queries;
            hits = stats.hits;
            misses = stats.misses;
        }
    }

    // TODO (required): cleanup on drop table, or else share between command stores
    private Int2ObjectHashMap<Instance<?, ?, ?>> instances = new Int2ObjectHashMap<>();
    private int nextIndex;

    private final Function<Runnable, Future<?>> saveExecutor;
    private final AccordCachingState.OnSaved onSaved;

    private int unreferenced = 0;
    private long maxSizeInBytes;
    private long bytesCached = 0;

    @VisibleForTesting
    final AccordStateCacheMetrics metrics;
    final Stats stats = new Stats();

    public AccordStateCache(Function<Runnable, Future<?>> saveExecutor, AccordCachingState.OnSaved onSaved, long maxSizeInBytes, AccordStateCacheMetrics metrics)
    {
        this.saveExecutor = saveExecutor;
        this.onSaved = onSaved;
        this.maxSizeInBytes = maxSizeInBytes;
        this.metrics = metrics;
    }

    @Override
    public void setCapacity(long sizeInBytes)
    {
        maxSizeInBytes = sizeInBytes;
        maybeEvictSomeNodes();
    }

    @Override
    public long capacity()
    {
        return maxSizeInBytes;
    }

    private void removeFromEvictionQueue(AccordCachingState<?, ?> node, boolean isNewlyReferencedOrEvicted)
    {
        node.unlink();
        if (isNewlyReferencedOrEvicted)
            unreferenced--;
    }

    private void addToEvictionQueue(AccordCachingState<?, ?> node, boolean isNewlyUnreferenced)
    {
        addLast(node);
        if (isNewlyUnreferenced)
            unreferenced++;
    }

    @SuppressWarnings("unchecked")
    private <K, V> void maybeUpdateSize(AccordCachingState<?, ?> node, ToLongFunction<?> estimator)
    {
        if (node.shouldUpdateSize())
        {
            long delta = ((AccordCachingState<K, V>) node).estimatedSizeOnHeapDelta((ToLongFunction<V>) estimator);
            bytesCached += delta;
            instanceFor(node).bytesCached += delta;
        }
    }

    /*
     * Roughly respects LRU semantics when evicting. Might consider prioritising keeping MODIFIED nodes around
     * for longer to maximise the chances of hitting system tables fewer times (or not at all).
     */
    private void maybeEvictSomeNodes()
    {
        if (bytesCached <= maxSizeInBytes)
            return;

        Iterator<AccordCachingState<?, ?>> iter = this.iterator();
        while (iter.hasNext() && bytesCached > maxSizeInBytes)
        {
            AccordCachingState<?, ?> node = iter.next();
            maybeEvict(node);
        }
    }

    @VisibleForTesting
    public <K, V> boolean maybeEvict(AccordCachingState<K, V> node)
    {
        checkState(node.references == 0);

        Status status = node.status(); // status() call completes (if completeable)
        switch (status)
        {
            default: throw new IllegalStateException("Unhandled status " + status);
            case LOADING:
                node.loading().loading.cancel(false);
            case LOADED:
                removeFromEvictionQueue(node, true);
                evict(node);
                return true;
            case MODIFIED:
                // schedule a save to disk, keep linked and in the cache map
                Instance<K, V, ?> instance = instanceFor(node);
                node.save(saveExecutor, instance.saveFunction, onSaved);
                maybeUpdateSize(node, instance.heapEstimator);
                boolean evict = node.status() == LOADED;
                removeFromEvictionQueue(node, evict);
                if (evict) evict(node);
                return evict;
        }
    }

    private static boolean isInEvictionQueue(AccordCachingState<?, ?> node)
    {
        return node.isInEvictionQueue();
    }

    private void evict(AccordCachingState<?, ?> node)
    {
        if (logger.isTraceEnabled())
            logger.trace("Evicting {} {} - {}", node.status(), node.key(), node.isLoaded() ? node.get() : null);

        checkState(!isInEvictionQueue(node));

        bytesCached -= node.lastQueriedEstimatedSizeOnHeap;
        Instance<?, ?, ?> instance = instanceFor(node);
        instance.bytesCached -= node.lastQueriedEstimatedSizeOnHeap;

        // TODO (expected): use listeners
        if (node.status() == LOADED && VALIDATE_LOAD_ON_EVICT)
            instance.validateLoadEvicted(node);

        AccordCachingState<?, ?> self = instanceFor(node).cache.remove(node.key());
        Invariants.checkState(self.references == 0);
        checkState(self == node, "Leaked node detected; was attempting to remove %s but cache had %s", node, self);
        if (instance.listeners != null)
            instance.listeners.forEach(l -> l.onEvict((AccordCachingState) node));
        node.evicted();
    }

    <K, V> Collection<AccordTask<?>> load(Function<Runnable, Future<?>> loadExecutor, AccordCachingState<K, V> node, AccordCachingState.OnLoaded onLoaded)
    {
        Instance<K, V, ?> instance = instanceFor(node);
        return node.load(loadExecutor, instance.loadFunction, onLoaded).waiters();
    }

    <K, V> void loaded(AccordCachingState<K, V> node, V value)
    {
        Instance<K, V, ?> instance = instanceFor(node);
        node.loaded(value);
        if (instance.listeners != null)
        {
            for (Listener<K, V> listener : instance.listeners)
                listener.onUpdate(node);
        }
    }

    <K, V> void failedToLoad(AccordCachingState<K, V> node)
    {
        Invariants.checkState(node.references == 0);
        if (!isInEvictionQueue(node))
        {
            Invariants.checkState(node.status() == EVICTED);
            return;
        }
        removeFromEvictionQueue(node, true);
        node.failedToLoad();
        evict(node);
    }

    <K, V> void saved(AccordCachingState<K, V> node, Object identity, Throwable fail)
    {
        if (node.saved(identity, fail) && node.referenceCount() == 0)
            this.addToEvictionQueue(node, false);
    }

    public <K, V, S extends AccordSafeState<K, V>> void release(S safeRef, AccordTask<?> owner)
    {
        instanceFor(safeRef).release(safeRef, owner);
    }

    public ImmutableStats stats()
    {
        return new ImmutableStats(stats);
    }

    private <K, V> Instance<K, V, ?> instanceFor(AccordCachingState<K, V> node)
    {
        return (Instance<K, V, ?>) instances.get(node.ownerIndex);
    }

    private <K, V, S extends AccordSafeState<K, V>> Instance<K, V, S> instanceFor(S safeRef)
    {
        return (Instance<K, V, S>) instances.get(safeRef.global().ownerIndex);
    }

    public <K, V, S extends AccordSafeState<K, V>> Instance<K, V, S> instance(
        Class<K> keyClass,
        Class<S> valClass,
        Function<AccordCachingState<K, V>, S> safeRefFactory,
        Function<K, V> loadFunction,
        Function<V, Runnable> saveFunction,
        BiFunction<K, V, Boolean> validateFunction,
        ToLongFunction<V> heapEstimator,
        AccordCachingState.Factory<K, V> nodeFactory)
    {
        int index = ++nextIndex;

        Instance<K, V, S> instance =
            new Instance<>(index, keyClass, safeRefFactory, loadFunction, saveFunction, validateFunction, heapEstimator, nodeFactory);

        Int2ObjectHashMap<Instance<?, ?, ?>> newInstances = new Int2ObjectHashMap<>(instances);
        newInstances.put(index, instance);
        instances = newInstances;

        return instance;
    }

    public <K, V, S extends AccordSafeState<K, V>> Instance<K, V, S> instance(
        Class<K> keyClass,
        Class<S> valClass,
        Function<AccordCachingState<K, V>, S> safeRefFactory,
        Function<K, V> loadFunction,
        Function<V, Runnable> saveFunction,
        BiFunction<K, V, Boolean> validateFunction,
        ToLongFunction<V> heapEstimator)
    {
        return instance(keyClass, valClass, safeRefFactory, loadFunction, saveFunction, validateFunction, heapEstimator, AccordCachingState.defaultFactory());
    }

    public Collection<Instance<?, ? ,? >> instances()
    {
        return instances.values();
    }

    public interface Listener<K, V>
    {
        default void onAdd(AccordCachingState<K, V> state) {}
        default void onUpdate(AccordCachingState<K, V> state) {}
        default void onRelease(AccordCachingState<K, V> state) {}
        default void onEvict(AccordCachingState<K, V> state) {}
    }

    public class Instance<K, V, S extends AccordSafeState<K, V>> implements CacheSize, Iterable<AccordCachingState<K, V>>
    {
        private final int index;
        private final Class<K> keyClass;
        private final Function<AccordCachingState<K, V>, S> safeRefFactory;
        private Function<K, V> loadFunction;
        private Function<V, Runnable> saveFunction;
        private final BiFunction<K, V, Boolean> validateFunction;
        private final ToLongFunction<V> heapEstimator;
        private long bytesCached;

        @VisibleForTesting
        final CacheAccessMetrics instanceMetrics;
        private final Stats stats = new Stats();
        private final Map<K, AccordCachingState<?, ?>> cache = new Object2ObjectHashMap<>();
        private final AccordCachingState.Factory<K, V> nodeFactory;
        private List<Listener<K, V>> listeners = null;

        public Instance(
            int index, Class<K> keyClass,
            Function<AccordCachingState<K, V>, S> safeRefFactory,
            Function<K, V> loadFunction,
            Function<V, Runnable> saveFunction,
            BiFunction<K, V, Boolean> validateFunction,
            ToLongFunction<V> heapEstimator,
            AccordCachingState.Factory<K, V> nodeFactory)
        {
            this.index = index;
            this.keyClass = keyClass;
            this.safeRefFactory = safeRefFactory;
            this.loadFunction = loadFunction;
            this.saveFunction = saveFunction;
            this.validateFunction = validateFunction;
            this.heapEstimator = heapEstimator;
            this.instanceMetrics = metrics.forInstance(keyClass);
            this.nodeFactory = nodeFactory;
        }

        public void register(Listener<K, V> l)
        {
            if (listeners == null)
                listeners = new ArrayList<>();
            listeners.add(l);
        }

        public void unregister(Listener<K, V> l)
        {
            if (listeners == null)
                throw new AssertionError("No listeners exist");
            if (!listeners.remove(l))
                throw new AssertionError("Listener was not registered");
            if (listeners.isEmpty())
                listeners = null;
        }

        public Stream<AccordCachingState<K, V>> stream()
        {
            return cache.entrySet().stream()
                        .filter(e -> instanceFor(e.getValue()) == this)
                        .map(e -> (AccordCachingState<K, V>) e.getValue());
        }

        @Override
        public Iterator<AccordCachingState<K, V>> iterator()
        {
            return stream().iterator();
        }

        public S acquire(K key)
        {
            AccordCachingState<K, V> node = acquire(key, false);
            return safeRefFactory.apply(node);
        }

        public S acquireIfLoaded(K key)
        {
            AccordCachingState<K, V> node = acquire(key, true);
            if (node == null)
                return null;
            return safeRefFactory.apply(node);
        }

        public S acquire(AccordCachingState<K, V> node)
        {
            Invariants.checkState(node.ownerIndex == index);
            acquireExisting(node, false);
            return safeRefFactory.apply(node);
        }

        private AccordCachingState<K, V> acquire(K key, boolean onlyIfLoaded)
        {
            incrementCacheQueries();
            @SuppressWarnings("unchecked")
            AccordCachingState<K, V> node = (AccordCachingState<K, V>) cache.get(key);
            return node == null
                 ? acquireAbsent(key, onlyIfLoaded)
                 : acquireExisting(node, onlyIfLoaded);
        }

        /*
         * Can only return a LOADING Node (or null)
         */
        private AccordCachingState<K, V> acquireAbsent(K key, boolean onlyIfLoaded)
        {
            incrementCacheMisses();
            if (onlyIfLoaded)
                return null;
            AccordCachingState<K, V> node = nodeFactory.create(key, index);
            node.readyToLoad();
            node.references++;

            Object prev = cache.put(key, node);
            Invariants.checkState(prev == null, "%s not absent from cache: %s already present", key, node);
            if (listeners != null)
                listeners.forEach(l -> l.onAdd(node));
            maybeUpdateSize(node, heapEstimator);
            metrics.objectSize.update(node.lastQueriedEstimatedSizeOnHeap);
            maybeEvictSomeNodes();
            return node;
        }

        /*
         * Can't return EVICTED or INITIALIZED
         */
        private AccordCachingState<K, V> acquireExisting(AccordCachingState<K, V> node, boolean onlyIfLoaded)
        {
            Status status = node.status(); // status() completes

            if (status.isLoaded())
                incrementCacheHits();
            else
                incrementCacheMisses();

            if (onlyIfLoaded && !status.isLoaded())
                return null;

            if (node.references == 0)
                removeFromEvictionQueue(node, true);

            node.references++;

            return node;
        }

        public void release(S safeRef, AccordTask<?> owner)
        {
            K key = safeRef.global().key();
            logger.trace("Releasing resources for {}: {}", key, safeRef);

            @SuppressWarnings("unchecked")
            AccordCachingState<K, V> node = (AccordCachingState<K, V>) cache.get(key);

            checkState(safeRef.global() != null, "safeRef node is null for %s", key);
            checkState(safeRef.global() == node, "safeRef node not in map: %s != %s", safeRef.global(), node);
            checkState(node.references > 0, "references (%d) are zero for %s (%s)", node.references, key, node);
            checkState(!isInEvictionQueue(node));

            if (safeRef.hasUpdate())
            {
                node.set(safeRef.current());
                if (listeners != null)
                {
                    for (Listener<K, V> listener : listeners)
                        listener.onUpdate(node);
                }
            }
            else if (node.isLoadingOrWaiting())
            {
                node.loadingOrWaiting().remove(owner);
            }
            safeRef.invalidate();

            maybeUpdateSize(node, heapEstimator);

            if (listeners != null)
                listeners.forEach(l -> l.onRelease(node));

            if (--node.references == 0)
            {
                Status status = node.status(); // status() completes
                switch (status)
                {
                    default: throw new IllegalStateException("Unhandled status " + status);
                    case WAITING_TO_LOAD:
                    case LOADING:
                    case LOADED:
                    case MODIFIED:
                        logger.trace("Moving {} with status {} to eviction queue", key, status);
                        addToEvictionQueue(node, true);
                    case SAVING:
                    case FAILED_TO_SAVE:
                        break; // can never evict, so no point in adding to eviction queue either
                }
            }

            // TODO (performance, expected): triggering on every release is potentially heavy
            maybeEvictSomeNodes();
        }

        void validateLoadEvicted(AccordCachingState<?, ?> node)
        {
            @SuppressWarnings("unchecked")
            AccordCachingState<K, V> state = (AccordCachingState<K, V>) node;
            K key = state.key();
            V evicted = state.get();
            if (!validateFunction.apply(key, evicted))
                throw new IllegalStateException("Reloaded value for key " + key + " is not equal to or fuller than evicted value " + evicted);
        }

        @VisibleForTesting
        public AccordCachingState<K, V> getUnsafe(K key)
        {
            //noinspection unchecked
            return (AccordCachingState<K, V>) cache.get(key);
        }

        public Set<K> keySet()
        {
            return cache.keySet();
        }

        @VisibleForTesting
        public boolean isReferenced(K key)
        {
            //noinspection unchecked
            AccordCachingState<K, V> node = (AccordCachingState<K, V>) cache.get(key);
            return node != null && node.references > 0;
        }

        @VisibleForTesting
        boolean keyIsReferenced(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
        {
            AccordCachingState<?, ?> node = cache.get(key);
            return node != null && node.references > 0;
        }

        @VisibleForTesting
        boolean keyIsCached(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
        {
            AccordCachingState<?, ?> node = cache.get(key);
            return node != null;
        }

        @VisibleForTesting
        int references(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
        {
            AccordCachingState<?, ?> node = cache.get(key);
            return node != null ? node.references : 0;
        }

        private void incrementCacheQueries()
        {
            instanceMetrics.requests.mark();
            metrics.requests.mark();
            stats.queries++;
            AccordStateCache.this.stats.queries++;
        }

        private void incrementCacheHits()
        {
            instanceMetrics.hits.mark();
            metrics.hits.mark();
            stats.hits++;
            AccordStateCache.this.stats.hits++;
        }

        private void incrementCacheMisses()
        {
            instanceMetrics.misses.mark();
            metrics.misses.mark();
            stats.misses++;
            AccordStateCache.this.stats.misses++;
        }

        public Stats stats()
        {
            return stats;
        }

        public ImmutableStats statsSnapshot()
        {
            return new ImmutableStats(stats);
        }

        public Stats globalStats()
        {
            return AccordStateCache.this.stats;
        }

        @VisibleForTesting
        public void unsafeSetLoadFunction(Function<K, V> loadFunction)
        {
            this.loadFunction = loadFunction;
        }

        public Function<K, V> unsafeGetLoadFunction()
        {
            return loadFunction;
        }

        @VisibleForTesting
        public void unsafeSetSaveFunction(Function<V, Runnable> saveFunction)
        {
            this.saveFunction = saveFunction;
        }

        public Function<V, Runnable> unsafeGetSaveFunction()
        {
            return saveFunction;
        }

        @Override
        public long capacity()
        {
            return AccordStateCache.this.capacity();
        }

        @Override
        public void setCapacity(long capacity)
        {
            throw new UnsupportedOperationException("Capacity is shared between all instances. Please set the capacity on the global cache");
        }

        @Override
        public int size()
        {
            return cache.size();
        }

        @Override
        public long weightedSize()
        {
            return bytesCached;
        }

        public long globalAllocated()
        {
            return AccordStateCache.this.bytesCached;
        }

        public int globalReferencedEntries()
        {
            return AccordStateCache.this.numReferencedEntries();
        }

        public int globalUnreferencedEntries()
        {
            return AccordStateCache.this.numUnreferencedEntries();
        }

        @Override
        public String toString()
        {
            return "Instance{" +
                   "index=" + index +
                   ", keyClass=" + keyClass +
                   '}';
        }
    }

    @VisibleForTesting
    void unsafeClear()
    {
        bytesCached = 0;
        metrics.reset();;
        instances.values().forEach(instance -> {
            instance.cache.forEach((k, v) -> Invariants.checkState(v.references == 0));
            instance.cache.clear();
            instance.bytesCached = 0;
            instance.instanceMetrics.reset();
        });
        //noinspection StatementWithEmptyBody
        while (null != poll());
    }

    @VisibleForTesting
    AccordCachingState<?, ?> head()
    {
        Iterator<AccordCachingState<?, ?>> iter = iterator();
        return iter.hasNext() ? iter.next() : null;
    }

    @VisibleForTesting
    AccordCachingState<?, ?> tail()
    {
        AccordCachingState<?,?> last = null;
        Iterator<AccordCachingState<?, ?>> iter = iterator();
        while (iter.hasNext())
            last = iter.next();
        return last;
    }

    @VisibleForTesting
    public void awaitSaveResults()
    {
        for (AccordCachingState<?, ?> node : this)
            if (node.status() == SAVING)
                node.saving().awaitUninterruptibly();
    }

    private int cacheSize()
    {
        int size = 0;
        for (Instance<?, ?, ?> instance : instances.values())
            size += instance.cache.size();
        return size;
    }

    @VisibleForTesting
    int numReferencedEntries()
    {
        return cacheSize() - unreferenced;
    }

    @VisibleForTesting
    int numUnreferencedEntries()
    {
        return unreferenced;
    }

    @Override
    public int size()
    {
        return cacheSize();
    }

    @Override
    public long weightedSize()
    {
        return bytesCached;
    }
}
