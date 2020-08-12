
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
package org.apache.cassandra.db.compaction;


import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;

/**
 * Manages the compaction strategies.
 *
 * Currently has two instances of actual compaction strategies - one for repaired data, one for unrepaired data, and
 * one for each pending repair. This is done to be able to totally separate the different sets of sstables.
 */
public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    private final ColumnFamilyStore cfs;
    private volatile AbstractCompactionStrategy repaired;
    private volatile AbstractCompactionStrategy unrepaired;
    private volatile PendingRepairManager pendingRepairs;
    private volatile boolean enabled = true;
    private volatile boolean isActive = true;
    private volatile CompactionParams params;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    /*
        We keep a copy of the schema compaction parameters here to be able to decide if we
        should update the compaction strategy in maybeReloadCompactionStrategy() due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
        we will use the new compaction parameters.
     */
    private volatile CompactionParams schemaCompactionParams;

    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;

        reload(cfs.metadata);
        params = cfs.metadata.params.compaction;
        enabled = params.isEnabled();
    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     *
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        readLock.lock();
        try
        {
            if (!isEnabled())
                return null;

            maybeReload(cfs.metadata);
            // first try to promote/demote sstables from completed repairs
            if (pendingRepairs.getNumPendingRepairFinishedTasks() > 0)
            {
                AbstractCompactionTask task = pendingRepairs.getNextRepairFinishedTask();
                if (task != null)
                {
                    return task;
                }
            }

            // sort compaction task suppliers by remaining tasks descending
            ArrayList<Pair<Integer, Supplier<AbstractCompactionTask>>> sortedSuppliers = new ArrayList<>(3);
            sortedSuppliers.add(Pair.create(repaired.getEstimatedRemainingTasks(), () -> repaired.getNextBackgroundTask(gcBefore)));
            sortedSuppliers.add(Pair.create(unrepaired.getEstimatedRemainingTasks(), () -> unrepaired.getNextBackgroundTask(gcBefore)));
            sortedSuppliers.add(Pair.create(pendingRepairs.getMaxEstimatedRemainingTasks(), () -> pendingRepairs.getNextBackgroundTask(gcBefore)));
            sortedSuppliers.sort((x, y) -> y.left - x.left);

            // return the first non-null task
            AbstractCompactionTask task;
            Iterator<Supplier<AbstractCompactionTask>> suppliers = Iterables.transform(sortedSuppliers, p -> p.right).iterator();
            do {
                task = suppliers.next().get();
            } while (suppliers.hasNext() && task == null);
            return task;
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * finds the oldest (by modification date) non-latest-version sstable on disk and creates an upgrade task for it
     * @return
     */
    @VisibleForTesting
    AbstractCompactionTask findUpgradeSSTableTask()
    {
        if (!isEnabled() || !DatabaseDescriptor.automaticSSTableUpgrade())
            return null;
        Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
        List<SSTableReader> potentialUpgrade = cfs.getLiveSSTables()
                                                  .stream()
                                                  .filter(s -> !compacting.contains(s) && !s.descriptor.version.isLatestVersion())
                                                  .sorted((o1, o2) -> {
                                                      File f1 = new File(o1.descriptor.filenameFor(Component.DATA));
                                                      File f2 = new File(o2.descriptor.filenameFor(Component.DATA));
                                                      return Longs.compare(f1.lastModified(), f2.lastModified());
                                                  }).collect(Collectors.toList());
        for (SSTableReader sstable : potentialUpgrade)
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UPGRADE_SSTABLES);
            if (txn != null)
            {
                logger.info("Running automatic sstable upgrade for {}", sstable);
                return getCompactionStrategyFor(sstable).getCompactionTask(txn, Integer.MIN_VALUE, Long.MAX_VALUE);
            }
        }
        return null;
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public boolean isActive()
    {
        return isActive;
    }

    public void resume()
    {
        writeLock.lock();
        try
        {
            isActive = true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public void pause()
    {
        writeLock.lock();
        try
        {
            isActive = false;
        }
        finally
        {
            writeLock.unlock();
        }

    }

    private void startup()
    {
        writeLock.lock();
        try
        {
            for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            {
                if (sstable.openReason != SSTableReader.OpenReason.EARLY)
                    getCompactionStrategyFor(sstable).addSSTable(sstable);
            }
            repaired.startup();
            unrepaired.startup();
            pendingRepairs.startup();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status
     * @param sstable
     * @return
     */
    private AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        readLock.lock();
        try
        {
            if (sstable.isPendingRepair())
                return pendingRepairs.getOrCreate(sstable);
            else if (sstable.isRepaired())
                return repaired;
            else
                return unrepaired;
        }
        finally
        {
            readLock.unlock();
        }
    }

    @VisibleForTesting
    AbstractCompactionStrategy getRepaired()
    {
        return repaired;
    }

    @VisibleForTesting
    AbstractCompactionStrategy getUnrepaired()
    {
        return unrepaired;
    }

    @VisibleForTesting
    AbstractCompactionStrategy getForPendingRepair(UUID sessionID)
    {
        return pendingRepairs.get(sessionID);
    }

    @VisibleForTesting
    Set<UUID> pendingRepairs()
    {
        return pendingRepairs.getSessions();
    }

    public boolean hasDataForPendingRepair(UUID sessionID)
    {
        return pendingRepairs.hasDataForSession(sessionID);
    }

    public void shutdown()
    {
        writeLock.lock();
        try
        {
            isActive = false;
            repaired.shutdown();
            unrepaired.shutdown();
            pendingRepairs.shutdown();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void maybeReload(CFMetaData metadata)
    {
        // compare the old schema configuration to the new one, ignore any locally set changes.
        if (metadata.params.compaction.equals(schemaCompactionParams))
            return;

        reloadWithWriteLock(metadata);
    }

    @VisibleForTesting
    void reloadWithWriteLock(CFMetaData metadata)
    {
        writeLock.lock();
        try
        {
            reload(metadata);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Reload the compaction strategies
     *
     * Called after changing configuration and at startup.
     * @param metadata
     */
    private void reload(CFMetaData metadata)
    {
        boolean disabledWithJMX = !enabled && shouldBeEnabled();
        setStrategy(metadata.params.compaction);
        schemaCompactionParams = metadata.params.compaction;

        if (disabledWithJMX || !shouldBeEnabled())
            disable();
        else
            enable();
        startup();
    }

    public void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables)
    {
        cfs.getTracker().replaceFlushed(memtable, sstables);
        if (sstables != null && !sstables.isEmpty())
            CompactionManager.instance.submitBackground(cfs);
    }

    public int getUnleveledSSTables()
    {
        readLock.lock();
        try
        {
            if (repaired instanceof LeveledCompactionStrategy && unrepaired instanceof LeveledCompactionStrategy)
            {
                int count = 0;
                count += ((LeveledCompactionStrategy)repaired).getLevelSize(0);
                count += ((LeveledCompactionStrategy)unrepaired).getLevelSize(0);
                for (AbstractCompactionStrategy strategy : pendingRepairs.getStrategies())
                    count += ((LeveledCompactionStrategy) strategy).getLevelSize(0);
                return count;
            }
            return 0;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public int[] getSSTableCountPerLevel()
    {
        readLock.lock();
        try
        {
            if (repaired instanceof LeveledCompactionStrategy && unrepaired instanceof LeveledCompactionStrategy)
            {
                int [] res = new int[LeveledManifest.MAX_LEVEL_COUNT];
                int[] repairedCountPerLevel = ((LeveledCompactionStrategy) repaired).getAllLevelSize();
                res = sumArrays(res, repairedCountPerLevel);
                int[] unrepairedCountPerLevel = ((LeveledCompactionStrategy) unrepaired).getAllLevelSize();
                res = sumArrays(res, unrepairedCountPerLevel);
                int[] pendingRepairCountPerLevel = pendingRepairs.getSSTableCountPerLevel();
                res = sumArrays(res, pendingRepairCountPerLevel);
                return res;
            }
            return null;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public int[] getSSTableCountPerTWCSBucket()
    {
        readLock.lock();
        try
        {
            if (repaired instanceof TimeWindowCompactionStrategy && unrepaired instanceof TimeWindowCompactionStrategy)
            {
                Map<Long, Integer> countRepaired = ((TimeWindowCompactionStrategy) repaired).getSSTableCountByBuckets();
                Map<Long, Integer> countUnrepaired = ((TimeWindowCompactionStrategy) unrepaired).getSSTableCountByBuckets();
                return sumCountsByBucket(countRepaired, countUnrepaired, 30);
            }
            return null;
        }
        finally
        {
            readLock.unlock();
        }
    }

    static int[] sumCountsByBucket(Map<Long, Integer> a, Map<Long, Integer> b, int max)
    {
        TreeMap<Long, Integer> merged = new TreeMap<>(Comparator.reverseOrder());
        Stream.concat(a.entrySet().stream(), b.entrySet().stream()).forEach(e -> merged.merge(e.getKey(), e.getValue(), Integer::sum));
        return merged.values().stream().limit(max).mapToInt(i -> i).toArray();
    }

    static int[] sumArrays(int[] a, int[] b)
    {
        int[] res = new int[Math.max(a.length, b.length)];
        for (int i = 0; i < res.length; i++)
        {
            if (i < a.length && i < b.length)
                res[i] = a[i] + b[i];
            else if (i < a.length)
                res[i] = a[i];
            else
                res[i] = b[i];
        }
        return res;
    }

    public Directories getDirectories()
    {
        readLock.lock();
        try
        {
            assert repaired.getClass().equals(unrepaired.getClass());
            return repaired.getDirectories();
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            readLock.lock();
            try
            {
                SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
                for (SSTableReader sstable : flushedNotification.added)
                {
                    getCompactionStrategyFor(sstable).addSSTable(sstable);
                }
            }
            finally
            {
                readLock.unlock();
            }
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            Set<SSTableReader> pendingAdded = new HashSet<>();
            Set<SSTableReader> pendingRemoved = new HashSet<>();
            Set<SSTableReader> repairedRemoved = new HashSet<>();
            Set<SSTableReader> repairedAdded = new HashSet<>();
            Set<SSTableReader> unrepairedRemoved = new HashSet<>();
            Set<SSTableReader> unrepairedAdded = new HashSet<>();

            // we need write lock here since we might be moving sstables between strategies
            writeLock.lock();
            try
            {
                for (SSTableReader sstable : listChangedNotification.removed)
                {
                    if (sstable.isPendingRepair())
                        pendingRemoved.add(sstable);
                    else if (sstable.isRepaired())
                        repairedRemoved.add(sstable);
                    else
                        unrepairedRemoved.add(sstable);
                }

                for (SSTableReader sstable : listChangedNotification.added)
                {
                    if (sstable.isPendingRepair())
                        pendingAdded.add(sstable);
                    else if (sstable.isRepaired())
                        repairedAdded.add(sstable);
                    else
                        unrepairedAdded.add(sstable);
                }

                pendingRepairs.replaceSSTables(pendingRemoved, pendingAdded);
                if (!repairedRemoved.isEmpty())
                {
                    repaired.replaceSSTables(repairedRemoved, repairedAdded);
                }
                else
                {
                    for (SSTableReader sstable : repairedAdded)
                        repaired.addSSTable(sstable);
                }

                if (!unrepairedRemoved.isEmpty())
                {
                    unrepaired.replaceSSTables(unrepairedRemoved, unrepairedAdded);
                }
                else
                {
                    for (SSTableReader sstable : unrepairedAdded)
                        unrepaired.addSSTable(sstable);
                }
            }
            finally
            {
                writeLock.unlock();
            }
        }
        else if (notification instanceof SSTableRepairStatusChanged)
        {
            // we need a write lock here since we move sstables from one strategy instance to another
            writeLock.lock();
            try
            {
                for (SSTableReader sstable : ((SSTableRepairStatusChanged) notification).sstables)
                {
                    if (sstable.isPendingRepair())
                    {
                        pendingRepairs.addSSTable(sstable);
                        unrepaired.removeSSTable(sstable);
                        repaired.removeSSTable(sstable);
                    }
                    else if (sstable.isRepaired())
                    {
                        pendingRepairs.removeSSTable(sstable);
                        unrepaired.removeSSTable(sstable);
                        repaired.addSSTable(sstable);
                    }
                    else
                    {
                        pendingRepairs.removeSSTable(sstable);
                        repaired.removeSSTable(sstable);
                        unrepaired.addSSTable(sstable);
                    }
                }
            }
            finally
            {
                writeLock.unlock();
            }
        }
        else if (notification instanceof SSTableDeletingNotification)
        {
            writeLock.lock();
            try
            {
                SSTableReader sstable = ((SSTableDeletingNotification)notification).deleting;
                if (sstable.isPendingRepair())
                    pendingRepairs.removeSSTable(sstable);
                else if (sstable.isRepaired())
                    repaired.removeSSTable(sstable);
                else
                    unrepaired.removeSSTable(sstable);
            }
            finally
            {
                writeLock.unlock();
            }
        }
        else if (notification instanceof SSTableMetadataChanged)
        {
            SSTableMetadataChanged lcNotification = (SSTableMetadataChanged) notification;
            handleMetadataChangedNotification(lcNotification.sstable, lcNotification.oldMetadata);
        }
    }

    private void handleMetadataChangedNotification(SSTableReader sstable, StatsMetadata oldMetadata)
    {
        AbstractCompactionStrategy acs = getCompactionStrategyFor(sstable);
        acs.metadataChanged(oldMetadata, sstable);
    }

    public void enable()
    {
        writeLock.lock();
        try
        {
            if (repaired != null)
                repaired.enable();
            if (unrepaired != null)
                unrepaired.enable();
            if (pendingRepairs != null)
                pendingRepairs.enable();
            // enable this last to make sure the strategies are ready to get calls.
            enabled = true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void disable()
    {
        writeLock.lock();
        try
        {
            // disable this first avoid asking disabled strategies for compaction tasks
            enabled = false;
            if (repaired != null)
                repaired.disable();
            if (unrepaired != null)
                unrepaired.disable();
            if (pendingRepairs != null)
                pendingRepairs.disable();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Create ISSTableScanner from the given sstables
     *
     * Delegates the call to the compaction strategies to allow LCS to create a scanner
     * @param sstables
     * @param ranges
     * @return
     */
    @SuppressWarnings("resource")
    public AbstractCompactionStrategy.ScannerList maybeGetScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        List<SSTableReader> pendingRepairSSTables = new ArrayList<>();
        List<SSTableReader> repairedSSTables = new ArrayList<>();
        List<SSTableReader> unrepairedSSTables = new ArrayList<>();
        Set<ISSTableScanner> scanners = new HashSet<>(sstables.size());
        readLock.lock();
        try
        {
            for (SSTableReader sstable : sstables)
            {
                if (sstable.isPendingRepair())
                    pendingRepairSSTables.add(sstable);
                else if (sstable.isRepaired())
                    repairedSSTables.add(sstable);
                else
                    unrepairedSSTables.add(sstable);
            }

            AbstractCompactionStrategy.ScannerList repairedScanners = repaired.getScanners(repairedSSTables, ranges);
            AbstractCompactionStrategy.ScannerList unrepairedScanners = unrepaired.getScanners(unrepairedSSTables, ranges);
            scanners.addAll(repairedScanners.scanners);
            scanners.addAll(unrepairedScanners.scanners);
            scanners.addAll(pendingRepairs.getScanners(pendingRepairSSTables, ranges));
        }
        catch (PendingRepairManager.IllegalSSTableArgumentException e)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, new ConcurrentModificationException(e));
        }
        finally
        {
            readLock.unlock();
        }
        return new AbstractCompactionStrategy.ScannerList(new ArrayList<>(scanners));
    }

    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        while (true)
        {
            try
            {
                return maybeGetScanners(sstables, ranges);
            }
            catch (ConcurrentModificationException e)
            {
                logger.debug("SSTable repairedAt/pendingRepaired values changed while getting scanners");
            }
        }
    }

    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, null);
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        readLock.lock();
        try
        {
            return unrepaired.groupSSTablesForAntiCompaction(sstablesToGroup);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public long getMaxSSTableBytes()
    {
        readLock.lock();
        try
        {
            return unrepaired.getMaxSSTableBytes();
        }
        finally
        {
            readLock.unlock();
        }
    }

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        validateForCompaction(txn.originals(), cfs, getDirectories());
        return getCompactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    private static void validateForCompaction(Iterable<SSTableReader> input, ColumnFamilyStore cfs, Directories directories)
    {
        SSTableReader firstSSTable = Iterables.getFirst(input, null);
        assert firstSSTable != null;
        boolean repaired = firstSSTable.isRepaired();
        boolean isPending = firstSSTable.isPendingRepair();
        UUID pendingRepair = firstSSTable.getSSTableMetadata().pendingRepair;
        for (SSTableReader sstable : input)
        {
            if (sstable.isRepaired() != repaired)
                throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
            if (isPending != sstable.isPendingRepair())
                throw new UnsupportedOperationException("You can't compact sstables pending for repair with non-pending ones");
            if (isPending && !pendingRepair.equals(sstable.getSSTableMetadata().pendingRepair))
                throw new UnsupportedOperationException("You can't compact sstables from different pending repair sessions");
        }
    }

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call() throws Exception
            {
                readLock.lock();
                try
                {
                    Collection<AbstractCompactionTask> repairedTasks = repaired.getMaximalTask(gcBefore, splitOutput);
                    Collection<AbstractCompactionTask> unrepairedTasks = unrepaired.getMaximalTask(gcBefore, splitOutput);
                    Collection<AbstractCompactionTask> pendingRepairTasks = pendingRepairs.getMaximalTasks(gcBefore, splitOutput);

                    List<AbstractCompactionTask> tasks = new ArrayList<>();
                    if (repairedTasks != null)
                    {
                        tasks.addAll(repairedTasks);
                    }
                    if (unrepairedTasks != null)
                    {
                        tasks.addAll(unrepairedTasks);
                    }
                    if (pendingRepairTasks != null)
                    {
                        tasks.addAll(pendingRepairTasks);
                    }
                    return !tasks.isEmpty() ? tasks : null;
                }
                finally
                {
                    readLock.unlock();
                }
            }
        }, false, false);
    }


    /**
     * Return a list of compaction tasks corresponding to the sstables requested. Split the sstables according
     * to whether they are repaired or not, and by disk location. Return a task per disk location and repair status
     * group.
     *
     * @param sstables the sstables to compact
     * @param gcBefore gc grace period, throw away tombstones older than this
     * @return a list of compaction tasks corresponding to the sstables requested
     */
    public List<AbstractCompactionTask> getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
    {
        readLock.lock();
        try
        {
            maybeReload(cfs.metadata);
            List<AbstractCompactionTask> ret = new ArrayList<>();

            final List<SSTableReader> pendingSSTables = sstables.stream().filter(s -> !s.isMarkedSuspect() && !s.isRepaired() && s.isPendingRepair()).collect(Collectors.toList());
            final List<SSTableReader> repairedSSTables = sstables.stream().filter(s -> !s.isMarkedSuspect() && s.isRepaired() && !s.isPendingRepair()).collect(Collectors.toList());
            final List<SSTableReader> unRepairedSSTables = sstables.stream().filter(s -> !s.isMarkedSuspect() && !s.isRepaired() && !s.isPendingRepair()).collect(Collectors.toList());

            assert sstables.size() == pendingSSTables.size() + repairedSSTables.size() + unRepairedSSTables.size();

            AbstractCompactionTask repairedTasks = !repairedSSTables.isEmpty() ? repaired.getUserDefinedTask(repairedSSTables, gcBefore) : null;
            AbstractCompactionTask unrepairedTasks = !unRepairedSSTables.isEmpty() ? unrepaired.getUserDefinedTask(unRepairedSSTables, gcBefore) : null;
            Collection<AbstractCompactionTask> pendingTasks = pendingRepairs.createUserDefinedTasks(pendingSSTables, gcBefore);

            if (repairedTasks == null && unrepairedTasks == null && pendingTasks.isEmpty())
                return null;

            if (repairedTasks != null)
                ret.add(repairedTasks);
            if (unrepairedTasks != null)
                ret.add(unrepairedTasks);

            ret.addAll(pendingTasks);

            return ret;
        }
        finally
        {
            readLock.unlock();
        }
    }


    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        validateForCompaction(sstables, cfs, getDirectories());
        return getCompactionStrategyFor(sstables.iterator().next()).getUserDefinedTask(sstables, gcBefore);
    }

    public int getEstimatedRemainingTasks()
    {
        readLock.lock();
        try
        {
            int tasks = 0;
            tasks += repaired.getEstimatedRemainingTasks();
            tasks += unrepaired.getEstimatedRemainingTasks();
            tasks += pendingRepairs.getEstimatedRemainingTasks();
            return tasks;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean shouldBeEnabled()
    {
        return params.isEnabled();
    }

    public String getName()
    {
        return unrepaired.getName();
    }

    public List<AbstractCompactionStrategy> getStrategies()
    {
        readLock.lock();
        try
        {
            Collection<AbstractCompactionStrategy> pending = pendingRepairs.getStrategies();
            List<AbstractCompactionStrategy> strategies = new ArrayList<>(pending.size() + 2);
            strategies.add(repaired);
            strategies.add(unrepaired);
            strategies.addAll(pending);
            return strategies;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void setNewLocalCompactionStrategy(CompactionParams params)
    {
        logger.info("Switching local compaction strategy from {} to {}}", this.params, params);
        writeLock.lock();
        try
        {
            setStrategy(params);
            if (shouldBeEnabled())
                enable();
            else
                disable();
            startup();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private void setStrategy(CompactionParams params)
    {
        if (repaired != null)
            repaired.shutdown();
        if (unrepaired != null)
            unrepaired.shutdown();
        if (pendingRepairs != null)
            pendingRepairs.shutdown();

        repaired = CFMetaData.createCompactionStrategyInstance(cfs, params);
        unrepaired = CFMetaData.createCompactionStrategyInstance(cfs, params);
        pendingRepairs = new PendingRepairManager(cfs, params);
        this.params = params;
    }

    public CompactionParams getCompactionParams()
    {
        return params;
    }

    public boolean onlyPurgeRepairedTombstones()
    {
        return Boolean.parseBoolean(params.options().get(AbstractCompactionStrategy.ONLY_PURGE_REPAIRED_TOMBSTONES));
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector collector, SerializationHeader header, LifecycleNewTracker lifecycleNewTracker)
    {
        readLock.lock();
        try
        {
            if (pendingRepair != ActiveRepairService.NO_PENDING_REPAIR)
            {
                return pendingRepairs.getOrCreate(pendingRepair).createSSTableMultiWriter(descriptor, keyCount, ActiveRepairService.UNREPAIRED_SSTABLE, pendingRepair, collector, header, lifecycleNewTracker);
            }
            else if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
            {
                return unrepaired.createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, collector, header, lifecycleNewTracker);
            }
            else
            {
                return repaired.createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, collector, header, lifecycleNewTracker);
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean supportsEarlyOpen()
    {
        return repaired.supportsEarlyOpen();
    }

    @VisibleForTesting
    PendingRepairManager getPendingRepairManager()
    {
        return pendingRepairs;
    }

    /**
     * Mutates sstable repairedAt times and notifies listeners of the change with the writeLock held. Prevents races
     * with other processes between when the metadata is changed and when sstables are moved between strategies.
     */
    public void mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair) throws IOException
    {
        Set<SSTableReader> changed = new HashSet<>();

        writeLock.lock();
        try
        {
            for (SSTableReader sstable: sstables)
            {
                sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, pendingRepair);
                sstable.reloadSSTableMetadata();
                verifyMetadata(sstable, repairedAt, pendingRepair);
                changed.add(sstable);
            }
        }
        finally
        {
            try
            {
                // if there was an exception mutating repairedAt, we should still notify for the
                // sstables that we were able to modify successfully before releasing the lock
                cfs.getTracker().notifySSTableRepairedStatusChanged(changed);
            }
            finally
            {
                writeLock.unlock();
            }
        }
    }

    private static void verifyMetadata(SSTableReader sstable, long repairedAt, UUID pendingRepair)
    {
        if (!Objects.equals(pendingRepair, sstable.getPendingRepair()))
            throw new IllegalStateException(String.format("Failed setting pending repair to %s on %s (pending repair is %s)", pendingRepair, sstable, sstable.getPendingRepair()));
        if (repairedAt != sstable.getRepairedAt())
            throw new IllegalStateException(String.format("Failed setting repairedAt to %d on %s (repairedAt is %d)", repairedAt, sstable, sstable.getRepairedAt()));
    }
}
