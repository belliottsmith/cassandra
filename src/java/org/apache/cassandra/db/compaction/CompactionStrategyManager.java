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


import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Manages the compaction strategies.
 *
 * Currently has two instances of actual compaction strategies - one for repaired data and one for
 * unrepaired data. This is done to be able to totally separate the different sets of sstables.
 */
public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    private final ColumnFamilyStore cfs;
    private volatile AbstractCompactionStrategy repaired;
    private volatile AbstractCompactionStrategy unrepaired;
    private volatile boolean enabled = true;
    public volatile boolean isActive = true;
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
        if (!isEnabled())
            return null;

        maybeReload(cfs.metadata);

        readLock.lock();
        try
        {
            if (repaired.getEstimatedRemainingTasks() > unrepaired.getEstimatedRemainingTasks())
            {
                AbstractCompactionTask repairedTask = repaired.getNextBackgroundTask(gcBefore);
                if (repairedTask != null)
                    return repairedTask;
                return unrepaired.getNextBackgroundTask(gcBefore);
            }
            else
            {
                AbstractCompactionTask unrepairedTask = unrepaired.getNextBackgroundTask(gcBefore);
                if (unrepairedTask != null)
                    return unrepairedTask;
                return repaired.getNextBackgroundTask(gcBefore);
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public void resume()
    {
        isActive = true;
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public void pause()
    {
        isActive = false;
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
            if (sstable.isRepaired())
                return repaired;
            else
                return unrepaired;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void shutdown()
    {
        writeLock.lock();
        try
        {
            isActive = false;
            repaired.shutdown();
            unrepaired.shutdown();
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
                return res;
            }
            return null;
        }
        finally
        {
            readLock.unlock();
        }
    }

    private static int[] sumArrays(int[] a, int[] b)
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

    public boolean shouldDefragment()
    {
        readLock.lock();
        try
        {
            assert repaired.getClass().equals(unrepaired.getClass());
            return repaired.shouldDefragment();
        }
        finally
        {
            readLock.unlock();
        }
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
                    if (sstable.isRepaired())
                        repaired.addSSTable(sstable);
                    else
                        unrepaired.addSSTable(sstable);
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
            Set<SSTableReader> repairedRemoved = new HashSet<>();
            Set<SSTableReader> repairedAdded = new HashSet<>();
            Set<SSTableReader> unrepairedRemoved = new HashSet<>();
            Set<SSTableReader> unrepairedAdded = new HashSet<>();

            for (SSTableReader sstable : listChangedNotification.removed)
            {
                if (sstable.isRepaired())
                    repairedRemoved.add(sstable);
                else
                    unrepairedRemoved.add(sstable);
            }
            for (SSTableReader sstable : listChangedNotification.added)
            {
                if (sstable.isRepaired())
                    repairedAdded.add(sstable);
                else
                    unrepairedAdded.add(sstable);
            }

            // we need write lock here since we might be moving sstables between strategies
            writeLock.lock();
            try
            {
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
                    if (sstable.isRepaired())
                    {
                        unrepaired.removeSSTable(sstable);
                        repaired.addSSTable(sstable);
                    }
                    else
                    {
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
            readLock.lock();
            try
            {
                SSTableReader sstable = ((SSTableDeletingNotification)notification).deleting;
                if (sstable.isRepaired())
                    repaired.removeSSTable(sstable);
                else
                    unrepaired.removeSSTable(sstable);
            }
            finally
            {
                readLock.unlock();
            }
        }
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
    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        List<SSTableReader> repairedSSTables = new ArrayList<>();
        List<SSTableReader> unrepairedSSTables = new ArrayList<>();
        for (SSTableReader sstable : sstables)
        {
            if (sstable.isRepaired())
                repairedSSTables.add(sstable);
            else
                unrepairedSSTables.add(sstable);
        }

        readLock.lock();
        try
        {
            Set<ISSTableScanner> scanners = new HashSet<>(sstables.size());
            AbstractCompactionStrategy.ScannerList repairedScanners = repaired.getScanners(repairedSSTables, ranges);
            AbstractCompactionStrategy.ScannerList unrepairedScanners = unrepaired.getScanners(unrepairedSSTables, ranges);
            scanners.addAll(repairedScanners.scanners);
            scanners.addAll(unrepairedScanners.scanners);
            return new AbstractCompactionStrategy.ScannerList(new ArrayList<>(scanners));
        }
        finally
        {
            readLock.unlock();
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
        return getCompactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
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

                    if (repairedTasks == null && unrepairedTasks == null)
                        return null;

                    if (repairedTasks == null)
                        return unrepairedTasks;
                    if (unrepairedTasks == null)
                        return repairedTasks;

                    List<AbstractCompactionTask> tasks = new ArrayList<>();
                    tasks.addAll(repairedTasks);
                    tasks.addAll(unrepairedTasks);
                    return tasks;
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

            final List<SSTableReader> repairedSSTables = sstables.stream().filter(s -> !s.isMarkedSuspect() && s.isRepaired()).collect(Collectors.toList());
            final List<SSTableReader> unRepairedSSTables = sstables.stream().filter(s -> !s.isMarkedSuspect() && !s.isRepaired()).collect(Collectors.toList());

            assert sstables.size() == repairedSSTables.size() + unRepairedSSTables.size();

            AbstractCompactionTask repairedTasks = repaired.getUserDefinedTask(repairedSSTables, gcBefore);
            AbstractCompactionTask unrepairedTasks = unrepaired.getUserDefinedTask(unRepairedSSTables, gcBefore);

            if (repairedTasks == null && unrepairedTasks == null)
                return null;

            if (repairedTasks != null)
                ret.add(repairedTasks);
            if (unrepairedTasks != null)
                ret.add(unrepairedTasks);

            return ret;
        }
        finally
        {
            readLock.unlock();
        }
    }


    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
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
            return Arrays.asList(repaired, unrepaired);
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
        repaired = CFMetaData.createCompactionStrategyInstance(cfs, params);
        unrepaired = CFMetaData.createCompactionStrategyInstance(cfs, params);
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

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, MetadataCollector collector, SerializationHeader header, LifecycleTransaction txn)
    {
        readLock.lock();
        try
        {
            if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
            {
                return unrepaired.createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, txn);
            }
            else
            {
                return repaired.createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, txn);
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
}
