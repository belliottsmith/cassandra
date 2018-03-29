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
import java.util.concurrent.TimeUnit;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.google.common.primitives.Doubles;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;

public class LeveledCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategy.class);
    private static final NoSpamLogger noSpam1m = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final String SSTABLE_SIZE_OPTION = "sstable_size_in_mb";
    private static final String SCHEDULED_COMPACTION_OPTION = "scheduled_compactions";

    private final long maxGCSSTableSize;
    private final int maxGCSSTables;

    /**
     * This is calculated as scheduledCompactionCycleTime / #subranges
     */
    private long millisUntilNextScheduledCompaction = 0;
    /**
     * lastScheduledCompactionTime is set once a compaction completes successfully (or if there is no compaction
     * to run for the given sub range). It is set to lastScheduledCompactionTime + millisUntilNextScheduledCompaction.
     */
    private long lastScheduledCompactionTime = -1;
    private Token lastScheduleCompactionToken = null;

    @VisibleForTesting
    final LeveledManifest manifest;
    private final int maxSSTableSizeInMB;
    private final boolean enableScheduledCompactions;

    public LeveledCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        int configuredMaxSSTableSize = 160;
        boolean configuredEnableScheduledCompactions = false;
        SizeTieredCompactionStrategyOptions localOptions = new SizeTieredCompactionStrategyOptions(options);
        if (options != null)
        {
            if (options.containsKey(SSTABLE_SIZE_OPTION))             
            {                 
                configuredMaxSSTableSize = Integer.parseInt(options.get(SSTABLE_SIZE_OPTION));                 
                if (!Boolean.getBoolean("cassandra.tolerate_sstable_size"))                 
                {                     
                    if (configuredMaxSSTableSize >= 1000)
                        logger.warn("Max sstable size of {}MB is configured for {}.{}; having a unit of compaction this large is probably a bad idea",
                                configuredMaxSSTableSize, cfs.name, cfs.getColumnFamilyName());
                    if (configuredMaxSSTableSize < 50)  
                        logger.warn("Max sstable size of {}MB is configured for {}.{}.  Testing done for CASSANDRA-5727 indicates that performance improves up to 160MB",
                                configuredMaxSSTableSize, cfs.name, cfs.getColumnFamilyName());
                }
            }
            if (options.containsKey(SCHEDULED_COMPACTION_OPTION))
            {
                configuredEnableScheduledCompactions = Boolean.parseBoolean(options.get(SCHEDULED_COMPACTION_OPTION));
            }
        }
        enableScheduledCompactions = configuredEnableScheduledCompactions;
        maxSSTableSizeInMB = configuredMaxSSTableSize;

        maxGCSSTableSize = Integer.getInteger("cassandra.aggressivegcls.max_sstable_size_mb", 10240) * 1024l * 1024l;
        maxGCSSTables = Integer.getInteger("cassandra.aggressivegcls.max_sstables", 20);

        manifest = new LeveledManifest(cfs, this.maxSSTableSizeInMB, localOptions);
        logger.trace("Created {}", manifest);
    }

    public int getLevelSize(int i)
    {
        return manifest.getLevelSize(i);
    }

    public int[] getAllLevelSize()
    {
        return manifest.getAllLevelSize();
    }

    @Override
    public void startup()
    {
        manifest.calculateLastCompactedKeys();
        super.startup();
    }

    /**
     * the only difference between background and maximal in LCS is that maximal is still allowed
     * (by explicit user request) even when compaction is disabled.
     */
    @SuppressWarnings("resource")
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        Collection<SSTableReader> previousCandidate = null;
        while (true)
        {
            OperationType op;
            LeveledManifest.CompactionCandidate candidate = manifest.getCompactionCandidates();
            if (candidate == null)
            {
                Boolean repaired = handlingRepaired(); // returns null if there are no sstables
                if (runScheduledCompactions() && repaired != null && timeForScheduledCompaction(repaired))
                {
                    logger.info("No standard compactions to do, checking if there is a scheduled compaction to run for {}.{} (repaired = {})", cfs.keyspace.getName(), cfs.getColumnFamilyName(), repaired);
                    ScheduledLeveledCompactionTask task = getNextScheduledCompactionTask(gcBefore, repaired);
                    if (task != null)
                        return task;
                }
                // if there is no sstable to compact in standard way, try compacting based on droppable tombstone ratio
                candidate = getTombstoneCompactionTask(gcBefore, DatabaseDescriptor.getEnableAggressiveGCCompaction());
                if (candidate == null)
                {
                    logger.trace("No compaction necessary for {}", this);
                    return null;
                }
                op = OperationType.TOMBSTONE_COMPACTION;
            }
            else
            {
                op = OperationType.COMPACTION;
            }

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
            if (candidate.sstables.equals(previousCandidate))
            {
                logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                            "unless it happens frequently, in which case it must be reported. Will retry later.",
                            candidate.sstables);
                return null;
            }

            LifecycleTransaction txn = cfs.getTracker().tryModify(candidate.sstables, OperationType.COMPACTION);
            if (txn != null)
            {
                LeveledCompactionTask newTask = new LeveledCompactionTask(cfs, txn, candidate.level, gcBefore, candidate.maxSSTableBytes, false);
                newTask.setCompactionType(op);
                return newTask;
            }
            previousCandidate = candidate.sstables;
        }
    }

    private boolean runScheduledCompactions()
    {
        // local partitioner does not support midpoint() which we need to split the ranges
        return !(cfs.getPartitioner() instanceof LocalPartitioner) &&
               enableScheduledCompactions &&
               DatabaseDescriptor.getEnableScheduledCompactions();
    }

    @VisibleForTesting
    boolean timeForScheduledCompaction(boolean repaired)
    {
        logger.debug("Checking if it is time for a scheduled compaction for {}.{} (repaired = {}) ({} > {} + {} ? {})",
                    cfs.keyspace.getName(),
                    cfs.getColumnFamilyName(),
                    repaired,
                    System.currentTimeMillis(),
                    lastScheduledCompactionTime,
                    millisUntilNextScheduledCompaction,
                    System.currentTimeMillis()  > lastScheduledCompactionTime + millisUntilNextScheduledCompaction);
        if (lastScheduledCompactionTime == -1)
        {
            Pair<Token, Long> lastRepair = SystemKeyspace.getLastSuccessfulScheduledCompaction(cfs.keyspace.getName(), cfs.getColumnFamilyName(), repaired);
            lastScheduleCompactionToken = lastRepair == null ? null : lastRepair.left;
            lastScheduledCompactionTime = lastRepair == null ? System.currentTimeMillis() : lastRepair.right;
            logger.info("Loaded last successful subrange compaction time = {} token = {} for {}.{}", lastScheduledCompactionTime, lastScheduleCompactionToken, cfs.keyspace.getName(), cfs.getColumnFamilyName());
        }

        if (System.currentTimeMillis() > lastScheduledCompactionTime + millisUntilNextScheduledCompaction * 2)
            noSpam1m.warn("Could not run a scheduled sub range compaction over the last {}ms", millisUntilNextScheduledCompaction);
        return System.currentTimeMillis() > lastScheduledCompactionTime + millisUntilNextScheduledCompaction;
    }

    @VisibleForTesting
    ScheduledLeveledCompactionTask getNextScheduledCompactionTask(int gcBefore, boolean repaired)
    {
        Range<Token> nextBounds = getNextBounds();
        if (nextBounds == null)
            return null;

        logger.info("Getting aggressive compaction candidates for {} {}.{}", nextBounds, cfs.keyspace.getName(), cfs.getColumnFamilyName());
        LeveledManifest.CompactionCandidate candidate = manifest.getAggressiveCompactionCandidates(nextBounds, DatabaseDescriptor.getMaxScheduledCompactionSSTableSizeBytes());
        Collection<SSTableReader> toCompact = candidate != null ? candidate.sstables : Collections.emptySet();
        logger.info("Got {} sstables for scheduled compaction for {}.{}: {}", toCompact.size(), cfs.keyspace.getName(), cfs.getTableName(), toCompact);
        if (candidate == null)
        {
            logger.debug("No sstables for {} in {}.{} ({})", nextBounds, cfs.keyspace.getName(), cfs.getColumnFamilyName(), repaired);
            // there were no sstables in the given range, move on to the next one!
            successfulSubrangeCompaction(nextBounds.right, lastScheduledCompactionTime + millisUntilNextScheduledCompaction, repaired);
        }
        else if (candidate.sstables.size() == 1 && DatabaseDescriptor.getSkipSingleSSTableScheduledCompactions())
        {
            logger.info("Skipping single sstable scheduled compaction for {}.{}", cfs.keyspace.getName(), cfs.getColumnFamilyName());
            successfulSubrangeCompaction(nextBounds.right, lastScheduledCompactionTime + millisUntilNextScheduledCompaction, repaired);
        }
        else if (candidate.sstables.size() <= DatabaseDescriptor.getMaxScheduledCompactionSSTableCount())
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(candidate.sstables, OperationType.TOMBSTONE_COMPACTION);
            if (txn != null)
            {
                logger.info("Creating scheduled compaction task for sstables {} into level {} for {}.{}",
                            candidate.sstables, candidate.level, cfs.keyspace.getName(), cfs.getColumnFamilyName());
                // note that we don't set startTime to currentTime since we might need to run several scheduled compactions to
                // keep 'pace' with the windows - say we are down for 1 day, then, as we come back, we might need to run several
                // compactions quickly to make sure we cover the whole range in getScheduledCompactionCycleTimeDays
                ScheduledLeveledCompactionTask task = new ScheduledLeveledCompactionTask(cfs,
                                                                                         txn,
                                                                                         candidate.level,
                                                                                         gcBefore,
                                                                                         candidate.maxSSTableBytes,
                                                                                         nextBounds,
                                                                                         lastScheduledCompactionTime + millisUntilNextScheduledCompaction,
                                                                                         repaired);
                task.setCompactionType(OperationType.TOMBSTONE_COMPACTION);
                return task;
            }
            else
            {
                logger.debug("Could not mark compacting: {} ", candidate.sstables);
            }
        }
        else if (candidate.sstables.size() > DatabaseDescriptor.getMaxScheduledCompactionSSTableCount())
        {
            logger.info("Too many sstables for scheduled compaction for {}.{}: {}", cfs.keyspace.getName(), cfs.getColumnFamilyName(), candidate.sstables.size());
        }
        return null;
    }

    /**
     * Checks the sstables if we are compacting repaired or unrepaired
     *
     * if there are no sstables, we can't know and instead return null
     *
     * @return true if this strategy instance handles repaired sstables, false if not, and null if there are no sstables
     */
    private Boolean handlingRepaired()
    {
        for (List<SSTableReader> sstables : manifest.generations)
        {
            for (SSTableReader sstable : sstables)
                return sstable.isRepaired();
        }
        return null;
    }

    /**
     * Figures out the next boundaries we should run a scheduled compaction on
     *
     * Gets the local ranges, splits them until there are at least DatabaseDescriptor.getScheduledCompactionRangeSplits()
     * subranges.
     *
     * Bounds are calculated as "last compacted token" -> "next boundary"
     */
    private Range<Token> getNextBounds()
    {
        Collection<Range<Token>> localRanges = StorageService.instance.getLocalRanges(cfs.keyspace.getName());
        if (localRanges == null || localRanges.size() == 0)
        {
            logger.warn("No local ranges - can't do scheduled compactions");
            return null;
        }
        List<Range<Token>> splitRanges = splitRanges(cfs.getPartitioner(), Range.normalize(localRanges), DatabaseDescriptor.getScheduledCompactionRangeSplits());
        if (splitRanges.isEmpty())
            return null;
        List<Token> bounds = new ArrayList<>();
        for (Range<Token> r : splitRanges)
            bounds.add(r.right);
        logger.debug("Got boundaries: {}", bounds);

        millisUntilNextScheduledCompaction = TimeUnit.SECONDS.toMillis(DatabaseDescriptor.getScheduledCompactionCycleTimeSeconds()) / bounds.size();
        if (lastScheduleCompactionToken == null)
        {
            logger.debug("no old successful subrange compactions, starting from beginning {} -> {}", bounds.get(0), bounds.get(1));
            return new Range<>(bounds.get(0), bounds.get(1));
        }

        for (Token t : bounds)
        {
            // note that we need to special handle the partitioner min token - having that in the bounds means that we are about to wrap around
            // and we need to create a special range where the right bound is min token. This is then handled in LeveledManifest#overlappingWithMin
            // reason bounds contains the min token is that when doing Range.normalize on a wrapping range, it becomes:
            // Range.normalize( (x, x] ) = [(minToken, minToken]]
            if (t.compareTo(lastScheduleCompactionToken) > 0 || t.equals(cfs.getPartitioner().getMinimumToken()))
                return new Range<>(lastScheduleCompactionToken, t);
        }
        // the last successful compaction token is beyond the last boundary - start over
        logger.debug("Wrapped around finding next bound, starting from beginning {} -> {} for {}.{}", bounds.get(0), bounds.get(1), cfs.keyspace.getName(), cfs.getColumnFamilyName());
        return new Range<>(bounds.get(0), bounds.get(1));

    }

    /**
     * Splits the given ranges until there is *at least* minSplits ranges
     *
     * If the number of input ranges is less than minSplits each input range is split in two parts. This is repeated until
     * there are at least minSplits subranges. This means that we can end up with at more than minSplits ranges.
     *
     * Note that if it is not possible to split the given ranges in minSplits parts, this method returns an empty list.
     *
     * A range is not split if the partitioner mid point is equal to either the right or left token of the given range.
     */
    @VisibleForTesting
    static List<Range<Token>> splitRanges(IPartitioner partitioner, List<Range<Token>> ranges, int minSplits)
    {
        List<Range<Token>> splitRanges = Lists.newArrayList(ranges);
        while (splitRanges.size() < Math.max(2, minSplits))
        {
            List<Range<Token>> newSplitRanges = new ArrayList<>(splitRanges.size() * 2);
            for (Range<Token> r : splitRanges)
            {
                Token midPoint = partitioner.midpoint(r.left, r.right);
                if (!midPoint.equals(r.left) && !midPoint.equals(r.right))
                {
                    newSplitRanges.add(new Range<>(r.left, midPoint));
                    newSplitRanges.add(new Range<>(midPoint, r.right));
                }
                else
                {
                    logger.info("Not splitting range {} - too narrow", r);
                    newSplitRanges.add(r);
                }
            }
            if (newSplitRanges.size() == splitRanges.size())
                return Collections.emptyList();
            splitRanges = newSplitRanges;
        }
        splitRanges.sort(Comparator.comparing(r -> r.left));
        return splitRanges;
    }

    @SuppressWarnings("resource")
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> sstables = manifest.getAllSSTables();

        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(sstables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        return Arrays.<AbstractCompactionTask>asList(new LeveledCompactionTask(cfs, txn, 0, gcBefore, getMaxSSTableBytes(), true));

    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {

        if (sstables.isEmpty())
            return null;

        LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }
        int level = sstables.size() > 1 ? 0 : sstables.iterator().next().getSSTableLevel();
        return new LeveledCompactionTask(cfs, transaction, level, gcBefore, level == 0 ? Long.MAX_VALUE : getMaxSSTableBytes(), false);
    }

    @Override
    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        assert txn.originals().size() > 0;
        int level = -1;
        // if all sstables are in the same level, we can set that level:
        for (SSTableReader sstable : txn.originals())
        {
            if (level == -1)
                level = sstable.getSSTableLevel();
            if (level != sstable.getSSTableLevel())
                level = 0;
        }
        return new LeveledCompactionTask(cfs, txn, level, gcBefore, maxSSTableBytes, false);
    }

    /**
     * Leveled compaction strategy has guarantees on the data contained within each level so we
     * have to make sure we only create groups of SSTables with members from the same level.
     * This way we won't end up creating invalid sstables during anti-compaction.
     * @param ssTablesToGroup
     * @return Groups of sstables from the same level
     */
    @Override
    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> ssTablesToGroup)
    {
        int groupSize = 2;
        Map<Integer, Collection<SSTableReader>> sstablesByLevel = new HashMap<>();
        for (SSTableReader sstable : ssTablesToGroup)
        {
            Integer level = sstable.getSSTableLevel();
            if (!sstablesByLevel.containsKey(level))
            {
                sstablesByLevel.put(level, new ArrayList<SSTableReader>());
            }
            sstablesByLevel.get(level).add(sstable);
        }

        Collection<Collection<SSTableReader>> groupedSSTables = new ArrayList<>();

        for (Collection<SSTableReader> levelOfSSTables : sstablesByLevel.values())
        {
            Collection<SSTableReader> currGroup = new ArrayList<>();
            for (SSTableReader sstable : levelOfSSTables)
            {
                currGroup.add(sstable);
                if (currGroup.size() == groupSize)
                {
                    groupedSSTables.add(currGroup);
                    currGroup = new ArrayList<>();
                }
            }

            if (currGroup.size() != 0)
                groupedSSTables.add(currGroup);
        }
        return groupedSSTables;

    }

    public int getEstimatedRemainingTasks()
    {
        return manifest.getEstimatedTasks();
    }

    public long getMaxSSTableBytes()
    {
        return maxSSTableSizeInMB * 1024L * 1024L;
    }

    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        Set<SSTableReader>[] sstablesPerLevel = manifest.getSStablesPerLevelSnapshot();

        Multimap<Integer, SSTableReader> byLevel = ArrayListMultimap.create();
        for (SSTableReader sstable : sstables)
        {
            int level = sstable.getSSTableLevel();
            // if an sstable is not on the manifest, it was recently added or removed
            // so we add it to level -1 and create exclusive scanners for it - see below (#9935)
            if (level >= sstablesPerLevel.length || !sstablesPerLevel[level].contains(sstable))
            {
                logger.warn("Live sstable {} from level {} is not on corresponding level in the leveled manifest." +
                            " This is not a problem per se, but may indicate an orphaned sstable due to a failed" +
                            " compaction not cleaned up properly.",
                             sstable.getFilename(), level);
                level = -1;
            }
            byLevel.get(level).add(sstable);
        }

        List<ISSTableScanner> scanners = new ArrayList<ISSTableScanner>(sstables.size());
        try
        {
            for (Integer level : byLevel.keySet())
            {
                // level can be -1 when sstables are added to Tracker but not to LeveledManifest
                // since we don't know which level those sstable belong yet, we simply do the same as L0 sstables.
                if (level <= 0)
                {
                    // L0 makes no guarantees about overlapping-ness.  Just create a direct scanner for each
                    for (SSTableReader sstable : byLevel.get(level))
                        scanners.add(sstable.getScanner(ranges, CompactionManager.instance.getRateLimiter()));
                }
                else
                {
                    // Create a LeveledScanner that only opens one sstable at a time, in sorted order
                    Collection<SSTableReader> intersecting = LeveledScanner.intersecting(byLevel.get(level), ranges);
                    if (!intersecting.isEmpty())
                    {
                        @SuppressWarnings("resource") // The ScannerList will be in charge of closing (and we close properly on errors)
                        ISSTableScanner scanner = new LeveledScanner(intersecting, ranges);
                        scanners.add(scanner);
                    }
                }
            }
        }
        catch (Throwable t)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }

        return new ScannerList(scanners);
    }

    @Override
    public void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        manifest.replace(removed, added);
    }

    @Override
    public void addSSTable(SSTableReader added)
    {
        manifest.add(added);
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        manifest.remove(sstable);
    }

    @Override
    protected Set<SSTableReader> getSSTables()
    {
        return manifest.getSSTables();
    }

    // Lazily creates SSTableBoundedScanner for sstable that are assumed to be from the
    // same level (e.g. non overlapping) - see #4142
    private static class LeveledScanner extends AbstractIterator<UnfilteredRowIterator> implements ISSTableScanner
    {
        private final Collection<Range<Token>> ranges;
        private final List<SSTableReader> sstables;
        private final Iterator<SSTableReader> sstableIterator;
        private final long totalLength;

        private ISSTableScanner currentScanner;
        private long positionOffset;

        public LeveledScanner(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
        {
            this.ranges = ranges;

            // add only sstables that intersect our range, and estimate how much data that involves
            this.sstables = new ArrayList<>(sstables.size());
            long length = 0;
            for (SSTableReader sstable : sstables)
            {
                this.sstables.add(sstable);
                long estimatedKeys = sstable.estimatedKeys();
                double estKeysInRangeRatio = 1.0;

                if (estimatedKeys > 0 && ranges != null)
                    estKeysInRangeRatio = ((double) sstable.estimatedKeysForRanges(ranges)) / estimatedKeys;

                length += sstable.uncompressedLength() * estKeysInRangeRatio;
            }

            totalLength = length;
            Collections.sort(this.sstables, SSTableReader.sstableComparator);
            sstableIterator = this.sstables.iterator();
            assert sstableIterator.hasNext(); // caller should check intersecting first
            currentScanner = sstableIterator.next().getScanner(ranges, CompactionManager.instance.getRateLimiter());
        }

        public static Collection<SSTableReader> intersecting(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
        {
            if (ranges == null)
                return Lists.newArrayList(sstables);

            Set<SSTableReader> filtered = new HashSet<>();
            for (Range<Token> range : ranges)
            {
                for (SSTableReader sstable : sstables)
                {
                    Range<Token> sstableRange = new Range<>(sstable.first.getToken(), sstable.last.getToken());
                    if (range == null || sstableRange.intersects(range))
                        filtered.add(sstable);
                }
            }
            return filtered;
        }


        public boolean isForThrift()
        {
            return false;
        }

        public CFMetaData metadata()
        {
            return sstables.get(0).metadata; // The ctor checks we have at least one sstable
        }

        protected UnfilteredRowIterator computeNext()
        {
            if (currentScanner == null)
                return endOfData();

            while (true)
            {
                if (currentScanner.hasNext())
                    return currentScanner.next();

                positionOffset += currentScanner.getLengthInBytes();
                currentScanner.close();
                if (!sstableIterator.hasNext())
                {
                    // reset to null so getCurrentPosition does not return wrong value
                    currentScanner = null;
                    return endOfData();
                }
                currentScanner = sstableIterator.next().getScanner(ranges, CompactionManager.instance.getRateLimiter());
            }
        }

        public void close()
        {
            if (currentScanner != null)
                currentScanner.close();
        }

        public long getLengthInBytes()
        {
            return totalLength;
        }

        public long getCurrentPosition()
        {
            return positionOffset + (currentScanner == null ? 0L : currentScanner.getCurrentPosition());
        }

        public String getBackingFiles()
        {
            return Joiner.on(", ").join(sstables);
        }
    }

    @Override
    public String toString()
    {
        return String.format("LCS@%d(%s)", hashCode(), cfs.name);
    }

    private LeveledManifest.CompactionCandidate getTombstoneCompactionTask(final int gcBefore, final boolean aggressive)
    {
        final int topLevel = manifest.getLevelCount();

        level:
        for (int i = manifest.getLevelCount(); i >= 0; i--)
        {
            // sort sstables by droppable ratio in descending order
            SortedSet<SSTableReader> sstables = manifest.getLevelSorted(i, new Comparator<SSTableReader>()
            {
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    double r1 = o1.getEstimatedDroppableTombstoneRatio(gcBefore);
                    double r2 = o2.getEstimatedDroppableTombstoneRatio(gcBefore);
                    return -1 * Doubles.compare(r1, r2);
                }
            });
            if (sstables.isEmpty())
                continue;

            Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
            for (SSTableReader sstable : sstables)
            {
                if (sstable.getEstimatedDroppableTombstoneRatio(gcBefore) <= tombstoneThreshold)
                    continue level;

                final LeveledManifest.CompactionCandidate singleCandidate = new LeveledManifest.CompactionCandidate(Collections.singleton(sstable),
                                                                                                                    sstable.getSSTableLevel(),
                                                                                                                    getMaxSSTableBytes());

                if (!aggressive)
                {
                    if (!compacting.contains(sstable) && !sstable.isMarkedSuspect() && worthDroppingTombstones(sstable, gcBefore))
                    {
                        return singleCandidate;
                    }
                    else
                    {
                        continue;
                    }
                }
                /*
                 * Aggressively drop tombstones. Find any SSTables with enough tombstones in topLevel or topLevel-1
                 * and compact it with all overlapping SSTables. This will allow removal of the tombstones and any
                 * data the tombstones are shadowing. We also include all overlaps in level 1 so we can insert into
                 * level 1.
                 */

                if (!compacting.contains(sstable) && !sstable.isMarkedSuspect())
                {
                    // only do the overlapping compaction on the top level or the one below to avoid compacting
                    // too many SSTables at once
                    if (i < topLevel - 1)
                    {
                        if (worthDroppingTombstones(sstable, gcBefore))
                        {
                            return singleCandidate;
                        }
                        continue;
                    }

                    final LeveledManifest.CompactionCandidate candidate = manifest.getAggressiveCompactionCandidates(new Range<>(sstable.first.getToken(), sstable.last.getToken()), maxGCSSTableSize);
                    assert candidate != null; // we can only return null if there are no sstables in the given range, but here we know that at least sstable exists
                    assert candidate.sstables.contains(sstable);
                    final Collection<SSTableReader> toCompact = candidate.sstables;

                    // With just one file let's do the full original check
                    if (toCompact.size() == 1)
                    {
                        // the sstable returned better be the one we gave it
                        assert sstable.equals(toCompact.iterator().next());

                        if (worthDroppingTombstones(sstable, gcBefore))
                        {
                            return singleCandidate;
                        }
                        continue;
                    }

                    if (toCompact.size() > maxGCSSTables)
                    {
                        logger.debug("Skipping tombstone compaction since too many SSTables would be compacted");
                        continue;
                    }

                    // check all SSTables are old enough so we avoid loops endlessly compacting the same set
                    final long now = System.currentTimeMillis();
                    boolean allYoung = true;
                    for (SSTableReader sst : toCompact)
                    {
                        if (sst.getCreationTimeFor(Component.DATA) + tombstoneCompactionInterval * 1000 < now)
                        {
                            allYoung = false;
                            break;
                        }
                    }
                    if (allYoung)
                    {
                        logger.debug("Skipping tombstone compaction since all SSTables are too young");
                        continue;
                    }

                    boolean compactable = true;
                    for (SSTableReader sst : toCompact)
                    {
                        if (compacting.contains(sst) || sst.isMarkedSuspect())
                        {
                            compactable = false;
                            break;
                        }
                    }
                    if (compactable)
                    {
                        logger.info("Compacting {} SSTables to attempt to remove tombstones", toCompact.size());
                        return candidate;
                    }
                    else
                    {
                        // fallback on old behavior
                        if (worthDroppingTombstones(sstable, gcBefore))
                        {
                            return singleCandidate;
                        }
                    }
                } else
                {
                    logger.debug("Skipping sstable {} because compacting/suspect", sstable);
                }
            }
        }
        return null;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);

        String size = options.containsKey(SSTABLE_SIZE_OPTION) ? options.get(SSTABLE_SIZE_OPTION) : "1";
        try
        {
            int ssSize = Integer.parseInt(size);
            if (ssSize < 1)
            {
                throw new ConfigurationException(String.format("%s must be larger than 0, but was %s", SSTABLE_SIZE_OPTION, ssSize));
            }
        }
        catch (NumberFormatException ex)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", size, SSTABLE_SIZE_OPTION), ex);
        }

        uncheckedOptions.remove(SSTABLE_SIZE_OPTION);
        // note that Boolean.parseBoolean (which we use to get SCHEDULED_COMPACTION_OPTION)
        // is friendly, returns false if it can't parse (instead of NFE like above)
        uncheckedOptions.remove(SCHEDULED_COMPACTION_OPTION);
        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        return uncheckedOptions;
    }

    @VisibleForTesting
    static class ScheduledLeveledCompactionTask extends LeveledCompactionTask
    {
        @VisibleForTesting
        final Range<Token> compactedRange;
        private final long startTime;
        private final boolean repaired;

        public ScheduledLeveledCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int level, int gcBefore, long maxSSTableBytes, Range<Token> compactingRange, long startTime, boolean repaired)
        {
            super(cfs, txn, level, gcBefore, maxSSTableBytes, false);
            this.compactedRange = compactingRange;
            this.startTime = startTime;
            this.repaired = repaired;
        }

        @Override
        public void runMayThrow() throws Exception
        {
            super.runMayThrow();
            CompactionStrategyManager csm = cfs.getCompactionStrategyManager();
// TODO: pending repair:
            if (csm.getStrategies().get(0) instanceof LeveledCompactionStrategy)
            {
                int idx = 0;
                if (!transaction.originals().iterator().next().isRepaired())
                    idx = 1;
                ((LeveledCompactionStrategy)csm.getStrategies().get(idx)).successfulSubrangeCompaction(compactedRange.right, startTime, repaired);
            }
        }
    }

    @VisibleForTesting
    void successfulSubrangeCompaction(Token token, long startTime, boolean repaired)
    {
        lastScheduleCompactionToken = token;
        lastScheduledCompactionTime = startTime;
        SystemKeyspace.successfulScheduledCompaction(cfs.keyspace.getName(), cfs.getColumnFamilyName(), repaired, token, startTime);
        logger.info("Successfully ran a scheduled compaction for {}.{} (repaired = {}) (last token = {}, startTime = {})", cfs.keyspace.getName(), cfs.getColumnFamilyName(), repaired, token, startTime);
    }

    @VisibleForTesting
    void resetSubrangeCompactionInfo()
    {
        lastScheduledCompactionTime = -1;
        lastScheduleCompactionToken = null;
        millisUntilNextScheduledCompaction = 0;
        SystemKeyspace.resetScheduledCompactions(cfs.keyspace.getName(), cfs.getColumnFamilyName());
    }
}
