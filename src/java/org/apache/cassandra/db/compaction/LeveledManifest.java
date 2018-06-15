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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.compaction.LeveledGenerations.MAX_LEVEL_COUNT;

public class LeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);

    /**
     * if we have more than MAX_COMPACTING_L0 sstables in L0, we will run a round of STCS with at most
     * cfs.getMaxCompactionThreshold() sstables.
     */
    private static final int MAX_COMPACTING_L0 = 32;

    /**
     * If we go this many rounds without compacting
     * in the highest level, we start bringing in sstables from
     * that level into lower level compactions
     */
    private static final int NO_COMPACTION_LIMIT = 25;

    private final ColumnFamilyStore cfs;

    private final LeveledGenerations generations;

    private final SSTableReader[] lastCompactedSSTables;
    private final long maxSSTableSizeInBytes;
    private final SizeTieredCompactionStrategyOptions options;
    private final int [] compactionCounter;
    private final int levelFanoutSize;

    LeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB, int fanoutSize, SizeTieredCompactionStrategyOptions options)
    {
        this.cfs = cfs;
        this.maxSSTableSizeInBytes = maxSSTableSizeInMB * 1024L * 1024L;
        this.options = options;
        this.levelFanoutSize = fanoutSize;

        lastCompactedSSTables = new SSTableReader[MAX_LEVEL_COUNT];
        generations = new LeveledGenerations();
        compactionCounter = new int[MAX_LEVEL_COUNT];
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int fanoutSize, List<SSTableReader> sstables)
    {
        return create(cfs, maxSSTableSize, fanoutSize, sstables, new SizeTieredCompactionStrategyOptions());
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int fanoutSize, Iterable<SSTableReader> sstables, SizeTieredCompactionStrategyOptions options)
    {
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSize, fanoutSize, options);

        // ensure all SSTables are in the manifest
        manifest.addSSTables(sstables);
        manifest.calculateLastCompactedKeys();
        return manifest;
    }

    /**
     * If we want to start compaction in level n, find the newest (by modification time) file in level n+1
     * and use its last token for last compacted key in level n;
     */
    void calculateLastCompactedKeys()
    {
        for (int i = 0; i < generations.levelCount() - 1; i++)
        {
            Set<SSTableReader> level = generations.get(i + 1);
            // this level is empty
            if (level.isEmpty())
                continue;

            SSTableReader sstableWithMaxModificationTime = null;
            long maxModificationTime = Long.MIN_VALUE;
            for (SSTableReader ssTableReader : level)
            {
                long modificationTime = ssTableReader.getCreationTimeFor(Component.DATA);
                if (modificationTime >= maxModificationTime)
                {
                    sstableWithMaxModificationTime = ssTableReader;
                    maxModificationTime = modificationTime;
                }
            }

            lastCompactedSSTables[i] = sstableWithMaxModificationTime;
        }
    }

    public synchronized void addSSTables(Iterable<SSTableReader> readers)
    {
        generations.addAll(readers);
    }

    public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        assert !removed.isEmpty(); // use add() instead of promote when adding new sstables
        if (logger.isTraceEnabled())
        {
            generations.logDistribution();
            logger.trace("Replacing [{}]", toString(removed));
        }

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        int minLevel = generations.remove(removed);

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (added.isEmpty())
            return;

        if (logger.isTraceEnabled())
            logger.trace("Adding [{}]", toString(added));
        generations.addAll(added);
        lastCompactedSSTables[minLevel] = SSTableReader.sstableOrdering.max(added);
    }

    private String toString(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
        {
            builder.append(sstable.descriptor.cfname)
                   .append('-')
                   .append(sstable.descriptor.generation)
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }

    public long maxBytesForLevel(int level, long maxSSTableSizeInBytes)
    {
        return maxBytesForLevel(level, levelFanoutSize, maxSSTableSizeInBytes);
    }

    public static long maxBytesForLevel(int level, int levelFanoutSize, long maxSSTableSizeInBytes)
    {
        if (level == 0)
            return 4L * maxSSTableSizeInBytes;
        double bytes = Math.pow(levelFanoutSize, level) * maxSSTableSizeInBytes;
        if (bytes > Long.MAX_VALUE)
            throw new RuntimeException("At most " + Long.MAX_VALUE + " bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
        return (long) bytes;
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    public synchronized CompactionCandidate getCompactionCandidates()
    {
        // during bootstrap we only do size tiering in L0 to make sure
        // the streamed files can be placed in their original levels
        if (StorageService.instance.isBootstrapMode())
        {
            List<SSTableReader> mostInteresting = getSSTablesForSTCS(cfs, options, generations.get(0));
            if (!mostInteresting.isEmpty())
            {
                logger.info("Bootstrapping - doing STCS in L0");
                return new CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE);
            }
            return null;
        }
        // LevelDB gives each level a score of how much data it contains vs its ideal amount, and
        // compacts the level with the highest score. But this falls apart spectacularly once you
        // get behind.  Consider this set of levels:
        // L0: 988 [ideal: 4]
        // L1: 117 [ideal: 10]
        // L2: 12  [ideal: 100]
        //
        // The problem is that L0 has a much higher score (almost 250) than L1 (11), so what we'll
        // do is compact a batch of cfs.getMaximumCompactionThreshold() sstables with all 117 L1 sstables, and put the
        // result (say, 120 sstables) in L1. Then we'll compact the next batch of cfs.getMaxCompactionThreshold(),
        // and so forth.  So we spend most of our i/o rewriting the L1 data with each batch.
        //
        // If we could just do *all* L0 a single time with L1, that would be ideal.  But we can't
        // since we might run out of memory
        //
        // LevelDB's way around this is to simply block writes if L0 compaction falls behind.
        // We don't have that luxury.
        //
        // So instead, we
        // 1) force compacting higher levels first, which minimizes the i/o needed to compact
        //    optimially which gives us a long term win, and
        // 2) if L0 falls behind, we will size-tiered compact it to reduce read overhead until
        //    we can catch up on the higher levels.
        //
        // This isn't a magic wand -- if you are consistently writing too fast for LCS to keep
        // up, you're still screwed.  But if instead you have intermittent bursts of activity,
        // it can help a lot.

        // Let's check that L0 is far enough behind to warrant STCS.
        // If it is, it will be used before proceeding any of higher level
        CompactionCandidate l0Compaction = getSTCSInL0CompactionCandidate();

        for (int i = generations.levelCount() - 1; i > 0; i--)
        {
            Set<SSTableReader> sstables = generations.get(i);
            if (sstables.isEmpty())
                continue; // mostly this just avoids polluting the debug log with zero scores
            // we want to calculate score excluding compacting ones
            Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
            Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getTracker().getCompacting());
            double score = (double) SSTableReader.getTotalBytes(remaining) / (double)maxBytesForLevel(i, maxSSTableSizeInBytes);
            logger.trace("Compaction score for level {} is {}", i, score);

            if (score > 1.001)
            {
                // before proceeding with a higher level, let's see if L0 is far enough behind to warrant STCS
                if (l0Compaction != null)
                    return l0Compaction;

                // L0 is fine, proceed with this level
                Collection<SSTableReader> candidates = getCandidatesFor(i);
                if (!candidates.isEmpty())
                {
                    int nextLevel = getNextLevel(candidates);
                    candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                    if (logger.isTraceEnabled())
                        logger.trace("Compaction candidates for L{} are {}", i, toString(candidates));
                    return new CompactionCandidate(candidates, nextLevel, maxSSTableSizeInBytes);
                }
                else
                {
                    logger.trace("No compaction candidates for L{}", i);
                }
            }
        }

        // Higher levels are happy, time for a standard, non-STCS L0 compaction
        if (generations.get(0).isEmpty())
            return null;
        Collection<SSTableReader> candidates = getCandidatesFor(0);
        if (candidates.isEmpty())
        {
            // Since we don't have any other compactions to do, see if there is a STCS compaction to perform in L0; if
            // there is a long running compaction, we want to make sure that we continue to keep the number of SSTables
            // small in L0.
            return l0Compaction;
        }
        return new CompactionCandidate(candidates, getNextLevel(candidates), maxSSTableSizeInBytes);
    }

    public synchronized CompactionCandidate getAggressiveCompactionCandidates(Range<Token> range, long maxSSTableSizeL0)
    {
        final Set<SSTableReader> overlaps = new HashSet<>();
        for (int i = generations.levelCount() - 1; i >= 0; i--)
        {
            if (!generations.get(i).isEmpty())
            {
                // overlappingWithMin handles the wrap-around case - if range.right == partitioner min token, all sstables
                // with end token larger than start will get included
                overlaps.addAll(overlappingWithMin(cfs.getPartitioner(), range.left, range.right, generations.get(i)));
            }
        }
        final Set<SSTableReader> toCompact = new HashSet<>(overlaps);

        // exclude files in L0 that are too large
        for (SSTableReader overlappingSSTable : overlaps)
        {
            if (overlappingSSTable.getSSTableLevel() == 0 && overlappingSSTable.onDiskLength() > maxSSTableSizeL0)
            {
                logger.info("Removing SSTable {} from tombstone/scheduled compaction since it's too large", overlappingSSTable);
                toCompact.remove(overlappingSSTable);
            }
        }

        if (toCompact.isEmpty()) // we now use this method with a range argument, there is a chance that the given range is empty
            return null;

        // if just one SSTable then we don't need to do any aggressive compaction
        if (toCompact.size() == 1)
        {
            SSTableReader sstable = toCompact.iterator().next();
            return new CompactionCandidate(toCompact, sstable.getSSTableLevel(),
                                           cfs.getCompactionStrategyManager().getMaxSSTableBytes());
        }

        // In the loop over generations above we add all sstables overlapping the range given
        // but since we drop everything in L1, we need to add all sstables overlapping the result of that method.
        // For example, we might have an sstable covering the full partitioner range in L2, but tiny (range-wise) sstables in L1
        // Then toCompact would currently contain the big L2 sstable, but not all L1 sstables. Here we add everything
        // that overlaps anything in L1 to make sure we can actually drop the result in L1 without causing overlap.
        if (!generations.get(1).isEmpty())
        {
            toCompact.addAll(overlapping(toCompact, generations.get(1)));
        }

        return new CompactionCandidate(toCompact, 1, cfs.getCompactionStrategyManager().getMaxSSTableBytes());
    }

    private CompactionCandidate getSTCSInL0CompactionCandidate()
    {
        if (!DatabaseDescriptor.getDisableSTCSInL0() && generations.get(0).size() > MAX_COMPACTING_L0)
        {
            List<SSTableReader> mostInteresting = getSSTablesForSTCS(cfs, options, generations.get(0));
            if (!mostInteresting.isEmpty())
            {
                logger.debug("L0 is too far behind, performing size-tiering there first");
                return new CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE);
            }
        }

        return null;
    }

    @VisibleForTesting
    static List<SSTableReader> getSSTablesForSTCS(ColumnFamilyStore cfs, SizeTieredCompactionStrategyOptions options, Collection<SSTableReader> sstables)
    {
        logger.debug("Getting SSTables for STCS in L0 for {}.{} (sstables size = {})", cfs.keyspace.getName(), cfs.getTableName(), sstables.size());
        Iterable<SSTableReader> candidates = cfs.getTracker().getUncompacting(sstables);
        List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                    options.bucketHigh,
                                                                                    options.bucketLow,
                                                                                    options.minSSTableSize);

        if (DatabaseDescriptor.getCompactBiggestSTCSBucketInL0())
            return bestBucket(buckets, cfs.getMinimumCompactionThreshold(), cfs.getMaximumCompactionThreshold());

        return SizeTieredCompactionStrategy.mostInterestingBucket(buckets,
                cfs.getMinimumCompactionThreshold(), cfs.getMaximumCompactionThreshold());
    }

    /**
     * Input buckets are bucketed by size - all sstables in a bucket are of "similar" size - this method considers
     * all buckets with a large ( > maxThreshold) number of sstables and then picks the one with the smallest
     * sum of the sstable size on disk.
     *
     * If no buckets have more than maxThreshold sstables, the largest one (by sstable count) is returned.
     */
    @VisibleForTesting
    static List<SSTableReader> bestBucket(List<List<SSTableReader>> buckets, int minThreshold, int maxThreshold)
    {
        List<SSTableReader> candidate = null;
        List<SSTableReader> biggest = null;
        for (List<SSTableReader> bucket : buckets)
        {
            bucket.sort(SSTableReader.sizeComparator); // smallest sstables first in the list
            if (bucket.size() >= maxThreshold)
            {
                // consider all buckets with many sstables
                List<SSTableReader> prunedBucket = bucket.subList(0, maxThreshold);
                if (candidate == null || bucketSize(candidate) > bucketSize(prunedBucket))
                    candidate = prunedBucket;
            }
            // also keep track of the biggest bucket in general if no buckets are above maxThreshold
            if (biggest == null || bucket.size() > biggest.size())
                biggest = bucket;
        }

        if (candidate == null) // no buckets had more than maxThreshold sstables
            candidate = biggest == null ? Collections.emptyList() : biggest.subList(0, Math.min(maxThreshold, biggest.size()));

        if (candidate.size() < minThreshold)
            return Collections.emptyList();

        logger.info("Got {} sstables for STCS in L0, max_threshold = {}, avg sstable size = {} bytes",
                    candidate.size(),
                    maxThreshold,
                    candidate.stream().mapToLong(SSTableReader::onDiskLength).sum() / candidate.size());
        return candidate;
    }

    private static long bucketSize(List<SSTableReader> sstables)
    {
        return sstables.stream().mapToLong(SSTableReader::onDiskLength).sum();
    }

    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
    private Collection<SSTableReader> getOverlappingStarvedSSTables(int targetLevel, Collection<SSTableReader> candidates)
    {
        Set<SSTableReader> withStarvedCandidate = new HashSet<>(candidates);

        for (int i = generations.levelCount() - 1; i > 0; i--)
            compactionCounter[i]++;
        compactionCounter[targetLevel] = 0;
        if (logger.isTraceEnabled())
        {
            for (int j = 0; j < compactionCounter.length; j++)
                logger.trace("CompactionCounter: {}: {}", j, compactionCounter[j]);
        }

        for (int i = generations.levelCount() - 1; i > 0; i--)
        {
            if (getLevelSize(i) > 0)
            {
                if (compactionCounter[i] > NO_COMPACTION_LIMIT)
                {
                    // we try to find an sstable that is fully contained within  the boundaries we are compacting;
                    // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
                    // this means that we will not create overlap in L2 if we add an sstable
                    // contained within 0 -> 33 to the compaction
                    PartitionPosition max = null;
                    PartitionPosition min = null;
                    for (SSTableReader candidate : candidates)
                    {
                        if (min == null || candidate.first.compareTo(min) < 0)
                            min = candidate.first;
                        if (max == null || candidate.last.compareTo(max) > 0)
                            max = candidate.last;
                    }
                    if (min == null || max == null || min.equals(max)) // single partition sstables - we cannot include a high level sstable.
                        return candidates;
                    Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
                    Range<PartitionPosition> boundaries = new Range<>(min, max);
                    for (SSTableReader sstable : generations.get(i))
                    {
                        Range<PartitionPosition> r = new Range<>(sstable.first, sstable.last);
                        if (boundaries.contains(r) && !compacting.contains(sstable))
                        {
                            logger.info("Adding high-level (L{}) {} to candidates", sstable.getSSTableLevel(), sstable);
                            withStarvedCandidate.add(sstable);
                            return withStarvedCandidate;
                        }
                    }
                }
                return candidates;
            }
        }

        return candidates;
    }

    public synchronized int getLevelSize(int i)
    {
        return generations.get(i).size();
    }

    public synchronized int[] getAllLevelSize()
    {
        return generations.getAllLevelSize();
    }

    @VisibleForTesting
    public synchronized int remove(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level >= 0 : reader + " not present in manifest: "+level;
        generations.remove(Collections.singleton(reader));
        return level;
    }

    public synchronized Set<SSTableReader> getSSTables()
    {
        return generations.allSSTables();
    }

    private static Set<SSTableReader> overlapping(Collection<SSTableReader> candidates, Iterable<SSTableReader> others)
    {
        assert !candidates.isEmpty();
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        Iterator<SSTableReader> iter = candidates.iterator();
        SSTableReader sstable = iter.next();
        Token first = sstable.first.getToken();
        Token last = sstable.last.getToken();
        while (iter.hasNext())
        {
            sstable = iter.next();
            first = first.compareTo(sstable.first.getToken()) <= 0 ? first : sstable.first.getToken();
            last = last.compareTo(sstable.last.getToken()) >= 0 ? last : sstable.last.getToken();
        }
        return overlapping(first, last, others);
    }

    private static Set<SSTableReader> overlappingWithBounds(SSTableReader sstable, Map<SSTableReader, Bounds<Token>> others)
    {
        return overlappingWithBounds(sstable.first.getToken(), sstable.last.getToken(), others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    @VisibleForTesting
    static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables)
    {
        return overlappingWithBounds(start, end, genBounds(sstables));
    }

    private static Set<SSTableReader> overlappingWithBounds(Token start, Token end, Map<SSTableReader, Bounds<Token>> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<SSTableReader> overlapped = new HashSet<>();
        Bounds<Token> promotedBounds = new Bounds<>(start, end);

        for (Map.Entry<SSTableReader, Bounds<Token>> pair : sstables.entrySet())
        {
            if (pair.getValue().intersects(promotedBounds))
                overlapped.add(pair.getKey());
        }
        return overlapped;
    }

    /**
     * special case for getting overlapping sstables - if end is partitioner.getMinToken() we return all sstables
     * which have an end token larger than start - meaning this grabs everything from start to the end of the full
     * partitioner range.
     */
    @VisibleForTesting
    static Set<SSTableReader> overlappingWithMin(IPartitioner partitioner, Token start, Token end, Iterable<SSTableReader> sstables)
    {
        if (start.compareTo(end) <= 0)
            return overlapping(start, end, sstables);

        assert end.equals(partitioner.getMinimumToken()) : "If start > end, end must be equal to partitioner min token";
        Set<SSTableReader> overlapped = new HashSet<>();
        for (SSTableReader candidate : sstables)
        {
            if (candidate.last.getToken().compareTo(start) > 0)
                overlapped.add(candidate);
        }
        return overlapped;
    }

    private static Map<SSTableReader, Bounds<Token>> genBounds(Iterable<SSTableReader> ssTableReaders)
    {
        Map<SSTableReader, Bounds<Token>> boundsMap = new HashMap<>();
        for (SSTableReader sstable : ssTableReaders)
        {
            boundsMap.put(sstable, new Bounds<>(sstable.first.getToken(), sstable.last.getToken()));
        }
        return boundsMap;
    }

    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are excluded
     * for prior failure), will return an empty list.  Never returns null.
     */
    private Collection<SSTableReader> getCandidatesFor(int level)
    {
        assert !generations.get(level).isEmpty();
        logger.trace("Choosing candidates for L{}", level);

        final Set<SSTableReader> compacting = cfs.getTracker().getCompacting();

        if (level == 0)
        {
            Set<SSTableReader> compactingL0 = getCompactingL0();

            PartitionPosition lastCompactingKey = null;
            PartitionPosition firstCompactingKey = null;
            for (SSTableReader candidate : compactingL0)
            {
                if (firstCompactingKey == null || candidate.first.compareTo(firstCompactingKey) < 0)
                    firstCompactingKey = candidate.first;
                if (lastCompactingKey == null || candidate.last.compareTo(lastCompactingKey) > 0)
                    lastCompactingKey = candidate.last;
            }

            // L0 is the dumping ground for new sstables which thus may overlap each other.
            //
            // We treat L0 compactions specially:
            // 1a. add sstables to the candidate set until we have at least maxSSTableSizeInMB
            // 1b. prefer choosing older sstables as candidates, to newer ones
            // 1c. any L0 sstables that overlap a candidate, will also become candidates
            // 2. At most max_threshold sstables from L0 will be compacted at once
            // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
            //    and the result of the compaction will stay in L0 instead of being promoted (see promote())
            //
            // Note that we ignore suspect-ness of L1 sstables here, since if an L1 sstable is suspect we're
            // basically screwed, since we expect all or most L0 sstables to overlap with each L1 sstable.
            // So if an L1 sstable is suspect we can't do much besides try anyway and hope for the best.
            Set<SSTableReader> candidates = new HashSet<>();
            Map<SSTableReader, Bounds<Token>> remaining = genBounds(Iterables.filter(generations.get(0), Predicates.not(SSTableReader::isMarkedSuspect)));

            for (SSTableReader sstable : ageSortedSSTables(remaining.keySet()))
            {
                if (candidates.contains(sstable))
                    continue;

                Sets.SetView<SSTableReader> overlappedL0 = Sets.union(Collections.singleton(sstable), overlappingWithBounds(sstable, remaining));
                if (!Sets.intersection(overlappedL0, compactingL0).isEmpty())
                    continue;

                for (SSTableReader newCandidate : overlappedL0)
                {
                    if (firstCompactingKey == null || lastCompactingKey == null || overlapping(firstCompactingKey.getToken(), lastCompactingKey.getToken(), Collections.singleton(newCandidate)).size() == 0)
                        candidates.add(newCandidate);
                    remaining.remove(newCandidate);
                }

                if (candidates.size() > cfs.getMaximumCompactionThreshold())
                {
                    // limit to only the cfs.getMaximumCompactionThreshold() oldest candidates
                    candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, cfs.getMaximumCompactionThreshold()));
                    break;
                }
            }

            // leave everything in L0 if we didn't end up with a full sstable's worth of data
            if (SSTableReader.getTotalBytes(candidates) > maxSSTableSizeInBytes)
            {
                // add sstables from L1 that overlap candidates
                // if the overlapping ones are already busy in a compaction, leave it out.
                // TODO try to find a set of L0 sstables that only overlaps with non-busy L1 sstables
                Set<SSTableReader> l1overlapping = overlapping(candidates, generations.get(1));
                if (Sets.intersection(l1overlapping, compacting).size() > 0)
                    return Collections.emptyList();
                if (!overlapping(candidates, compactingL0).isEmpty())
                    return Collections.emptyList();
                candidates = Sets.union(candidates, l1overlapping);
            }
            if (candidates.size() < 2)
                return Collections.emptyList();
            else
                return candidates;
        }

        // look for a non-suspect keyspace to compact with, starting with where we left off last time,
        // and wrapping back to the beginning of the generation if necessary
        Map<SSTableReader, Bounds<Token>> sstablesNextLevel = genBounds(generations.get(level + 1));
        Iterator<SSTableReader> levelIterator = generations.wrappingIterator(level, lastCompactedSSTables[level]);
        while (levelIterator.hasNext())
        {
            SSTableReader sstable = levelIterator.next();
            Set<SSTableReader> candidates = Sets.union(Collections.singleton(sstable), overlappingWithBounds(sstable, sstablesNextLevel));

            if (Iterables.any(candidates, SSTableReader::isMarkedSuspect))
                continue;
            if (Sets.intersection(candidates, compacting).isEmpty())
                return candidates;
        }

        // all the sstables were suspect or overlapped with something suspect
        return Collections.emptyList();
    }

    private Set<SSTableReader> getCompactingL0()
    {
        Set<SSTableReader> sstables = new HashSet<>();
        Set<SSTableReader> levelSSTables = new HashSet<>(generations.get(0));
        for (SSTableReader sstable : cfs.getTracker().getCompacting())
        {
            if (levelSSTables.contains(sstable))
                sstables.add(sstable);
        }
        return sstables;
    }

    @VisibleForTesting
    List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates)
    {
        return ImmutableList.sortedCopyOf(SSTableReader.maxTimestampAscending, candidates);
    }

    public synchronized Set<SSTableReader>[] getSStablesPerLevelSnapshot()
    {
        return generations.snapshot();
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public synchronized int getLevelCount()
    {
        for (int i = generations.levelCount() - 1; i >= 0; i--)
        {
            if (generations.get(i).size() > 0)
                return i;
        }
        return 0;
    }

    public synchronized int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[generations.levelCount()];

        for (int i = generations.levelCount() - 1; i >= 0; i--)
        {
            Set<SSTableReader> sstables = generations.get(i);
            // If there is 1 byte over TBL - (MBL * 1.001), there is still a task left, so we need to round up.
            estimated[i] = (long)Math.ceil((double)Math.max(0L, SSTableReader.getTotalBytes(sstables) - (long)(maxBytesForLevel(i, maxSSTableSizeInBytes) * 1.001)) / (double)maxSSTableSizeInBytes);
            tasks += estimated[i];
        }

        if (!DatabaseDescriptor.getDisableSTCSInL0() && generations.get(0).size() > cfs.getMaximumCompactionThreshold())
        {
            int l0compactions = generations.get(0).size() / cfs.getMaximumCompactionThreshold();
            tasks += l0compactions;
            estimated[0] += l0compactions;
        }

        logger.trace("Estimating {} compactions to do for {}.{}",
                     Arrays.toString(estimated), cfs.keyspace.getName(), cfs.name);
        return Ints.checkedCast(tasks);
    }

    public int getNextLevel(Collection<SSTableReader> sstables)
    {
        int maximumLevel = Integer.MIN_VALUE;
        int minimumLevel = Integer.MAX_VALUE;
        for (SSTableReader sstable : sstables)
        {
            maximumLevel = Math.max(sstable.getSSTableLevel(), maximumLevel);
            minimumLevel = Math.min(sstable.getSSTableLevel(), minimumLevel);
        }

        int newLevel;
        if (minimumLevel == 0 && minimumLevel == maximumLevel && SSTableReader.getTotalBytes(sstables) < maxSSTableSizeInBytes)
        {
            newLevel = 0;
        }
        else
        {
            newLevel = minimumLevel == maximumLevel ? maximumLevel + 1 : maximumLevel;
            assert newLevel > 0;
        }
        return newLevel;
    }

    @VisibleForTesting
    synchronized Set<SSTableReader> getLevel(int level)
    {
        return ImmutableSet.copyOf(generations.get(level));
    }

    synchronized List<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator)
    {
        return ImmutableList.sortedCopyOf(comparator, generations.get(level));
    }

    /**
     * Checks the sstables if we are compacting repaired, unrepaired or pending sstables, and which data directory we are responsible for
     *
     * if there are no sstables or the sstables are pending, we return null
     *
     * @return a pair where .left is true if this strategy instance handles repaired sstables, false if not
     *                  and .right contains the data directory for the sstables in this strategy
     *        or null if we have no sstables or if we are handling pending sstables
     *
     */
    synchronized Pair<Boolean, String> getStrategyInformation()
    {
        for (int i = 0, maxLevel = getLevelCount() - 1; i <= maxLevel; i++)
        {
            Set<SSTableReader> sstables = generations.get(i);
            for (SSTableReader sstable : sstables)
            {
                if (sstable.isPendingRepair())
                {
                    logger.debug("SSTables pending repair, not running scheduled compactions for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
                    return null;
                }
                return Pair.create(sstable.isRepaired(), cfs.getDirectories().getDataDirectoryForFile(sstable.descriptor).location.getAbsolutePath());
            }
        }
        return null;
    }

    synchronized void newLevel(SSTableReader sstable, int oldLevel)
    {
        generations.newLevel(sstable, oldLevel);
        lastCompactedSSTables[oldLevel] = sstable;
    }

    public static class CompactionCandidate
    {
        public final Collection<SSTableReader> sstables;
        public final int level;
        public final long maxSSTableBytes;

        public CompactionCandidate(Collection<SSTableReader> sstables, int level, long maxSSTableBytes)
        {
            this.sstables = sstables;
            this.level = level;
            this.maxSSTableBytes = maxSSTableBytes;
        }
    }
}
