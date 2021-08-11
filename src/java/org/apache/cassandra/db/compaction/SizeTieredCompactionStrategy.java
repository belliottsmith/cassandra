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
import java.util.Map.Entry;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

public class SizeTieredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(SizeTieredCompactionStrategy.class);

    private static final Comparator<Pair<List<SSTableReader>,Double>> bucketsByHotnessComparator = new Comparator<Pair<List<SSTableReader>, Double>>()
    {
        public int compare(Pair<List<SSTableReader>, Double> o1, Pair<List<SSTableReader>, Double> o2)
        {
            int comparison = Double.compare(o1.right, o2.right);
            if (comparison != 0)
                return comparison;

            // break ties by compacting the smallest sstables first (this will probably only happen for
            // system tables and new/unread sstables)
            return Long.compare(avgSize(o1.left), avgSize(o2.left));
        }

        private long avgSize(List<SSTableReader> sstables)
        {
            long n = 0;
            for (SSTableReader sstable : sstables)
                n += sstable.bytesOnDisk();
            return n / sstables.size();
        }
    };

    protected SizeTieredCompactionStrategyOptions sizeTieredOptions;
    protected volatile int estimatedRemainingTasks;
    @VisibleForTesting
    protected final Set<SSTableReader> sstables = new HashSet<>();

    public SizeTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.sizeTieredOptions = new SizeTieredCompactionStrategyOptions(options);
    }

    private synchronized List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        // make local copies so they can't be changed out from under us mid-method
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Iterable<SSTableReader> candidates = filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains));

        List<List<SSTableReader>> buckets = getBuckets(createSSTableAndLengthPairs(candidates), sizeTieredOptions.bucketHigh, sizeTieredOptions.bucketLow, sizeTieredOptions.minSSTableSize);
        logger.trace("Compaction buckets are {}", buckets);
        estimatedRemainingTasks = getEstimatedCompactionsByTasks(cfs, buckets);
        cfs.getCompactionStrategyManager().compactionLogger.pending(this, estimatedRemainingTasks);
        List<SSTableReader> mostInteresting = mostInterestingBucket(buckets, minThreshold, maxThreshold);
        if (!mostInteresting.isEmpty())
            return mostInteresting;

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        return Collections.singletonList(Collections.max(sstablesWithTombstones, SSTableReader.sizeComparator));
    }


    /**
     * @param buckets list of buckets from which to return the most interesting, where "interesting" is the total hotness for reads
     * @param minThreshold minimum number of sstables in a bucket to qualify as interesting
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this)
     * @return a bucket (list) of sstables to compact
     */
    public static List<SSTableReader> mostInterestingBucket(List<List<SSTableReader>> buckets, int minThreshold, int maxThreshold)
    {
        // skip buckets containing less than minThreshold sstables, and limit other buckets to maxThreshold sstables
        final List<Pair<List<SSTableReader>, Double>> prunedBucketsAndHotness = new ArrayList<>(buckets.size());
        for (List<SSTableReader> bucket : buckets)
        {
            Pair<List<SSTableReader>, Double> bucketAndHotness = trimToThresholdWithHotness(bucket, maxThreshold);
            if (bucketAndHotness != null && bucketAndHotness.left.size() >= minThreshold)
                prunedBucketsAndHotness.add(bucketAndHotness);
        }
        if (prunedBucketsAndHotness.isEmpty())
            return Collections.emptyList();

        Pair<List<SSTableReader>, Double> hottest = Collections.max(prunedBucketsAndHotness, bucketsByHotnessComparator);
        return hottest.left;
    }

    /**
     * Returns a (bucket, hotness) pair or null if there were not enough sstables in the bucket to meet minThreshold.
     * If there are more than maxThreshold sstables, the coldest sstables will be trimmed to meet the threshold.
     **/
    @VisibleForTesting
    static Pair<List<SSTableReader>, Double> trimToThresholdWithHotness(List<SSTableReader> bucket, int maxThreshold)
    {
        // Sort by sstable hotness (descending). We first build a map because the hotness may change during the sort.
        final Map<SSTableReader, Double> hotnessSnapshot = getHotnessMap(bucket);
        Collections.sort(bucket, new Comparator<SSTableReader>()
        {
            public int compare(SSTableReader o1, SSTableReader o2)
            {
                return -1 * Double.compare(hotnessSnapshot.get(o1), hotnessSnapshot.get(o2));
            }
        });

        // and then trim the coldest sstables off the end to meet the maxThreshold
        List<SSTableReader> prunedBucket = bucket.subList(0, Math.min(bucket.size(), maxThreshold));

        // bucket hotness is the sum of the hotness of all sstable members
        double bucketHotness = 0.0;
        for (SSTableReader sstr : prunedBucket)
            bucketHotness += hotness(sstr);

        return Pair.create(prunedBucket, bucketHotness);
    }

    private static Map<SSTableReader, Double> getHotnessMap(Collection<SSTableReader> sstables)
    {
        Map<SSTableReader, Double> hotness = new HashMap<>(sstables.size());
        for (SSTableReader sstable : sstables)
            hotness.put(sstable, hotness(sstable));
        return hotness;
    }

    /**
     * Returns the reads per second per key for this sstable, or 0.0 if the sstable has no read meter
     */
    private static double hotness(SSTableReader sstr)
    {
        // system tables don't have read meters, just use 0.0 for the hotness
        return sstr.getReadMeter() == null ? 0.0 : sstr.getReadMeter().twoHourRate() / sstr.estimatedKeys();
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        List<SSTableReader> previousCandidate = null;
        while (true)
        {
            List<SSTableReader> hottestBucket = getNextBackgroundSSTables(gcBefore);

            if (hottestBucket.isEmpty())
                return null;

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
            if (hottestBucket.equals(previousCandidate))
            {
                logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                            "unless it happens frequently, in which case it must be reported. Will retry later.",
                            hottestBucket);
                return null;
            }

            LifecycleTransaction transaction = cfs.getTracker().tryModify(hottestBucket, OperationType.COMPACTION);
            if (transaction != null)
                return new CompactionTask(cfs, transaction, gcBefore);
            previousCandidate = hottestBucket;
        }
    }

    @SuppressWarnings("resource")
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        if (splitOutput)
            return Arrays.<AbstractCompactionTask>asList(new SplittingCompactionTask(cfs, txn, gcBefore));
        return Arrays.<AbstractCompactionTask>asList(new CompactionTask(cfs, txn, gcBefore));
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new CompactionTask(cfs, transaction, gcBefore).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public static List<Pair<SSTableReader, Long>> createSSTableAndLengthPairs(Iterable<SSTableReader> sstables)
    {
        List<Pair<SSTableReader, Long>> sstableLengthPairs = new ArrayList<>(Iterables.size(sstables));
        for(SSTableReader sstable : sstables)
            sstableLengthPairs.add(Pair.create(sstable, sstable.onDiskLength()));
        return sstableLengthPairs;
    }

    /*
     * Group files of similar size into buckets.
     */
    public static <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files, double bucketHigh, double bucketLow, long minSSTableSize)
    {
        // Sort the list in order to get deterministic results during the grouping below
        List<Pair<T, Long>> sortedFiles = new ArrayList<Pair<T, Long>>(files);
        Collections.sort(sortedFiles, new Comparator<Pair<T, Long>>()
        {
            public int compare(Pair<T, Long> p1, Pair<T, Long> p2)
            {
                return p1.right.compareTo(p2.right);
            }
        });

        Map<Long, List<T>> buckets = new HashMap<Long, List<T>>();

        outer:
        for (Pair<T, Long> pair: sortedFiles)
        {
            long size = pair.right;

            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
            for (Entry<Long, List<T>> entry : buckets.entrySet())
            {
                List<T> bucket = entry.getValue();
                long oldAverageSize = entry.getKey();
                if ((size > (oldAverageSize * bucketLow) && size < (oldAverageSize * bucketHigh))
                    || (size < minSSTableSize && oldAverageSize < minSSTableSize))
                {
                    // remove and re-add under new new average size
                    buckets.remove(oldAverageSize);
                    long totalSize = bucket.size() * oldAverageSize;
                    long newAverageSize = (totalSize + size) / (bucket.size() + 1);
                    bucket.add(pair.left);
                    buckets.put(newAverageSize, bucket);
                    continue outer;
                }
            }

            // no similar bucket found; put it in a new one
            ArrayList<T> bucket = new ArrayList<T>();
            bucket.add(pair.left);
            buckets.put(size, bucket);
        }

        return new ArrayList<List<T>>(buckets.values());
    }

    public static int getEstimatedCompactionsByTasks(ColumnFamilyStore cfs, List<List<SSTableReader>> tasks)
    {
        int n = 0;
        for (List<SSTableReader> bucket : tasks)
        {
            if (bucket.size() >= cfs.getMinimumCompactionThreshold())
                n += Math.ceil((double)bucket.size() / cfs.getMaximumCompactionThreshold());
        }
        return n;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    @Override
    protected Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }

    public String toString()
    {
        return String.format("SizeTieredCompactionStrategy[%s/%s]",
            cfs.getMinimumCompactionThreshold(),
            cfs.getMaximumCompactionThreshold());
    }

    private static class SplittingCompactionTask extends CompactionTask
    {
        public SplittingCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore)
        {
            super(cfs, txn, gcBefore);
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction txn,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new SplittingSizeTieredCompactionWriter(cfs, directories, txn, nonExpiredSSTables);
        }
    }

    /**
     * We first prune the bucket under consideration by maxSSTableCount and maxSizeBytes - no pruned bucket contains
     * more than maxSSTableCount sstables or more than maxSizeBytes. Then among those we grab the "best bucket by count and size"
     * - that is the biggest bucket (by count), or if count is equal, the smallest on disk.
     *
     * Also, for TWCS we track the smallest (by size) unpruned bucket as we should always do a compaction if we can there.
     *
     * Note that maxThreshold (default 32) is different from maxSSTableCount (default 1000) - this method deals with
     * 2 cases - first the case where we are behind and have tons of small sstables, in this case we limit the sstable
     * count in the compaction to maxSSTableCount - we want to quickly reduce the count without creating huge sstables.
     * The other case is when there are many sstables, but we can't find a bucket where the total size of at least minThreshold
     * sstables is below maxSizeBytes - in this case we run a regular STCS compaction limited by maxThreshold (only
     * for TWCS, for LCS we start doing L0 -> L1 compactions instead to avoid huge sstables in L0)
     *
     *
     */
    @VisibleForTesting
    static List<SSTableReader> bestBucket(List<List<SSTableReader>> buckets,
                                          int minThreshold,
                                          int maxThreshold,
                                          long maxSizeBytes,
                                          int maxSSTableCount,
                                          boolean trackSmallest)
    {
        List<SSTableReader> candidate = null;
        List<SSTableReader> smallest = null;
        for (List<SSTableReader> bucket : buckets)
        {
            bucket.sort(SSTableReader.sizeComparator); // smallest sstables first in the list
            List<SSTableReader> prunedBucket = pruneByCountAndSize(bucket,
                                                                   maxSSTableCount,
                                                                   maxSizeBytes);
            candidate = bestBucketByCountOrSize(candidate, prunedBucket, minThreshold);

            if (trackSmallest && bucket.size() >= minThreshold)
            {
                // in this case we don't care about sstable count at all - just
                // make sure we create the smallest possible sstable
                if (smallest == null || bucketSize(bucket) < bucketSize(smallest))
                    smallest = bucket;
            }
        }

        if (candidate == null || candidate.size() < minThreshold)
        {
            // we use maxThreshold on purpose here - if there is no "bestbucket" candidate we need to do a regular
            // STCS compaction over the smallest (on-disk) bucket for TWCS to avoid creating huge sstables too quickly.
            candidate = smallest == null ? Collections.emptyList() : smallest.subList(0, Math.min(maxThreshold, smallest.size()));
        }

        if (candidate.size() < minThreshold)
            return Collections.emptyList();

        long candidateSizeBytes = bucketSize(candidate);
        logger.info("Got {} sstables ({} bytes) for biggest bucket STCS, max_threshold = {}, avg sstable size = {} bytes, maxSizeBytes = {}, maxSSTableCount = {}",
                    candidate.size(),
                    candidateSizeBytes,
                    maxThreshold,
                    candidateSizeBytes / candidate.size(),
                    maxSizeBytes,
                    maxSSTableCount);
        return candidate;
    }

    /**
     * we consider the best bucket to be the largest one (by sstable count).
     *
     * If current candidate and prunedBucket have the same count the smallest by on-disk size is better.
     */
    private static List<SSTableReader> bestBucketByCountOrSize(@Nullable List<SSTableReader> candidate,
                                                               List<SSTableReader> prunedBucket,
                                                               int minThreshold)
    {
        int curSize = candidate == null ? 0 : candidate.size();
        int newSize = prunedBucket.size();

        // best bucket is the one with the biggest count of sstables
        if (newSize > curSize && newSize >= minThreshold)
            return prunedBucket;

        // if the count is the same, the best bucket is the smallest (by size on disk) one;
        if (candidate != null && newSize == curSize && bucketSize(prunedBucket) < bucketSize(candidate))
            return prunedBucket;

        return candidate;
    }

    /**
     * Prunes the bucket by count and size
     *
     * Limits the number of sstable in the compaction to maxSSTableCount and the maximum size of the compaction to
     * maxSizeBytes.
     *
     * @param bucket a bucket to prune, sorted by sstable size ascending
     * @param maxSSTableCount max number of sstables in the resulting bucket
     * @param maxSizeBytes max total size of the pruned bucket
     *
     */
    private static List<SSTableReader> pruneByCountAndSize(List<SSTableReader> bucket,
                                                           int maxSSTableCount,
                                                           long maxSizeBytes)
    {
        List<SSTableReader> pruned = new ArrayList<>(bucket.size());
        long accumulatedSize = 0;
        for (SSTableReader sstable : bucket)
        {
            accumulatedSize += sstable.onDiskLength();
            if (accumulatedSize > maxSizeBytes || pruned.size() >= maxSSTableCount)
                return pruned;
            pruned.add(sstable);
        }
        return pruned;
    }

    private static long bucketSize(List<SSTableReader> sstables)
    {
        return sstables.stream().mapToLong(SSTableReader::onDiskLength).sum();
    }
}
