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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.BreaksJMX;

/**
 * The MBean interface for ColumnFamilyStore
 */
public interface ColumnFamilyStoreMBean
{
    /**
     * @return the name of the column family
     */
    @Deprecated
    public String getColumnFamilyName();

    public String getTableName();

    /**
     * force a major compaction of this column family
     *
     * @param splitOutput true if the output of the major compaction should be split in several sstables
     */
    public void forceMajorCompaction(boolean splitOutput) throws ExecutionException, InterruptedException;

    /**
     * force a major compaction of specified key range in this column family
     */
    @BreaksJMX("This API was released in 3.10 using a parameter that takes Range of Token, which can only be done IFF client has Cassandra binaries in the classpath")
    @Deprecated
    public void forceCompactionForTokenRange(Collection<Range<Token>> tokenRanges) throws ExecutionException, InterruptedException;

    /**
     * force a major compaction of specified key range in this column family
     */
    public void forceCompactionForTokenRanges(String... tokenRanges);

    /**
     * Gets the minimum number of sstables in queue before compaction kicks off
     */
    public int getMinimumCompactionThreshold();

    /**
     * Sets the minimum number of sstables in queue before compaction kicks off
     */
    public void setMinimumCompactionThreshold(int threshold);

    /**
     * Gets the maximum number of sstables in queue before compaction kicks off
     */
    public int getMaximumCompactionThreshold();

    /**
     * Sets the maximum and maximum number of SSTables in queue before compaction kicks off
     */
    public void setCompactionThresholds(int minThreshold, int maxThreshold);

    /**
     * Sets the maximum number of sstables in queue before compaction kicks off
     */
    public void setMaximumCompactionThreshold(int threshold);

    /**
     * Sets the compaction parameters locally for this node
     *
     * Note that this will be set until an ALTER with compaction = {..} is executed or the node is restarted
     *
     * @param options compaction options with the same syntax as when doing ALTER ... WITH compaction = {..}
     */
    public void setCompactionParametersJson(String options);
    public String getCompactionParametersJson();

    /**
     * Sets the compaction parameters locally for this node
     *
     * Note that this will be set until an ALTER with compaction = {..} is executed or the node is restarted
     *
     * @param options compaction options map
     */
    public void setCompactionParameters(Map<String, String> options);
    public Map<String, String> getCompactionParameters();

    /**
     * Get the compression parameters
     */
    public Map<String,String> getCompressionParameters();

    public String getCompressionParametersJson();

    /**
     * Set the compression parameters locally for this node
     * @param opts map of string names to values
     */
    public void setCompressionParameters(Map<String,String> opts);
    public void setCompressionParametersJson(String options);

    /**
     * Set new crc check chance
     */
    public void setCrcCheckChance(double crcCheckChance);

    public boolean isAutoCompactionDisabled();

    public long estimateKeys();


    /**
     * Returns a list of the names of the built column indexes for current store
     * @return list of the index names
     */
    public List<String> getBuiltIndexes();

    /**
     * Returns a list of filenames that contain the given key on this node
     * @param key
     * @return list of filenames containing the key
     */
    public List<String> getSSTablesForKey(String key);

    /**
     * Returns a list of filenames that contain the given key on this node
     * @param key
     * @param hexFormat if key is in hex string format
     * @return list of filenames containing the key
     */
    public List<String> getSSTablesForKey(String key, boolean hexFormat);

    /**
     * Load new sstables from the given directory
     *
     * @param srcPaths the path to the new sstables - if it is an empty set, the data directories will be scanned
     * @param resetLevel if the level should be reset to 0 on the new sstables
     * @param clearRepaired if repaired info should be wiped from the new sstables
     * @param verifySSTables if the new sstables should be verified that they are not corrupt
     * @param verifyTokens if the tokens in the new sstables should be verified that they are owned by the current node
     * @param invalidateCaches if row cache should be invalidated for the keys in the new sstables
     * @param extendedVerify if we should run an extended verify checking all values in the new sstables
     *
     * @return list of failed import directories
     */
    @Deprecated
    public List<String> importNewSSTables(Set<String> srcPaths,
                                           boolean resetLevel,
                                           boolean clearRepaired,
                                           boolean verifySSTables,
                                           boolean verifyTokens,
                                           boolean invalidateCaches,
                                           boolean extendedVerify);

    /**
     * Load new sstables from the given directory
     *
     * @param srcPaths the path to the new sstables - if it is an empty set, the data directories will be scanned
     * @param resetLevel if the level should be reset to 0 on the new sstables
     * @param clearRepaired if repaired info should be wiped from the new sstables
     * @param verifySSTables if the new sstables should be verified that they are not corrupt
     * @param verifyTokens if the tokens in the new sstables should be verified that they are owned by the current node
     * @param invalidateCaches if row cache should be invalidated for the keys in the new sstables
     * @param extendedVerify if we should run an extended verify checking all values in the new sstables
     * @param copyData if we should copy data from source paths instead of moving them
     *
     * @return list of failed import directories
     */
    public List<String> importNewSSTables(Set<String> srcPaths,
                                          boolean resetLevel,
                                          boolean clearRepaired,
                                          boolean verifySSTables,
                                          boolean verifyTokens,
                                          boolean invalidateCaches,
                                          boolean extendedVerify,
                                          boolean copyData);

    @Deprecated
    public void loadNewSSTables();
    /**
     * @return the number of SSTables in L0.  Always return 0 if Leveled compaction is not enabled.
     */
    public int getUnleveledSSTables();

    /**
     * @return the effective gcgs time for this CF (see CompactionController.gcBefore)
     */
     public long getGCBeforeSeconds();

    /**
     * @return sstable count for each level. null unless leveled compaction is used.
     *         array index corresponds to level(int[0] is for level 0, ...).
     */
    public int[] getSSTableCountPerLevel();

    /**
     * @return total size on disk for each level. null unless leveled compaction is used.
     *         array index corresponds to level(int[0] is for level 0, ...).
     */
    public long[] getPerLevelSizeBytes();

    /**
     * @return sstable count for each bucket in TWCS. null unless time window compaction is used.
     *         array index corresponds to bucket(int[0] is for most recent, ...).
     */
    public int[] getSSTableCountPerTWCSBucket();

    /**
     * @return sstable fanout size for level compaction strategy.
     */
    public int getLevelFanoutSize();

    /**
     * Get the ratio of droppable tombstones to real columns (and non-droppable tombstones)
     * @return ratio
     */
    public double getDroppableTombstoneRatio();

    /**
     * @return the size of SSTables in "snapshots" subdirectory which aren't live anymore
     */
    public long trueSnapshotsSize();

    /**
     * begin sampling for a specific sampler with a given capacity.  The cardinality may
     * be larger than the capacity, but depending on the use case it may affect its accuracy
     */
    public void beginLocalSampling(String sampler, int capacity, int durationMillis);

    /**
     * @return top <i>count</i> items for the sampler since beginLocalSampling was called
     */
    public List<CompositeData> finishLocalSampling(String sampler, int count) throws OpenDataException;

    /*
        Is Compaction space check enabled
     */
    public boolean isCompactionDiskSpaceCheckEnabled();

    /*
       Enable/Disable compaction space check
     */
    public void compactionDiskSpaceCheck(boolean enable);

    public void setNeverPurgeTombstones(boolean value);

    public boolean getNeverPurgeTombstones();

    /**
     * Check SSTables whether or not they are misplaced.
     * @return true if any of the SSTables is misplaced.
     *         If all SSTables are correctly placed or the partitioner does not support splitting, it returns false.
     */
    public boolean hasMisplacedSSTables();

    public List<String> getDataPaths() throws IOException;

    public Map<String, Long> getTopSizePartitions();
    public Long getTopSizePartitionsLastUpdate();
    public Map<String, Long> getTopTombstonePartitions();
    public Long getTopTombstonePartitionsLastUpdate();

    /**
     * @see org.apache.cassandra.metrics.TableMetrics#sstablesPerReadHistogram
     * @return a histogram of the number of sstable data files accessed per read: reading this property resets it
     */
    public long[] getRecentSSTablesPerReadHistogramV3();

    /**
     * @see org.apache.cassandra.metrics.TableMetrics#sstablesPerReadHistogram
     * @return a histogram of the number of sstable data files accessed per read
     */
    public long[] getSSTablesPerReadHistogramV3();

    /**
     * @see org.apache.cassandra.metrics.TableMetrics#readLatency
     * @return an array representing the latency histogram
     */
    public long[] getRecentReadLatencyHistogramMicrosV3();

    /**
     * @see org.apache.cassandra.metrics.TableMetrics#writeLatency
     * @return an array representing the latency histogram
     */
    public long[] getRecentWriteLatencyHistogramMicrosV3();

    /**
     * @see org.apache.cassandra.metrics.TableMetrics#rangeLatency
     * @return an array representing the latency histogram
     */
    public long[] getRecentRangeLatencyHistogramMicrosV3();

    /**
     * @see org.apache.cassandra.metrics.TableMetrics#estimatedPartitionSizeHistogram
     */
    public long[] getEstimatedRowSizeHistogram();
    /**
     * @see org.apache.cassandra.metrics.TableMetrics#estimatedColumnCountHistogram
     */
    public long[] getEstimatedColumnCountHistogram();
    /**
     * @see org.apache.cassandra.metrics.TableMetrics#casPrepare
     */
    public long[] getCasPrepareLatencyHistogram();
    /**
     * @see org.apache.cassandra.metrics.TableMetrics#casPropose
     */
    public long[] getCasProposeLatencyHistogram();
    /**
     * @see org.apache.cassandra.metrics.TableMetrics#casCommit
     */
    public long[] getCasCommitLatencyHistogram();
    /**
     * @see org.apache.cassandra.metrics.TableMetrics#coordinatorScanLatency
     */
    public long[] getCoordinatorScanLatencyHistogram();

    public void setChristmasPatchDisabled(boolean disable);
    public boolean isChristmasPatchDisabled();
}
