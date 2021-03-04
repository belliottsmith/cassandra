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

import java.util.List;
import java.util.Map;
import javax.management.openmbean.TabularData;

import org.apache.cassandra.config.DatabaseDescriptor;

public interface CompactionManagerMBean
{
    /** List of running compaction objects. */
    public List<Map<String, String>> getCompactions();

    /** List of running compaction summary strings. */
    public List<String> getCompactionSummary();

    /** compaction history **/
    public TabularData getCompactionHistory();

    /**
     * Triggers the compaction of user specified sstables.
     * You can specify files from various keyspaces and columnfamilies.
     * If you do so, user defined compaction is performed several times to the groups of files
     * in the same keyspace/columnfamily.
     *
     * @param dataFiles a comma separated list of sstable file to compact.
     *                  must contain keyspace and columnfamily name in path(for 2.1+) or file name itself.
     */
    public void forceUserDefinedCompaction(String dataFiles);

    /**
     * Stop all running compaction-like tasks having the provided {@code type}.
     * @param type the type of compaction to stop. Can be one of:
     *   - COMPACTION
     *   - VALIDATION
     *   - CLEANUP
     *   - SCRUB
     *   - INDEX_BUILD
     */
    public void stopCompaction(String type);

    /**
     * Stop an individual running compaction using the compactionId.
     * @param compactionId Compaction ID of compaction to stop. Such IDs can be found in
     *                     the transaction log files whose name starts with compaction_,
     *                     located in the table transactions folder.
     */
    public void stopCompactionById(String compactionId);

    /**
     * Returns core size of compaction thread pool
     */
    public int getCoreCompactorThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     * @param number New maximum of compaction threads
     */
    public void setCoreCompactorThreads(int number);

    /**
     * Returns maximum size of compaction thread pool
     */
    public int getMaximumCompactorThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     * @param number New maximum of compaction threads
     */
    public void setMaximumCompactorThreads(int number);

    /**
     * Returns core size of validation thread pool
     */
    public int getCoreValidationThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     * @param number New maximum of compaction threads
     */
    public void setCoreValidationThreads(int number);

    /**
     * Returns size of validator thread pool
     */
    public int getMaximumValidatorThreads();

    /**
     * Allows user to resize maximum size of the validator thread pool.
     * @param number New maximum of validator threads
     */
    public void setMaximumValidatorThreads(int number);

    /** Configures aggresive GC compaction mode of LCS **/
    public void setEnableAggressiveGCCompaction(boolean enable);
    public boolean getEnableAggressiveGCCompaction();

    /**
     * Enable / disable STCS in L0
     */
    public boolean getDisableSTCSInL0();
    public void setDisableSTCSInL0(boolean disabled);

    /**
     *
     * Column Index Tuning
     */
    public int getColumnIndexMaxSizeInKB();
    public void setColumnIndexMaxSizeInKB(int size);
    public int getColumnIndexMaxCount();
    public void setColumnIndexMaxCount(int count);
    public void setEnableScheduledCompactions(boolean enable);
    public boolean getEnableScheduledCompactions();
    public void setScheduledCompactionRangeSplits(int value);
    public int getScheduledCompactionRangeSplits();

    /**
     * sets the cycle time for scheduled compactions
     *
     * format is for example "60d" for 60 days, "5h" for 5 hours or "60s" for 60 seconds
     */
    public void setScheduledCompactionCycleTime(String time);
    public long getScheduledCompactionCycleTimeSeconds();
    public boolean getSkipSingleSSTableScheduledCompactions();
    public void setSkipSingleSSTableScheduledCompactions(boolean val);
    public long getMaxScheduledCompactionSSTableSizeBytes();
    public void setMaxScheduledCompactionSSTableSizeBytes(long size);
    public int getMaxScheduledCompactionSSTableCount();
    public void setMaxScheduledCompactionSSTableCount(int count);
    /**
     * Get automatic sstable upgrade enabled
     */
    public boolean getAutomaticSSTableUpgradeEnabled();
    /**
     * Set if automatic sstable upgrade should be enabled
     */
    public void setAutomaticSSTableUpgradeEnabled(boolean enabled);

    /**
     * Get the number of concurrent sstable upgrade tasks we should run
     * when automatic sstable upgrades are enabled
     */
    public int getMaxConcurrentAutoUpgradeTasks();

    /**
     * Set the number of concurrent sstable upgrade tasks we should run
     * when automatic sstable upgrades are enabled
     */
    public void setMaxConcurrentAutoUpgradeTasks(int value);

    /**
     * Get whether we should compact the biggest (by sstable count) STCS bucket in L0
     */
    public boolean getCompactBiggestSTCSBucketInL0();

    /**
     * Set whether we should cocmpact the biggest (by sstable count) STCS bucket in L0
     * @return
     */
    public void setCompactBiggestSTCSBucketInL0(boolean value);

    /**
     * If we are doing biggest STCS bucket compactions, this is the maximum total amount of bytes allowed in a compaction
     */
    public long getBiggestBucketMaxSizeBytes();

    /**
     * If we are doing biggest STCS bucket compactions, this is the maximum total number of sstables allowed in a compaction
     */
    public int getBiggestBucketMaxSSTableCount();

    public void setBiggestBucketMaxSizeBytes(long maxSizeBytes);

    public void setBiggestBucketMaxSSTableCount(int maxSSTableCount);
    /**
     * Set whether we allow TWCS to drop sstables without checking for overlaps
     * @param allow
     */
    public void setAllowUnsafeAggressiveSSTableExpiration(boolean allow);
    /**
     * Do we allow TWCS to drop sstables without checking for overlaps?
     */
    public boolean allowUnsafeAggressiveSSTableExpiration();
}
