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
package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface StorageProxyMBean
{
    public long getTotalHints();

    public boolean getHintedHandoffEnabled();
    public void setHintedHandoffEnabled(boolean b);
    public void enableHintsForDC(String dc);
    public void disableHintsForDC(String dc);
    public Set<String> getHintedHandoffDisabledDCs();
    public int getMaxHintWindow();
    public void setMaxHintWindow(int ms);
    public int getMaxHintsInProgress();
    public void setMaxHintsInProgress(int qs);
    public int getHintsInProgress();

    public Long getRpcTimeout();
    public void setRpcTimeout(Long timeoutInMillis);
    public Long getReadRpcTimeout();
    public void setReadRpcTimeout(Long timeoutInMillis);
    public Long getWriteRpcTimeout();
    public void setWriteRpcTimeout(Long timeoutInMillis);
    public Long getCounterWriteRpcTimeout();
    public void setCounterWriteRpcTimeout(Long timeoutInMillis);
    public Long getCasContentionTimeout();
    public void setCasContentionTimeout(Long timeoutInMillis);
    public Long getRangeRpcTimeout();
    public void setRangeRpcTimeout(Long timeoutInMillis);
    public Long getTruncateRpcTimeout();
    public void setTruncateRpcTimeout(Long timeoutInMillis);

    public String getArtificialLatencyVerbs();
    public void setArtificialLatencyVerbs(String commaDelimitedVerbs);
    public int getArtificialLatencyMillis();
    public void setArtificialLatencyMillis(int timeoutInMillis);
    public boolean getArtificialLatencyOnlyPermittedConsistencyLevels();
    public void setArtificialLatencyOnlyPermittedConsistencyLevels(boolean onlyPermitted);

    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections);
    public Long getNativeTransportMaxConcurrentConnections();

    public void reloadTriggerClasses();

    public long getReadRepairAttempted();
    public long getReadRepairRepairedBlocking();
    public long getReadRepairRepairedBackground();
    public long getReadRepairRepairTimedOut();

    public void loadPartitionBlacklist();
    public int getPartitionBlacklistLoadAttempts();
    public int getPartitionBlacklistLoadSuccesses();
    public void setEnablePartitionBlacklist(boolean enabled);
    public void setEnableBlacklistWrites(boolean enabled);
    public void setEnableBlacklistReads(boolean enabled);
    public void setEnableBlacklistRangeReads(boolean enabled);
    public boolean blacklistKey(String keyspace, String cf, String keyAsString);

    public int getOtcBacklogExpirationInterval();
    public void setOtcBacklogExpirationInterval(int intervalInMillis);

    /** Returns each live node's schema version */
    public Map<String, List<String>> getSchemaVersions();

    public int getNumberOfTables();

    public String setIdealConsistencyLevel(String cl);

    /**
     * Start the fully query logger.
     * @param path Path where the full query log will be stored. If null cassandra.yaml value is used.
     * @param rollCycle How often to create a new file for query data (MINUTELY, DAILY, HOURLY)
     * @param blocking Whether threads submitting queries to the query log should block if they can't be drained to the filesystem or alternatively drops samples and log
     * @param maxQueueWeight How many bytes of query data to queue before blocking or dropping samples
     * @param maxLogSize How many bytes of log data to store before dropping segments. Might not be respected if a log file hasn't rolled so it can be deleted.
     */
    public void configureFullQueryLogger(String path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize);

    /**
     * Disable the full query logger if it is enabled.
     * Also delete any generated files in the last used full query log path as well as the one configure in cassandra.yaml
     */
    public void resetFullQueryLogger();

    /**
     * Stop logging queries but leave any generated files on disk.
     */
    public void stopFullQueryLogger();

    public void useDeterministicTableID(boolean value);

    public void logBlockingReadRepairAttemptsForNSeconds(int seconds);

    public long[] getRecentReadLatencyHistogramMicrosV3();
    public long[] getRecentWriteLatencyHistogramMicrosV3();
    public long[] getRecentRangeLatencyHistogramMicrosV3();
    public long[] getRecentCasReadLatencyHistogramMicrosV3();
    public long[] getRecentCasWriteLatencyHistogramMicrosV3();
    public long[] getRecentViewWriteLatencyHistogramMicrosV3();
    public long[] getCasReadContentionHistogram();
    public long[] getCasWriteContentionHistogram();

    public long[] getRecentClientRequestReadConsistencyLevelOneMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelOneMicrosV3();
    public long[] getRecentClientRequestReadConsistencyLevelTwoMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelTwoMicrosV3();
    public long[] getRecentClientRequestReadConsistencyLevelThreeMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelThreeMicrosV3();
    public long[] getRecentClientRequestReadConsistencyLevelQuorumMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelQuorumMicrosV3();
    public long[] getRecentClientRequestReadConsistencyLevelLocalOneMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelLocalOneMicrosV3();
    public long[] getRecentClientRequestReadConsistencyLevelLocalQuorumMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelLocalQuorumMicrosV3();
    public long[] getRecentClientRequestReadConsistencyLevelEachQuorumMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelEachQuorumMicrosV3();
    public long[] getRecentClientRequestReadConsistencyLevelSerialMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelSerialMicrosV3();
    public long[] getRecentClientRequestReadConsistencyLevelLocalSerialMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelLocalSerialMicrosV3();
    public long[] getRecentClientRequestWriteConsistencyLevelAnyMicrosV3();
    public long[] getBytesReadPerQueryEstimatedHistogram();
    public long[] getBytesWrittenPerQueryEstimatedHistogram();

    /**
     * Tracking and reporting of variances in the repaired data set across replicas at read time
     */
    void enableRepairedDataTrackingForRangeReads();
    void disableRepairedDataTrackingForRangeReads();
    boolean getRepairedDataTrackingEnabledForRangeReads();

    void enableRepairedDataTrackingForPartitionReads();
    void disableRepairedDataTrackingForPartitionReads();
    boolean getRepairedDataTrackingEnabledForPartitionReads();

    void enableRepairedDataTrackingExclusions();
    void disableRepairedDataTrackingExclusions();
    boolean getRepairedDataTrackingExclusionsEnabled();
    String getRepairedDataTrackingExclusions();

    void enableReportingUnconfirmedRepairedDataMismatches();
    void disableReportingUnconfirmedRepairedDataMismatches();
    boolean getReportingUnconfirmedRepairedDataMismatchesEnabled();

    void enableSnapshotOnRepairedDataMismatch();
    void disableSnapshotOnRepairedDataMismatch();
    boolean getSnapshotOnRepairedDataMismatchEnabled();

    void enableSnapshotOnDuplicateRowDetection();
    void disableSnapshotOnDuplicateRowDetection();
    boolean getSnapshotOnDuplicateRowDetectionEnabled();
    boolean getCheckForDuplicateRowsDuringReads();
    void enableCheckForDuplicateRowsDuringReads();
    void disableCheckForDuplicateRowsDuringReads();
    boolean getCheckForDuplicateRowsDuringCompaction();
    void enableCheckForDuplicateRowsDuringCompaction();
    void disableCheckForDuplicateRowsDuringCompaction();

    void enableSecondaryIndex();
    void disableSecondaryIndex();
    boolean getSecondaryIndexEnabled();

    void setPaxosVariant(String variant);
    String getPaxosVariant();

    void setPaxosContentionStrategy(String variant);
    String getPaxosContentionStrategy();

    void setPaxosCoordinatorLockingDisabled(boolean disabled);
    boolean getPaxosCoordinatorLockingDisabled();
}
