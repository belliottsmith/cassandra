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

import org.apache.cassandra.db.ConsistencyLevel;

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

    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections);
    public Long getNativeTransportMaxConcurrentConnections();

    public void reloadTriggerClasses();

    public long getReadRepairAttempted();
    public long getReadRepairRepairedBlocking();
    public long getReadRepairRepairedBackground();

    public void loadPartitionBlacklist();
    public void setEnablePartitionBlacklist(boolean enabled);
    public void setEnableBlacklistWrites(boolean enabled);
    public void setEnableBlacklistReads(boolean enabled);
    public void setEnableBlacklistRangeReads(boolean enabled);
    public boolean blacklistKey(String keyspace, String cf, String keyAsString);

    /** Returns each live node's schema version */
    public Map<String, List<String>> getSchemaVersions();

    public int getNumberOfTables();

    public void setIdealConsistencyLevel(String cl);

    public long[] getRecentReadLatencyHistogramMicrosV3();
    public long[] getRecentWriteLatencyHistogramMicrosV3();
    public long[] getRecentRangeLatencyHistogramMicrosV3();
    public long[] getRecentCasReadLatencyHistogramMicrosV3();
    public long[] getRecentCasWriteLatencyHistogramMicrosV3();
    public long[] getRecentViewWriteLatencyHistogramMicrosV3();
    public long[] getCasReadContentionHistogram();
    public long[] getCasWriteContentionHistogram();
}
