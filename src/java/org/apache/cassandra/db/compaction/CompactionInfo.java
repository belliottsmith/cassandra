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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.cassandra.io.sstable.format.SSTableReader;

public final class CompactionInfo
{
    private final CFMetaData cfm;
    private final OperationType tasktype;
    private final long completed;
    private final long total;
    private final Unit unit;
    private final UUID compactionId;
    private final ImmutableSet<SSTableReader> sstables;

    public CompactionInfo(CFMetaData cfm, OperationType tasktype, long bytesComplete, long totalBytes, UUID compactionId, Collection<SSTableReader> sstables)
    {
        this(cfm, tasktype, bytesComplete, totalBytes, Unit.BYTES, compactionId, sstables);
    }

    private CompactionInfo(CFMetaData cfm, OperationType tasktype, long completed, long total, Unit unit, UUID compactionId, Collection<SSTableReader> sstables)
    {
        this.tasktype = tasktype;
        this.completed = completed;
        this.total = total;
        this.cfm = cfm;
        this.unit = unit;
        this.compactionId = compactionId;
        this.sstables = ImmutableSet.copyOf(sstables);
    }

    /**
     * Special compaction info where we always need to cancel the compaction - for example ViewBuilderTask and AutoSavingCache where we don't know
     * the sstables at construction
     */
    public static CompactionInfo withoutSSTables(CFMetaData cfm, OperationType tasktype, long completed, long total, Unit unit, UUID compactionId)
    {
        return new CompactionInfo(cfm, tasktype, completed, total, unit, compactionId, ImmutableSet.of());
    }

    /** @return A copy of this CompactionInfo with updated progress. */
    public CompactionInfo forProgress(long complete, long total)
    {
        return new CompactionInfo(cfm, tasktype, complete, total, unit, compactionId, sstables);
    }

    public UUID getId()
    {
        return cfm != null ? cfm.cfId : null;
    }

    public String getKeyspace()
    {
        return cfm != null ? cfm.ksName : null;
    }

    public String getColumnFamily()
    {
        return cfm != null ? cfm.cfName : null;
    }

    public CFMetaData getCFMetaData()
    {
        return cfm;
    }

    public long getCompleted()
    {
        return completed;
    }

    public long getTotal()
    {
        return total;
    }

    public OperationType getTaskType()
    {
        return tasktype;
    }

    public UUID compactionId()
    {
        return compactionId;
    }

    public Set<SSTableReader> getSSTables()
    {
        return sstables;
    }

    /**
     * Note that this estimate is based on the amount of data we have left to read - it assumes input
     * size = output size for a compaction, which is not really true, but should most often provide a worst case
     * remaining write size.
     */
    public long estimatedRemainingWriteBytes()
    {
        if (unit == Unit.BYTES && tasktype.writesData)
            return getTotal() - getCompleted();
        return 0;
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(getTaskType());
        if (cfm != null)
        {
            buff.append('@').append(getId()).append('(');
            buff.append(getKeyspace()).append(", ").append(getColumnFamily()).append(", ");
        }
        else
        {
            buff.append('(');
        }
        buff.append(getCompleted()).append('/').append(getTotal());
        return buff.append(')').append(unit).toString();
    }

    public Map<String, String> asMap()
    {
        Map<String, String> ret = new HashMap<String, String>();
        ret.put("id", getId() == null ? "" : getId().toString());
        ret.put("keyspace", getKeyspace());
        ret.put("columnfamily", getColumnFamily());
        ret.put("completed", Long.toString(completed));
        ret.put("total", Long.toString(total));
        ret.put("taskType", tasktype.toString());
        ret.put("unit", unit.toString());
        ret.put("compactionId", compactionId == null ? "" : compactionId.toString());
        ret.put("sstables", Joiner.on(',').join(sstables));
        return ret;
    }

    boolean shouldStop(Predicate<SSTableReader> sstablePredicate)
    {
        if (sstables.isEmpty())
        {
            return true;
        }
        return Iterables.any(sstables, sstablePredicate);
    }

    public static abstract class Holder
    {
        private volatile boolean stopRequested = false;
        public abstract CompactionInfo getCompactionInfo();
        double load = StorageMetrics.load.getCount();
        double reportedSeverity = 0d;

        public void stop()
        {
            stopRequested = true;
        }

        /**
         * if this compaction involves several/all tables we can safely check globalCompactionsPaused
         * in isStopRequested() below
         */
        public abstract boolean isGlobal();

        public boolean isStopRequested()
        {
            return stopRequested || (isGlobal() && CompactionManager.instance.globalCompactionsPaused());
        }

        /**
         * report event on the size of the compaction.
         */
        public void started()
        {
            reportedSeverity = getCompactionInfo().getTotal() / load;
            StorageService.instance.reportSeverity(reportedSeverity);
        }

        /**
         * remove the event complete
         */
        public void finished()
        {
            if (reportedSeverity != 0d)
                StorageService.instance.reportSeverity(-(reportedSeverity));
            reportedSeverity = 0d;
        }
    }

    public static enum Unit
    {
        BYTES("bytes"), RANGES("ranges"), KEYS("keys");

        private final String name;

        private Unit(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }

        public static boolean isFileSize(String unit)
        {
            return BYTES.toString().equals(unit);
        }
    }
}
