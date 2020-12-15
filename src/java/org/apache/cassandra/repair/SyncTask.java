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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;

/**
 * SyncTask takes the difference of MerkleTrees between two nodes
 * and perform necessary operation to repair replica.
 */
public abstract class SyncTask extends AbstractFuture<SyncStat> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(SyncTask.class);

    protected final RepairJobDesc desc;
    public final InetAddress firstEndpoint;
    public final InetAddress secondEndpoint;
    protected final PreviewKind previewKind;

    public final List<Range<Token>> rangesToSync;

    protected volatile SyncStat stat;
    protected long startTime = Long.MIN_VALUE;

    public SyncTask(RepairJobDesc desc, InetAddress firstEndpoint, InetAddress secondEndpoint, List<Range<Token>> rangesToSync, PreviewKind previewKind)
    {
        this.desc = desc;
        this.firstEndpoint = firstEndpoint;
        this.secondEndpoint = secondEndpoint;
        this.rangesToSync = rangesToSync;
        this.previewKind = previewKind;
        this.stat = new SyncStat(new NodePair(firstEndpoint, secondEndpoint), rangesToSync.size());
    }

    protected abstract void startSync();

    /**
     * Compares trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        startTime = System.currentTimeMillis();
        // choose a repair method based on the significance of the difference
        String format = String.format("%s Endpoints %s and %s %%s for %s", previewKind.logPrefix(desc.sessionId), firstEndpoint, secondEndpoint, desc.columnFamily);
        if (rangesToSync.isEmpty())
        {
            logger.info(String.format(format, "are consistent"));
            Tracing.traceRepair("Endpoint {} is consistent with {} for {}", firstEndpoint, secondEndpoint, desc.columnFamily);
            set(stat);
            return;
        }

        // non-0 difference: perform streaming repair
        logger.info(String.format(format, "have " + rangesToSync.size() + " range(s) out of sync"));
        Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}", firstEndpoint, rangesToSync.size(), secondEndpoint, desc.columnFamily);
        startSync();
    }

    public SyncStat getCurrentStat()
    {
        return stat;
    }

    protected void finished()
    {
        if (startTime != Long.MIN_VALUE)
            Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily).metric.syncTime.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }

    boolean isLocal()
    {
        return false;
    }

    public NodePair nodePair()
    {
        return new NodePair(firstEndpoint, secondEndpoint);
    }

    public String toString()
    {
        return "SyncTask{" +
               "desc=" + desc +
               ", firstEndpoint=" + firstEndpoint +
               ", secondEndpoint=" + secondEndpoint +
               ", previewKind=" + previewKind +
               ", rangesToSync=" + rangesToSync +
               "} ";
    }
}
