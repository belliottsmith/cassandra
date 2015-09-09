/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.notifications.*;

public abstract class AbstractUniformCompactionStrategy extends AbstractCompactionStrategy implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractUniformCompactionStrategy.class);

    protected final Set<SSTableReader> sstables = Sets.newSetFromMap(new ConcurrentHashMap<SSTableReader, Boolean>());

    protected AbstractUniformCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
    }

    @Override
    public void startup()
    {
        super.startup();
        sstables.clear();
        sstables.addAll(cfs.getSSTables());
        cfs.getDataTracker().subscribe(this);
        logger.debug("{} subscribed to the data tracker.", this);
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        cfs.getDataTracker().unsubscribe(this);
        logger.debug("{} unsubscribed from the data tracker.", this);
    }

    protected boolean markCompacting(Collection<SSTableReader> mark)
    {
        for (SSTableReader reader : mark)
            if (!sstables.contains(reader))
                return false;
        return cfs.getDataTracker().markCompacting(mark);
    }

    protected Collection<SSTableReader> markAllCompacting()
    {
        if (sstables.isEmpty() || !cfs.getDataTracker().markCompacting(sstables))
            return null;
        return ImmutableList.copyOf(sstables);
    }

    protected Set<SSTableReader> getUncompacting()
    {
        Set<SSTableReader> result = new HashSet<>(sstables);
        result.removeAll(cfs.getDataTracker().getCompacting());
        return result;
    }

    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            sstables.add(((SSTableAddedNotification) notification).added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            sstables.removeAll(((SSTableListChangedNotification) notification).removed);
            sstables.addAll(((SSTableListChangedNotification) notification).added);
        }
    }
}
