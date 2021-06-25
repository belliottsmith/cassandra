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

package org.apache.cassandra.metrics;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Pair;

public class SamplingManager
{
    private static final Logger logger = LoggerFactory.getLogger(SamplingManager.class);

    /**
     * Tracks the active scheduled sampling tasks.
     * The key of the map is a pair of keyspace and table. Both keyspace and table are nullable
     * The value of the map is the current scheduled task
     */
    private final ConcurrentHashMap<Pair<String, String>, Future<?>>
        activeSamplingTasks = new ConcurrentHashMap<>();

    public static String formatResult(ResultBuilder resultBuilder)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos))
        {
            for (String sampler : Lists.newArrayList("READS", "WRITES", "CAS_CONTENTIONS")) {
                resultBuilder.forType(Sampler.SamplerType.valueOf(sampler), "Frequency of " + sampler.toLowerCase().replaceAll("_", " ") + " by partition")
                  .addColumn("Table", "table")
                  .addColumn("Partition", "value")
                  .addColumn("Count", "count")
                  .addColumn("+/-", "error")
                  .print(ps);
            }

            resultBuilder.forType(Sampler.SamplerType.WRITE_SIZE, "Max mutation size by partition")
              .addColumn("Table", "table")
              .addColumn("Partition", "value")
              .addColumn("Bytes", "count")
              .print(ps);

            resultBuilder.forType(Sampler.SamplerType.LOCAL_READ_TIME, "Longest read query times")
              .addColumn("Query", "value")
              .addColumn("Microseconds", "count")
              .print(ps);

            return baos.toString();
        }
    }

    public static Iterable<ColumnFamilyStore> getTables(String ks, String table)
    {
        // for all tables
        if (ks == null)
            return ColumnFamilyStore.all();

        Keyspace keyspace = Keyspace.open(ks);
        // for all tables under the `ks`
        if (table == null)
            return keyspace.getColumnFamilyStores();
        else // for a specific table
            return Collections.singletonList(keyspace.getColumnFamilyStore(table));
    }

    public boolean register(String ks, String table, int duration, int interval, int capacity, int count, List<String> samplers)
    {
        Pair<String, String> key = Pair.create(ks, table);
        if (!canSchedule(key))
            return false;

        // 'begin' always runs before 'finish'
        Future<?> sf = ScheduledExecutors.optionalTasks
                           .submit(getSamplingBegin(key, getTables(ks, table), duration, interval, capacity, count, samplers));
        activeSamplingTasks.put(key, sf);
        return true;
    }

    public boolean unregister(String ks, String table)
    {
        Future<?> task = activeSamplingTasks.remove(Pair.create(ks, table));
        if (task != null)
        {
            task.cancel(false);
        }
        return task != null;
    }

    public List<String> all()
    {
        Enumeration<Pair<String, String>> keys = activeSamplingTasks.keys();
        List<String> all = new ArrayList<>();
        while (keys.hasMoreElements())
        {
            all.add(formatJobId(keys.nextElement()));
        }
        return all;
    }

    // validate if a schedule on the ks and table can be permitted
    // returns false, if there are overlapping tables already being sampled
    private boolean canSchedule(Pair<String, String> ksTable)
    {
        Pair<String, String> sampleAll = Pair.create(null, null);
        // there is a schedule that works on all tables. Overlapping guaranteed.
        if (activeSamplingTasks.containsKey(sampleAll)
            || (!activeSamplingTasks.isEmpty() && ksTable.equals(sampleAll)))
            return false;
        // there is an exactly duplicated schedule
        else if (activeSamplingTasks.containsKey(ksTable))
            return false;
        else
            // make sure has no overlapping tables under the keyspace
            return !activeSamplingTasks.containsKey(Pair.create(ksTable.left, null));
    }

    private String formatJobId(Pair<String, String> jobId)
    {
        return wildCardMaybe(jobId.left) + '.' + wildCardMaybe(jobId.right);
    }

    private String wildCardMaybe(String input)
    {
        return input == null ? "*" : input;
    }


    // begin sampling and schedule a future task to finish sampling
    private Runnable getSamplingBegin(Pair<String, String> jobId, Iterable<ColumnFamilyStore> tables, int duration, int interval, int capacity, int count, List<String> samplers)
    {
        return () ->
        {
            for (String sampler : samplers)
            {
                for (ColumnFamilyStore cfs : tables)
                {
                    cfs.beginLocalSampling(sampler, capacity, duration);
                }
            }
            Future<?> fut = ScheduledExecutors.optionalTasks
                                .schedule(getSamplingFinish(jobId, tables, duration, interval, capacity, count, samplers),
                                          interval,
                                          TimeUnit.MILLISECONDS);
            activeSamplingTasks.put(jobId, fut);
        };
    }

    // finish the sampling and begin a new one right after
    private Runnable getSamplingFinish(Pair<String, String> jobId, Iterable<ColumnFamilyStore> tables, int duration, int interval, int capacity, int count, List<String> samplers)
    {
        return () ->
        {
            Map<String, List<CompositeData>> results = new HashMap<>();
            for (String sampler : samplers)
            {
                List<CompositeData> topk = new ArrayList<>();
                for (ColumnFamilyStore cfs : tables)
                {
                    try
                    {
                        topk.addAll(cfs.finishLocalSampling(sampler, count));
                    }
                    catch (OpenDataException e)
                    {
                        logger.warn("Failed to retrive the sampled data. Abort the background sampling job: {}.", formatJobId(jobId), e);
                        activeSamplingTasks.remove(jobId);
                        return;
                    }
                }

                topk.sort((left, right) -> Long.compare((long) right.get("count"), (long) left.get("count")));
                // sublist is not serializable for jmx
                topk = new ArrayList<>(topk.subList(0, Math.min(topk.size(), count)));
                results.put(sampler, topk);
            }
            AtomicBoolean first = new AtomicBoolean(false);
            ResultBuilder rb = new ResultBuilder(first, results, samplers);
            logger.info(formatResult(rb));
            Future<?> fut = ScheduledExecutors.optionalTasks
                                .submit(getSamplingBegin(jobId, tables, duration, interval, capacity, count, samplers));
            activeSamplingTasks.put(jobId, fut);
        };
    }

    public static class ResultBuilder
    {
        protected Sampler.SamplerType type;
        protected String description;
        protected AtomicBoolean first;
        protected Map<String, List<CompositeData>> results;
        protected List<String> targets;
        protected List<Pair<String, String>> dataKeys;

        public ResultBuilder(AtomicBoolean first, Map<String, List<CompositeData>> results, List<String> targets)
        {
            super();
            this.first = first;
            this.results = results;
            this.targets = targets;
            this.dataKeys = new ArrayList<>();
            this.dataKeys.add(Pair.create("  ", "  "));
        }

        public SamplingManager.ResultBuilder forType(Sampler.SamplerType type, String description)
        {
            SamplingManager.ResultBuilder rb = new SamplingManager.ResultBuilder(first, results, targets);
            rb.type = type;
            rb.description = description;
            return rb;
        }

        public SamplingManager.ResultBuilder addColumn(String title, String key)
        {
            this.dataKeys.add(Pair.create(title, key));
            return this;
        }

        protected String get(CompositeData cd, String key)
        {
            if (cd.containsKey(key))
                return cd.get(key).toString();
            return key;
        }

        public void print(PrintStream ps)
        {
            if (targets.contains(type.toString()))
            {
                if (!first.get())
                    ps.println();
                first.set(false);
                ps.println(description + ':');
                TableBuilder out = new TableBuilder();
                out.add(dataKeys.stream().map(p -> p.left).collect(Collectors.toList()).toArray(new String[] {}));
                List<CompositeData> topk = results.get(type.toString());
                for (CompositeData cd : topk)
                {
                    out.add(dataKeys.stream().map(p -> get(cd, p.right)).collect(Collectors.toList()).toArray(new String[] {}));
                }
                if (topk.size() == 0)
                {
                    ps.println("   Nothing recorded during sampling period...");
                }
                else
                {
                    out.printTo(ps);
                }
            }
        }
    }
}
