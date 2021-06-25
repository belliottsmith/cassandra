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
package org.apache.cassandra.tools.nodetool;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import org.apache.cassandra.metrics.Sampler.SamplerType;
import org.apache.cassandra.metrics.SamplingManager;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import com.google.common.collect.Lists;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

@Command(name = "profileload", description = "Low footprint profiling of activity for a period of time")
public class ProfileLoad extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <cfname> <duration>", description = "The keyspace, column family name, and duration in milliseconds (Default: 10000)")
    private List<String> args = new ArrayList<>();

    @Option(name = "-s", description = "Capacity of the sampler, higher for more accuracy (Default: 256)")
    private int capacity = 256;

    @Option(name = "-k", description = "Number of the top samples to list (Default: 10)")
    private int topCount = 10;

    @Option(name = "-a", description = "Comma separated list of samplers to use (Default: all)")
    private String samplers = join(SamplerType.values(), ',');

    @Option(name = {"-i", "--interval"}, description = "Schedule a new job that samples every interval seconds (Default: 10) in the background")
    private int intervalSecs = -1;

    @Option(name = {"-t", "--stop"}, description = "Stop the scheduled sampling job identified by <keyspace> and <cfname>")
    private boolean shouldStop = false;

    @Option(name = {"-l", "--list"}, description = "List the scheduled sampling jobs")
    private boolean shouldList = false;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3 || args.size() == 2 || args.size() == 1 || args.size() == 0,
                      "Invalid arguments, either [keyspace table/* duration] or [keyspace table/*] or [duration] or no args.\n" +
                      "Optionally, use * to represent all tables under the keyspace.");
        checkArgument(topCount < capacity,
                      "TopK count (-k) option must be smaller then the summary capacity (-s)");
        String keyspace = null;
        String table = null;
        int durationMillis = 10000;
        int intervalMillis = (int) TimeUnit.SECONDS.toMillis(intervalSecs);
        /* There are 3 possible outcomes after processing the args.
         * - keyspace == null && table == null. It indicates to sample all tables
         * - keyspace == KEYSPACE && table == *. It indicates to sample all tables under the specified KEYSPACE
         * - keyspace = KEYSPACE && table == TABLE. It indicates to sample the table, KEYSPACE.TABLE
         */
        if (args.size() == 3)
        {
            keyspace = args.get(0);
            table = args.get(1);
            durationMillis = Integer.parseInt(args.get(2));
        }
        else if (args.size() == 2)
        {
            keyspace = args.get(0);
            table = args.get(1);
            table = table.equals("*") ? null : table; // reset table to null if sampling all tables with wildcard (*).
        }
        else if (args.size() == 1)
        {
            durationMillis = Integer.parseInt(args.get(0));
        }
        checkArgument(!hasInterval() || intervalMillis >= durationMillis,
                      String.format("Invalid scheduled sampling interval. Expecting interval >= duration, but interval: %s; duration: %s",
                                    intervalMillis, durationMillis));
        // generate the list of samplers
        List<String> targets = Lists.newArrayList();
        List<String> available = Arrays.stream(SamplerType.values()).map(Enum::toString).collect(Collectors.toList());
        for (String s : samplers.split(","))
        {
            String sampler = s.trim().toUpperCase();
            checkArgument(available.contains(sampler), String.format("'%s' sampler is not available from: %s", s, Arrays.toString(SamplerType.values())));
            targets.add(sampler);
        }

        Map<String, List<CompositeData>> results;
        try
        {
            // handle scheduled samplings, i.e. start or stop
            if (hasInterval() || shouldStop)
            {
                // keyspace and table are nullable
                boolean opSuccess = probe.handleScheduledSampling(keyspace, table, capacity, topCount, durationMillis, intervalMillis, targets, shouldStop);
                if (!opSuccess)
                {
                    if (shouldStop)
                        probe.output().out.printf("Unable to stop the non-exist scheduled sampling for keyspace: %s, table: %s%n", keyspace, table);
                    else
                        probe.output().out.printf("Unable to schedule sampling for keyspace: %s, table: %s due to overlapping. " +
                                                  "Stop the existing sampling jobs first.%n", keyspace, table);
                }
                return;
            }
            else if (shouldList)
            {
                probe.output().out.println(String.join(System.lineSeparator(), probe.getSampleTasks()));
                return;
            }
            else
            {
                // blocking sample all the tables or all the tables under a keyspace
                if (keyspace == null || table == null)
                    results = probe.getPartitionSample(keyspace, capacity, durationMillis, topCount, targets);
                else // blocking sample the specific table
                    results = probe.getPartitionSample(keyspace, table, capacity, durationMillis, topCount, targets);
            }
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }

        AtomicBoolean first = new AtomicBoolean(true);
        SamplingManager.ResultBuilder rb = new SamplingManager.ResultBuilder(first, results, targets);
        probe.output().out.println(SamplingManager.formatResult(rb));
    }

    private boolean hasInterval()
    {
        return intervalSecs != -1;
    }
}
