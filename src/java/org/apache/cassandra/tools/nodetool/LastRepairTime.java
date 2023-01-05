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

import java.util.*;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "lrt", description = "Print last repair time for keyspace/table/token")
public class LastRepairTime extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table> <tokenOrKey>", description = "The keyspace, table name and token/key")
    private List<String> args = new ArrayList<>();

    @Option(title = "for_token", name = { "-t", "--token"}, description = "Get last repair time for token, not key")
    private boolean forToken = false;

    @Override
    public void execute(NodeProbe probe)
    {
        if (args.size() < 3)
            throw new RuntimeException("Supply keyspace, table and token or key");

        System.out.println(forToken
                           ? probe.getLastRepairTimeForToken(args.get(0), args.get(1), args.get(2))
                           : probe.getLastRepairTimeForKey(args.get(0), args.get(1), args.get(2)));
    }
}