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

import java.io.PrintStream;

import io.airlift.command.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getsnapshotthrottle", description = "Print the snapshot_links_per_second throttle for snapshot/clearsnapshot")
public class GetSnapshotThrottle extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        long throttle = probe.getSnapshotLinksPerSecond();
        if (throttle > 0)
            out.println("Current snapshot throttle: " + throttle + " links/s");
        else
            out.println("Snapshot throttle is disabled");
    }
}
