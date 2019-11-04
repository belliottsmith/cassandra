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
package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public final class TraceKeyspace
{
    private TraceKeyspace()
    {
    }

    public static final String NAME = "system_traces";

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen                0: original definition*
     * gen                1: removal of default_time_to_live (3.0)
     * gen 1577836800000000: maps to Jan 1 2020; an arbitrary date by which we assume no nodes older than 2.0.2
     *                       will ever start; see the note below for why this is necessary
     *
     * * See rdar://56815261 [Increase system_auth keyspace GENERATION to override pre-2.0.2 created table params]
     *   TL;DR: until CASSANDRA-6016 (Oct 13, 2.0.2) and in all of 1.2, we used to create system_traces keyspace and
     *   tables in the same way that we created the purely local 'system' keyspace - using current time on node bounce
     *   (+1). For new definitions to take place, we need to bump the generation even further than that.
     */
    public static final long GENERATION = 1577836800000000L;

    public static final String SESSIONS = "sessions";
    public static final String EVENTS = "events";

    private static final CFMetaData Sessions =
        compile(SESSIONS,
                "tracing sessions",
                "CREATE TABLE %s ("
                + "session_id uuid,"
                + "command text,"
                + "client inet,"
                + "coordinator inet,"
                + "duration int,"
                + "parameters map<text, text>,"
                + "request text,"
                + "started_at timestamp,"
                + "PRIMARY KEY ((session_id)))");

    private static final CFMetaData Events =
        compile(EVENTS,
                "tracing events",
                "CREATE TABLE %s ("
                + "session_id uuid,"
                + "event_id timeuuid,"
                + "activity text,"
                + "source inet,"
                + "source_elapsed int,"
                + "thread text,"
                + "PRIMARY KEY ((session_id), event_id))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), NAME)
                         .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.simpleTransient(2), Tables.of(Sessions, Events));
    }

    static Mutation makeStartSessionMutation(ByteBuffer sessionId,
                                             InetAddress client,
                                             Map<String, String> parameters,
                                             String request,
                                             long startedAt,
                                             String command,
                                             int ttl)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Sessions, FBUtilities.timestampMicros(), ttl, sessionId)
                                 .clustering()
                                 .add("client", client)
                                 .add("coordinator", FBUtilities.getBroadcastAddress())
                                 .add("request", request)
                                 .add("started_at", new Date(startedAt))
                                 .add("command", command);

        for (Map.Entry<String, String> entry : parameters.entrySet())
            adder.addMapEntry("parameters", entry.getKey(), entry.getValue());
        return adder.build();
    }

    static Mutation makeStopSessionMutation(ByteBuffer sessionId, int elapsed, int ttl)
    {
        return new RowUpdateBuilder(Sessions, FBUtilities.timestampMicros(), ttl, sessionId)
               .clustering()
               .add("duration", elapsed)
               .build();
    }

    static Mutation makeEventMutation(ByteBuffer sessionId, String message, int elapsed, String threadName, int ttl)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Events, FBUtilities.timestampMicros(), ttl, sessionId)
                                 .clustering(UUIDGen.getTimeUUID());
        adder.add("activity", message);
        adder.add("source", FBUtilities.getBroadcastAddress());
        adder.add("thread", threadName);
        if (elapsed >= 0)
            adder.add("source_elapsed", elapsed);
        return adder.build();
    }
}
