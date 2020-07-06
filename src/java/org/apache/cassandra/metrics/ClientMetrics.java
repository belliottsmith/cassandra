/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.transport.Server;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public final class ClientMetrics
{
    public static final ClientMetrics instance = new ClientMetrics();

    private static final MetricNameFactory factory = new DefaultNameFactory("Client");

    private volatile boolean initialized = false;
    private Collection<Server> servers = Collections.emptyList();

    private Meter authSuccess;
    private Meter authFailure;

    private AtomicInteger pausedConnections;
    private Gauge<Integer> pausedConnectionsGauge;
    private Meter requestDiscarded;

    private Timer messageQueueLatency;

    private ClientMetrics()
    {
    }

    public void markAuthSuccess()
    {
        authSuccess.mark();
    }

    public void markAuthFailure()
    {
        authFailure.mark();
    }

    public void pauseConnection() { pausedConnections.incrementAndGet(); }
    public void unpauseConnection() { pausedConnections.decrementAndGet(); }

    public void markRequestDiscarded() { requestDiscarded.mark(); }

    public void recordMessageQueueLatency(long latency, TimeUnit timeUnit)
    {
        messageQueueLatency.update(latency, timeUnit);
    }

    public synchronized void init(Collection<Server> servers)
    {
        if (initialized)
            return;

        this.servers = servers;

        registerGauge("connectedNativeClients",       this::countConnectedClients);
        registerGauge("connectedNativeClientsByUser", this::countConnectedClientsByUser);
        registerGauge("connections",                  this::connectedClients);
        registerGauge("clientsByProtocolVersion",     this::connectedClientsByProtocolVersion);

        authSuccess = registerMeter("AuthSuccess");
        authFailure = registerMeter("AuthFailure");

        pausedConnections = new AtomicInteger();
        pausedConnectionsGauge = registerGauge("PausedConnections", pausedConnections::get);
        requestDiscarded = registerMeter("RequestDiscarded");

        messageQueueLatency = registerTimer("MessageQueueLatency");

        initialized = true;
    }

    private int countConnectedClients()
    {
        int count = 0;

        for (Server server : servers)
            count += server.getConnectedClients();

        return count;
    }

    private Map<String, Integer> countConnectedClientsByUser()
    {
        Map<String, Integer> counts = new HashMap<>();

        for (Server server : servers)
        {
            server.getConnectedClientsByUser()
                  .forEach((username, count) -> counts.put(username, counts.getOrDefault(username, 0) + count));
        }

        return counts;
    }

    private List<Map<String, String>> connectedClients()
    {
        List<Map<String, String>> clients = new ArrayList<>();

        for (Server server : servers)
            for (Map<String, String> client : server.getConnectionStates())
                clients.add(client);

        return clients;
    }

    private List<Map<String, String>> connectedClientsByProtocolVersion()
    {

        List<Map<String, String>> result = new ArrayList<>();
        for (Server server : servers)
        {
            result.addAll(server.getClientsByProtocolVersion());
        }
        return result;
    }

    public <T> Gauge<T> registerGauge(String name, Gauge<T> gauge)
    {

        return Metrics.register(factory.createMetricName(name), gauge);
    }

    public Meter registerMeter(String name)
    {
        return Metrics.meter(factory.createMetricName(name));
    }

    public Timer registerTimer(String name)
    {
        return Metrics.timer(factory.createMetricName(name));
    }
}
