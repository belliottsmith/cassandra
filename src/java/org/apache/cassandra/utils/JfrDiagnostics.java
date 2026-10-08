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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Period;
import jdk.jfr.StackTrace;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingJfrEvents;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.execution.AccordExecutor;

/**
 * JFR diagnostic events for finding the cause of latency hiccups, enabled with
 * {@code -Dcassandra.jfr.diagnostic_events=true}:
 * <ul>
 *   <li>{@code cassandra.net.Outbound}, {@code cassandra.net.Inbound}, {@code cassandra.net.InboundPeer},
 *       {@code cassandra.net.Backpressure}: see {@link MessagingJfrEvents}</li>
 *   <li>{@code cassandra.os.Tcp}: this node's (network namespace's) TCP counters from /proc/net/snmp and
 *       /proc/net/netstat: segments, retransmits, RTOs, loss recovery, drops</li>
 *   <li>{@code cassandra.accord.Executor}: each Accord executor's queues and cache</li>
 * </ul>
 * The periodic events default to a 1 s period; a recording's settings may change it (e.g. 100 ms). Registering
 * costs nothing while no recording enables the events: JFR only invokes the hooks for enabled periodic events.
 * The hooks are registered by {@link MessagingService} on construction and removed on its shutdown, so in-JVM dtest
 * instances each have their own.
 */
public final class JfrDiagnostics
{
    private static final Logger logger = LoggerFactory.getLogger(JfrDiagnostics.class);

    private final List<Runnable> hooks = new ArrayList<>();

    private JfrDiagnostics() {}

    @Name("cassandra.os.Tcp")
    @Label("TCP Counters")
    @Category({ "Cassandra", "Operating System" })
    @Description("Cumulative TCP counters of this node's network namespace (/proc/net/snmp, /proc/net/netstat); -1 if absent")
    @Period("1 s")
    @StackTrace(false)
    public static class Tcp extends Event
    {
        @Label("Local") public String local;
        @Label("Local Accord Id") public int localAccordId = -1;
        @Label("Current Established") public long currEstab = -1;
        @Label("Active Opens") public long activeOpens = -1;
        @Label("Estab Resets") public long estabResets = -1;
        @Label("In Segments") public long inSegs = -1;
        @Label("Out Segments") public long outSegs = -1;
        @Label("Retransmitted Segments") public long retransSegs = -1;
        @Label("In Errors") public long inErrs = -1;
        @Label("Out Resets") public long outRsts = -1;
        @Label("RTO Timeouts") public long tcpTimeouts = -1;
        @Label("Loss Probes") public long tcpLossProbes = -1;
        @Label("Loss Probe Recoveries") public long tcpLossProbeRecovery = -1;
        @Label("Fast Retransmits") public long tcpFastRetrans = -1;
        @Label("Slow Start Retransmits") public long tcpSlowStartRetrans = -1;
        @Label("Lost Retransmits") public long tcpLostRetransmit = -1;
        @Label("SACK Recoveries") public long tcpSackRecovery = -1;
        @Label("Spurious RTOs") public long tcpSpuriousRTOs = -1;
        @Label("Retransmit Failures") public long tcpRetransFail = -1;
        @Label("Backlog Drops") public long tcpBacklogDrop = -1;
        @Label("Receive Queue Drops") public long tcpRcvQDrop = -1;
        @Label("Out Of Order Queued") public long tcpOFOQueue = -1;
        @Label("Receive Pruned") public long pruneCalled = -1;
        @Label("Zero Window Advertised") public long tcpToZeroWindowAdv = -1;
        @Label("Want Zero Window Advertised") public long tcpWantZeroWindowAdv = -1;
    }

    @Name("cassandra.accord.Executor")
    @Label("Accord Executor")
    @Category({ "Cassandra", "Accord" })
    @Description("Periodic state of each Accord executor (racy reads)")
    @Period("1 s")
    @StackTrace(false)
    public static class Executor extends Event
    {
        @Label("Local") public String local;
        @Label("Local Accord Id") public int localAccordId = -1;
        @Label("Executor Id") public int executorId;
        @Label("Waiting To Run") @Description("tasks ready to run, waiting for their command store or a thread")
        public int waitingToRun;
        @Label("Preparing To Run") @Description("tasks loading their state") public int preparingToRun;
        @Label("Running") public int running;
        @Label("Cache Entries") public int cacheSize;
        @Label("Cache Weighted Size") @DataAmount public long cacheWeightedSize;
        @Label("Cache Capacity") @DataAmount public long cacheCapacity;
    }

    /**
     * Registers the periodic hooks if {@code cassandra.jfr.diagnostic_events} is set.
     * @return the registration, to {@link #close} on shutdown, or null
     */
    public static JfrDiagnostics register(MessagingService messaging)
    {
        if (!MessagingJfrEvents.ENABLED)
            return null;
        try
        {
            JfrDiagnostics d = new JfrDiagnostics();
            MessagingJfrEvents.setBackpressureContext(JfrDiagnostics::context);
            d.add(MessagingJfrEvents.Outbound.class, () -> MessagingJfrEvents.emitOutbound(messaging, context()));
            d.add(MessagingJfrEvents.Inbound.class, () -> MessagingJfrEvents.emitInbound(messaging, context()));
            d.add(MessagingJfrEvents.InboundPeer.class, () -> MessagingJfrEvents.emitInboundPeers(messaging, context()));
            d.add(Tcp.class, JfrDiagnostics::emitTcp);
            d.add(Executor.class, JfrDiagnostics::emitExecutors);
            logger.info("Registered JFR diagnostic events (cassandra.net.*, cassandra.os.Tcp, cassandra.accord.Executor)");
            return d;
        }
        catch (Throwable t)
        {
            logger.warn("Could not register JFR diagnostic events", t);
            return null;
        }
    }

    private void add(Class<? extends Event> type, Runnable hook)
    {
        Runnable safe = () -> {
            try
            {
                hook.run();
            }
            catch (Throwable t)
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, java.util.concurrent.TimeUnit.MINUTES,
                                 "JFR diagnostic event {} failed: {}", type.getSimpleName(), t);
            }
        };
        FlightRecorder.addPeriodicEvent(type, safe);
        hooks.add(safe);
    }

    public void close()
    {
        MessagingJfrEvents.setBackpressureContext(null);
        for (Runnable hook : hooks)
            FlightRecorder.removePeriodicEvent(hook);
        hooks.clear();
    }

    private static String localName()
    {
        try
        {
            return FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort();
        }
        catch (Throwable t)
        {
            return "?";
        }
    }

    private static int localAccordId()
    {
        Node.Id id = AccordService.nodeId();
        return id == null ? -1 : id.id;
    }

    private static int accordId(InetAddressAndPort endpoint)
    {
        try
        {
            if (!AccordService.isSetup())
                return -1;
            Node.Id id = AccordService.instance().endpointMapper().mappedIdOrNull(endpoint);
            return id == null ? -1 : id.id;
        }
        catch (Throwable t)
        {
            return -1;
        }
    }

    private static MessagingJfrEvents.Context context()
    {
        return new MessagingJfrEvents.Context(localName(), localAccordId(), JfrDiagnostics::accordId);
    }

    private static void emitTcp()
    {
        Map<String, Long> v = new HashMap<>();
        readProcNet(Path.of("/proc/net/snmp"), v);
        readProcNet(Path.of("/proc/net/netstat"), v);
        if (v.isEmpty())
            return;
        Tcp e = new Tcp();
        e.local = localName();
        e.localAccordId = localAccordId();
        e.currEstab = v.getOrDefault("Tcp.CurrEstab", -1L);
        e.activeOpens = v.getOrDefault("Tcp.ActiveOpens", -1L);
        e.estabResets = v.getOrDefault("Tcp.EstabResets", -1L);
        e.inSegs = v.getOrDefault("Tcp.InSegs", -1L);
        e.outSegs = v.getOrDefault("Tcp.OutSegs", -1L);
        e.retransSegs = v.getOrDefault("Tcp.RetransSegs", -1L);
        e.inErrs = v.getOrDefault("Tcp.InErrs", -1L);
        e.outRsts = v.getOrDefault("Tcp.OutRsts", -1L);
        e.tcpTimeouts = v.getOrDefault("TcpExt.TCPTimeouts", -1L);
        e.tcpLossProbes = v.getOrDefault("TcpExt.TCPLossProbes", -1L);
        e.tcpLossProbeRecovery = v.getOrDefault("TcpExt.TCPLossProbeRecovery", -1L);
        e.tcpFastRetrans = v.getOrDefault("TcpExt.TCPFastRetrans", -1L);
        e.tcpSlowStartRetrans = v.getOrDefault("TcpExt.TCPSlowStartRetrans", -1L);
        e.tcpLostRetransmit = v.getOrDefault("TcpExt.TCPLostRetransmit", -1L);
        e.tcpSackRecovery = v.getOrDefault("TcpExt.TCPSackRecovery", -1L);
        e.tcpSpuriousRTOs = v.getOrDefault("TcpExt.TCPSpuriousRTOs", -1L);
        e.tcpRetransFail = v.getOrDefault("TcpExt.TCPRetransFail", -1L);
        e.tcpBacklogDrop = v.getOrDefault("TcpExt.TCPBacklogDrop", -1L);
        e.tcpRcvQDrop = v.getOrDefault("TcpExt.TCPRcvQDrop", -1L);
        e.tcpOFOQueue = v.getOrDefault("TcpExt.TCPOFOQueue", -1L);
        e.pruneCalled = v.getOrDefault("TcpExt.PruneCalled", -1L);
        e.tcpToZeroWindowAdv = v.getOrDefault("TcpExt.TCPToZeroWindowAdv", -1L);
        e.tcpWantZeroWindowAdv = v.getOrDefault("TcpExt.TCPWantZeroWindowAdv", -1L);
        e.commit();
    }

    /** /proc/net/{snmp,netstat}: pairs of "<Proto>: <names...>" / "<Proto>: <values...>" lines */
    static void readProcNet(Path file, Map<String, Long> into)
    {
        List<String> lines;
        try
        {
            if (!Files.isReadable(file))
                return;
            lines = Files.readAllLines(file);
        }
        catch (IOException e)
        {
            return;
        }
        for (int i = 0; i + 1 < lines.size(); i += 2)
        {
            String[] names = lines.get(i).split("\\s+"), values = lines.get(i + 1).split("\\s+");
            if (names.length != values.length || names.length < 2 || !names[0].equals(values[0]))
                continue;
            String proto = names[0].substring(0, names[0].length() - 1);
            for (int j = 1; j < names.length; j++)
            {
                try
                {
                    into.put(proto + '.' + names[j], Long.parseLong(values[j]));
                }
                catch (NumberFormatException ignore)
                {
                }
            }
        }
    }

    private static void emitExecutors()
    {
        if (!AccordService.isSetup())
            return;
        List<AccordExecutor> executors;
        try
        {
            executors = AccordService.instance().executors();
        }
        catch (Throwable t)
        {
            return;
        }
        if (executors == null)
            return;
        String local = localName();
        int localId = localAccordId();
        for (AccordExecutor executor : executors)
        {
            Executor e = new Executor();
            e.local = local;
            e.localAccordId = localId;
            e.executorId = executor.executorId();
            e.waitingToRun = executor.unsafeWaitingToRunCount();
            e.preparingToRun = executor.unsafePreparingToRunCount();
            e.running = executor.unsafeRunningCount();
            e.cacheSize = executor.size();
            e.cacheWeightedSize = executor.weightedSize();
            e.cacheCapacity = executor.capacity();
            e.commit();
        }
    }
}
