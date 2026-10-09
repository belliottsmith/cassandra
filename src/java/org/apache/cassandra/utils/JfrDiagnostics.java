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
import jdk.jfr.Timespan;

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
 *   <li>{@code cassandra.os.Pressure}: the container's (cgroup v2) CPU, memory and I/O pressure stall times, memory
 *       use and events, and the host's major page faults and swapping (/proc/vmstat): whether threads were stalled
 *       by the kernel rather than by Cassandra</li>
 *   <li>{@code cassandra.os.Threads}: this JVM's threads in uninterruptible sleep (D) or running (R), from
 *       /proc/self/task, with the kernel wait channel of each D thread: threads the profiler cannot sample because
 *       they are blocked in the kernel (page faults on unlocked mappings, mmap_lock contention, ...)</li>
 *   <li>{@code cassandra.accord.Executor}: each Accord executor's queues and cache</li>
 * </ul>
 * {@code cassandra.os.Pressure} also records the host's CPU steal time (/proc/stat): time the hypervisor did not run
 * this (virtual) machine's CPUs. A stalled thread with no CPU time, no samples and no pressure stall time, while
 * other threads keep running, is what a descheduled vCPU looks like from inside the guest.
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

    @Name("cassandra.os.Pressure")
    @Label("Pressure Stalls")
    @Category({ "Cassandra", "Operating System" })
    @Description("Cumulative pressure stall times of this container's cgroup (/sys/fs/cgroup/*.pressure: 'some' = at least "
                 + "one task stalled, 'full' = all non-idle tasks stalled), its memory use and events, and the host's major "
                 + "faults and swap traffic (/proc/vmstat); -1 if absent")
    @Period("1 s")
    @StackTrace(false)
    public static class Pressure extends Event
    {
        @Label("Local") public String local;
        @Label("Local Accord Id") public int localAccordId = -1;
        @Label("CPU Some") @Timespan(Timespan.MICROSECONDS) public long cpuSome = -1;
        @Label("CPU Full") @Timespan(Timespan.MICROSECONDS) public long cpuFull = -1;
        @Label("Memory Some") @Timespan(Timespan.MICROSECONDS) public long memorySome = -1;
        @Label("Memory Full") @Timespan(Timespan.MICROSECONDS) public long memoryFull = -1;
        @Label("IO Some") @Timespan(Timespan.MICROSECONDS) public long ioSome = -1;
        @Label("IO Full") @Timespan(Timespan.MICROSECONDS) public long ioFull = -1;
        @Label("Memory Current") @DataAmount public long memoryCurrent = -1;
        @Label("Swap Current") @DataAmount public long swapCurrent = -1;
        @Label("Memory Events High") public long memoryHigh = -1;
        @Label("Memory Events Max") public long memoryMax = -1;
        @Label("Memory Events OOM") public long memoryOom = -1;
        @Label("Cgroup Major Faults") public long cgroupMajorFaults = -1;
        @Label("Cgroup File Dirty") @DataAmount public long cgroupFileDirty = -1;
        @Label("Cgroup File Writeback") @DataAmount public long cgroupFileWriteback = -1;
        @Label("Host Major Faults") public long hostMajorFaults = -1;
        @Label("Host Pages Swapped In") public long hostSwapIn = -1;
        @Label("Host Pages Swapped Out") public long hostSwapOut = -1;
        @Label("Host Direct Reclaim Scans") public long hostDirectScan = -1;
        @Label("Host Steal") @Timespan(Timespan.MILLISECONDS) @Description("cumulative CPU steal time, all CPUs (/proc/stat)")
        public long hostSteal = -1;
        @Label("Host Max CPU Steal") @Timespan(Timespan.MILLISECONDS) @Description("the most steal time of any one CPU since the previous event")
        public long hostMaxCpuSteal = -1;
    }

    @Name("cassandra.os.Threads")
    @Label("Kernel Thread States")
    @Category({ "Cassandra", "Operating System" })
    @Description("This JVM's threads in uninterruptible sleep (D) and running (R) states (/proc/self/task/*/stat), with the "
                 + "kernel wait channel (wchan) of each D thread")
    @Period("1 s")
    @StackTrace(false)
    public static class Threads extends Event
    {
        @Label("Local") public String local;
        @Label("Local Accord Id") public int localAccordId = -1;
        @Label("Threads") public int threads;
        @Label("Running") public int running;
        @Label("Uninterruptible") public int uninterruptible;
        @Label("Uninterruptible Threads") @Description("name(tid):wchan of each D thread, at most 32")
        public String uninterruptibleThreads;
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
            d.add(Pressure.class, JfrDiagnostics::emitPressure);
            d.add(Threads.class, JfrDiagnostics::emitThreads);
            d.add(Executor.class, JfrDiagnostics::emitExecutors);
            logger.info("Registered JFR diagnostic events (cassandra.net.*, cassandra.os.Tcp, cassandra.os.Pressure, cassandra.os.Threads, cassandra.accord.Executor)");
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

    private static final Path CGROUP = Path.of("/sys/fs/cgroup");

    private static void emitPressure()
    {
        Pressure e = new Pressure();
        e.local = localName();
        e.localAccordId = localAccordId();
        long[] v = readPressure(CGROUP.resolve("cpu.pressure"));
        e.cpuSome = v[0]; e.cpuFull = v[1];
        v = readPressure(CGROUP.resolve("memory.pressure"));
        e.memorySome = v[0]; e.memoryFull = v[1];
        v = readPressure(CGROUP.resolve("io.pressure"));
        e.ioSome = v[0]; e.ioFull = v[1];
        e.memoryCurrent = readLong(CGROUP.resolve("memory.current"));
        e.swapCurrent = readLong(CGROUP.resolve("memory.swap.current"));
        Map<String, Long> events = readKeyValues(CGROUP.resolve("memory.events"));
        e.memoryHigh = events.getOrDefault("high", -1L);
        e.memoryMax = events.getOrDefault("max", -1L);
        e.memoryOom = events.getOrDefault("oom", -1L);
        Map<String, Long> stat = readKeyValues(CGROUP.resolve("memory.stat"));
        e.cgroupMajorFaults = stat.getOrDefault("pgmajfault", -1L);
        e.cgroupFileDirty = stat.getOrDefault("file_dirty", -1L);
        e.cgroupFileWriteback = stat.getOrDefault("file_writeback", -1L);
        Map<String, Long> vmstat = readKeyValues(Path.of("/proc/vmstat"));
        e.hostMajorFaults = vmstat.getOrDefault("pgmajfault", -1L);
        e.hostSwapIn = vmstat.getOrDefault("pswpin", -1L);
        e.hostSwapOut = vmstat.getOrDefault("pswpout", -1L);
        e.hostDirectScan = vmstat.getOrDefault("pgscan_direct", -1L);
        readSteal(e);
        e.commit();
    }

    private static final int USER_HZ_MS = 10; // /proc/stat is in USER_HZ (100/s) on Linux
    private static long[] previousCpuSteal; // guarded by the JFR periodic thread

    /** /proc/stat: "cpu  user nice system idle iowait irq softirq steal ..." and the same per "cpuN" */
    private static void readSteal(Pressure e)
    {
        List<String> lines = readLines(Path.of("/proc/stat"));
        List<Long> perCpu = new ArrayList<>();
        for (String line : lines)
        {
            if (!line.startsWith("cpu")) continue;
            String[] f = line.trim().split("\\s+");
            if (f.length < 9) continue;
            long steal;
            try { steal = Long.parseLong(f[8]); } catch (NumberFormatException ex) { continue; }
            if (f[0].equals("cpu")) e.hostSteal = steal * USER_HZ_MS;
            else perCpu.add(steal);
        }
        long[] now = perCpu.stream().mapToLong(Long::longValue).toArray();
        long[] previous = previousCpuSteal;
        if (previous != null && previous.length == now.length)
        {
            long max = 0;
            for (int i = 0; i < now.length; i++) max = Math.max(max, now[i] - previous[i]);
            e.hostMaxCpuSteal = max * USER_HZ_MS;
        }
        previousCpuSteal = now;
    }

    private static final Path SELF_TASKS = Path.of("/proc/self/task");

    private static void emitThreads()
    {
        if (!Files.isDirectory(SELF_TASKS))
            return;
        Threads e = new Threads();
        e.local = localName();
        e.localAccordId = localAccordId();
        StringBuilder blocked = new StringBuilder();
        int listed = 0;
        try (java.nio.file.DirectoryStream<Path> tasks = Files.newDirectoryStream(SELF_TASKS))
        {
            for (Path task : tasks)
            {
                List<String> stat = readLines(task.resolve("stat"));
                if (stat.isEmpty()) continue;
                // "<tid> (<comm>) <state> ...": comm may contain spaces and parentheses
                String line = stat.get(0);
                int open = line.indexOf('('), close = line.lastIndexOf(')');
                if (open < 0 || close < 0 || close + 2 >= line.length()) continue;
                e.threads++;
                char state = line.charAt(close + 2);
                if (state == 'R') e.running++;
                else if (state == 'D')
                {
                    e.uninterruptible++;
                    if (listed++ < 32)
                    {
                        List<String> wchan = readLines(task.resolve("wchan"));
                        if (blocked.length() > 0) blocked.append(", ");
                        blocked.append(line, open + 1, close).append('(').append(task.getFileName()).append("):")
                               .append(wchan.isEmpty() ? "?" : wchan.get(0));
                    }
                }
            }
        }
        catch (IOException | RuntimeException ex)
        {
            return;
        }
        e.uninterruptibleThreads = blocked.toString();
        e.commit();
    }

    /** {some, full} total= of a PSI file ("some avg10=0.00 avg60=0.00 avg300=0.00 total=123"), -1 if absent */
    static long[] readPressure(Path file)
    {
        long[] v = { -1, -1 };
        for (String line : readLines(file))
        {
            int i = line.indexOf("total=");
            if (i < 0) continue;
            try
            {
                long total = Long.parseLong(line.substring(i + 6).trim());
                if (line.startsWith("some")) v[0] = total;
                else if (line.startsWith("full")) v[1] = total;
            }
            catch (NumberFormatException ignore)
            {
            }
        }
        return v;
    }

    /** "key value" lines (memory.events, memory.stat, /proc/vmstat) */
    static Map<String, Long> readKeyValues(Path file)
    {
        Map<String, Long> map = new HashMap<>();
        for (String line : readLines(file))
        {
            int i = line.indexOf(' ');
            if (i <= 0) continue;
            try
            {
                map.put(line.substring(0, i), Long.parseLong(line.substring(i + 1).trim()));
            }
            catch (NumberFormatException ignore)
            {
            }
        }
        return map;
    }

    private static long readLong(Path file)
    {
        List<String> lines = readLines(file);
        try
        {
            return lines.isEmpty() ? -1 : Long.parseLong(lines.get(0).trim());
        }
        catch (NumberFormatException e)
        {
            return -1; // e.g. "max"
        }
    }

    private static List<String> readLines(Path file)
    {
        try
        {
            return Files.isReadable(file) ? Files.readAllLines(file) : List.of();
        }
        catch (IOException e)
        {
            return List.of();
        }
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
