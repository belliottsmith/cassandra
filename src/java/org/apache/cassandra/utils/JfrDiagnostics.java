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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        @Label("Waiting On Txn Or Key") @Description("tasks waiting for another task's transaction or key") public int waitingOnTxnOrKey;
        @Label("Running") public int running;
        @Label("Cache Entries") public int cacheSize;
        @Label("Cache Weighted Size") @DataAmount public long cacheWeightedSize;
        @Label("Cache Capacity") @DataAmount public long cacheCapacity;
        @Label("Max Working Capacity") @DataAmount
        @Description("the cache size at which loading is paused (capacity plus working_set_size per executor)")
        public long maxWorkingCapacity;
        @Label("Loading Paused") @Description("loads are paused because the cache is over its max working capacity")
        public boolean loadingPaused;
        @Label("Loading Pauses") @Description("times loading has been paused since startup (cumulative)")
        public long loadingPauses;
        @Label("Pending") @Description("tasks awaiting submission or completion under the executor lock")
        public int pending;
        @Label("Deferred Completions") @Description("completions deferred because a dispatch pass exceeded its completion budget")
        public int deferredCompletions;
        @Label("Deferred Completions Total") @Description("completions ever deferred (cumulative)")
        public long deferredCompletionsTotal;
        @Label("Loading Paused Time") @Timespan(Timespan.NANOSECONDS)
        @Description("total time loading has been paused since startup (cumulative, including any current pause)")
        public long loadingPausedTime;
    }

    @Name("cassandra.accord.Store")
    @Label("Accord Command Store")
    @Category({ "Cassandra", "Accord" })
    @Description("Periodic per command store queue state, taken under its executor's lock")
    @Period("1 s")
    @StackTrace(false)
    public static class Store extends Event
    {
        @Label("Local Accord Id") public int localAccordId = -1;
        @Label("Executor Id") public int executorId;
        @Label("Command Store Id") public int commandStoreId;
        @Label("Waiting") @Description("tasks waiting on another task's transaction or key") public int waiting;
        @Label("Loading") @Description("tasks loading their state or scanning ranges") public int loading;
        @Label("Queued") @Description("tasks ready to run, queued at the store") public int queued;
        @Label("Running") public int running;
        @Label("Oldest Waiting") @Timespan(Timespan.NANOSECONDS) public long oldestWaiting;
        @Label("Oldest Loading") @Timespan(Timespan.NANOSECONDS) public long oldestLoading;
        @Label("Oldest Queued") @Timespan(Timespan.NANOSECONDS) public long oldestQueued;
        @Label("Runs") @Description("tasks run by the store (cumulative)") public long runs;
        @Label("Run Time") @Timespan(Timespan.NANOSECONDS) @Description("time spent running the store's tasks (cumulative)")
        public long runTime;
    }

    @Name("cassandra.accord.WaitingTask")
    @Label("Accord Waiting Task")
    @Category({ "Cassandra", "Accord" })
    @Description("One of the oldest tasks of an executor waiting on another task's transaction or key, with the first cache " +
                 "entry it is blocked on (emitted with cassandra.accord.Store)")
    @StackTrace(false)
    public static class WaitingTask extends Event
    {
        @Label("Local Accord Id") public int localAccordId = -1;
        @Label("Executor Id") public int executorId;
        @Label("Command Store Id") public int commandStoreId;
        @Label("Task") public String task;
        @Label("Txn Id") public String txnId;
        @Label("State") public String state;
        @Label("Age") @Timespan(Timespan.NANOSECONDS) public long age;
        @Label("Position") public long position;
        @Label("Fifo") public boolean fifo;
        @Label("Refs") @Description("cache entries the task references") public int refs;
        @Label("Not Held") @Description("of those, entries it does not yet hold") public int notHeld;
        @Label("Blocked On") @Description("of those, entries locked by another task or on which it may not yet run") public int blockedOn;
        @Label("Entry") @Description("the first entry the task is blocked on (locked by another task, or not yet runnable on)") public String entry;
        @Label("Holder") @Description("the task holding (locking) that entry, if any") public String holder;
        @Label("Holder State") public String holderState;
        @Label("Holder Age") @Timespan(Timespan.NANOSECONDS) public long holderAge;
        @Label("Queue Size") @Description("tasks queued on the entry") public int queueSize;
        @Label("Fifo Size") public int fifoSize;
        @Label("Runnable Prefix") @Description("how many of the queued tasks may run") public int runnablePrefix;
        @Label("Index In Queue") @Description("this task's index among the queued tasks (-1 if absent)") public int indexInQueue;
        @Label("Queue Head") @Description("the first queued task: kind, txn, state, age, fifo") public String queueHead;
        @Label("Head In Blocking") @Description("1 if the queue head is a batched task with this entry among its blocking keys (processed first), 0 if not, -1 if n/a")
        public int headInBlocking;
        @Label("Head In Not Blocking") @Description("1 if the queue head is a batched task with this entry among its ready, non-blocking keys")
        public int headInNotBlocking;
        @Label("Head Blocking") @Description("the batched queue head's blocking keys") public int headBlocking;
        @Label("Head Not Blocking") @Description("the batched queue head's ready, non-blocking keys") public int headNotBlocking;
        @Label("Head Processed") @Description("keys the batched queue head has processed") public int headProcessed;
        @Label("Head Keys") @Description("keys of the batched queue head") public int headKeys;
    }

    // waiting tasks reported per executor per cassandra.accord.Store period, and the minimum wait to be reported
    private static final int WAITING_MAX = Integer.getInteger("cassandra.jfr.accord.waiting_max", 8);
    private static final long WAITING_MIN_NANOS = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Long.getLong("cassandra.jfr.accord.waiting_min_ms", 100));

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
            d.add(Store.class, JfrDiagnostics::emitStores);
            logger.info("Registered JFR diagnostic events (cassandra.net.*, cassandra.os.Tcp, cassandra.os.Pressure, cassandra.os.Threads, cassandra.accord.Executor, cassandra.accord.Store, cassandra.accord.WaitingTask)");
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

    private static final String[] TCP_KEYS = {
        "Tcp.CurrEstab", "Tcp.ActiveOpens", "Tcp.EstabResets", "Tcp.InSegs", "Tcp.OutSegs", "Tcp.RetransSegs",
        "Tcp.InErrs", "Tcp.OutRsts", "TcpExt.TCPTimeouts", "TcpExt.TCPLossProbes", "TcpExt.TCPLossProbeRecovery",
        "TcpExt.TCPFastRetrans", "TcpExt.TCPSlowStartRetrans", "TcpExt.TCPLostRetransmit", "TcpExt.TCPSackRecovery",
        "TcpExt.TCPSpuriousRTOs", "TcpExt.TCPRetransFail", "TcpExt.TCPBacklogDrop", "TcpExt.TCPRcvQDrop",
        "TcpExt.TCPOFOQueue", "TcpExt.PruneCalled", "TcpExt.TCPToZeroWindowAdv", "TcpExt.TCPWantZeroWindowAdv"
    };

    private static void emitTcp()
    {
        long[] v = new long[TCP_KEYS.length];
        Arrays.fill(v, -1);
        boolean any = readProcNet("/proc/net/snmp", TCP_KEYS, v);
        any |= readProcNet("/proc/net/netstat", TCP_KEYS, v);
        if (!any)
            return;
        Tcp e = new Tcp();
        e.local = localName();
        e.localAccordId = localAccordId();
        e.currEstab = v[0];
        e.activeOpens = v[1];
        e.estabResets = v[2];
        e.inSegs = v[3];
        e.outSegs = v[4];
        e.retransSegs = v[5];
        e.inErrs = v[6];
        e.outRsts = v[7];
        e.tcpTimeouts = v[8];
        e.tcpLossProbes = v[9];
        e.tcpLossProbeRecovery = v[10];
        e.tcpFastRetrans = v[11];
        e.tcpSlowStartRetrans = v[12];
        e.tcpLostRetransmit = v[13];
        e.tcpSackRecovery = v[14];
        e.tcpSpuriousRTOs = v[15];
        e.tcpRetransFail = v[16];
        e.tcpBacklogDrop = v[17];
        e.tcpRcvQDrop = v[18];
        e.tcpOFOQueue = v[19];
        e.pruneCalled = v[20];
        e.tcpToZeroWindowAdv = v[21];
        e.tcpWantZeroWindowAdv = v[22];
        e.commit();
    }

    private static final String CGROUP = "/sys/fs/cgroup/";
    private static final String[] MEMORY_EVENTS_KEYS = { "high", "max", "oom" };
    private static final String[] MEMORY_STAT_KEYS = { "pgmajfault", "file_dirty", "file_writeback" };
    private static final String[] VMSTAT_KEYS = { "pgmajfault", "pswpin", "pswpout", "pgscan_direct" };

    private static void emitPressure()
    {
        Pressure e = new Pressure();
        e.local = localName();
        e.localAccordId = localAccordId();
        long[] v = readPressure(CGROUP + "cpu.pressure");
        e.cpuSome = v[0]; e.cpuFull = v[1];
        v = readPressure(CGROUP + "memory.pressure");
        e.memorySome = v[0]; e.memoryFull = v[1];
        v = readPressure(CGROUP + "io.pressure");
        e.ioSome = v[0]; e.ioFull = v[1];
        e.memoryCurrent = readLong(CGROUP + "memory.current");
        e.swapCurrent = readLong(CGROUP + "memory.swap.current");
        v = readKeyValues(CGROUP + "memory.events", MEMORY_EVENTS_KEYS);
        e.memoryHigh = v[0]; e.memoryMax = v[1]; e.memoryOom = v[2];
        v = readKeyValues(CGROUP + "memory.stat", MEMORY_STAT_KEYS);
        e.cgroupMajorFaults = v[0]; e.cgroupFileDirty = v[1]; e.cgroupFileWriteback = v[2];
        v = readKeyValues("/proc/vmstat", VMSTAT_KEYS);
        e.hostMajorFaults = v[0]; e.hostSwapIn = v[1]; e.hostSwapOut = v[2]; e.hostDirectScan = v[3];
        readSteal(e);
        e.commit();
    }

    private static final int USER_HZ_MS = 10; // /proc/stat is in USER_HZ (100/s) on Linux
    // per-CPU steal of the previous and the current sample, swapped after each (guarded by the JFR periodic thread)
    private static long[] previousCpuSteal = new long[0], currentCpuSteal = new long[0];
    private static int previousCpuCount = -1;

    /** /proc/stat: "cpu  user nice system idle iowait irq softirq steal ..." and the same per "cpuN" */
    private static void readSteal(Pressure e)
    {
        ProcText t = ProcText.read("/proc/stat");
        if (t == null)
            return;
        int cpus = 0;
        for (int line = 0; line < t.length; line = t.nextLine(line))
        {
            int end = t.lineEnd(line);
            if (!t.startsWith(line, end, "cpu"))
                continue;
            // fields: label user nice system idle iowait irq softirq steal
            int f = t.skipSpaces(t.tokenEnd(line, end), end);
            for (int i = 1; i < 8 && f < end; i++)
                f = t.skipSpaces(t.tokenEnd(f, end), end);
            long steal = t.parseLong(f, end);
            if (steal < 0)
                continue;
            if (line + 3 < end && t.buf[line + 3] == ' ')
            {
                e.hostSteal = steal * USER_HZ_MS;
            }
            else
            {
                if (cpus == currentCpuSteal.length)
                    currentCpuSteal = Arrays.copyOf(currentCpuSteal, Math.max(8, cpus * 2));
                currentCpuSteal[cpus++] = steal;
            }
        }
        if (previousCpuCount == cpus)
        {
            long max = 0;
            for (int i = 0; i < cpus; i++) max = Math.max(max, currentCpuSteal[i] - previousCpuSteal[i]);
            e.hostMaxCpuSteal = max * USER_HZ_MS;
        }
        long[] swap = previousCpuSteal;
        previousCpuSteal = currentCpuSteal;
        currentCpuSteal = swap; // grown on demand above
        previousCpuCount = cpus;
    }

    private static final File SELF_TASKS = new File("/proc/self/task");
    private static final String SELF_TASKS_PREFIX = "/proc/self/task/";

    private static void emitThreads()
    {
        String[] tids = SELF_TASKS.list();
        if (tids == null)
            return;
        Threads e = new Threads();
        e.local = localName();
        e.localAccordId = localAccordId();
        StringBuilder blocked = new StringBuilder();
        StringBuilder path = new StringBuilder(SELF_TASKS_PREFIX.length() + 16).append(SELF_TASKS_PREFIX);
        int listed = 0;
        for (String tid : tids)
        {
            path.setLength(SELF_TASKS_PREFIX.length());
            ProcText t = ProcText.read(path.append(tid).append("/stat").toString());
            if (t == null) continue;
            // "<tid> (<comm>) <state> ...": comm may contain spaces and parentheses, so find the last ')'
            int end = t.lineEnd(0), open = t.indexOf(0, end, '('), close = t.lastIndexOf(0, end, ')');
            if (open < 0 || close < 0 || close + 2 >= end) continue;
            e.threads++;
            byte state = t.buf[close + 2];
            if (state == 'R') e.running++;
            else if (state == 'D')
            {
                e.uninterruptible++;
                if (listed++ < 32)
                {
                    String comm = t.string(open + 1, close);
                    path.setLength(SELF_TASKS_PREFIX.length());
                    ProcText w = ProcText.read(path.append(tid).append("/wchan").toString());
                    if (blocked.length() > 0) blocked.append(", ");
                    blocked.append(comm).append('(').append(tid).append("):")
                           .append(w == null || w.length == 0 ? "?" : w.string(0, w.lineEnd(0)));
                }
            }
        }
        e.uninterruptibleThreads = blocked.toString();
        e.commit();
    }

    /** {some, full} total= of a PSI file ("some avg10=0.00 avg60=0.00 avg300=0.00 total=123"), -1 if absent */
    static long[] readPressure(String file)
    {
        long[] v = { -1, -1 };
        ProcText t = ProcText.read(file);
        if (t == null)
            return v;
        for (int line = 0; line < t.length; line = t.nextLine(line))
        {
            int end = t.lineEnd(line);
            int i = t.indexOf(line, end, "total=");
            if (i < 0) continue;
            long total = t.parseLong(i + 6, end);
            if (t.startsWith(line, end, "some")) v[0] = total;
            else if (t.startsWith(line, end, "full")) v[1] = total;
        }
        return v;
    }

    /** the values of the given keys in a file of "key value" lines (memory.events, memory.stat, /proc/vmstat), -1 if absent */
    static long[] readKeyValues(String file, String[] keys)
    {
        long[] v = new long[keys.length];
        Arrays.fill(v, -1);
        ProcText t = ProcText.read(file);
        if (t == null)
            return v;
        for (int line = 0; line < t.length; line = t.nextLine(line))
        {
            int end = t.lineEnd(line), space = t.indexOf(line, end, ' ');
            if (space <= line) continue;
            for (int k = 0; k < keys.length; k++)
            {
                if (t.equals(line, space, keys[k]))
                {
                    v[k] = t.parseLong(space + 1, end);
                    break;
                }
            }
        }
        return v;
    }

    /** the number on the first line of a file, -1 if absent or not a number (e.g. "max") */
    private static long readLong(String file)
    {
        ProcText t = ProcText.read(file);
        return t == null ? -1 : t.parseLong(0, t.lineEnd(0));
    }

    /**
     * /proc/net/{snmp,netstat}: pairs of "<Proto>: <names...>" / "<Proto>: <values...>" lines. Sets values[k] for each
     * keys[k] ("<Proto>.<name>") found; returns whether the file could be read.
     */
    static boolean readProcNet(String file, String[] keys, long[] values)
    {
        ProcText t = ProcText.read(file);
        if (t == null)
            return false;
        int[] column = new int[keys.length];
        for (int names = 0; names < t.length; )
        {
            int namesEnd = t.lineEnd(names), vals = t.nextLine(names);
            if (vals >= t.length) break;
            int valsEnd = t.lineEnd(vals), colon = t.indexOf(names, namesEnd, ':');
            if (colon > names && t.startsWith(vals, valsEnd, t.buf, names, colon - names + 1))
            {
                // the column of each wanted key of this protocol in the names line
                Arrays.fill(column, -1);
                boolean anyWanted = false;
                for (int k = 0; k < keys.length; k++)
                {
                    String key = keys[k];
                    int dot = key.indexOf('.');
                    if (dot != colon - names || !t.equals(names, colon, key, 0, dot)) continue;
                    int col = 1;
                    for (int f = t.skipSpaces(colon + 1, namesEnd); f < namesEnd; col++)
                    {
                        int fe = t.tokenEnd(f, namesEnd);
                        if (t.equals(f, fe, key, dot + 1, key.length())) { column[k] = col; anyWanted = true; break; }
                        f = t.skipSpaces(fe, namesEnd);
                    }
                }
                if (anyWanted)
                {
                    int col = 1;
                    for (int f = t.skipSpaces(t.indexOf(vals, valsEnd, ':') + 1, valsEnd); f < valsEnd; col++)
                    {
                        int fe = t.tokenEnd(f, valsEnd);
                        for (int k = 0; k < keys.length; k++)
                            if (column[k] == col) values[k] = t.parseLong(f, fe);
                        f = t.skipSpaces(fe, valsEnd);
                    }
                }
            }
            names = t.nextLine(vals);
        }
        return true;
    }

    /**
     * A small text file from /proc or /sys/fs/cgroup, read into a buffer reused by the thread and parsed in place.
     * The periodic events read dozens of these ten times a second (one per JVM thread for cassandra.os.Threads), and
     * Files.readAllLines allocates tens of kilobytes of reader buffers and strings for each file.
     */
    static final class ProcText
    {
        private static final ThreadLocal<ProcText> REUSED = ThreadLocal.withInitial(ProcText::new);

        byte[] buf = new byte[16 << 10];
        int length;

        /** the file's contents in the calling thread's buffer, or null if it could not be read; valid until the next read */
        static ProcText read(String file)
        {
            ProcText t = REUSED.get();
            try (FileInputStream in = new FileInputStream(file))
            {
                int n = 0;
                while (true)
                {
                    if (n == t.buf.length)
                        t.buf = Arrays.copyOf(t.buf, n * 2);
                    int r = in.read(t.buf, n, t.buf.length - n);
                    if (r < 0) break;
                    n += r;
                }
                t.length = n;
                return t;
            }
            catch (IOException | SecurityException e)
            {
                return null;
            }
        }

        int lineEnd(int from)
        {
            int i = from;
            while (i < length && buf[i] != '\n') i++;
            return i;
        }

        int nextLine(int from)
        {
            return Math.min(length, lineEnd(from) + 1);
        }

        int skipSpaces(int i, int end)
        {
            while (i < end && (buf[i] == ' ' || buf[i] == '\t')) i++;
            return i;
        }

        int tokenEnd(int i, int end)
        {
            while (i < end && buf[i] != ' ' && buf[i] != '\t') i++;
            return i;
        }

        int indexOf(int from, int end, char c)
        {
            for (int i = from; i < end; i++)
                if (buf[i] == c) return i;
            return -1;
        }

        int lastIndexOf(int from, int end, char c)
        {
            for (int i = end - 1; i >= from; i--)
                if (buf[i] == c) return i;
            return -1;
        }

        int indexOf(int from, int end, String s)
        {
            for (int i = from; i + s.length() <= end; i++)
                if (equals(i, i + s.length(), s)) return i;
            return -1;
        }

        boolean startsWith(int from, int end, String s)
        {
            return from + s.length() <= end && equals(from, from + s.length(), s);
        }

        boolean startsWith(int from, int end, byte[] other, int otherFrom, int len)
        {
            if (from + len > end) return false;
            for (int i = 0; i < len; i++)
                if (buf[from + i] != other[otherFrom + i]) return false;
            return true;
        }

        /** whether buf[from, end) equals s (ASCII) */
        boolean equals(int from, int end, String s)
        {
            return equals(from, end, s, 0, s.length());
        }

        /** whether buf[from, end) equals s[sFrom, sEnd) (ASCII) */
        boolean equals(int from, int end, String s, int sFrom, int sEnd)
        {
            if (end - from != sEnd - sFrom) return false;
            for (int i = 0; i < end - from; i++)
                if (buf[from + i] != s.charAt(sFrom + i)) return false;
            return true;
        }

        /** the (optionally negative) integer at the start of buf[from, end) after any spaces, or -1 if there is none */
        long parseLong(int from, int end)
        {
            int i = skipSpaces(from, end);
            boolean negative = i < end && buf[i] == '-';
            if (negative) i++;
            if (i >= end || buf[i] < '0' || buf[i] > '9')
                return -1;
            long v = 0;
            while (i < end && buf[i] >= '0' && buf[i] <= '9')
                v = v * 10 + (buf[i++] - '0');
            return negative ? -v : v;
        }

        String string(int from, int end)
        {
            return new String(buf, from, end - from, StandardCharsets.ISO_8859_1);
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
            e.waitingOnTxnOrKey = executor.unsafeWaitingOnTxnOrKeyCount();
            e.running = executor.unsafeRunningCount();
            e.cacheSize = executor.size();
            e.cacheWeightedSize = executor.weightedSize();
            e.cacheCapacity = executor.capacity();
            e.maxWorkingCapacity = executor.unsafeMaxWorkingCapacity();
            e.loadingPaused = executor.unsafeIsLoadingPaused();
            e.loadingPauses = executor.unsafeLoadingPauses();
            e.loadingPausedTime = executor.unsafeLoadingPausedNanos();
            e.pending = executor.unsafePendingCount();
            e.deferredCompletions = executor.unsafeDeferredCompletionCount();
            e.deferredCompletionsTotal = executor.unsafeDeferredCompletionTotal();
            e.commit();
        }
    }

    private static void emitStores()
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
        int localId = localAccordId();
        boolean reportWaiting = jdk.jfr.FlightRecorder.isInitialized() && new WaitingTask().isEnabled();
        for (AccordExecutor executor : executors)
        {
            int executorId = executor.executorId();
            executor.diagnosticsSnapshot(WAITING_MIN_NANOS, reportWaiting ? WAITING_MAX : 0, d -> {
                Store e = new Store();
                e.localAccordId = localId;
                e.executorId = executorId;
                e.commandStoreId = d.commandStoreId;
                e.waiting = d.waiting;
                e.loading = d.loading;
                e.queued = d.queued;
                e.running = d.running;
                e.oldestWaiting = d.oldestWaitingNanos;
                e.oldestLoading = d.oldestLoadingNanos;
                e.oldestQueued = d.oldestQueuedNanos;
                e.runs = d.runs;
                e.runTime = d.runNanos;
                e.commit();
            }, w -> {
                WaitingTask e = new WaitingTask();
                e.localAccordId = localId;
                e.executorId = executorId;
                e.commandStoreId = w.commandStoreId;
                e.task = w.task;
                e.txnId = w.txnId;
                e.state = w.state;
                e.age = w.ageNanos;
                e.position = w.position;
                e.fifo = w.fifo;
                e.refs = w.refs;
                e.notHeld = w.notHeld;
                e.blockedOn = w.blockedOn;
                e.entry = w.entry;
                e.holder = w.holder;
                e.holderState = w.holderState;
                e.holderAge = w.holderAgeNanos;
                e.queueSize = w.queueSize;
                e.fifoSize = w.fifoSize;
                e.runnablePrefix = w.runnablePrefix;
                e.indexInQueue = w.indexInQueue;
                e.queueHead = w.queueHead;
                e.headInBlocking = w.headInBlocking;
                e.headInNotBlocking = w.headInNotBlocking;
                e.headBlocking = w.headBlocking;
                e.headNotBlocking = w.headNotBlocking;
                e.headProcessed = w.headProcessed;
                e.headKeys = w.headKeys;
                e.commit();
            });
        }
    }
}
