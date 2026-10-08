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

package org.apache.cassandra.net;

import java.util.Map;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.epoll.EpollTcpInfo;
import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Period;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * JFR events describing internode messaging, to tell apart the causes of late messages: Cassandra's outbound queue
 * ({@code pending*}), Netty/socket backpressure ({@code flushingBytes}, {@code writable}, {@link Backpressure}), TCP
 * (TCP_INFO: RTT, cwnd, retransmits, loss), and the receiving side ({@code InboundPeer.scheduled*}).
 *
 * <p>Registered by {@link org.apache.cassandra.utils.JfrDiagnostics} when {@code -Dcassandra.jfr.diagnostic_events=true};
 * like any JFR event they are only recorded while a recording enables them (e.g. async-profiler's {@code jfrsync} with
 * a .jfc that turns on the {@code cassandra.*} events). The periodic events read counters without synchronisation, so
 * a value may be slightly stale; counters are cumulative (diff consecutive events for rates).
 *
 * <p>TCP_INFO fields are only available with the native epoll transport (Linux), and are -1 otherwise. Times are as
 * Linux reports them: RTT and RTO in microseconds, {@code last*} in milliseconds ago. {@code tcpCaState} is the
 * congestion avoidance state: 0 Open, 1 Disorder, 2 CWR, 3 Recovery (fast retransmit), 4 Loss (RTO).
 */
public final class MessagingJfrEvents
{
    public static final boolean ENABLED = CassandraRelevantProperties.JFR_DIAGNOSTIC_EVENTS.getBoolean();

    private MessagingJfrEvents() {}

    @StackTrace(false)
    abstract static class ConnectionEvent extends Event
    {
        @Label("Local") @Description("This node's broadcast address")
        public String local;
        @Label("Local Accord Id")
        public int localAccordId = -1;
        @Label("Peer")
        public String peer;
        @Label("Peer Accord Id") @Description("The peer's Accord node id (the n<id> of DX spans), or -1")
        public int peerAccordId = -1;
        @Label("Connection Type") @Description("URGENT_MESSAGES, SMALL_MESSAGES or LARGE_MESSAGES")
        public String type;

        // TCP_INFO (Linux epoll transport only; -1 if unavailable)
        @Label("TCP State") public int tcpState = -1;
        @Label("TCP CA State") @Description("0 Open, 1 Disorder, 2 CWR, 3 Recovery, 4 Loss")
        public int tcpCaState = -1;
        @Label("TCP Consecutive RTOs") public int tcpRetransmits = -1;
        @Label("TCP Backoff") public int tcpBackoff = -1;
        @Label("TCP RTT") @Timespan(Timespan.MICROSECONDS) public long tcpRtt = -1;
        @Label("TCP RTT Variance") @Timespan(Timespan.MICROSECONDS) public long tcpRttVar = -1;
        @Label("TCP RTO") @Timespan(Timespan.MICROSECONDS) public long tcpRto = -1;
        @Label("TCP Send Congestion Window") @Description("in segments") public long tcpSndCwnd = -1;
        @Label("TCP Send Slow Start Threshold") public long tcpSndSsthresh = -1;
        @Label("TCP Send MSS") @DataAmount public long tcpSndMss = -1;
        @Label("TCP Unacked Segments") public long tcpUnacked = -1;
        @Label("TCP Lost Segments") public long tcpLost = -1;
        @Label("TCP Retransmitted Segments In Flight") public long tcpRetrans = -1;
        @Label("TCP Total Retransmits") public long tcpTotalRetrans = -1;
        @Label("TCP Receive Space") @DataAmount public long tcpRcvSpace = -1;
        @Label("TCP Receive RTT") @Timespan(Timespan.MICROSECONDS) public long tcpRcvRtt = -1;
        @Label("TCP Last Data Sent") @Timespan(Timespan.MILLISECONDS) public long tcpLastDataSent = -1;
        @Label("TCP Last Data Received") @Timespan(Timespan.MILLISECONDS) public long tcpLastDataRecv = -1;
        @Label("TCP Last Ack Received") @Timespan(Timespan.MILLISECONDS) public long tcpLastAckRecv = -1;
        @Label("Socket Send Buffer") @DataAmount @Description("SO_SNDBUF now (autotuned unless internode_socket_send_buffer_size is set)")
        public long soSndBuf = -1;
        @Label("Socket Receive Buffer") @DataAmount @Description("SO_RCVBUF now (autotuned unless internode_socket_receive_buffer_size is set)")
        public long soRcvBuf = -1;

        void tcp(Channel channel, EpollTcpInfo info)
        {
            if (!(channel instanceof EpollSocketChannel) || !channel.isOpen())
                return;
            try
            {
                EpollSocketChannel epoll = (EpollSocketChannel) channel;
                epoll.tcpInfo(info);
                soSndBuf = epoll.config().getSendBufferSize();
                soRcvBuf = epoll.config().getReceiveBufferSize();
            }
            catch (Throwable t)
            {
                return; // closed concurrently
            }
            tcpState = info.state();
            tcpCaState = info.caState();
            tcpRetransmits = info.retransmits();
            tcpBackoff = info.backoff();
            tcpRtt = info.rtt();
            tcpRttVar = info.rttvar();
            tcpRto = info.rto();
            tcpSndCwnd = info.sndCwnd();
            tcpSndSsthresh = info.sndSsthresh();
            tcpSndMss = info.sndMss();
            tcpUnacked = info.unacked();
            tcpLost = info.lost();
            tcpRetrans = info.retrans();
            tcpTotalRetrans = info.totalRetrans();
            tcpRcvSpace = info.rcvSpace();
            tcpRcvRtt = info.rcvRtt();
            tcpLastDataSent = info.lastDataSent();
            tcpLastDataRecv = info.lastDataRecv();
            tcpLastAckRecv = info.lastAckRecv();
        }
    }

    @Name("cassandra.net.Outbound")
    @Label("Outbound Connection")
    @Category({ "Cassandra", "Messaging" })
    @Description("Periodic state of each outbound internode connection: Cassandra's queue, Netty backpressure, TCP_INFO")
    @Period("1 s")
    public static class Outbound extends ConnectionEvent
    {
        @Label("Connected") public boolean connected;
        @Label("Writable") @Description("false while flushingBytes is above the high water mark, i.e. the socket is not draining")
        public boolean writable;
        @Label("Pending Count") @Description("messages queued in Cassandra, not yet written to Netty")
        public int pendingCount;
        @Label("Pending Bytes") @DataAmount public long pendingBytes;
        @Label("Flushing Bytes") @DataAmount @Description("written to Netty but not yet flushed to the socket, or -1")
        public long flushingBytes = -1;
        @Label("Submitted Count") public long submittedCount;
        @Label("Sent Count") public long sentCount;
        @Label("Sent Bytes") @DataAmount public long sentBytes;
        @Label("Overloaded Count") public long overloadedCount;
        @Label("Expired Count") public long expiredCount;
        @Label("Error Count") public long errorCount;
        @Label("Connection Attempts") public long connectionAttempts;
        @Label("Successful Connections") public long successfulConnections;
    }

    @Name("cassandra.net.Inbound")
    @Label("Inbound Connection")
    @Category({ "Cassandra", "Messaging" })
    @Description("Periodic state of each inbound internode connection: frames received and throttling, TCP_INFO")
    @Period("1 s")
    public static class Inbound extends ConnectionEvent
    {
        @Label("Received Count") public long receivedCount;
        @Label("Received Bytes") @DataAmount public long receivedBytes;
        @Label("Throttled Count") @Description("times reading stopped for lack of capacity") public long throttledCount;
        @Label("Throttled Time") @Timespan(Timespan.NANOSECONDS) public long throttledNanos;
        @Label("Corrupt Frames Recovered") public long corruptFramesRecovered;
        @Label("Corrupt Frames Unrecovered") public long corruptFramesUnrecovered;
    }

    @Name("cassandra.net.InboundPeer")
    @Label("Inbound Peer")
    @Category({ "Cassandra", "Messaging" })
    @Description("Periodic state of all inbound connections from one peer: messages received but not yet processed")
    @Period("1 s")
    @StackTrace(false)
    public static class InboundPeer extends Event
    {
        @Label("Local") public String local;
        @Label("Local Accord Id") public int localAccordId = -1;
        @Label("Peer") public String peer;
        @Label("Peer Accord Id") public int peerAccordId = -1;
        @Label("Connections") public int connections;
        @Label("Received Count") public long receivedCount;
        @Label("Scheduled Count") @Description("received, waiting to be processed") public long scheduledCount;
        @Label("Scheduled Bytes") @DataAmount public long scheduledBytes;
        @Label("Processed Count") public long processedCount;
        @Label("Using Capacity") @DataAmount public long usingCapacity;
        @Label("Using Endpoint Reserve") @DataAmount public long usingEndpointReserveCapacity;
        @Label("Throttled Count") public long throttledCount;
        @Label("Expired Count") public long expiredCount;
        @Label("Error Count") public long errorCount;
    }

    @Name("cassandra.net.Backpressure")
    @Label("Outbound Backpressure")
    @Category({ "Cassandra", "Messaging" })
    @Description("An outbound connection stopped writing because the socket was not draining (flushingBytes above the high "
                 + "water mark), until it drained below the low water mark. TCP_INFO is taken when it started.")
    public static class Backpressure extends ConnectionEvent
    {
        @Label("Flushing Bytes At Start") @DataAmount public long flushingBytes;
        @Label("Pending Count At Start") public int pendingCountAtStart;
        @Label("Pending Bytes At Start") @DataAmount public long pendingBytesAtStart;
        @Label("Pending Count At End") public int pendingCountAtEnd;
        @Label("Pending Bytes At End") @DataAmount public long pendingBytesAtEnd;
    }

    /** naming and Accord ids for the events, supplied by JfrDiagnostics */
    public static final class Context
    {
        final String local;
        final int localAccordId;
        final ToIntFunction<InetAddressAndPort> accordId;

        public Context(String local, int localAccordId, ToIntFunction<InetAddressAndPort> accordId)
        {
            this.local = local;
            this.localAccordId = localAccordId;
            this.accordId = accordId;
        }

        void fill(ConnectionEvent e, InetAddressAndPort peer, ConnectionType type)
        {
            e.local = local;
            e.localAccordId = localAccordId;
            e.peer = peer.getHostAddressAndPort();
            e.peerAccordId = accordId.applyAsInt(peer);
            e.type = type.name();
        }
    }

    public static void emitOutbound(MessagingService messaging, Context context)
    {
        EpollTcpInfo info = new EpollTcpInfo();
        for (Map.Entry<InetAddressAndPort, OutboundConnections> en : messaging.channelManagers.entrySet())
        {
            OutboundConnections pool = en.getValue();
            for (OutboundConnection connection : new OutboundConnection[]{ pool.urgent, pool.small, pool.large })
            {
                Outbound e = new Outbound();
                context.fill(e, en.getKey(), connection.type());
                e.connected = connection.isConnected();
                e.writable = connection.unsafeIsWritable();
                e.pendingCount = connection.pendingCount();
                e.pendingBytes = connection.pendingBytes();
                e.flushingBytes = connection.unsafeFlushingBytes();
                e.submittedCount = connection.submittedCount();
                e.sentCount = connection.sentCount();
                e.sentBytes = connection.sentBytes();
                e.overloadedCount = connection.overloadedCount();
                e.expiredCount = connection.expiredCount();
                e.errorCount = connection.errorCount();
                e.connectionAttempts = connection.connectionAttempts();
                e.successfulConnections = connection.successfulConnections();
                e.tcp(connection.unsafeChannel(), info);
                e.commit();
            }
        }
    }

    public static void emitInbound(MessagingService messaging, Context context)
    {
        EpollTcpInfo info = new EpollTcpInfo();
        for (Map.Entry<InetAddressAndPort, InboundMessageHandlers> en : messaging.messageHandlers.entrySet())
        {
            for (InboundMessageHandler handler : en.getValue().unsafeHandlers())
            {
                Inbound e = new Inbound();
                context.fill(e, en.getKey(), handler.type());
                e.receivedCount = handler.receivedCount;
                e.receivedBytes = handler.receivedBytes;
                e.throttledCount = handler.throttledCount;
                e.throttledNanos = handler.throttledNanos;
                e.corruptFramesRecovered = handler.corruptFramesRecovered;
                e.corruptFramesUnrecovered = handler.corruptFramesUnrecovered;
                e.tcp(handler.channel, info);
                e.commit();
            }
        }
    }

    public static void emitInboundPeers(MessagingService messaging, Context context)
    {
        for (Map.Entry<InetAddressAndPort, InboundMessageHandlers> en : messaging.messageHandlers.entrySet())
        {
            InboundMessageHandlers handlers = en.getValue();
            InboundPeer e = new InboundPeer();
            e.local = context.local;
            e.localAccordId = context.localAccordId;
            e.peer = en.getKey().getHostAddressAndPort();
            e.peerAccordId = context.accordId.applyAsInt(en.getKey());
            e.connections = handlers.count();
            e.receivedCount = handlers.receivedCount();
            e.scheduledCount = handlers.scheduledCount();
            e.scheduledBytes = handlers.scheduledBytes();
            e.processedCount = handlers.processedCount();
            e.usingCapacity = handlers.usingCapacity();
            e.usingEndpointReserveCapacity = handlers.usingEndpointReserveCapacity();
            e.throttledCount = handlers.throttledCount();
            e.expiredCount = handlers.expiredCount();
            e.errorCount = handlers.errorCount();
            e.commit();
        }
    }

    // -- Backpressure: begun and ended by OutboundConnection.EventLoopDelivery on its event loop

    private static volatile Supplier<Context> backpressureContext;

    /** how Backpressure events name the nodes (resolved when one starts), or null to stop recording them */
    public static void setBackpressureContext(Supplier<Context> context)
    {
        backpressureContext = context;
    }

    /** @return the started event, or null if it is not being recorded */
    static Backpressure beginBackpressure(OutboundConnection connection, long flushingBytes, Channel channel)
    {
        Supplier<Context> contexts = backpressureContext;
        if (contexts == null)
            return null;
        Backpressure e = new Backpressure();
        if (!e.isEnabled())
            return null;
        e.begin();
        Context context = contexts.get();
        context.fill(e, connection.settings().to, connection.type());
        e.flushingBytes = flushingBytes;
        e.pendingCountAtStart = connection.pendingCount();
        e.pendingBytesAtStart = connection.pendingBytes();
        e.tcp(channel, new EpollTcpInfo());
        return e;
    }

    static void endBackpressure(Backpressure e, OutboundConnection connection)
    {
        e.end();
        e.pendingCountAtEnd = connection.pendingCount();
        e.pendingBytesAtEnd = connection.pendingBytes();
        e.commit();
    }
}
