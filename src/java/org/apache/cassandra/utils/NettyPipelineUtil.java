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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.epoll.EpollTcpInfo;
import io.netty.handler.logging.ByteBufFormat;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

// Optional ChannelHandlers for debugging Netty pipelines
@SuppressWarnings("unused")
public class NettyPipelineUtil
{
    public static ChannelHandler[] getDebugHandlers()
    {
        return new ChannelHandler[]{
            new TcpInfoHandler(),
            new LoggingHandler(LogLevel.INFO, ByteBufFormat.SIMPLE)
        };
    };

    static class TcpInfoHandler extends ChannelDuplexHandler
    {
        static final Logger logger = LoggerFactory.getLogger(TcpInfoHandler.class);

        static void logTcpInfo(String label, ChannelHandlerContext ctx)
        {
            Channel channel = ctx.channel();
            if (channel instanceof EpollSocketChannel)
            {
                try {
                    EpollTcpInfo tcpInfo = ((EpollSocketChannel) channel).tcpInfo();
                    logger.info("Channel {} got EpollTcpInfo {} - {}", channel.id(), label, toDebugString(tcpInfo));
                }
                catch (Throwable t)
                {
                    logger.info("Could not log EpollTcpInfo for channel {} label {} due to throwable {}", channel, label, t.getMessage());
                }
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception
        {
            logTcpInfo("disconnect", ctx);
            super.disconnect(ctx, promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception
        {
            logTcpInfo("close", ctx);
            super.close(ctx, promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception
        {
            super.deregister(ctx, promise);
        }

        @Override
        public void read(ChannelHandlerContext ctx) throws Exception
        {
            logTcpInfo("read", ctx);
            super.read(ctx);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
        {
            logTcpInfo("write", ctx);
            super.write(ctx, msg, promise);
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception
        {
            logTcpInfo("flush", ctx);
            super.flush(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception
        {
            logTcpInfo("channelInactive", ctx);
            super.channelInactive(ctx);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
        {
            logTcpInfo("channelWritabilityChanged", ctx);
            super.channelWritabilityChanged(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            logTcpInfo("exceptionCaught", ctx);
            super.exceptionCaught(ctx, cause);
        }
    }

    static String toDebugString(EpollTcpInfo tcpInfo)
    {
        return "EpollTcpInfo{" +
               "state " + tcpInfo.state() +
               ", caState " + tcpInfo.caState() +
               ", retransmits " + tcpInfo.retransmits() +
               ", probes " + tcpInfo.probes() +
               ", backoff " + tcpInfo.backoff() +
               ", options " + tcpInfo.options() +
               ", sndWscale " + tcpInfo.sndWscale() +
               ", rcvWscale " + tcpInfo.rcvWscale() +
               ", rto " + tcpInfo.rto() +
               ", ato " + tcpInfo.ato() +
               ", sndMss " + tcpInfo.sndMss() +
               ", rcvMss " + tcpInfo.rcvMss() +
               ", unacked " + tcpInfo.unacked() +
               ", sacked " + tcpInfo.sacked() +
               ", lost " + tcpInfo.lost() +
               ", retrans " + tcpInfo.retrans() +
               ", fackets " + tcpInfo.fackets() +
               ", lastDataSent " + tcpInfo.lastDataSent() +
               ", lastAckSent " + tcpInfo.lastAckSent() +
               ", lastDataRecv " + tcpInfo.lastDataRecv() +
               ", lastAckRecv " + tcpInfo.lastAckRecv() +
               ", pmtu " + tcpInfo.pmtu() +
               ", rcvSsthresh " + tcpInfo.rcvSsthresh() +
               ", rtt " + tcpInfo.rtt() +
               ", rttvar " + tcpInfo.rttvar() +
               ", sndSsthresh " + tcpInfo.sndSsthresh() +
               ", sndCwnd " + tcpInfo.sndCwnd() +
               ", advmss " + tcpInfo.advmss() +
               ", reordering " + tcpInfo.reordering() +
               ", rcvRtt " + tcpInfo.rcvRtt() +
               ", rcvSpace " + tcpInfo.rcvSpace() +
               ", totalRetrans " + tcpInfo.totalRetrans() +
               '}';
    }
}
