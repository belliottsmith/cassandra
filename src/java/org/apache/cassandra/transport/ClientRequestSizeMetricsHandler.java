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

package org.apache.cassandra.transport;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;

@ChannelHandler.Sharable
public class ClientRequestSizeMetricsHandler extends MessageToMessageCodec<ByteBuf, ByteBuf>
{
    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> results)
    {
        final long messageSize = buf.writerIndex() - buf.readerIndex();
        ClientRequestSizeMetrics.totalBytesRead.inc(messageSize);
        ClientRequestSizeMetrics.bytesReadPerQueryHistogram.update(messageSize);
        ClientRequestSizeMetrics.bytesReadPerQueryEstimatedHistogram.add(messageSize);
        buf.retain();
        results.add(buf);
    }

    @Override
    public void encode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> results)
    {
        final long messageSize = buf.writerIndex() - buf.readerIndex();
        ClientRequestSizeMetrics.totalBytesWritten.inc(messageSize);
        ClientRequestSizeMetrics.bytesWrittenPerQueryHistogram.update(messageSize);
        ClientRequestSizeMetrics.bytesWrittenPerQueryEstimatedHistogram.add(messageSize);
        buf.retain();
        results.add(buf);
    }
}
