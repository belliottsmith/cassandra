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

package org.apache.cassandra.service.accord;

import java.util.Map;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.ResponseContext;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * The context for replying to a request, recording when the request was received (by the monotonic clock), so that
 * the reply may report the time spent at this replica ({@link ParamType#RECEIPT_TO_REPLY_NANOS}).
 */
class ReceivedResponseContext implements ResponseContext
{
    private final ResponseContext request;
    private final long receivedAtNanos;

    /**
     * @param request the received request's context; should be constructed on receipt, i.e. on the IMMEDIATE stage
     */
    ReceivedResponseContext(ResponseContext request)
    {
        this.request = request;
        this.receivedAtNanos = nanoTime();
    }

    long elapsedNanos()
    {
        return Math.max(0, nanoTime() - receivedAtNanos);
    }

    @Override public long id() { return request.id(); }
    @Override public InetAddressAndPort from() { return request.from(); }
    @Override public Verb verb() { return request.verb(); }
    @Override public long expiresAtNanos() { return request.expiresAtNanos(); }
    @Override public boolean hasFlag(MessageFlag flag) { return request.hasFlag(flag); }
    @Override public Map<ParamType, Object> params() { return request.params(); }

    @Override
    public String toString()
    {
        return request.toString();
    }
}
