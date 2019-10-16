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
package org.apache.cassandra.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.concurrent.Accumulator;

public abstract class ResponseResolver
{
    protected static final Logger logger = LoggerFactory.getLogger(ResponseResolver.class);

    protected final Keyspace keyspace;
    protected final ReadCommand command;
    protected final ConsistencyLevel consistency;

    // Accumulator gives us non-blocking thread-safety with optimal algorithmic constraints
    protected final Accumulator<MessageIn<ReadResponse>> responses;
    /**
     * Ordinarily we expect to receive no more responses than we requested.
     * Since this would be a correctness error, we throw {@link IllegalStateException} if this occurs,
     * however in {@link StorageProxy.SinglePartitionReadLifecycle#maybeSendAdditionalDataRequests()}
     * we may submit additional requests to more nodes than we originally anticipated, and more than we need.
     * If the original and extra requests all respond, we may overflow, so we permit this without complaint in this
     * case only.
     */
    private boolean isResponseOverflowPermitted;

    public ResponseResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount)
    {
        this.keyspace = keyspace;
        this.command = command;
        this.consistency = consistency;
        this.responses = new Accumulator<>(maxResponseCount);
    }

    public abstract PartitionIterator getData();
    public abstract PartitionIterator resolve() throws DigestMismatchException;

    /**
     * Compares received responses, potentially triggering a digest mismatch (for a digest resolver) and read-repairs
     * (for a data resolver).
     * <p>
     * This is functionally equivalent to calling {@link #resolve()} and consuming the result, but can be slightly more
     * efficient in some case due to the fact that we don't care about the result itself. This is used when doing
     * asynchronous read-repairs.
     *
     * @throws DigestMismatchException if it's a digest resolver and the responses don't match.
     */
    public abstract void compareResponses() throws DigestMismatchException;

    public abstract boolean isDataPresent();

    public void setResponseOverflowPermitted()
    {
        isResponseOverflowPermitted = true;
    }

    public void preprocess(MessageIn<ReadResponse> message)
    {
        boolean added = responses.addIfNotFull(message);
        if (!added && !isResponseOverflowPermitted)
            throw new IllegalStateException();
    }

    public Iterable<MessageIn<ReadResponse>> getMessages()
    {
        return responses;
    }
}
