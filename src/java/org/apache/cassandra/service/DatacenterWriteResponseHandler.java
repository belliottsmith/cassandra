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

import java.net.InetAddress;
import java.util.Collection;

import com.google.common.base.Predicate;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

/**
 * This class blocks for a quorum of responses _in the local datacenter only_ (CL.LOCAL_QUORUM).
 */
public class DatacenterWriteResponseHandler<T> extends WriteResponseHandler<T>
{
    public DatacenterWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                          Collection<InetAddress> pendingEndpoints,
                                          ConsistencyLevel consistencyLevel,
                                          KeyspaceMetrics keyspaceMetrics,
                                          AbstractReplicationStrategy replicationStrategySnapshot,
                                          Runnable callback,
                                          WriteType writeType,
                                          Predicate<InetAddress> isAlive)
    {
        super(naturalEndpoints, pendingEndpoints, consistencyLevel, keyspaceMetrics, replicationStrategySnapshot, callback, writeType, isAlive);
        assert consistencyLevel.isDatacenterLocal();
    }

    @Override
    public void response(MessageIn<T> message)
    {
        if (message == null || waitingFor(message.from))
        {
            super.response(message);
        }
        else
        {
            //WriteResponseHandler.response will call logResponseToIdealCLDelegate() so only do it if not calling WriteResponseHandler.response.
            //Must be last after all subclass processing
            logResponseToIdealCLDelegate(message);
        }
    }

    @Override
    protected int totalBlockFor()
    {
        // during bootstrap, include pending endpoints (only local here) in the count
        // or we may fail the consistency level guarantees (see #833, #8058)
        return consistencyLevel.blockFor(replicationStrategySnapshot) + consistencyLevel.countLocalEndpoints(pendingEndpoints);
    }

    @Override
    protected boolean waitingFor(InetAddress from)
    {
        return consistencyLevel.isLocal(from);
    }
}
