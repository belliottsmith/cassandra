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

package org.apache.cassandra.distributed.impl;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;

public class Listen implements IListen
{
    final Instance instance;
    public Listen(Instance instance)
    {
        this.instance = instance;
    }

    public Cancel schema(Runnable onChange)
    {
        final AtomicBoolean cancel = new AtomicBoolean();
        instance.isolatedExecutor.execute(() -> {
            UUID prev = instance.schemaVersion();
            while (true)
            {
                if (cancel.get())
                    return;

                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));

                UUID cur = instance.schemaVersion();
                if (!prev.equals(cur))
                    onChange.run();
                prev = cur;
            }
        });
        return () -> cancel.set(true);
    }

    static class GossipChangeSubscriber implements IEndpointStateChangeSubscriber
    {
        final Runnable onChange;
        GossipChangeSubscriber(Runnable onChange) 
        { 
            this.onChange = onChange;
            Gossiper.instance.register(this);
        }
        public void onJoin(InetAddress endpoint, EndpointState epState) { onChange.run(); }
        public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) { onChange.run(); }
        public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) { onChange.run(); }
        public void onAlive(InetAddress endpoint, EndpointState state) { onChange.run(); }
        public void onDead(InetAddress endpoint, EndpointState state) { onChange.run(); }
        public void onRemove(InetAddress endpoint) { onChange.run(); }
        public void onRestart(InetAddress endpoint, EndpointState state) { onChange.run(); }
    }

    public Cancel gossip(Runnable onChange)
    {
        Object unsubscribe = instance.appliesOnInstance(GossipChangeSubscriber::new).apply(onChange);
        return () -> instance.acceptsOnInstance((Object o) -> Gossiper.instance.unregister((IEndpointStateChangeSubscriber) o)).accept(unsubscribe);
    }
}
