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

package org.apache.cassandra.distributed;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.IInvokableInstance;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.gms.Gossiper;

/**
 * A simple cluster supporting only the 'current' Cassandra version, offering easy access to the convenience methods
 * of IInvokableInstance on each node.
 */
public class Cluster extends AbstractCluster<IInvokableInstance> implements ICluster, AutoCloseable
{
    private Cluster(File root, Versions.Version version, List<InstanceConfig> configs, ClassLoader sharedClassLoader, BiConsumer<ClassLoader, Integer> instanceInit)
    {
        super(root, version, configs, sharedClassLoader, instanceInit);
    }

    protected IInvokableInstance newInstanceWrapper(int generation, Versions.Version version, InstanceConfig config)
    {
        return new Wrapper(generation, version, config);
    }

    public static Builder<IInvokableInstance, Cluster> build(int nodeCount)
    {
        return new Builder<>(nodeCount, Cluster::new);
    }

    public static Cluster create(int nodeCount, Consumer<IInstanceConfig> configUpdater) throws IOException
    {
        return build(nodeCount).withConfig(configUpdater).start();
    }

    public static Cluster create(int nodeCount) throws Throwable
    {
        return build(nodeCount).start();
    }

    /**
     * Will wait for a schema change AND agreement that occurs after it is created
     * (and precedes the invocation to waitForAgreement)
     *
     * Works by simply checking if all UUIDs agree after any schema version change event,
     * so long as the waitForAgreement method has been entered (indicating the change has
     * taken place on the coordinator)
     *
     * This could perhaps be made a little more robust, but this should more than suffice.
     */
    public class GossipAgreementMonitor extends ChangeAgreementMonitor
    {
        final int[] betweenNodes;
        public GossipAgreementMonitor(int[] betweenNodes)
        {
            super(new ArrayList<>(betweenNodes.length));
            this.betweenNodes = betweenNodes;
            for (int node : betweenNodes)
                cleanup.add(get(node).listen().gossip(this::signal));
        }

        protected boolean hasReachedAgreement()
        {
            // verify endpoint state for each node in the cluster, but only on those nominated nodes we want agreement for
            for (int i = 1 ; i <= size() ; ++i)
            {
                List<String> states = new ArrayList<>(betweenNodes.length);
                for (int j = 0 ; j < betweenNodes.length ; j++)
                {
                    states.add(get(betweenNodes[j])
                            .appliesOnInstance((InetAddress inet) -> Objects.toString(Gossiper.instance.getEndpointStateForEndpoint(inet)))
                            .apply(get(betweenNodes[j]).broadcastAddressAndPort().address));
                }
                if (1 != states.stream().distinct().count())
                    return false;
            }
            return true;
        }
    }

    // TODO: this should be a cross-version feature, but for now this will do
    public void waitForGossipAgreement(int ... betweenNodes)
    {
        get(1).sync(() -> {
            try (GossipAgreementMonitor monitor = new GossipAgreementMonitor(betweenNodes))
            {
                monitor.await();
            }
        }).run();
    }

}

