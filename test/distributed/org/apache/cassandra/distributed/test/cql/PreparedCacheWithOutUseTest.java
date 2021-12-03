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
package org.apache.cassandra.distributed.test.cql;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.RecomputingSupplier;

public class PreparedCacheWithOutUseTest extends TestBaseImpl
{
    @Test
    public void prepareKeyspace() throws IOException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.values()))
                                      .start();
             com.datastax.driver.core.Cluster driver = JavaDriverUtils.create(cluster);
             Session session = driver.connect())
        {
            // add a old version to gossip..
            IInvokableInstance node = cluster.get(1);
            addOldNodeToGossip(node, node.getMessagingVersion());

            session.prepare("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
            session.prepare("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
        }
    }

    private static void addOldNodeToGossip(IInvokableInstance node, int netVersion)
    {
        node.runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
            try
            {
                InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.0.2");
                Gossiper.instance.initializeNodeUnsafe(endpoint, UUID.randomUUID(), netVersion, 1);
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (state.isAlive() && !Gossiper.instance.isDeadState(state))
                    Gossiper.instance.realMarkAlive(endpoint, state);

                for (Pair<ApplicationState, VersionedValue> p : Arrays.asList(
                    Pair.create(ApplicationState.STATUS, factory().normal(Arrays.asList(token("-1")))),
                    Pair.create(ApplicationState.STATUS_WITH_PORT, factory().normal(Arrays.asList(token("-1")))),

                    Pair.create(ApplicationState.NET_VERSION, factory().networkVersion(netVersion)),
                    Pair.create(ApplicationState.RELEASE_VERSION, factory().releaseVersion("4.0.0.36"))
                ))
                {
                    ApplicationState as = p.left;
                    VersionedValue vv = p.right;
                    state.addApplicationState(as, vv);
                    StorageService.instance.onChange(endpoint, as, vv);
                }

                // clear cache
                Field field = Gossiper.class.getDeclaredField("minVersionSupplier");
                field.setAccessible(true);
                RecomputingSupplier<CassandraVersion> cache = (RecomputingSupplier<CassandraVersion>) field.get(Gossiper.instance);
                cache.recompute();
            }
            catch (Exception e)
            {
                if (e instanceof RuntimeException)
                    throw (RuntimeException) e;
                throw new RuntimeException(e);
            }
        }));
    }

    private static Token token(String str)
    {
        return DatabaseDescriptor.getPartitioner().getTokenFactory().fromString(str);
    }

    private static VersionedValue.VersionedValueFactory factory()
    {
        return new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());
    }
}
