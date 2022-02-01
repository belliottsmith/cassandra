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

package org.apache.cassandra.cql3;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.RecomputingSupplier;

import static org.apache.cassandra.cql3.QueryProcessor.computeId;
import static org.junit.Assert.assertEquals;

public class QueryProcessorTest extends CQLTester
{
    private static final InetAddressAndPort host1 = getByName("127.0.0.1");
    private static final InetAddressAndPort host2 = getByName("127.0.0.2");

    private static final Map<InetAddressAndPort, UUID> hosts = ImmutableMap.of(host1, UUID.randomUUID(),
                                                                        host2, UUID.randomUUID());

    @Before
    public void before()
    {
        QueryProcessor.clearPreparedStatementsCache();
        QueryProcessor.instance.unsafeReset();
    }

    @Test
    public void testMixed4_0_0_20()
    {
        testMixedOldOld(false);
    }
    @Test
    public void testMixed3_0_19()
    {
        testMixedOldOld(true);
    }

    @Test
    public void testMixed4_0_0_40()
    {
        testMixedOldNew(false);
    }

    @Test
    public void testMixed3_0_24()
    {
        testMixedOldNew(true);
    }

    /**
     * new behaviour, but mixed 3.0 / 4.0
     */
    @Test
    public void testMixed3_0_24_29()
    {
        addNodeToGossip(host1, "4.0.0.46");
        addNodeToGossip(host2, "3.0.24.30");
        createTable("create table %s (id int primary key, x int)");
        String nonQualifiedQuery = "select * from "+currentTable()+" where id = 11";
        String qualifiedQuery = "select * from " + keyspace() + '.' + currentTable() + " where id = 11";
        assertPreparedIds(nonQualifiedQuery,
                          computeId(nonQualifiedQuery, keyspace()),
                          qualifiedQuery,
                          computeId(qualifiedQuery, null),
                          computeId(qualifiedQuery, null));
        upgradeAndAssertModern(nonQualifiedQuery, qualifiedQuery);
    }

    /**
     * old old behaviour is < 3.0.24 and < 4.0.0.37
     *
     * always append keyspace if set
     */
    public void testMixedOldOld(boolean threeo)
    {
        addNodeToGossip(host1, "4.0.0.46");
        addNodeToGossip(host2, threeo ? "3.0.19.55" : "4.0.0.20");
        createTable("create table %s (id int primary key, x int)");
        String nonQualifiedQuery = "select * from "+currentTable()+" where id = 11";
        String qualifiedQuery = "select * from " + keyspace() + '.' + currentTable() + " where id = 11";
        // always include keyspace if set, both qualified and non-qualified
        assertPreparedIds(nonQualifiedQuery,
                          computeId(nonQualifiedQuery, keyspace()),
                          qualifiedQuery,
                          computeId(qualifiedQuery, keyspace()),
                          computeId(qualifiedQuery, null));
        // and upgrade
        upgradeAndAssertModern(nonQualifiedQuery, qualifiedQuery);
    }

    /**
     * old new behaviour is < 3.0.24.29 and < 4.0.0.47
     * @param threeo
     */
    public void testMixedOldNew(boolean threeo)
    {
        addNodeToGossip(host1, "4.0.0.46");
        addNodeToGossip(host2, threeo ? "3.0.24.10" : "4.0.0.40");
        createTable("create table %s (id int primary key, x int)");
        String nonQualifiedQuery = "select * from "+currentTable()+" where id = 11";
        String qualifiedQuery = "select * from " + keyspace() + '.' + currentTable() + " where id = 11";
        // never include keyspace:
        assertPreparedIds(nonQualifiedQuery,
                          computeId(nonQualifiedQuery, null),
                          qualifiedQuery,
                          computeId(qualifiedQuery, null),
                          computeId(qualifiedQuery, null));

        upgradeAndAssertModern(nonQualifiedQuery, qualifiedQuery);
    }

    @Test
    public void testUpgraded()
    {
        addNodeToGossip(host1, "4.0.0.46");
        addNodeToGossip(host2, "4.0.0.46");
        createTable("create table %s (id int primary key, x int)");
        String nonQualifiedQuery = "select * from "+currentTable()+" where id = 11";
        String qualifiedQuery = "select * from " + keyspace() + '.' + currentTable() + " where id = 11";
        assertPreparedIds(nonQualifiedQuery,
                          computeId(nonQualifiedQuery, keyspace()),
                          qualifiedQuery,
                          computeId(qualifiedQuery, null),
                          computeId(qualifiedQuery, null));

    }

    private void assertPreparedIds(String nonQualifiedQuery,
                                   MD5Digest expectedNonQualifiedId,
                                   String fullyQualifiedQuery,
                                   MD5Digest expectedQualifiedIdKS,
                                   MD5Digest expectedQualifiedIdNoKS)
    {
        ClientState state = ClientState.forInternalCalls();

        // non-qualified query:
        state.setKeyspace(keyspace());
        ResultMessage.Prepared prepared = QueryProcessor.instance.prepare(nonQualifiedQuery, state);
        assertEquals(expectedNonQualifiedId, prepared.statementId);
        // fully qualified:
        prepared = QueryProcessor.instance.prepare(fullyQualifiedQuery, state);
        assertEquals(expectedQualifiedIdKS, prepared.statementId);

        state = ClientState.forInternalCalls();
        prepared = QueryProcessor.instance.prepare(fullyQualifiedQuery, state);
        assertEquals(expectedQualifiedIdNoKS, prepared.statementId);

        // unqualified again to catch the cached-case in QueryProcessor.prepare(..)
        state.setKeyspace(keyspace());
        prepared = QueryProcessor.instance.prepare(nonQualifiedQuery, state);
        assertEquals(expectedNonQualifiedId, prepared.statementId);
    }

    private void upgradeAndAssertModern(String nonQualifiedQuery,
                                        String fullyQualifiedQuery)
    {
        addNodeToGossip(host2, "4.0.0.46");
        assertPreparedIds(nonQualifiedQuery,
                          computeId(nonQualifiedQuery, keyspace()),
                          fullyQualifiedQuery,
                          computeId(fullyQualifiedQuery, null),
                          computeId(fullyQualifiedQuery, null));
    }

    private void addNodeToGossip(InetAddressAndPort endpoint, String version)
    {
        Gossiper.runInGossipStageBlocking(() ->
                                          {
                                              try
                                              {
                                                  Gossiper.instance.initializeNodeUnsafe(endpoint, hosts.get(endpoint), 1);
                                                  EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                                                  if (state.isAlive() && !Gossiper.instance.isDeadState(state))
                                                      Gossiper.instance.realMarkAlive(endpoint, state);

                                                  for (Pair<ApplicationState, VersionedValue> p : Arrays.asList(
                                                  Pair.create(ApplicationState.STATUS, factory().normal(Collections.singletonList(token("-1")))),

                                                  Pair.create(ApplicationState.NET_VERSION, factory().networkVersion(MessagingService.current_version)),
                                                  Pair.create(ApplicationState.RELEASE_VERSION, factory().releaseVersion(version))
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
                                                  throw new RuntimeException(e);
                                              }
                                          });
    }
    private static Token token(String str)
    {
        return DatabaseDescriptor.getPartitioner().getTokenFactory().fromString(str);
    }
    private static VersionedValue.VersionedValueFactory factory()
    {
        return new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());
    }

    private static InetAddressAndPort getByName(String name)
    {
        try
        {
            return InetAddressAndPort.getByName(name);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
