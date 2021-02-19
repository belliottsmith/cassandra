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

package org.apache.cassandra.distributed.test;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.fail;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class NativeProtocolTest extends TestBaseImpl
{
    @Test
    public void withClientRequests() throws Throwable
    {
        try (ICluster ignored = init(builder().withNodes(3)
                                              .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                              .start()))
        {

            try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
                 Session session = cluster.connect())
            {
                session.execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
                session.execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) values (1,1,1);");
                Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl;").setConsistencyLevel(ConsistencyLevel.ALL);
                final ResultSet resultSet = session.execute(select);
                assertRows(RowUtil.toObjects(resultSet), row(1, 1, 1));
                Assert.assertEquals(3, cluster.getMetadata().getAllHosts().size());
            }
        }
    }

    @Test
    public void withCounters() throws Throwable
    {
        try (ICluster ignored = init(builder().withNodes(3)
                                              .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                              .start()))
        {
            final com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
            Session session = cluster.connect();
            session.execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck counter, PRIMARY KEY (pk));");
            session.execute("UPDATE " + KEYSPACE + ".tbl set ck = ck + 10 where pk = 1;");
            Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl;").setConsistencyLevel(ConsistencyLevel.ALL);
            final ResultSet resultSet = session.execute(select);
            assertRows(RowUtil.toObjects(resultSet), row(1, 10L));
            Assert.assertEquals(3, cluster.getMetadata().getAllHosts().size());
            session.close();
            cluster.close();
        }
    }

    @Test
    public void testReprepareNewBehaviour() throws Throwable
    {
        testReprepare(PrepareBehaviour::newBehaviour, true, false);
        testReprepare(PrepareBehaviour::newBehaviour, false, false);
    }

    @Test
    public void testReprepareMixedVersion() throws Throwable
    {
        testReprepare(PrepareBehaviour::oldBehaviour, true, true);
        testReprepare(PrepareBehaviour::oldBehaviour, false, false);
    }

    public void testReprepare(BiConsumer<ClassLoader, Integer> instanceInitializer, boolean withUse, boolean skipBrokenBehaviours) throws Throwable
    {
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(2)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .withInstanceInitializer(instanceInitializer)
                                                            .start()))
        {
            ForceHostLoadBalancingPolicy lbp = new ForceHostLoadBalancingPolicy();
            c.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            // 1 has old behaviour
            for (int firstContact : new int[]{ 1, 2 })
            {
                try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                                .addContactPoint("127.0.0.1")
                                                                                                .addContactPoint("127.0.0.2")
                                                                                                .withLoadBalancingPolicy(lbp)
                                                                                                .build();
                     Session session = cluster.connect())
                {
                    lbp.setPrimary(firstContact);
                    final PreparedStatement select = session.prepare(withKeyspace("SELECT * FROM %s.tbl"));
                    session.execute(select.bind());

                    c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));

                    lbp.setPrimary(firstContact == 1 ? 2 : 1);

                    if (withUse)
                        session.execute(withKeyspace("USE %s"));

                    // Re-preparing on the node
                    if (!skipBrokenBehaviours && firstContact == 1)
                        session.execute(select.bind());

                    c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));

                    lbp.setPrimary(firstContact);

                    // Re-preparing on the node with old behaviour will break no matter where the statement was initially prepared
                    if (!skipBrokenBehaviours)
                        session.execute(select.bind());

                    c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));
                }
            }
        }
    }

    @Test
    public void testReprepareMixedVersionWithoutReset() throws Throwable
    {
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(2)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .withInstanceInitializer(PrepareBehaviour::oldBehaviour)
                                                            .start()))
        {
            ForceHostLoadBalancingPolicy lbp = new ForceHostLoadBalancingPolicy();
            c.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            // 1 has old behaviour
            for (int firstContact : new int[]{ 1, 2 })
            {
                for (boolean withUse : new boolean[]{ true, false })
                {
                    for (boolean clearBetweenExecutions : new boolean[]{ true, false })
                    {
                        try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                                        .addContactPoint("127.0.0.1")
                                                                                                        .addContactPoint("127.0.0.2")
                                                                                                        .withLoadBalancingPolicy(lbp)
                                                                                                        .build();
                             Session session = cluster.connect())
                        {
                            if (withUse)
                                session.execute(withKeyspace("USE %s"));

                            lbp.setPrimary(firstContact);
                            final PreparedStatement select = session.prepare(withKeyspace("SELECT * FROM %s.tbl"));
                            session.execute(select.bind());

                            if (clearBetweenExecutions)
                                c.get(2).runOnInstance(QueryProcessor::clearPreparedStatementsCache);
                            lbp.setPrimary(firstContact == 1 ? 2 : 1);
                            session.execute(select.bind());

                            if (clearBetweenExecutions)
                                c.get(2).runOnInstance(QueryProcessor::clearPreparedStatementsCache);
                            lbp.setPrimary(firstContact);
                            session.execute(select.bind());

                            c.get(2).runOnInstance(QueryProcessor::clearPreparedStatementsCache);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testReprepareTwoKeyspacesNewBehaviour() throws Throwable
    {
        testReprepareTwoKeyspaces(PrepareBehaviour::newBehaviour);
    }

    @Test
    public void testReprepareTwoKeyspacesMixedVersion() throws Throwable
    {
        testReprepareTwoKeyspaces(PrepareBehaviour::oldBehaviour);
    }

    public void testReprepareTwoKeyspaces(BiConsumer<ClassLoader, Integer> instanceInitializer) throws Throwable
    {
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(2)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .withInstanceInitializer(instanceInitializer)
                                                            .start()))
        {
            c.schemaChange(withKeyspace("CREATE KEYSPACE %s2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            c.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            ForceHostLoadBalancingPolicy lbp = new ForceHostLoadBalancingPolicy();

            for (int firstContact : new int[]{ 1, 2 })
                try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                                .addContactPoint("127.0.0.1")
                                                                                                .addContactPoint("127.0.0.2")
                                                                                                .withLoadBalancingPolicy(lbp)
                                                                                                .build();
                     Session session = cluster.connect())
                {
                    {
                        session.execute(withKeyspace("USE %s"));
                        c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));

                        lbp.setPrimary(firstContact);
                        final PreparedStatement select = session.prepare(withKeyspace("SELECT * FROM %s.tbl"));
                        session.execute(select.bind());

                        c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));

                        lbp.setPrimary(firstContact == 1 ? 2 : 1);
                        session.execute(withKeyspace("USE %s2"));
                        try
                        {
                            session.execute(select.bind());
                        }
                        catch (DriverInternalError e)
                        {
                            Assert.assertTrue(e.getCause().getMessage().contains("can't execute it on"));
                            continue;
                        }
                        fail("Should have thrown");
                    }
                }
        }
    }

    public static class PrepareBehaviour
    {
        static void newBehaviour(ClassLoader cl, int nodeNumber)
        {
            // do nothing
        }

        static void oldBehaviour(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 1)
            {
                new ByteBuddy().rebase(QueryProcessor.class) // note that we need to `rebase` when we use @SuperCall
                               .method(named("prepare").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(PrepareBehaviour.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static ResultMessage.Prepared prepare(String queryString, QueryState queryState)
        {
            ClientState clientState = queryState.getClientState();
            ResultMessage.Prepared existing = QueryProcessor.getStoredPreparedStatement(queryString, clientState.getRawKeyspace(), false);
            if (existing != null)
                return existing;

            ParsedStatement.Prepared prepared = QueryProcessor.getStatement(queryString, clientState);
            int boundTerms = prepared.statement.getBoundTerms();
            if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
                throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));
            assert boundTerms == prepared.boundNames.size();

            return QueryProcessor.storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared, false);
        }

    }

    private static class ForceHostLoadBalancingPolicy implements LoadBalancingPolicy {

        private final List<Host> hosts = new CopyOnWriteArrayList<Host>();
        private int currentPrimary = 0;

        public void setPrimary(int idx) {
            this.currentPrimary = idx - 1; // arrays are 0-based
        }

        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
            this.hosts.addAll(hosts);
            this.hosts.sort(Comparator.comparingInt(h -> h.getAddress().getAddress()[3]));
        }

        @Override
        public HostDistance distance(Host host) {
            return HostDistance.LOCAL;
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            if (hosts.isEmpty()) return Collections.emptyIterator();
            return Iterators.singletonIterator(hosts.get(currentPrimary));
        }

        @Override
        public void onAdd(Host host) {
            onUp(host);
        }

        @Override
        public void onUp(Host host) {
            hosts.add(host);
        }

        @Override
        public void onDown(Host host) {
            // no-op
        }

        @Override
        public void onRemove(Host host) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }
}