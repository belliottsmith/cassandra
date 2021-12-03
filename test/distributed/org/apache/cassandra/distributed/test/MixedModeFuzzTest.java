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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.PreparedStatementHelper;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import mme.cassandraclient.shaded.com.google.common.base.Supplier;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class MixedModeFuzzTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ReprepareFuzzTest.class);

    @Test
    public void mixedModeFuzzTest() throws Throwable
    {
        try (ICluster<IInvokableInstance> c = builder().withNodes(2)
                                                       .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                       .withInstanceInitializer(PrepareBehaviour::oldNewBehaviour)
                                                       .start())
        {
            // Long string to make us invalidate caches occasionally
            String veryLongString = "very";
            for (int i = 0; i < 2; i++)
                veryLongString += veryLongString;
            final String qualified = "SELECT pk as " + veryLongString + "%d, ck as " + veryLongString + "%d FROM ks%d.tbl";
            final String unqualified = "SELECT pk as " + veryLongString + "%d, ck as " + veryLongString + "%d FROM tbl";

            int KEYSPACES = 3;
            final int STATEMENTS_PER_KS = 2;

            for (int i = 0; i < KEYSPACES; i++)
            {
                c.schemaChange(withKeyspace("CREATE KEYSPACE ks" + i + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
                c.schemaChange(withKeyspace("CREATE TABLE ks" + i + ".tbl (pk int, ck int, PRIMARY KEY (pk, ck));"));
                for (int j = 0; j < i; j++)
                    c.coordinator(1).execute("INSERT INTO ks" + i + ".tbl (pk, ck) VALUES (?, ?)", ConsistencyLevel.ALL, 1, j);
            }

            List<Thread> threads = new ArrayList<>();
            AtomicBoolean interrupt = new AtomicBoolean(false);
            AtomicReference<Throwable> thrown = new AtomicReference<>();

            int INFREQUENT_ACTION_COEF = 100;

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            for (int i = 0; i < 3; i++)
            {
                int seed = i;
                threads.add(new Thread(() -> {
                    com.datastax.driver.core.Cluster cluster = null;
                    Map<String, Session> sessions = new HashMap<>();
                    try
                    {
                        AtomicBoolean nodeWithFix = new AtomicBoolean(false);

                        Supplier<com.datastax.driver.core.Cluster> clusterSupplier = () -> {
                            return com.datastax.driver.core.Cluster.builder()
                                                                   .addContactPoint("127.0.0.1")
                                                                   .addContactPoint("127.0.0.2")
                                                                   .build();
                        };

                        AtomicBoolean allUpgraded = new AtomicBoolean(false);
                        Random rng = new Random(seed);
                        boolean reconnected = false;
                        Map<Pair<Integer, Integer>, PreparedStatement> qualifiedStatements = new HashMap<>();
                        Map<Pair<Integer, Integer>, PreparedStatement> unqualifiedStatements = new HashMap<>();

                        cluster = clusterSupplier.get();
                        for (int j = 0; j < KEYSPACES; j++)
                        {
                            String ks = "ks" + j;
                            sessions.put(ks, cluster.connect(ks));
                            Assert.assertEquals(sessions.get(ks).getLoggedKeyspace(), ks);
                        }

                        long firstVersionBump = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        long secondVersionBump = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        long reconnectAfter = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                        while (!interrupt.get() && (System.nanoTime() < deadline))
                        {
                            nodeWithFix.set(rng.nextBoolean());
                            final int ks = rng.nextInt(KEYSPACES);
                            final int statementIdx = rng.nextInt(STATEMENTS_PER_KS);
                            final Pair<Integer, Integer> statementId = Pair.create(ks, statementIdx);

                            int v = rng.nextInt(INFREQUENT_ACTION_COEF + 1);
                            Action[] pool;
                            if (v == INFREQUENT_ACTION_COEF)
                                pool = infrequent;
                            else
                                pool = frequent;

                            Action action = pool[rng.nextInt(pool.length)];
                            //logger.info(String.format("Executing %s on the node %s. ks %d", action, nodeWithFix.get() ? "1" : "2", ks));
                            switch (action)
                            {
                                case BUMP_VERSION:
                                    if (System.nanoTime() < firstVersionBump)
                                        break;

                                    c.stream().forEach(node -> node.runOnInstance(() -> {
                                        if (version.get().equals(INITIAL_VERSION))
                                        {
                                            CassandraVersion upgradeTo = QueryProcessor.SKIP_KEYSPACE_FOR_QUALIFIED_STATEMENTS_SINCE_40;
                                            while (!version.get().equals(upgradeTo))
                                                if (version.compareAndSet(INITIAL_VERSION, upgradeTo))
                                                {
                                                    logger.info("Bumped version to " + upgradeTo);
                                                    break;
                                                }
                                        }
                                        else if (version.get().equals(QueryProcessor.SKIP_KEYSPACE_FOR_QUALIFIED_STATEMENTS_SINCE_40) && System.nanoTime() > secondVersionBump)
                                        {
                                            CassandraVersion upgradeTo = QueryProcessor.USE_KEYSPACE_FOR_NON_QUALIFIED_STATEMENTS_SINCE_40;
                                            while (!version.get().equals(upgradeTo))
                                            {
                                                if (version.compareAndSet(QueryProcessor.SKIP_KEYSPACE_FOR_QUALIFIED_STATEMENTS_SINCE_40, upgradeTo))
                                                {
                                                    logger.info("Bumped version to " + upgradeTo);
                                                    allUpgraded.set(true);
                                                    break;
                                                }
                                            }
                                        }
                                    }));
                                    break;
                                case EXECUTE_QUALIFIED:
                                    if (!qualifiedStatements.containsKey(statementId))
                                        continue;

                                    try
                                    {
                                        int counter = 0;
                                        BoundStatement boundStatement = qualifiedStatements.get(statementId).bind();
                                        boundStatement.setHost(getHost(cluster, nodeWithFix.get()));

                                        for (Iterator<Object[]> iter = RowUtil.toObjects(sessions.get("ks" + ks).execute(boundStatement)); iter.hasNext(); )
                                        {
                                            Object[] current = iter.next();
                                            int v0 = (int) current[0];
                                            int v1 = (int) current[1];
                                            Assert.assertEquals(v0, 1);
                                            Assert.assertEquals(v1, counter++);
                                        }

                                        if (nodeWithFix.get())
                                            Assert.assertEquals(ks, counter);

                                    }
                                    catch (Throwable t)
                                    {
                                        if (t.getCause() != null &&
                                            t.getCause().getMessage().contains("Statement was prepared on keyspace"))
                                            continue;

                                        throw t;
                                    }

                                    break;

                                case EXECUTE_UNQUALIFIED:
                                    if (!unqualifiedStatements.containsKey(statementId))
                                        continue;

                                    try
                                    {
                                        BoundStatement boundStatement = unqualifiedStatements.get(statementId).bind();
                                        boundStatement.setHost(getHost(cluster, nodeWithFix.get()));
                                        int counter = 0;
                                        for (Iterator<Object[]> iter = RowUtil.toObjects(sessions.get("ks" + ks).execute(boundStatement)); iter.hasNext(); )
                                        {
                                            Object[] current = iter.next();
                                            int v0 = (int) current[0];
                                            int v1 = (int) current[1];
                                            Assert.assertEquals(v0, 1);
                                            Assert.assertEquals(v1, counter++);
                                        }

                                        if (nodeWithFix.get() && allUpgraded.get())
                                        {
                                            Assert.assertEquals(unqualifiedStatements.get(statementId).getQueryKeyspace() + " " + ks + " " + statementId,
                                                                ks,
                                                                counter);
                                        }
                                    }
                                    catch (Throwable t)
                                    {
                                        if (t.getMessage().contains("ID mismatch while trying to reprepare") ||
                                            (t.getCause() != null && t.getCause().getMessage().contains("Statement was prepared on keyspace")))
                                        {
                                            logger.info("Detected id mismatch, skipping as it is expected: ");
                                            continue;
                                        }

                                        throw t;
                                    }

                                    break;
                                case FORGET_PREPARED:
                                    Map<Pair<Integer, Integer>, PreparedStatement> toCleanup = rng.nextBoolean() ? qualifiedStatements : unqualifiedStatements;
                                    Set<Pair<Integer, Integer>> toDrop = new HashSet<>();
                                    for (Pair<Integer, Integer> e : toCleanup.keySet())
                                    {
                                        if (rng.nextBoolean())
                                            toDrop.add(e);
                                    }

                                    for (Pair<Integer, Integer> e : toDrop)
                                        toCleanup.remove(e);
                                    toDrop.clear();
                                    break;
                                case CLEAR_CACHES:
                                    if (!nodeWithFix.get() && !allUpgraded.get())
                                        continue;

                                    c.get(nodeWithFix.get() ? 1 : 2).runOnInstance(() -> {
                                        SystemKeyspace.loadPreparedStatements((id, query, keyspace) -> {
                                            if (rng.nextBoolean())
                                                QueryProcessor.instance.evictPrepared(id);
                                            return true;
                                        });
                                    });
                                    break;

                                case PREPARE_QUALIFIED:
                                    if (unqualifiedStatements.containsKey(statementId))
                                        continue;
                                    try
                                    {
                                        String qs = String.format(qualified, statementIdx, statementIdx, ks);
                                        String keyspace = "ks" + ks;
                                        PreparedStatement preparedQualified = sessions.get("ks" + ks).prepare(qs);

                                        // With prepared qualified, keyspace will be set to the keyspace of the statement when it was first executed
                                        if (allUpgraded.get())
                                            PreparedStatementHelper.assertHashWithoutKeyspace(preparedQualified, qs, keyspace);
                                        qualifiedStatements.put(statementId, preparedQualified);
                                    }
                                    catch (Throwable t)
                                    {
                                        throw t;
                                    }
                                    break;
                                case PREPARE_UNQUALIFIED:
                                    if (unqualifiedStatements.containsKey(statementId))
                                        continue;
                                    try
                                    {
                                        String qs = String.format(unqualified, statementIdx, statementIdx);
                                        // we don't know where it's going to be executed
                                        PreparedStatement preparedUnqalified = sessions.get("ks" + ks).prepare(qs);
                                        unqualifiedStatements.put(Pair.create(ks, statementIdx), preparedUnqalified);
                                    }
                                    catch (InvalidQueryException iqe)
                                    {
                                        if (!iqe.getMessage().contains("No keyspace has been"))
                                            throw iqe;
                                    }
                                    catch (Throwable t)
                                    {
                                        throw t;
                                    }
                                    break;
                                case BOUNCE_CLIENT:
                                    if (System.nanoTime() < reconnectAfter)
                                        break;

                                    if (!reconnected)
                                    {
                                        for (Session s : sessions.values())
                                            s.close();
                                        cluster.close();
                                        cluster = clusterSupplier.get();
                                        for (int j = 0; j < KEYSPACES; j++)
                                            sessions.put("ks" + j, cluster.connect("ks" + j));
                                        qualifiedStatements.clear();
                                        unqualifiedStatements.clear();
                                        reconnected = true;
                                    }


                                    break;
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        interrupt.set(true);
                        t.printStackTrace();
                        while (true)
                        {
                            Throwable seen = thrown.get();
                            Throwable merged = Throwables.merge(seen, t);
                            if (thrown.compareAndSet(seen, merged))
                                break;
                        }
                        throw t;
                    }
                    finally
                    {
                        logger.info("Exiting...");
                        if (cluster != null)
                            cluster.close();
                    }
                }));
            }

            for (Thread thread : threads)
                thread.start();

            for (Thread thread : threads)
                thread.join();

            if (thrown.get() != null)
                throw thrown.get();
        }
    }

    private enum Action
    {
        BUMP_VERSION,
        EXECUTE_QUALIFIED,
        EXECUTE_UNQUALIFIED,
        PREPARE_QUALIFIED,
        PREPARE_UNQUALIFIED,
        FORGET_PREPARED,
        CLEAR_CACHES,
        BOUNCE_CLIENT
    }

    private static Action[] frequent = new Action[]{ Action.EXECUTE_UNQUALIFIED,
                                                     Action.PREPARE_UNQUALIFIED,
                                                     Action.PREPARE_QUALIFIED,
                                                     Action.EXECUTE_QUALIFIED
    };

    private static Action[] infrequent = new Action[]{Action.BUMP_VERSION,
                                                      Action.BOUNCE_CLIENT,
                                                      Action.CLEAR_CACHES,
                                                      Action.FORGET_PREPARED
    };


    public static class PrepareBehaviour
    {
        static void oldNewBehaviour(ClassLoader cl, int nodeNumber)
        {
            DynamicType.Builder.MethodDefinition.ReceiverTypeDefinition<QueryProcessor> klass =
            new ByteBuddy().rebase(QueryProcessor.class)
                           .method(named("skipKeyspaceForQualifiedStatements"))
                           .intercept(MethodDelegation.to(MultiBehaviour.class))
                           .method(named("useKeyspaceForNonQualifiedStatements"))
                           .intercept(MethodDelegation.to(MultiBehaviour.class));

            if (nodeNumber == 2)
            {
                klass = klass.method(named("prepare").and(takesArguments(3)))
                             .intercept(MethodDelegation.to(MultiBehaviour.class));
            }

            klass.make()
                 .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }
    }

    private static CassandraVersion INITIAL_VERSION = new CassandraVersion("4.0.0.0");
    private static volatile AtomicReference<CassandraVersion> version = new AtomicReference<>(INITIAL_VERSION);

    public static class MultiBehaviour
    {
        private static final Object sync = new Object();
        private static volatile boolean skipKeyspaceForQualifiedStatements = false;
        private static volatile boolean useKeyspaceForNonQualifiedStatements = false;

        public static boolean skipKeyspaceForQualifiedStatements()
        {
            if (skipKeyspaceForQualifiedStatements)
                return true;

            synchronized (sync)
            {
                CassandraVersion minVersion = version.get();
                if (minVersion.compareTo(QueryProcessor.SKIP_KEYSPACE_FOR_QUALIFIED_STATEMENTS_SINCE_40) >= 0)
                    skipKeyspaceForQualifiedStatements = true;

                return skipKeyspaceForQualifiedStatements;
            }
        }

        public static boolean useKeyspaceForNonQualifiedStatements()
        {
            if (useKeyspaceForNonQualifiedStatements)
                return true;

            synchronized (sync)
            {
                CassandraVersion minVersion = version.get();
                if (minVersion.compareTo(QueryProcessor.SKIP_KEYSPACE_FOR_QUALIFIED_STATEMENTS_SINCE_40) >= 0)
                {
                    useKeyspaceForNonQualifiedStatements = true;
                    QueryProcessor.clearPreparedStatementsCache();
                    QueryProcessor.instance.preloadPreparedStatements();
                }

                return useKeyspaceForNonQualifiedStatements;
            }
        }

        public static ResultMessage.Prepared prepare(String queryString, ClientState clientState, Map<String, ByteBuffer> customPayload)
        {
            boolean skipKeyspaceForQualifiedStatements = skipKeyspaceForQualifiedStatements();
            boolean useKeyspaceForNonQualifiedStatements = useKeyspaceForNonQualifiedStatements();

            // Expected behaviour
            if (skipKeyspaceForQualifiedStatements && useKeyspaceForNonQualifiedStatements)
                return QueryProcessor.instance.prepare(queryString, clientState);

            ResultMessage.Prepared existing;

            if (skipKeyspaceForQualifiedStatements)
                existing = QueryProcessor.getStoredPreparedStatement(queryString, null);
            else
                existing = QueryProcessor.getStoredPreparedStatement(queryString, clientState.getRawKeyspace());

            if (existing != null)
                return existing;

            QueryHandler.Prepared prepared = QueryProcessor.parseAndPrepare(queryString, clientState, false);
            CQLStatement statement = prepared.statement;

            int boundTerms = statement.getBindVariables().size();
            if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
                throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));

            if (prepared.fullyQualified)
            {
                if (skipKeyspaceForQualifiedStatements)
                {
                    ResultMessage.Prepared qualifiedWithoutKeyspace = QueryProcessor.storePreparedStatement(queryString, null, prepared);
                    // when this version was deployed, we stored both statements, but removed one without keyspace
                    if (clientState.getRawKeyspace() != null)
                        QueryProcessor.storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);

                    return qualifiedWithoutKeyspace;
                }
                else
                {
                    // Initially, we would always use a keyspace
                    return QueryProcessor.storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);
                }
            }
            else
            {
                if (useKeyspaceForNonQualifiedStatements)
                {
                    Assert.fail("This shouldn't have happened: we should have exited to expected behaviour");
                    throw new RuntimeException();
                }
                else
                {
                    ResultMessage.Prepared qualifiedWithoutKeyspace = QueryProcessor.storePreparedStatement(queryString, null, prepared);
                    // when this version was deployed, we stored both statements, but removed one without keyspace
                    if (clientState.getRawKeyspace() != null)
                        QueryProcessor.storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);

                    return qualifiedWithoutKeyspace;
                }
            }
        }
    }

    public static Host getHost(Cluster cluster, boolean hostWithFix)
    {
        for (Iterator<Host> iter = cluster.getMetadata().getAllHosts().iterator(); iter.hasNext(); )
        {
            Host h = iter.next();
            if (hostWithFix)
            {
                if (h.getAddress().toString().contains("127.0.0.1"))
                    return h;
            }
            else
            {
                if (h.getAddress().toString().contains("127.0.0.2"))
                    return h;
            }
        }
        return null;
    }
}