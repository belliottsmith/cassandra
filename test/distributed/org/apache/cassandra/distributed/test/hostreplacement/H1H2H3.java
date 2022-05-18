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
package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Shared;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIPER_QUARANTINE_DELAY;

public class H1H2H3 extends TestBaseImpl
{
    static final int PEER_1 = 1;
    static final int PEER_2 = 3;
    static final int H1 = 2;
    static final int H2 = 4;
    static final int H3 = 5;

    @BeforeClass
    public static void setupState()
    {
        // attempt to disable FacClient eviction
        GOSSIPER_QUARANTINE_DELAY.setInt(Math.toIntExact(TimeUnit.MINUTES.toMillis(5)));
    }

    static Cluster createCluster() throws IOException
    {
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(3);
        return Cluster.build(3)
                      .withTokenSupplier(n -> n > 3 ? tokenSupplier.token(3) : tokenSupplier.token(n))
                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false)
                                        .set(Constants.KEY_DTEST_API_DISABLE_BOOTSTRAP_HACK, true))
                      .withInstanceInitializer(BBHelper::install)
                      .start();
    }

    static void attachBugExceptionHandler(Cluster cluster)
    {
        // There is a race condition where H3 gossips to PEER_1 and PEER_2 that it is replacing H2, and this happens-before
        // we shutdown due this failure (can't replace as H2 never came up, so we fail updating TokenMetadata)
        cluster.setUncaughtExceptionsFilter((num, t) -> {
            if (num != H3 && t.getMessage().startsWith("Node /127.0.0.5:7012 is trying to replace node /127.0.0.4:7012 with tokens [] with a different set of tokens"))
            {
                State.LATCH.countDown();
                return true;
            }
            return false;
        });
    }

    static IInvokableInstance haltH1(Cluster cluster)
    {
        IInvokableInstance h1 = cluster.get(H1);
        ClusterUtils.stopAbrupt(cluster, h1);

        // wait for h1 to be seen as down
        ClusterUtils.awaitNotAlive(cluster, h1);

        ClusterUtils.assertGossipInfo(cluster, h1, "STATUS_WITH_PORT", "NORMAL", "TOKENS", "<hidden>");
        return h1;
    }

    static IInvokableInstance failH2(Cluster cluster)
    {
        IInvokableInstance h1 = cluster.get(H1);
        IInvokableInstance h2 = ClusterUtils.addInstance(cluster, h1.config(), c -> c.set("auto_bootstrap", true));
        Assertions.assertThatThrownBy(() -> ClusterUtils.startHostReplacement(h1, h2));
        // just to make sure it properly shuts down
        ClusterUtils.stopAbrupt(cluster, h2);

        ClusterUtils.assertGossipInfo(cluster, h1.config().broadcastAddress(), "STATUS_WITH_PORT", "NORMAL", "TOKENS", "<hidden>");
        ClusterUtils.assertGossipInfo(cluster, h2.config().broadcastAddress(), "STATUS_WITH_PORT", "BOOT_REPLACE", "TOKENS", "<hidden>");

        return h2;
    }

    static IInvokableInstance h3Replace(Cluster cluster, IInvokableInstance toReplace, boolean consistentRangemovement)
    {
        IInvokableInstance h3 = ClusterUtils.addInstance(cluster, toReplace.config(), c -> c.set("auto_bootstrap", true));
        return ClusterUtils.startHostReplacement(toReplace, h3, (ignore, props) -> props.setProperty("cassandra.consistent.rangemovement", Boolean.toString(consistentRangemovement)));
    }

    @Shared
    public static final class State
    {
        public static final CountDownLatch LATCH = new CountDownLatch(1);
    }

    public static class BBHelper
    {
        public static void install(ClassLoader cl, int num)
        {
            if (num == H2)
            {
                new ByteBuddy().rebase(StorageService.class)
                               .method(named("finishJoiningRing"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
            else if (num == H3)
            {
                new ByteBuddy().rebase(StorageService.class)
                               .method(named("handleStateBootreplacing"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void finishJoiningRing(boolean didBootstrap, Collection<Token> tokens)
        {
            throw new RuntimeException("Failing Bootstrap");
        }

        @SuppressWarnings("unused")
        public static void handleStateBootreplacing(InetAddressAndPort newNode, String[] pieces, @SuperCall Runnable fn)
        {
            if (Thread.currentThread().getName().contains("GossipStage"))
            {
                fn.run();
            }
            else
            {
                // no-op to avoid exception
                try
                {
                    State.LATCH.await();
                    fn.run();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
