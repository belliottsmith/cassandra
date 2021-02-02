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

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

import static junit.framework.TestCase.fail;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertTrue;

public class RequestFailTest extends TestBaseImpl
{
    @Test
    public void testRequestBootstrapFail() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withInstanceInitializer(BBBootstrap::install)
                                           .start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, i int)");

            try
            {
                cluster.coordinator(1).execute("select * from " + KEYSPACE + ".tbl where id = 55", ConsistencyLevel.ALL);
                fail("Query should fail");
            }
            catch (Throwable t)
            {
                assertTrue(cluster.get(1).callOnInstance(() -> BBBootstrap.e instanceof IsBootstrappingException));
            }
        }
    }

    public static class BBBootstrap
    {
        public static volatile Exception e = null;
        static void install(ClassLoader cl, int nodeNumber)
        {
            ByteBuddy bb = new ByteBuddy();
            bb.redefine(StorageService.class)
              .method(named("isBootstrapMode"))
              .intercept(MethodDelegation.to(BBBootstrap.class))
              .make()
              .load(cl, ClassLoadingStrategy.Default.INJECTION);

            bb.rebase(StorageProxy.class)
              .method(named("logRequestException"))
              .intercept(MethodDelegation.to(BBBootstrap.class))
              .make()
              .load(cl, ClassLoadingStrategy.Default.INJECTION);

        }

        public static boolean isBootstrapMode()
        {
            return true;
        }

        /**
         * todo: remove this once we can grep the logs
         */
        public static void logRequestException(final Exception exception, final Collection<? extends ReadCommand> commands, @SuperCall Runnable zuper)
        {
            e = exception;
            zuper.run();
        }
    }
}
