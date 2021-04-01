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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.BlacklistedDirectories;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.ActiveCompactions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CompactionDiskSpaceTest extends TestBaseImpl
{
    @Test
    public void testUnwritableDirs() throws IOException
    {
        try(Cluster cluster = init(Cluster.build(1)
                                          .withDataDirCount(3)
                                          .withInstanceInitializer(BB::install)
                                          .start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, x int) with compaction = {'class':'SizeTieredCompactionStrategy'}");
            cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, x) values (1,1)", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);
            cluster.setUncaughtExceptionsFilter((t) -> t.getMessage() != null  && t.getMessage().contains("Not enough space for compaction of distributed_test_keyspace.tbl"));
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                BB.estimatedRemaining.set(Long.MAX_VALUE);
                BlacklistedDirectories.maybeMarkUnwritable(cfs.getDirectories().getWriteableLocation(1).location);
                try
                {
                    cfs.forceMajorCompaction();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                BlacklistedDirectories.clearUnwritableUnsafe();
                try
                {
                    cfs.forceMajorCompaction();
                    fail("compaction should fail if there are no unwritable directories");
                }
                catch (Exception ignored)
                {
                    //ignore
                }
                BB.estimatedRemaining.set(0);
            });
        }
    }

    public static class BB
    {
        static final AtomicLong estimatedRemaining = new AtomicLong();
        public static void install(ClassLoader cl, Integer node)
        {
            new ByteBuddy().rebase(ActiveCompactions.class)
                           .method(named("estimatedRemainingWriteBytes"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static long estimatedRemainingWriteBytes()
        {
            return estimatedRemaining.get();
        }
    }
}
