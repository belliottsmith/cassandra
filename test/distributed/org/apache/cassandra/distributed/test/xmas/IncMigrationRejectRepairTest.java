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

package org.apache.cassandra.distributed.test.xmas;

import java.io.IOException;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertTrue;

public class IncMigrationRejectRepairTest extends TestBaseImpl
{
    @Test
    public void testReject() throws IOException
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.set("enable_christmas_patch", true)
                                                                      .set("incremental_updates_last_repaired",false)
                                                                      .with(NETWORK, GOSSIP))
                                          .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);
            cluster.forEach(i -> i.flush(KEYSPACE));

            // setting repairedAt to 2 seconds ago to avoid having to sleep before the repairs below
            cluster.get(1).runOnInstance(() -> ActiveRepairService.instance.setIncRepairedAtUnsafe(System.currentTimeMillis() - 2000, true, false));

            // reject inc repairs
            cluster.get(1).nodetoolResult("repair", "-pr", KEYSPACE, "tbl").asserts().failure();
            cluster.get(2).nodetoolResult("repair", "-pr", KEYSPACE, "tbl").asserts().failure();

            // preview/validation repair
            cluster.get(1).nodetoolResult("repair", "-pr", "-vd", KEYSPACE, "tbl").asserts().failure();
            cluster.get(2).nodetoolResult("repair", "-pr", "-vd", KEYSPACE, "tbl").asserts().failure();

            // fully repair to allow inc repairs to continue
            cluster.get(1).nodetoolResult("repair", "-full", "-pr", KEYSPACE, "tbl").asserts().success();
            cluster.get(1).nodetoolResult("repair", "-pr", KEYSPACE, "tbl").asserts().success();

            cluster.get(2).nodetoolResult("repair", "-full", "-pr", KEYSPACE, "tbl").asserts().success();
            cluster.get(2).nodetoolResult("repair", "-pr", KEYSPACE, "tbl").asserts().success();
        }
    }

    @Test
    public void testClearIncRepairMigrations() throws IOException
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.set("enable_christmas_patch", true)
                                                                      .set("incremental_updates_last_repaired",false)
                                                                      .with(NETWORK, GOSSIP))
                                          .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key)"));
            for (int i = 0; i < 100; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl2 (id) values (?)"), ConsistencyLevel.ALL, i);
            }
            cluster.forEach(i -> i.flush(KEYSPACE));

            // setting repairedAt to 2 seconds ago to avoid having to sleep before the repairs below
            cluster.get(1).runOnInstance(() -> ActiveRepairService.instance.setIncRepairedAtUnsafe(System.currentTimeMillis() - 2000, true, false));

            cluster.get(1).runOnInstance(() -> {
                ActiveRepairService.clearIncRepairMigrations();
                assertEquals(2, SystemKeyspace.getIncRepairMigrations().stream().filter(irm -> irm.keyspace.equals(KEYSPACE)).count());
            });

            cluster.get(1).nodetoolResult("repair", "-full", KEYSPACE, "tbl").asserts().success();

            cluster.get(1).runOnInstance(() -> {
                ActiveRepairService.clearIncRepairMigrations();
                assertEquals(1, SystemKeyspace.getIncRepairMigrations().stream().filter(irm -> irm.keyspace.equals(KEYSPACE)).count());
            });

            cluster.get(1).nodetoolResult("repair", "-full", "-pr", KEYSPACE, "tbl2").asserts().success(); //note "-pr"
            cluster.get(1).runOnInstance(() -> {
                ActiveRepairService.clearIncRepairMigrations();
                // should still have the inc repair migration since we repaired node1 with -pr, that only covers one of the two local ranges
                assertEquals(1, SystemKeyspace.getIncRepairMigrations().stream().filter(irm -> irm.keyspace.equals(KEYSPACE)).count());
            });

            cluster.get(2).nodetoolResult("repair", "-full", "-pr", KEYSPACE, "tbl2").asserts().success(); //note "-pr"
            cluster.get(1).runOnInstance(() -> {
                ActiveRepairService.clearIncRepairMigrations();
                // and now the migration should be gone, -pr on node2
                assertEquals(0, SystemKeyspace.getIncRepairMigrations().stream().filter(irm -> irm.keyspace.equals(KEYSPACE)).count());
            });
        }
    }

    @Test
    public void testMarkFailure() throws IOException
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.with(NETWORK, GOSSIP))
                                          .withInstanceInitializer(BB::install)
                                          .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);
            cluster.forEach(i -> i.flush(KEYSPACE));

            cluster.get(1).runOnInstance(() -> {
                try
                {
                    ActiveRepairService.instance.startIncRepairMigrationUnsafe(true);
                    fail("should have failed");
                }
                catch (Exception e)
                {
                    assertTrue(e.getMessage().contains("Failed setting repairedAt on some sstables"));
                }
            });
        }
    }

    public static class BB
    {
        public static void install(ClassLoader cl, int instance)
        {
            new ByteBuddy().redefine(MetadataSerializer.class)
                           .method(named("mutateRepairMetadata"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void mutateRepairMetadata(Descriptor descriptor, long newRepairedAt, TimeUUID newPendingRepair, boolean isTransient) throws IOException
        {
            throw new IOException("test");
        }
    }
}
