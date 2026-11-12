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

package org.apache.cassandra.service.accord;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.coordinate.tracking.DurabilityTracker;
import accord.impl.SizeOfIntersectionSorter;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.accord.topology.AccordTopology;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.transformations.AccordMarkHardRemoved;
import org.apache.cassandra.tcm.transformations.AccordMarkStale;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTopologyUtils.token;

/**
 * Stale and hard removed replicas are both excluded from durability coordination, so TCM must refuse to mark replicas
 * stale or hard removed if together they would exceed the failures a shard tolerates; otherwise every sync point over
 * the shard fails constructing its DurabilityTracker. A stale replica may always be hard removed.
 */
public class AccordStaleHardRemovedTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void markStaleCountsHardRemoved()
    {
        ClusterMetadata metadata = cluster(5);
        metadata = accept(metadata, stale(4));
        metadata = accept(metadata, hardRemoved(5));
        reject(metadata, stale(3));
    }

    @Test
    public void markHardRemovedCountsStale()
    {
        ClusterMetadata metadata = cluster(5);
        metadata = accept(metadata, stale(3, 4));
        reject(metadata, hardRemoved(5));
    }

    @Test
    public void staleReplicaMayBeHardRemoved()
    {
        ClusterMetadata metadata = cluster(5);
        metadata = accept(metadata, stale(3, 4));
        accept(metadata, hardRemoved(4));
    }

    @Test
    public void hardRemovalRaisesToleratedFailures()
    {
        // an even topology that has lost its majority may hard remove replicas to make progress
        Assert.assertEquals(2, Shard.maxFailures(4, 2));
        ClusterMetadata metadata = cluster(4);
        metadata = accept(metadata, hardRemoved(3, 4));
        reject(metadata, stale(2));
    }

    private static Set<NodeId> ids(int... ids)
    {
        ImmutableSet.Builder<NodeId> builder = ImmutableSet.builder();
        for (int id : ids)
            builder.add(new NodeId(id));
        return builder.build();
    }

    private static Transformation stale(int... ids)
    {
        return new AccordMarkStale(ids(ids));
    }

    private static Transformation hardRemoved(int... ids)
    {
        // force, as the nodes are still present in the directory
        return new AccordMarkHardRemoved(ids(ids), true);
    }

    private static ClusterMetadata accept(ClusterMetadata prev, Transformation transformation)
    {
        Transformation.Result result = transformation.execute(prev);
        Assert.assertTrue("TCM rejected " + transformation + ": " + result, result.isSuccess());
        ClusterMetadata next = result.success().metadata;

        // ExecuteSyncPoint constructs this tracker for every sync point
        Topology topology = AccordTopology.createAccordTopology(next);
        new DurabilityTracker(new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, topology));
        return next;
    }

    private static void reject(ClusterMetadata prev, Transformation transformation)
    {
        Assert.assertTrue("TCM accepted " + transformation, transformation.execute(prev).isRejected());
    }

    private static ClusterMetadata cluster(int rf)
    {
        TableMetadata table = parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks").build();
        KeyspaceMetadata keyspace = KeyspaceMetadata.create("ks", KeyspaceParams.simple(rf), Tables.of(table));
        ClusterMetadata.Transformer transformer = new ClusterMetadata(Murmur3Partitioner.instance).transformer();
        transformer.with(new DistributedSchema(Keyspaces.of(keyspace)));
        for (int i = 1; i <= rf; i++)
            AccordTopologyUtils.addNode(transformer, i, token(-1000 + i * 300));
        ClusterMetadata metadata = transformer.build().metadata;
        ReplicationParams replication = keyspace.params.replication;
        AbstractReplicationStrategy strategy = AbstractReplicationStrategy.createReplicationStrategy(keyspace.name, replication);
        DataPlacements.Builder placements = metadata.placements().unbuild();
        DataPlacement placement = strategy.calculateDataPlacement(Epoch.EMPTY, metadata.tokenMap.toRanges(), metadata);
        placements.with(replication, placement);
        return metadata.transformer().with(placements.build()).build().metadata;
    }
}
