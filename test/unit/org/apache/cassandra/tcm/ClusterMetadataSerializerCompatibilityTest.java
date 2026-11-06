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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that {@link ClusterMetadata} serialized by an earlier build can still be read, and that
 * re-serializing it at the same {@link Version} keeps that build's layout.
 * Round-trip tests over generated metadata cannot catch this, as they use the same code on both sides.
 */
public class ClusterMetadataSerializerCompatibilityTest
{
    /**
     * Written at {@link Version#V8} by a build that serialized {@code AccordFastPath} and
     * {@code AccordStaleReplicas}, i.e. before they were replaced by {@link AccordNodeInfos}.
     * Shared with {@code LocalRangesAllSettledBench}.
     */
    private static final String LEGACY_V8 = "cluster_metadata/CASSANDRA-21144_clustermetadata.gz";

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        // LockedRanges deserialization looks up the partitioner via ClusterMetadata.current()
        ClusterMetadataService.setInstance(ClusterMetadataTestHelper.syncInstanceForTest());
    }

    @Test
    public void deserializeLegacyV8() throws IOException
    {
        byte[] bytes = readResource(LEGACY_V8);
        DataInputBuffer in = new DataInputBuffer(bytes);
        ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);

        // A field read with the wrong layout misaligns everything after it, which may not throw
        assertThat(in.available()).describedAs("unconsumed trailing bytes").isZero();

        assertThat(metadata.epoch).isEqualTo(Epoch.create(48015));
        assertThat(metadata.accordNodeInfos).isEqualTo(AccordNodeInfos.EMPTY);
        assertThat(metadata.consensusMigrationState).isEqualTo(ConsensusMigrationState.EMPTY);

        // These follow the Accord fields, so are the first to be corrupted by a layout mismatch there
        assertThat(metadata.lockedRanges.lastModified()).isEqualTo(Epoch.create(48014));
        assertThat(metadata.lockedRanges.locked).hasSize(43);
        assertThat(metadata.inProgressSequences.isEmpty()).isFalse();
        assertThat(Iterables.size(metadata.inProgressSequences)).isEqualTo(metadata.lockedRanges.locked.size());
    }

    @Test
    public void reserializeLegacyV8PreservesLength() throws IOException
    {
        // Nodes on the build that wrote this data must be able to read what we write at the same version.
        // Bytes are not compared as some collections (e.g. replication options) are not ordered
        // deterministically, but dropping or adding a field changes the length.
        byte[] bytes = readResource(LEGACY_V8);
        Version version = Version.fromInt(new DataInputBuffer(bytes).readUnsignedVInt32());
        assertThat(version).isEqualTo(Version.V8);
        ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, new DataInputBuffer(bytes));

        assertThat(VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, metadata, version)).isEqualTo(bytes.length);
        try (DataOutputBuffer out = new DataOutputBuffer(bytes.length))
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, out, version);
            assertThat(out.getLength()).isEqualTo(bytes.length);
        }
    }

    @Test
    public void roundTripLegacyV8() throws IOException
    {
        ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer,
                                                                         new DataInputBuffer(readResource(LEGACY_V8)));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            AsymmetricMetadataSerializers.testSerde(out, ClusterMetadata.serializer, metadata, Version.V8);
        }
    }

    @Test
    public void roundTripLegacyV8AtCurrentVersion() throws IOException
    {
        ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer,
                                                                         new DataInputBuffer(readResource(LEGACY_V8)));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            AsymmetricMetadataSerializers.testSerde(out, ClusterMetadata.serializer, metadata, NodeVersion.CURRENT_METADATA_VERSION);
        }
    }

    private static byte[] readResource(String name) throws IOException
    {
        try (InputStream in = new GZIPInputStream(ClusterMetadataSerializerCompatibilityTest.class.getClassLoader().getResourceAsStream(name)))
        {
            return in.readAllBytes();
        }
    }
}
