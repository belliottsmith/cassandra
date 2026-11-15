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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.messages.Apply;
import accord.messages.BacklogClaim;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadData.ReadyWithDefinition;
import accord.messages.ReadTxnDataIfReady;
import accord.primitives.Ballot;
import accord.primitives.FullKeyRoute;
import accord.primitives.PartialDeps;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnDataResult;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.coordinate.ExecuteFlag.NO_WAIT;
import static accord.primitives.Routable.Domain.Key;
import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Serialization of the messages introduced for backlog execution and minimal Apply
 */
public class BacklogMessageSerializersTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void readTxnDataIfReady()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(txnIds(), routes(), AccordGens.timestamps()).check((txnId, route, executeAt) -> {
            // with NO_WAIT the read is opportunistic; without, it waits for Stable stores to become ready
            for (boolean wait : new boolean[] { false, true })
            {
                ReadTxnDataIfReady read = ReadTxnDataIfReady.SerializerSupport.create(txnId, route, executeAt, executeAt.epoch(), wait ? ExecuteFlags.none() : ExecuteFlags.none().with(NO_WAIT));
                ReadData result = roundTrip(buffer, ReadDataSerializer.request, read);
                assertThat(result).isInstanceOf(ReadTxnDataIfReady.class);
                assertThat(result.txnId).isEqualTo(txnId);
                assertThat(result.scope).isEqualTo(route);
                assertThat(result.executeAt()).isEqualTo(executeAt);
                assertThat(result.executeAtEpoch).isEqualTo(executeAt.epoch());
                assertThat(result.flags.bits()).isEqualTo(read.flags.bits());
                assertThat(result.flags.contains(NO_WAIT)).isEqualTo(!wait);
                assertThat(result.partialTxn()).isNull();
            }
        });
    }

    @Test
    public void readyWithDefinitionAndNotReady() throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        ReadReply notReady = roundTrip(buffer, ReadDataSerializer.reply, CommitOrReadNack.NotReady);
        assertThat(notReady).isSameAs(CommitOrReadNack.NotReady);
        assertThat(notReady.isFinal()).isTrue();

        for (PartialDeps deps : new PartialDeps[] { null, PartialDeps.NONE })
        {
            ReadReply reply = roundTrip(buffer, ReadDataSerializer.reply, new ReadyWithDefinition(null, deps));
            assertThat(reply).isInstanceOf(ReadyWithDefinition.class);
            assertThat(reply.isFinal()).isFalse();
            assertThat(((ReadyWithDefinition) reply).partialTxn).isNull();
            assertThat(((ReadyWithDefinition) reply).partialDeps).isEqualTo(deps);
        }
    }

    @Test
    public void applyNoDeps()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(txnIds(Txn.Kind.Read), routes(), AccordGens.timestamps()).check((txnId, route, executeAt) -> {
            long epoch = executeAt.epoch();
            Apply apply = Apply.SerializationSupport.create(txnId, Ballot.ZERO, route, epoch, epoch, epoch, Apply.Kind.NoDeps, executeAt,
                                                            null, null, null, null, TxnDataResult.PERSISTABLE, ExecuteFlags.none());
            Apply result = roundTrip(buffer, ApplySerializers.request, apply);
            assertThat(result.kind).isEqualTo(Apply.Kind.NoDeps);
            assertThat(result.txnId).isEqualTo(txnId);
            assertThat(result.executeAt).isEqualTo(executeAt);
            assertThat(result.deps()).isNull();
            assertThat(result.txn()).isNull();
        });
    }

    @Test
    public void backlogClaim()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(txnIds()).check(txnId -> {
            BacklogClaim result = roundTrip(buffer, BacklogClaimSerializers.request, new BacklogClaim(txnId));
            assertThat(result.txnId).isEqualTo(txnId);
        });
    }

    private static <T> T roundTrip(DataOutputBuffer buffer, org.apache.cassandra.io.VersionedSerializer<T, Version> serializer, T input) throws IOException
    {
        buffer.clear();
        long expectedSize = serializer.serializedSize(input, Version.LATEST);
        serializer.serialize(input, buffer, Version.LATEST);
        assertThat(buffer.getLength()).isEqualTo(expectedSize);
        DataInputBuffer in = new DataInputBuffer(buffer.unsafeGetBufferAndFlip(), false);
        T result = serializer.deserialize(in, Version.LATEST);
        assertThat(in.available()).isEqualTo(0);
        return result;
    }

    private static Gen<TxnId> txnIds(Txn.Kind ... kinds)
    {
        if (kinds.length == 0) kinds = new Txn.Kind[] { Txn.Kind.Read, Txn.Kind.Write };
        return AccordGens.txnIds(Gens.pick(kinds), Gens.constant(Key));
    }

    private static Gen<FullKeyRoute> routes()
    {
        return rs -> {
            Gen<TokenKey> keyGen = AccordGenerators.routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN), Gens.constant(AccordGenerators.RoutingKeyKind.TOKEN), fromQT(CassandraGenerators.murmurToken()), Murmur3Partitioner.instance);
            List<TokenKey> keys = Gens.lists(keyGen).unique().ofSizeBetween(1, 10).next(rs);
            keys.sort(Comparator.naturalOrder());
            return new FullKeyRoute(keys.get(0), keys.toArray(RoutingKey[]::new));
        };
    }
}
