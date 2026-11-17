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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.mockito.Mockito;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.service.accord.AccordObjectSizes;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;

/**
 * Verify that every query answered by {@link SerializedPartialDeps} is equivalent to the same query against
 * the materialised {@link PartialDeps}, for every layout of the serialized form, and that key-domain queries are
 * answered without deserializing the {@link KeyDeps}.
 */
public class SerializedPartialDepsTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    // all four combinations of key/range layout; keysByTxnId, rangesByTxnId
    private static final List<DepsSerializers.AbstractDepsSerializer<PartialDeps>> SERIALIZERS = Arrays.asList(
        DepsSerializers.partialDepsById,
        DepsSerializers.partialDepsByKey,
        new DepsSerializers.PartialDepsSerializer(false, false, DepsSerializers.tokenRange),
        new DepsSerializers.PartialDepsSerializer(true, false, DepsSerializers.tokenRange)
    );

    @Test
    public void equivalence()
    {
        qt().check(rs -> {
            IPartitioner partitioner = AccordGenerators.partitioner().next(rs);
            Schema.instance = Mockito.mock(SchemaProvider.class);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            Mockito.when(Schema.instance.getExistingTablePartitioner(Mockito.any())).thenReturn(partitioner);

            Gen<TxnId> keyTxnIdGen = txnIdGen(rs, Routable.Domain.Key);
            Gen<TxnId> rangeTxnIdGen = txnIdGen(rs, Routable.Domain.Range);
            KeyDeps keyDeps = keyDepsGen(AccordGenerators.routingKeysGen(partitioner), keyTxnIdGen).next(rs);
            RangeDeps rangeDeps = rs.decide(0.3f) ? RangeDeps.NONE : AccordGens.rangeDeps(AccordGenerators.range(partitioner), rangeTxnIdGen).next(rs);
            Deps deps = new Deps(keyDeps, rangeDeps);
            if (!deps.isEmpty() && rs.nextBoolean())
                deps = deps.markUnstableBefore(deps.txnId(rs.nextInt(deps.txnIdCount())));
            Participants<?> covering = rs.nextBoolean() || deps.keyDeps.keys().isEmpty() ? AccordGenerators.ranges(partitioner).next(rs)
                                                                                           : deps.keyDeps.keys();
            PartialDeps partialDeps = new PartialDeps(covering, deps.keyDeps, deps.rangeDeps);

            List<TxnId> probes = probes(rs, partialDeps, keyTxnIdGen, rangeTxnIdGen);
            List<Unseekables<?>> intersecting = intersecting(rs, partialDeps, partitioner);
            for (DepsSerializers.AbstractDepsSerializer<PartialDeps> serializer : SERIALIZERS)
                check(partialDeps, serializer, probes, intersecting);
        });
    }

    private static void check(PartialDeps expected, DepsSerializers.AbstractDepsSerializer<PartialDeps> serializer, List<TxnId> probes, List<Unseekables<?>> intersecting) throws IOException
    {
        SoftAssertions checks = new SoftAssertions();
        SerializedPartialDeps view = SerializedPartialDeps.serialize(expected, serializer);
        // our size estimate should not depend on which components have been lazily deserialized
        long estimatedSize = AccordObjectSizes.depsView(view);
        checks.assertThat(view.keysByTxnId()).isEqualTo(serializer.keysByTxnId);
        checks.assertThat(view.rangesByTxnId()).isEqualTo(serializer.rangesByTxnId);

        // we must faithfully reproduce the serialized form
        try (DataOutputBuffer expectBytes = new DataOutputBuffer(); DataOutputBuffer actualBytes = new DataOutputBuffer())
        {
            serializer.serialize(expected, expectBytes);
            view.serialize(actualBytes);
            checks.assertThat(actualBytes.toByteArray()).isEqualTo(expectBytes.toByteArray());
            checks.assertThat((long)view.serializedSize()).isEqualTo(serializer.serializedSize(expected));
        }

        // queries that should not require deserializing the KeyDeps
        checks.assertThat(view.isEmpty()).isEqualTo(expected.isEmpty());
        checks.assertThat(view.txnIdCount()).isEqualTo(expected.txnIdCount());
        for (TxnId probe : probes)
        {
            if (probe.is(Routable.Domain.Key))
                checks.assertThat(view.findKeyTxnId(probe)).describedAs("%s", probe).isEqualTo(Arrays.binarySearch(KeyDeps.SerializerSupport.txnIds(expected.keyDeps), probe));
            checks.assertThat(view.participants(probe)).describedAs("%s", probe).isEqualTo(expected.participants(probe));
            for (Unseekables<?> select : intersecting)
            {
                checks.assertThat(view.isStableDepCovering(probe, select))
                      .describedAs("%s %s", probe, select)
                      .isEqualTo(expected.isStableDepCovering(probe, select));
            }
        }
        for (int i = 0 ; i < expected.keyDeps.txnIdCount() ; ++i)
        {
            TxnId expect = expected.keyDeps.txnIdWithFlags(i);
            TxnId actual = view.keyTxnIdWithFlags(i);
            checks.assertThat(actual.equalsStrict(expect)).describedAs("%s != %s", actual, expect).isTrue();
            checks.assertThat(view.keyParticipants(i)).describedAs("%s", expect).isEqualTo(expected.keyDeps.participants(i));
        }
        checks.assertThat(view.memoizedKeyDeps()).describedAs("KeyDeps should not have been deserialized").isNull();

        // the partially deserialized components
        checks.assertThat(view.keys()).isEqualTo(expected.keyDeps.keys());
        checks.assertThat(view.covering()).isEqualTo(expected.covering);
        checks.assertThat(view.rangeDeps()).isEqualTo(expected.rangeDeps);
        checks.assertThat(view.memoizedKeyDeps()).describedAs("KeyDeps should not have been deserialized").isNull();
        checks.assertThat(AccordObjectSizes.depsView(view)).describedAs("estimate changed by lazy deserialization").isEqualTo(estimatedSize);
        if (DatabaseDescriptor.getPartitioner().isFixedLength())
        {
            checks.assertThat(AccordObjectSizes.estimateRoutingKeys(view.keyCount(), view.keysSerializedSize()))
                  .describedAs("estimate should be exact for fixed length partitioners")
                  .isEqualTo(AccordObjectSizes.routingKeys(view.keys()));
        }
        checks.assertThat(view.keyDeps()).isEqualTo(expected.keyDeps);
        checkFlags(checks, expected.keyDeps.txnIdsWithFlags(), view.keyDeps().txnIdsWithFlags());
        checkFlags(checks, expected.rangeDeps.txnIdsWithFlags(), view.rangeDeps().txnIdsWithFlags());

        // the materialised form
        PartialDeps materialized = view.materialize();
        checks.assertThat(materialized).isEqualTo(expected);
        checks.assertThat(view.materialize()).isSameAs(materialized);
        checks.assertThat(view.isSameAs(materialized)).isTrue();
        checks.assertThat(view.isSameAs(expected)).isFalse();

        // queries should be unaffected by memoisation
        for (TxnId probe : probes)
        {
            checks.assertThat(view.participants(probe)).describedAs("%s", probe).isEqualTo(expected.participants(probe));
            for (Unseekables<?> select : intersecting)
            {
                checks.assertThat(view.isStableDepCovering(probe, select))
                      .describedAs("%s %s", probe, select)
                      .isEqualTo(expected.isStableDepCovering(probe, select));
            }
        }

        // unmemoize discards only the large components
        RoutingKeys keys = view.keys();
        RangeDeps rangeDeps = view.rangeDeps();
        view.unmemoize();
        checks.assertThat(view.memoizedKeyDeps()).isNull();
        checks.assertThat(view.isSameAs(materialized)).isFalse();
        checks.assertThat(view.memoizedKeys()).isSameAs(keys);
        checks.assertThat(view.memoizedRangeDeps()).isSameAs(rangeDeps);
        checks.assertThat(AccordObjectSizes.depsView(view)).describedAs("estimate not restored by unmemoize").isEqualTo(estimatedSize);
        checks.assertThat(view.materialize()).isNotSameAs(materialized).isEqualTo(expected);
        checks.assertAll();
    }

    private static void checkFlags(SoftAssertions checks, List<TxnId> expect, List<TxnId> actual)
    {
        checks.assertThat(actual.size()).isEqualTo(expect.size());
        for (int i = 0 ; i < Math.min(expect.size(), actual.size()) ; ++i)
            checks.assertThat(actual.get(i).equalsStrict(expect.get(i))).describedAs("%s != %s", actual.get(i), expect.get(i)).isTrue();
    }

    /**
     * Either an arbitrary TxnId, or one drawn from a narrow domain, so that we exercise a range of packed widths
     * (including zero-width columns)
     */
    private static Gen<TxnId> txnIdGen(RandomSource rs, Routable.Domain domain)
    {
        Gen<Txn.Kind> kinds = domain == Routable.Domain.Key ? Gens.pick(Txn.Kind.Write, Txn.Kind.Read)
                                                            : Gens.pick(Txn.Kind.Write, Txn.Kind.Read, Txn.Kind.ExclusiveSyncPoint);
        switch (rs.nextInt(3))
        {
            default: throw new AssertionError();
            case 0: return AccordGens.txnIds(kinds, ignore -> domain);
            case 1:
            {
                long epoch = rs.nextLong(1, 1000);
                long hlc = rs.nextLong(1, 1L << 50);
                int node = rs.nextInt(1, 100);
                return AccordGens.txnIds(r -> epoch + r.nextInt(0, 3), r -> hlc + r.nextInt(0, 1000), r -> node + r.nextInt(0, 5), kinds, ignore -> domain);
            }
            case 2:
            {
                long epoch = rs.nextLong(1, 1000);
                int node = rs.nextInt(1, 100);
                return AccordGens.txnIds(r -> epoch, AccordGens.hlcs(), r -> node, kinds, ignore -> domain);
            }
        }
    }

    /**
     * Unlike {@link AccordGens#keyDeps}, we draw TxnId for each key from a shared pool, so that a TxnId may be
     * a dependency of multiple keys, and we occasionally generate much larger collections
     */
    private static Gen<KeyDeps> keyDepsGen(Gen<? extends RoutingKey> keyGen, Gen<TxnId> txnIdGen)
    {
        return rs -> {
            if (rs.decide(0.1f))
                return KeyDeps.NONE;

            boolean large = rs.decide(0.1f);
            int keyCount = rs.decide(0.3f) ? 1 : rs.nextInt(1, large ? 200 : 10);
            int poolSize = rs.decide(0.2f) ? 1 : rs.nextInt(1, large ? 500 : 20);
            TreeSet<TxnId> pool = new TreeSet<>();
            for (int i = 0 ; i < poolSize * 2 && pool.size() < poolSize ; ++i)
                pool.add(txnIdGen.next(rs));
            TxnId[] txnIds = pool.toArray(new TxnId[0]);

            TreeSet<RoutingKey> keys = new TreeSet<>();
            for (int i = 0 ; i < keyCount * 2 && keys.size() < keyCount ; ++i)
                keys.add(keyGen.next(rs));

            float density = rs.nextFloat();
            try (KeyDeps.Builder builder = KeyDeps.builder())
            {
                for (RoutingKey key : keys)
                {
                    builder.nextKey(key);
                    boolean any = false;
                    for (TxnId txnId : txnIds)
                    {
                        if (rs.decide(density))
                        {
                            builder.add(txnId);
                            any = true;
                        }
                    }
                    if (!any)
                        builder.add(txnIds[rs.nextInt(txnIds.length)]);
                }
                return builder.build();
            }
        };
    }

    private static List<TxnId> probes(RandomSource rs, PartialDeps deps, Gen<TxnId> keyTxnIdGen, Gen<TxnId> rangeTxnIdGen)
    {
        List<TxnId> probes = new ArrayList<>();
        // every present TxnId, stripped of any non-identity flags (as a caller would provide)
        for (int i = 0 ; i < deps.keyDeps.txnIdCount() ; ++i)
            probes.add(deps.keyDeps.txnId(i));
        for (int i = 0 ; i < deps.rangeDeps.txnIdCount() ; ++i)
            probes.add(deps.rangeDeps.txnId(i));
        // absent TxnId, including near neighbours of present ones
        for (int i = 0, count = rs.nextInt(1, 10) ; i < count ; ++i)
        {
            probes.add(keyTxnIdGen.next(rs));
            probes.add(rangeTxnIdGen.next(rs));
        }
        for (int i = 0 ; i < deps.keyDeps.txnIdCount() && i < 10 ; ++i)
        {
            TxnId txnId = deps.keyDeps.txnId(rs.nextInt(deps.keyDeps.txnIdCount()));
            probes.add(new TxnId(txnId.epoch(), txnId.hlc() + 1, 0, txnId.kind(), txnId.domain(), txnId.cardinality(), txnId.node));
            probes.add(new TxnId(txnId.epoch(), txnId.hlc(), 0, txnId.kind(), txnId.domain(), txnId.cardinality(), new Node.Id(txnId.node.id + 1)));
            if (txnId.hlc() > 0)
                probes.add(new TxnId(txnId.epoch(), txnId.hlc() - 1, 0, txnId.kind(), txnId.domain(), txnId.cardinality(), txnId.node));
        }
        return probes;
    }

    private static List<Unseekables<?>> intersecting(RandomSource rs, PartialDeps deps, IPartitioner partitioner)
    {
        List<Unseekables<?>> result = new ArrayList<>();
        result.add(RoutingKeys.EMPTY);
        result.add(Ranges.EMPTY);
        result.add(deps.covering);
        RoutingKeys keys = deps.keyDeps.keys();
        if (!keys.isEmpty())
        {
            result.add(keys);
            result.add(RoutingKeys.of(keys.get(rs.nextInt(keys.size()))));
            result.add(keys.toRanges());
        }
        for (int i = 0 ; i < deps.keyDeps.txnIdCount() && i < 3 ; ++i)
            result.add(deps.keyDeps.participants(rs.nextInt(deps.keyDeps.txnIdCount())));
        for (int i = 0 ; i < deps.rangeDeps.txnIdCount() && i < 3 ; ++i)
            result.add(deps.rangeDeps.participants(rs.nextInt(deps.rangeDeps.txnIdCount())));
        if (deps.rangeDeps.rangeCount() > 0)
        {
            Range range = deps.rangeDeps.range(rs.nextInt(deps.rangeDeps.rangeCount()));
            result.add(Ranges.of(range));
        }
        result.add(AccordGenerators.ranges(partitioner).next(rs));
        return result;
    }
}
