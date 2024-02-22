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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Key;
import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.CommonAttributes.Mutable;
import accord.local.Listeners;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.KeyDeps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.RangeDeps;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.AccordGens;
import accord.utils.AsymmetricComparator;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.SortedArrays;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.simulator.RandomSource.Choices;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.AsymmetricOrdering;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.local.Status.Durability.NotDurable;
import static accord.utils.Property.qt;
import static accord.utils.SortedArrays.Search.FAST;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;

public class CommandsForKeySerializerTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        // need to create the accord test table as generating random txn is not currently supported
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    private static List<Command> generateObjectGraph(int txnIdCount, Supplier<TxnId> txnIdSupplier, Supplier<SaveStatus> saveStatusSupplier, Function<TxnId, PartialTxn> txnSupplier, Function<TxnId, Timestamp> timestampSupplier, IntSupplier missingCountSupplier, RandomSource source)
    {
        class Info
        {
            final TxnId txnId;
            final SaveStatus saveStatus;
            final PartialTxn txn;
            final Timestamp executeAt;
            final List<TxnId> deps = new ArrayList<>();
            final List<TxnId> missing = new ArrayList<>();

            Info(TxnId txnId, PartialTxn txn, SaveStatus saveStatus, Timestamp executeAt)
            {
                this.txnId = txnId;
                this.saveStatus = saveStatus;
                this.txn = txn;
                this.executeAt = executeAt;
            }

            CommonAttributes attributes()
            {
                Mutable mutable = new Mutable(txnId);
                if (saveStatus.known.isDefinitionKnown())
                    mutable.partialTxn(txn);

                mutable.route(txn.keys().toRoute(txn.keys().get(0).someIntersectingRoutingKey(null)));
                mutable.durability(NotDurable);
                if (saveStatus.known.deps.hasProposedOrDecidedDeps())
                {
                    try (KeyDeps.Builder builder = KeyDeps.builder();)
                    {
                        for (TxnId id : deps)
                            builder.add((Key)txn.keys().get(0), id);
                        mutable.partialDeps(new PartialDeps(AccordTestUtils.fullRange(txn), builder.build(), RangeDeps.NONE));
                    }
                }

                return mutable;
            }

            Command build()
            {
                switch (saveStatus)
                {
                    default: throw new AssertionError("Unhandled saveStatus: " + saveStatus);
                    case Uninitialised:
                    case NotDefined:
                        return Command.SerializerSupport.notDefined(attributes(), Ballot.ZERO);
                    case PreAccepted:
                        return Command.SerializerSupport.preaccepted(attributes(), executeAt, Ballot.ZERO);
                    case Accepted:
                    case AcceptedInvalidate:
                    case AcceptedWithDefinition:
                    case AcceptedInvalidateWithDefinition:
                    case PreCommittedWithDefinition:
                    case PreCommittedWithDefinitionAndAcceptedDeps:
                    case PreCommittedWithAcceptedDeps:
                    case PreCommitted:
                        return Command.SerializerSupport.accepted(attributes(), saveStatus, executeAt, Ballot.ZERO, Ballot.ZERO);

                    case Committed:
                        return Command.SerializerSupport.committed(attributes(), saveStatus, executeAt, Ballot.ZERO, Ballot.ZERO, null);

                    case Stable:
                    case ReadyToExecute:
                        return Command.SerializerSupport.committed(attributes(), saveStatus, executeAt, Ballot.ZERO, Ballot.ZERO, Command.WaitingOn.EMPTY);

                    case PreApplied:
                    case Applying:
                    case Applied:
                        return Command.SerializerSupport.executed(attributes(), saveStatus, executeAt, Ballot.ZERO, Ballot.ZERO, Command.WaitingOn.EMPTY, new Writes(txnId, executeAt, txn.keys(), new TxnWrite(Collections.emptyList(), true)), new TxnData());

                    case TruncatedApplyWithDeps:
                    case TruncatedApply:
                        return Command.SerializerSupport.truncatedApply(attributes(), saveStatus, executeAt, null, null);

                    case TruncatedApplyWithOutcome:
                        return Command.SerializerSupport.truncatedApply(attributes(), saveStatus, executeAt, new Writes(txnId, executeAt, txn.keys(), new TxnWrite(Collections.emptyList(), true)), new TxnData());

                    case Erased:
                    case ErasedOrInvalidated:
                    case Invalidated:
                        return Command.SerializerSupport.invalidated(txnId, Listeners.Immutable.EMPTY);
                }
            }
        }

        Info[] infos = new Info[txnIdCount];
        for (int i = 0 ; i < txnIdCount ; ++i)
        {
            TxnId txnId = txnIdSupplier.get();
            SaveStatus saveStatus = saveStatusSupplier.get();
            Timestamp executeAt = null;
            if (saveStatus.known.executeAt.compareTo(Status.KnownExecuteAt.ExecuteAtProposed) >= 0)
                executeAt = timestampSupplier.apply(txnId);

            infos[i] = new Info(txnId, txnSupplier.apply(txnId), saveStatus, executeAt);
        }
        Arrays.sort(infos, Comparator.comparing(o -> o.txnId));
        for (int i = 0 ; i < txnIdCount ; ++i)
        {
            if (!infos[i].saveStatus.known.deps.hasProposedOrDecidedDeps())
                continue;

            Timestamp knownBefore = infos[i].saveStatus.known.deps.hasCommittedOrDecidedDeps() ? infos[i].executeAt : infos[i].txnId;
            int limit = SortedArrays.binarySearch(infos, 0, infos.length, knownBefore, (a, b) -> a.compareTo(b.txnId), FAST);
            if (limit < 0) limit = -1 - i;

            List<TxnId> deps = infos[i].deps;
            List<TxnId> missing = infos[i].missing;
            for (int j = 0 ; j < limit ; ++j)
                deps.add(infos[j].txnId);

            int missingCount = Math.min(limit, missingCountSupplier.getAsInt());
            while (missingCount > 0)
            {
                int remove = source.nextInt(deps.size());
                missing.add(deps.get(remove));
                deps.set(remove, deps.get(deps.size() - 1));
                deps.remove(deps.size() - 1);
                --missingCount;
            }
            deps.sort(TxnId::compareTo);
            missing.sort(TxnId::compareTo);
        }

        Command[] commands = new Command[infos.length];
        for (int i = 0 ; i < commands.length ; ++i)
            commands[i] = infos[i].build();
        return Arrays.asList(commands);
    }

    private static Function<Timestamp, TxnId> txnIdSupplier(LongUnaryOperator epochSupplier, LongUnaryOperator hlcSupplier, Supplier<Txn.Kind> kindSupplier, Supplier<Node.Id> idSupplier)
    {
        return min -> new TxnId(epochSupplier.applyAsLong(min == null ? 1 : min.epoch()), hlcSupplier.applyAsLong(min == null ? 1 : min.hlc() + 1), kindSupplier.get(), Routable.Domain.Key, idSupplier.get());
    }

    private static Function<Timestamp, Timestamp> timestampSupplier(LongUnaryOperator epochSupplier, LongUnaryOperator hlcSupplier, IntSupplier flagSupplier, Supplier<Node.Id> idSupplier)
    {
        return min -> Timestamp.fromValues(epochSupplier.applyAsLong(min == null ? 1 : min.epoch()), hlcSupplier.applyAsLong(min == null ? 1 : min.hlc() + 1), flagSupplier.getAsInt(), idSupplier.get());
    }

    private static <T extends Timestamp> Function<Timestamp, T> timestampSupplier(Set<Timestamp> unique, Function<Timestamp, T> supplier)
    {
        return min -> {
            T candidate = supplier.apply(min);
            while (!unique.add(candidate))
            {
                T next = supplier.apply(min);
                if (next.equals(candidate)) min = candidate;
                else candidate = next;
            }
            return candidate;
        };
    }

    @Test
    public void serde()
    {
        Random random = new Random();
        for (int i = 0 ; i < 10000 ; ++i)
        {
            long seed = random.nextLong();
            testOne(seed);
        }
    }

    private static void testOne(long seed)
    {
        try
        {
            System.out.println(seed);
            RandomSource source = RandomSource.wrap(new Random(seed));

            // TODO (required): produce broader variety of distributions, including executeAt with lower HLC but higher epoch
            final LongUnaryOperator epochSupplier; {
                long maxEpoch = source.nextLong(1, 10);
                epochSupplier = min -> min >= maxEpoch ? min : maxEpoch == 1 ? 1 : source.nextLong(min, maxEpoch);
            }
            final LongUnaryOperator hlcSupplier; {
                long maxHlc = source.nextLong(10, 1000000);
                hlcSupplier = min -> min >= maxHlc ? min : source.nextLong(min, maxHlc);
            }
            final Supplier<Node.Id> idSupplier; {
                int maxId = source.nextInt(1, 10);
                Int2ObjectHashMap<Node.Id> lookup = new Int2ObjectHashMap<>();
                idSupplier = () -> lookup.computeIfAbsent(maxId == 1 ? 1 : source.nextInt(1, maxId), Node.Id::new);
            }
            final IntSupplier flagSupplier = () -> 0;
            final Supplier<Txn.Kind> kindSupplier = () -> {
                float v = source.nextFloat();
                if (v < 0.5) return Txn.Kind.Read;
                if (v < 0.95) return Txn.Kind.Write;
                if (v < 0.99) return Txn.Kind.ExclusiveSyncPoint;
                return Txn.Kind.EphemeralRead; // not actually a valid value for CFK
            };

            boolean permitMissing = source.decide(0.75f);
            final IntSupplier missingCountSupplier; {
                if (!permitMissing)
                {
                    missingCountSupplier = () -> 0;
                }
                else
                {
                    float zeroChance = source.nextFloat();
                    int maxMissing = source.nextInt(1, 10);
                    missingCountSupplier = () -> {
                        float v = source.nextFloat();
                        if (v < zeroChance) return 0;
                        return source.nextInt(0, maxMissing);
                    };
                }
            }

            Choices<SaveStatus> saveStatusChoices = Choices.uniform(SaveStatus.values());
            Supplier<SaveStatus> saveStatusSupplier = () -> {
                SaveStatus result = saveStatusChoices.choose(source);
                while (result == SaveStatus.TruncatedApplyWithDeps) // not a real save status
                    result = saveStatusChoices.choose(source);
                return result;
            };

            Set<Timestamp> uniqueTs = new TreeSet<>();
            final Function<Timestamp, TxnId> txnIdSupplier = timestampSupplier(uniqueTs, txnIdSupplier(epochSupplier, hlcSupplier, kindSupplier, idSupplier));
            boolean permitExecuteAt = source.decide(0.75f);
            final Function<TxnId, Timestamp> executeAtSupplier;
            {
                if (!permitExecuteAt)
                {
                    executeAtSupplier = id -> id;
                }
                else
                {
                    Function<Timestamp, Timestamp> rawTimestampSupplier = timestampSupplier(uniqueTs, timestampSupplier(epochSupplier, hlcSupplier, flagSupplier, idSupplier));
                    float useTxnIdChance = source.nextFloat();
                    BooleanSupplier useTxnId = () -> source.decide(useTxnIdChance);
                    executeAtSupplier = txnId -> useTxnId.getAsBoolean() ? txnId : rawTimestampSupplier.apply(txnId);
                }
            }

            PartialTxn txn = createPartialTxn(0);
            Key key = (Key) txn.keys().get(0);
            List<Command> commands = generateObjectGraph(source.nextInt(0, 100), () -> txnIdSupplier.apply(null), saveStatusSupplier, ignore -> txn, executeAtSupplier, missingCountSupplier, source);
            CommandsForKey cfk = new CommandsForKey(key);
            for (Command command : commands)
                cfk = cfk.update(null, command);

            ByteBuffer buffer = CommandsForKeySerializer.toBytesWithoutKey(cfk);
            CommandsForKey roundTrip = CommandsForKeySerializer.fromBytes(key, buffer);
            Assert.assertEquals(cfk, roundTrip);
        }
        catch (Throwable t)
        {
            throw new AssertionError(seed + " seed failed", t);
        }
    }

    @Test
    public void test()
    {
        var tableGen = AccordGenerators.fromQT(CassandraGenerators.TABLE_ID_GEN);
        var txnIdGen = AccordGens.txnIds(rs -> rs.nextLong(0, 100), rs -> rs.nextLong(100), rs -> rs.nextInt(10));
        qt().check(rs -> {
            TableId table = tableGen.next(rs);
            PartitionKey pk = new PartitionKey(table, Murmur3Partitioner.instance.decorateKey(Murmur3Partitioner.LongToken.keyForToken(rs.nextLong())));
            var redudentBefore = txnIdGen.next(rs);
            TxnId[] ids = Gens.arrays(TxnId.class, rs0 -> {
                TxnId next = txnIdGen.next(rs0);
                while (next.compareTo(redudentBefore) <= 0)
                    next = txnIdGen.next(rs0);
                return next;
            }).unique().ofSizeBetween(0, 10).next(rs);
            CommandsForKey.Info[] info = new CommandsForKey.Info[ids.length];
            for (int i = 0; i < info.length; i++)
                info[i] = rs.pick(CommandsForKey.InternalStatus.values()).asNoInfo;
            Arrays.sort(ids, Comparator.naturalOrder());
            CommandsForKey expected = CommandsForKey.SerializerSupport.create(pk, redudentBefore, ids, info);

            ByteBuffer buffer = CommandsForKeySerializer.toBytesWithoutKey(expected);
            CommandsForKey roundTrip = CommandsForKeySerializer.fromBytes(pk, buffer);
            Assert.assertEquals(expected, roundTrip);
        });
    }

    @Test
    public void thereAndBackAgain()
    {
        long tokenValue = -2311778975040348869L;
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Murmur3Partitioner.LongToken.keyForToken(tokenValue));
        PartitionKey pk = new PartitionKey(TableId.fromString("1b255f4d-ef25-40a6-0000-000000000009"), key);
        CommandsForKey expected = CommandsForKey.SerializerSupport.create(pk,
                                                     TxnId.fromValues(0,0,0,0),
                                                     new TxnId[] {TxnId.fromValues(11,34052499,2,1)},
                                                     new CommandsForKey.Info[] {CommandsForKey.InternalStatus.PREACCEPTED.asNoInfo});

        ByteBuffer buffer = CommandsForKeySerializer.toBytesWithoutKey(expected);
        CommandsForKey roundTrip = CommandsForKeySerializer.fromBytes(pk, buffer);
        Assert.assertEquals(expected, roundTrip);
    }
}