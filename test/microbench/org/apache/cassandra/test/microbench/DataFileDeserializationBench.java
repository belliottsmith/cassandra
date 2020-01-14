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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ICompactionController;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.UnfilteredDeserializer;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowBuilder;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.test.microbench.data.DataGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static org.apache.cassandra.db.rows.EncodingStats.Collector.collect;
import static org.apache.cassandra.net.MessagingService.current_version;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class DataFileDeserializationBench
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }
    final AtomicInteger uniqueThreadInitialisation = new AtomicInteger();

    @Param({"4"})
    int clusteringCount;

    // a value of -1 indicates to send all inserts to a single row,
    // using the clusterings to build CellPath and write our rows into a Map
    @Param({"-1", "1", "8", "64"})
    int columnCount;

    @Param({"0", "0.5", "1"})
    float rowOverlap;

    @Param({"1", "32", "256"})
    int rowCount;

    @Param({"1", "32", "256"})
    int partitionCount;

    @Param({"8", "256"})
    int valueSize;

    @Param({"RANDOM", "SEQUENTIAL"})
    DataGenerator.Distribution distribution;

    @Param({"RANDOM", "SEQUENTIAL"})
    DataGenerator.Distribution timestamps;

    @Param({"false"})
    boolean uniquePerTrial;

    // hacky way to help us predict memory requirements
    @Param({"4"})
    int threadsForTest;

    @State(Scope.Benchmark)
    public static class GlobalState
    {
        DataGenerator generator;

        @Setup(Level.Trial)
        public void setup(DataFileDeserializationBench bench)
        {
            generator = new DataGenerator(bench.clusteringCount, bench.columnCount, bench.rowOverlap, bench.rowCount, bench.valueSize, 1, bench.distribution, bench.timestamps);
        }
    }

    @State(Scope.Thread)
    public static class ThreadState
    {
        DataGenerator.PartitionGenerator generator;
        int partitionCount;
        boolean uniquePerTrial;
        TableMetadata schema;

        // maybe precomputed work, e.g. if measuring allocations to avoid counting construction of work
        OneFile[] files;
        int counter;

        // the work to do next invocation
        OneFile next;

        @Setup(Level.Trial)
        public void preTrial(DataFileDeserializationBench bench, GlobalState state)
        {
            partitionCount = bench.partitionCount;
            uniquePerTrial = bench.uniquePerTrial;
            generator = state.generator.newGenerator(bench.uniqueThreadInitialisation.incrementAndGet());
            schema = state.generator.schema();
            if (!bench.uniquePerTrial)
            {
                int uniqueCount = min(256, max(1, (int) (Runtime.getRuntime().maxMemory() / (2 * (bench.threadsForTest * partitionCount * generator.averageSizeInBytesOfOneBatch())))));
                files = IntStream.range(0, uniqueCount)
                                 .mapToObj(f -> generate())
                                 .toArray(OneFile[]::new);
            }
        }

        @Setup(Level.Invocation)
        public void preInvocation()
        {
            if (uniquePerTrial)
            {
                next = generate();
            }
            else
            {
                next = files[counter++];
                if (counter == files.length) counter = 0;
            }
        }

        private OneFile generate()
        {
            List<PartitionUpdate> partitions = IntStream.range(0, partitionCount)
                                                        .mapToObj(p -> generator.generate(key(p)).get(0))
                                                        .collect(Collectors.toList());

            return new OneFile(schema, partitions);
        }
    }

    // to avoid too much surgery, we just ser/deser directly, instead of using SSTableIterator or ISSTableScanner
    private static class OneFile implements Supplier<UnfilteredPartitionIterator>
    {
        final TableMetadata schema;
        final SerializationHeader header;
        final ByteBuffer serialized;

        public OneFile(TableMetadata schema, List<PartitionUpdate> partitions)
        {
            this.schema = schema;
            this.header = new SerializationHeader(true, schema, schema.regularAndStaticColumns(),
                                             EncodingStats.merge(partitions, p -> collect(p.staticRow(), p.iterator(), p.deletionInfo())));

            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                for (PartitionUpdate partition : partitions)
                {
                    PartitionPosition.serializer.serialize(partition.partitionKey(), out, current_version);
                    DeletionTime.serializer.serialize(partition.partitionLevelDeletion(), out);
                    if (header.hasStatic())
                        UnfilteredSerializer.serializer.serializeStaticRow(partition.staticRow(), header, out, current_version);
                    UnfilteredRowIterator iterator = partition.unfilteredIterator();
                    long prevPos = 0;
                    while (iterator.hasNext())
                    {
                        long pos = out.position();
                        UnfilteredSerializer.serializer.serialize(iterator.next(), header, out, pos - prevPos, current_version);
                        prevPos = pos;
                    }
                    UnfilteredSerializer.serializer.writeEndOfPartition(out);
                }
                serialized = out.asNewBuffer();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        }

        public UnfilteredPartitionIterator get()
        {
            return new UnfilteredPartitionIterator()
            {
                final SerializationHelper helper = new SerializationHelper(schema, current_version, SerializationHelper.Flag.LOCAL);
                final Row.Builder builder = new RowBuilder();
                final DataInputBuffer in = new DataInputBuffer(serialized.duplicate(), false);

                public TableMetadata metadata()
                {
                    return schema;
                }

                public boolean hasNext()
                {
                    return in.available() > 0;
                }

                public UnfilteredRowIterator next()
                {
                    try
                    {
                        UnfilteredDeserializer deserializer = UnfilteredDeserializer.create(schema, in, header, helper);
                        DecoratedKey key = (DecoratedKey) PartitionPosition.serializer.deserialize(in, schema.partitioner, current_version);
                        DeletionTime partitionDeletion = DeletionTime.serializer.deserialize(in);
                        Row staticRow = !header.hasStatic() ? Rows.EMPTY_STATIC_ROW : UnfilteredSerializer.serializer.deserializeStaticRow(in, header, helper);
                        return new AbstractUnfilteredRowIterator(schema, key, partitionDeletion, header.columns(), staticRow, false, header.stats())
                        {
                            protected Unfiltered computeNext()
                            {
                                try
                                {
                                    if (!deserializer.hasNext())
                                        return endOfData();
                                    return deserializer.readNext();
                                }
                                catch (IOException e)
                                {
                                    throw new RuntimeException(e);
                                }
                            }
                        };
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                }

                public void close()
                {
                }
            };
        }

        void perform()
        {
            try (UnfilteredPartitionIterator partitionIterator = get())
            {
                while (partitionIterator.hasNext())
                {
                    UnfilteredRowIterator rowIterator = partitionIterator.next();
                    while (rowIterator.hasNext())
                        rowIterator.next();
                }
            }
        }
    }

    private static DecoratedKey key(int i)
    {
        ByteBuffer v = Int32Type.instance.decompose(i);
        return new BufferDecoratedKey(new ByteOrderedPartitioner().getToken(v), v);
    }

    @Benchmark
    public void scan(ThreadState state)
    {
        state.next.perform();
    }
}
