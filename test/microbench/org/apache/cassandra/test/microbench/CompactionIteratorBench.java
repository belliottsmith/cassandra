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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ICompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
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

import static java.lang.Math.*;
import static java.lang.Math.max;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class CompactionIteratorBench
{
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

    // ratio of streams sharing a given partition
    @Param({"0", "0.5", "1"})
    float partitionOverlap;

    @Param({"1", "32", "256"})
    int partitionCount;

    @Param({"8", "256"})
    int valueSize;

    @Param({"1", "4", "32"})
    int streamCount;

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
        ICompactionController controller;

        @Setup(Level.Trial)
        public void setup(CompactionIteratorBench bench)
        {
            generator = new DataGenerator(bench.clusteringCount, bench.columnCount, bench.rowOverlap, bench.rowCount, bench.valueSize, round(max(1, bench.streamCount * bench.partitionOverlap)), bench.distribution, bench.timestamps);
            controller = controller(generator.schema());
        }
    }

    @State(Scope.Thread)
    public static class ThreadState
    {
        DataGenerator.PartitionGenerator generator;
        int totalPartitionCount;
        boolean uniquePerTrial;
        int streamCount;
        ICompactionController controller;

        // maybe precomputed work, e.g. if measuring allocations to avoid counting construction of work
        OneCompaction[] compactions;
        int counter;

        // the work to do next invocation
        OneCompaction next;

        @Setup(Level.Trial)
        public void preTrial(CompactionIteratorBench bench, GlobalState state)
        {
            totalPartitionCount = bench.partitionOverlap <= 0.0001f ? bench.partitionCount * bench.streamCount
                                                                    : round(bench.partitionCount / bench.partitionOverlap);
            streamCount = bench.streamCount;
            uniquePerTrial = bench.uniquePerTrial;
            generator = state.generator.newGenerator(bench.uniqueThreadInitialisation.incrementAndGet());
            controller = state.controller;
            if (!bench.uniquePerTrial)
            {
                int uniqueCount = min(256, max(1, (int) (Runtime.getRuntime().maxMemory() / (2 * (bench.threadsForTest * totalPartitionCount * bench.streamCount * generator.averageSizeInBytesOfOneBatch())))));
                compactions = IntStream.range(0, uniqueCount)
                                       .mapToObj(f -> generate())
                                       .toArray(OneCompaction[]::new);
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
                next = compactions[counter++];
                if (counter == compactions.length) counter = 0;
            }
        }

        private OneCompaction generate()
        {
            List<List<PartitionUpdate>> partitions = IntStream.range(0, totalPartitionCount)
                                                              .mapToObj(p -> generator.generate(key(p)))
                                                              .collect(Collectors.toList());

            // input is a list of partitions, made up of multiple versions of the partition to merge
            // need to split this amongst the streams, randomly
            List<PartitionUpdate>[] inverted = new List[streamCount];
            for (int i = 0 ; i < inverted.length ; ++i)
                inverted[i] = new ArrayList<>();

            Random random = new Random(0);
            for (List<PartitionUpdate> partition : partitions)
            {
                shuffle(random, inverted, 0, partition.size(), 0, inverted.length);
                for (int i = 0 ; i < partition.size() ; ++i)
                    inverted[i].add(partition.get(i));
            }

            List<ISSTableScanner> scanners = new ArrayList<>(streamCount);
            for (int i = 0 ; i < inverted.length ; ++i)
                scanners.add(new OneSSTableScanner(inverted[i]));

            return new OneCompaction(controller, scanners);
        }
    }

    private static class OneSSTableScanner implements ISSTableScanner
    {
        final List<PartitionUpdate> partitions;
        int next;

        public OneSSTableScanner(List<PartitionUpdate> partitions)
        {
            this.partitions = partitions;
        }

        public long getLengthInBytes() { return 0; }
        public long getCompressedLengthInBytes() { return 0; }
        public long getCurrentPosition() { return 0; }
        public long getBytesScanned() { return 0; }
        public Set<SSTableReader> getBackingSSTables() { return Collections.emptySet(); }
        public TableMetadata metadata() { return partitions.get(0).metadata(); }
        public void close() { next = 0; }

        public boolean hasNext()
        {
            return next < partitions.size();
        }

        public UnfilteredRowIterator next()
        {
            return partitions.get(next++).unfilteredIterator();
        }
    }

    private static class OneCompaction
    {
        final ICompactionController controller;
        final List<ISSTableScanner> scanners;

        public OneCompaction(ICompactionController controller, List<ISSTableScanner> scanners)
        {
            this.controller = controller;
            this.scanners = scanners;
        }

        void perform()
        {
            try (CompactionIterator iter = new CompactionIterator(OperationType.COMPACTION, scanners, controller, 0, UUID.randomUUID());)
            {
                while (!iter.hasNext())
                    iter.next();
            }
        }
    }

    private static DecoratedKey key(int i)
    {
        ByteBuffer v = Int32Type.instance.decompose(i);
        return new BufferDecoratedKey(new ByteOrderedPartitioner().getToken(v), v);
    }

    private static ICompactionController controller(TableMetadata metadata)
    {
        return new ICompactionController()
        {
            public boolean compactingRepaired()
            {
                return false;
            }

            public String getKeyspace()
            {
                return "";
            }

            public String getColumnFamily()
            {
                return "";
            }

            public TableMetadata metadata()
            {
                return metadata;
            }

            public Iterable<UnfilteredRowIterator> shadowSources(DecoratedKey key, boolean tombstoneOnly)
            {
                return null;
            }

            public int gcBefore()
            {
                return 0;
            }

            public CompactionParams.TombstoneOption tombstoneOption()
            {
                return CompactionParams.TombstoneOption.NONE;
            }

            public LongPredicate getPurgeEvaluator(DecoratedKey key)
            {
                return time -> false;
            }

            public boolean isActive()
            {
                return true;
            }

            public void invalidateCachedPartition(DecoratedKey key)
            {
            }

            public boolean onlyPurgeRepairedTombstones()
            {
                return false;
            }

            public SecondaryIndexManager indexManager()
            {
                return null;
            }

            public boolean hasIndexes()
            {
                return false;
            }

            public void close() throws Exception
            {
            }
        };
    }

    private static void shuffle(Random random, Object[] data, int trgOffset, int trgSize, int srcOffset, int srcSize)
    {
        for (int i = 0 ; i < trgSize ; ++i)
        {
            int swap = srcOffset + srcSize == 0 ? 0 : random.nextInt(srcSize);
            Object tmp = data[swap];
            data[swap] = data[i + trgOffset];
            data[i + trgOffset] = tmp;
        }
    }

    @Benchmark
    public void compact(ThreadState state)
    {
        state.next.perform();
    }
}
