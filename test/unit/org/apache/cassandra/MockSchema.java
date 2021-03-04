/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.BufferedSegmentedFile;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MockSchema
{
    static
    {
        Memory offsets = Memory.allocate(4);
        offsets.setInt(0, 0);
        indexSummary = new IndexSummary(Murmur3Partitioner.instance, offsets, 0, Memory.allocate(4), 0, 0, 1, 1);
    }

    private static final AtomicInteger id = new AtomicInteger();
    public static final Keyspace ks = Keyspace.mockKS(KeyspaceMetadata.create("mockks", KeyspaceParams.simpleTransient(2)));

    public static final IndexSummary indexSummary;
    public static final File tempFile = temp("mocksegmentedfile");

    public static Memtable memtable(ColumnFamilyStore cfs)
    {
        return new Memtable(cfs.metadata);
    }

    public static SSTableReader sstable(int generation, ColumnFamilyStore cfs)
    {
        return sstable(generation, false, cfs);
    }

    public static SSTableReader sstable(int generation, long first, long last, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, false, cfs, readerBounds(first), readerBounds(last));
    }

    public static SSTableReader sstable(int generation, long first, long last, int minLocalDeletionTime, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, false, cfs, readerBounds(first), readerBounds(last), minLocalDeletionTime);
    }

    public static SSTableReader sstable(int generation, boolean keepRef, ColumnFamilyStore cfs)
    {
        return sstable(generation, 0, keepRef, cfs);
    }

    public static SSTableReader sstable(int generation, int size, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, false, cfs);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, keepRef, cfs, readerBounds(generation), readerBounds(generation));
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, ColumnFamilyStore cfs, DecoratedKey first, DecoratedKey last)
    {
        return sstable(generation, size, keepRef, cfs, first, last, Integer.MAX_VALUE);
    }

    public static SSTableReader sstableWithTimestamp(int generation, int size, long timestamp, ColumnFamilyStore cfs)
    {
        return sstable(generation, size, false, cfs, readerBounds(generation), readerBounds(generation), Integer.MAX_VALUE, timestamp);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, ColumnFamilyStore cfs, DecoratedKey first, DecoratedKey last, int minLocalDeletionTime)
    {
        return sstable(generation, size, keepRef, cfs, first, last, minLocalDeletionTime, System.currentTimeMillis() * 1000);
    }

    public static SSTableReader sstable(int generation, int size, boolean keepRef, ColumnFamilyStore cfs, DecoratedKey first, DecoratedKey last, int minLocalDeletionTime, long timestamp)
    {
        Descriptor descriptor = new Descriptor(cfs.getDirectories().getDirectoryForNewSSTables(),
                                               cfs.keyspace.getName(),
                                               cfs.getColumnFamilyName(),
                                               generation);
        Set<Component> components = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.TOC);
        for (Component component : components)
        {
            File file = new File(descriptor.filenameFor(component));
            try
            {
                file.createNewFile();
            }
            catch (IOException e)
            {
            }
        }
        // create the segmentedFile with the correct size for SSTableReader#onDiskLength
        SegmentedFile segmentedFile = new BufferedSegmentedFile(new ChannelProxy(tempFile), RandomAccessReader.DEFAULT_BUFFER_SIZE, size);
        if (size > 0)
        {
            try
            {
                File file = new File(descriptor.filenameFor(Component.DATA));
                try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
                {
                    raf.setLength(size);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        SerializationHeader header = SerializationHeader.make(cfs.metadata, Collections.emptyList());
        MetadataCollector collector = new MetadataCollector(cfs.metadata.comparator);
        collector.update(new DeletionTime(timestamp, minLocalDeletionTime));
        StatsMetadata metadata = (StatsMetadata) collector.finalizeMetadata(cfs.metadata.partitioner.getClass().getCanonicalName(), 0.01f, ActiveRepairService.UNREPAIRED_SSTABLE, null, header)
                                                          .get(MetadataType.STATS);
        SSTableReader reader = SSTableReader.internalOpen(descriptor, components, cfs.metadata,
                                                          segmentedFile.sharedCopy(), segmentedFile.sharedCopy(), indexSummary.sharedCopy(),
                                                          new AlwaysPresentFilter(), 1L, metadata, SSTableReader.OpenReason.NORMAL, header);
        reader.first = first;
        reader.last = last;
        if (!keepRef)
            reader.selfRef().release();
        return reader;
    }

    public static ColumnFamilyStore newCFS()
    {
        return newCFS(ks.getName());
    }

    public static ColumnFamilyStore newCFS(Consumer<CFMetaData> updateMetadata)
    {
        return newCFS(ks.getName(), updateMetadata);
    }

    public static ColumnFamilyStore newCFS(String ksname)
    {
        String cfname = "mockcf" + (id.incrementAndGet());
        return newCFS(ksname, cfname, newCFMetaData(ksname, cfname));
    }

    public static ColumnFamilyStore newCFS(String ksname, Consumer<CFMetaData> updateMetadata)
    {
        String cfname = "mockcf" + (id.incrementAndGet());
        CFMetaData metadata = newCFMetaData(ksname, cfname);
        updateMetadata.accept(metadata);
        return newCFS(ksname, cfname, metadata);
    }

    private static ColumnFamilyStore newCFS(String ksname, String cfname, CFMetaData metadata)
    {
        Keyspace keyspace = Keyspace.mockKS(KeyspaceMetadata.create(ksname, KeyspaceParams.simpleTransient(2)));
        return new ColumnFamilyStore(keyspace, cfname, 0, metadata, new Directories(metadata), false, false);
    }

    public static ColumnFamilyStore newCFSOverrideFlush()
    {
        String cfname = "mockcf" + (id.incrementAndGet());
        CFMetaData metadata = newCFMetaData(ks.getName(), cfname);
        Keyspace keyspace = Keyspace.mockKS(KeyspaceMetadata.create(ks.getName(), KeyspaceParams.simpleTransient(2)));
        return new ColumnFamilyStore(keyspace, cfname, 0, metadata, new Directories(metadata), false, false) {
            @Override
            public ReplayPosition forceBlockingFlush()
            {
                return ReplayPosition.NONE;
            }
        };
    }

    public static CFMetaData newCFMetaData(String ksname, String cfname)
    {
        CFMetaData metadata = CFMetaData.Builder.create(ksname, cfname)
                                                .addPartitionKey("key", UTF8Type.instance)
                                                .addClusteringColumn("col", UTF8Type.instance)
                                                .addRegularColumn("value", UTF8Type.instance)
                                                .withPartitioner(Murmur3Partitioner.instance)
                                                .build();
        metadata.caching(CachingParams.CACHE_NOTHING);
        return metadata;
    }

    public static BufferDecoratedKey readerBounds(long generation)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(generation), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    private static File temp(String id)
    {
        File file = FileUtils.createTempFile(id, "tmp");
        file.deleteOnExit();
        return file;
    }

    public static void cleanup()
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                continue;
            String[] children = dir.list();
            for (String child : children)
                FileUtils.deleteRecursive(new File(dir, child));
        }
    }
}
