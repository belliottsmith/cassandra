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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.LazyToString;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.ObjectSizes;

public class RowIndexEntry<T> implements IMeasurableMemory
{
    private static final NoSpamLogger LARGE_PARTITION_LOGGER = NoSpamLogger.getLogger(LoggerFactory.getLogger(IndexSerializer.class), 1, TimeUnit.SECONDS);
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowIndexEntry(0));

    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    protected int promotedSize(IndexHelper.IndexInfo.Serializer idxSerializer)
    {
        return 0;
    }

    public static RowIndexEntry<IndexHelper.IndexInfo> create(long position, DeletionTime deletionTime, ColumnIndex index)
    {
        assert index != null;
        assert deletionTime != null;

        // we only consider the columns summary when determining whether to create an IndexedEntry,
        // since if there are insufficient columns to be worth indexing we're going to seek to
        // the beginning of the row anyway, so we might as well read the tombstone there as well.
        if (index.columnsIndex.size() > 1)
            return new IndexedEntry(position, deletionTime, index.partitionHeaderLength, index.columnsIndex);
        else
            return new RowIndexEntry<>(position);
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return !columnsIndex().isEmpty();
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the offset to the start of the header information for this row.
     * For some formats this may not be the start of the row.
     */
    public long headerOffset()
    {
        return 0;
    }

    /**
     * The length of the row header (partition key, partition deletion and static row).
     * This value is only provided for indexed entries and this method will throw
     * {@code UnsupportedOperationException} if {@code !isIndexed()}.
     */
    public long headerLength()
    {
        throw new UnsupportedOperationException();
    }

    public List<T> columnsIndex()
    {
        return Collections.emptyList();
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    public interface IndexSerializer<T>
    {
        void serialize(RowIndexEntry<T> rie, DataOutputPlus out) throws IOException;
        RowIndexEntry<T> deserialize(DataInputPlus in, ByteBuffer key) throws IOException;
        int serializedSize(RowIndexEntry<T> rie);
    }

    private static String extractQuery(ReadCommand query)
    {
        String queryStr;
        try {
            queryStr = query.toCQLString();
        }
        catch (Exception e)
        {
            // in testing found that internal code can cause toCQLString to throw a validation exception (data doesn't match type)
            // but its not expected to happen in CQL since it is supposed to validate; this gaurd is a defensive check
            // to pretect from cases not caught by CQL
            queryStr = "SELECT <unknown> FROM " + query.metadata().ksName + "." + query.metadata().cfName + " WHERE <unknown>";
            LARGE_PARTITION_LOGGER.error("ReadCommand(" + queryStr + ") caused .toCQLString() throw a unexpected exception", e);
        }
        return queryStr;
    }

    public static class Serializer implements IndexSerializer<IndexHelper.IndexInfo>
    {
        private final IndexHelper.IndexInfo.Serializer idxSerializer;
        private final Descriptor descriptor;

        public Serializer(CFMetaData metadata, Descriptor descriptor, SerializationHeader header)
        {
            this.idxSerializer = new IndexHelper.IndexInfo.Serializer(metadata, descriptor.version, header);
            this.descriptor = descriptor;
        }

        public void serialize(RowIndexEntry<IndexHelper.IndexInfo> rie, DataOutputPlus out) throws IOException
        {
            assert descriptor.version.storeRows() : "We read old index files but we should never write them";

            out.writeUnsignedVInt(rie.position);
            out.writeUnsignedVInt(rie.promotedSize(idxSerializer));

            if (rie.isIndexed())
            {
                out.writeUnsignedVInt(rie.headerLength());
                DeletionTime.serializer.serialize(rie.deletionTime(), out);
                out.writeUnsignedVInt(rie.columnsIndex().size());

                // Calculate and write the offsets to the IndexInfo objects.

                int[] offsets = new int[rie.columnsIndex().size()];

                if (out.hasPosition())
                {
                    // Out is usually a SequentialWriter, so using the file-pointer is fine to generate the offsets.
                    // A DataOutputBuffer also works.
                    long start = out.position();
                    int i = 0;
                    for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    {
                        offsets[i] = i == 0 ? 0 : (int)(out.position() - start);
                        i++;
                        idxSerializer.serialize(info, out);
                    }
                }
                else
                {
                    // Not sure this branch will ever be needed, but if it is called, it has to calculate the
                    // serialized sizes instead of simply using the file-pointer.
                    int i = 0;
                    int offset = 0;
                    for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    {
                        offsets[i++] = offset;
                        idxSerializer.serialize(info, out);
                        offset += idxSerializer.serializedSize(info);
                    }
                }

                for (int off : offsets)
                    out.writeInt(off);
            }
        }


        @VisibleForTesting
        public static long estimateMaterializedIndexSize(int entries, int bytes)
        {
            long overhead = IndexHelper.IndexInfo.EMPTY_SIZE
                            + AbstractClusteringPrefix.EMPTY_SIZE
                            + DeletionTime.EMPTY_SIZE;

            return (overhead * entries) + bytes;
        }

        public RowIndexEntry<IndexHelper.IndexInfo> deserialize(DataInputPlus in, ByteBuffer key) throws IOException
        {
            if (!descriptor.version.storeRows())
            {
                long position = in.readLong();

                int size = in.readInt();
                if (size > 0)
                {
                    DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);

                    int entries = in.readInt();
                    maybeAbortLargeIndexRead(key, entries, size);

                    List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<>(entries);

                    long headerLength = 0L;
                    for (int i = 0; i < entries; i++)
                    {
                        IndexHelper.IndexInfo info = idxSerializer.deserialize(in);
                        columnsIndex.add(info);
                        if (i == 0)
                            headerLength = info.getOffset();
                    }

                    maybeLogLargePartitionIndexWarning(key, columnsIndex);

                    return new IndexedEntry(position, deletionTime, headerLength, columnsIndex);
                }
                else
                {
                    return new RowIndexEntry<>(position);
                }
            }

            long position = in.readUnsignedVInt();

            int size = (int)in.readUnsignedVInt();
            if (size > 0)
            {
                long headerLength = in.readUnsignedVInt();
                DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);

                int entries = (int)in.readUnsignedVInt();
                maybeAbortLargeIndexRead(key, entries, size);

                List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<>(entries);
                for (int i = 0; i < entries; i++)
                    columnsIndex.add(idxSerializer.deserialize(in));

                in.skipBytesFully(entries * TypeSizes.sizeof(0));

                maybeLogLargePartitionIndexWarning(key, columnsIndex);

                return new IndexedEntry(position, deletionTime, headerLength, columnsIndex);
            }
            else
            {
                return new RowIndexEntry<>(position);
            }
        }

        private void maybeAbortLargeIndexRead(ByteBuffer key, int entries, int bytes)
        {
            long estimatedMemory = estimateMaterializedIndexSize(entries, bytes);
            long failureThreshold = DatabaseDescriptor.getLargePartitionIndexFailureThreshold();
            if (failureThreshold <= 0 || estimatedMemory <= failureThreshold)
                return;
            // only check when command is present that way only reads are captured
            ReadCommand command = ReadCommand.getCommand();
            if (command == null || Schema.isInternalKeyspace(command.metadata().ksName))
                return;

            // only dedup on the ks/cf/key triplet
            String keyStr;
            try
            {
                keyStr = command.metadata().getKeyValidator().getString(key);
            }
            catch (Exception e)
            {
                // if parsing the key fails, fall back to normal .toString()
                // This isn't expected to happen, but trying to be defensive so the behavior isn't changed for large
                // partition queries.
                LARGE_PARTITION_LOGGER.error("Partition key failed to parse with {}", command.metadata().getKeyValidator(), e);
                keyStr = ByteBufferUtil.bytesToHex(key);
            }

            LARGE_PARTITION_LOGGER.warn("Aborting large partition index read "
                                        + descriptor.ksname + "/" + descriptor.cfname + ":" + keyStr
                                        + " (estimated index memory {}) from sstable {} with {} index info entries; query: {}",
                                        estimatedMemory, descriptor.generation, entries,
                                        LazyToString.of(() -> extractQuery(command)));

            byte[] currentParam = MessageParams.get(ReadCommand.INDEX_SIZE_ABORT);
            if (currentParam == null || ByteArrayUtil.getLong(currentParam) < estimatedMemory)
                MessageParams.add(ReadCommand.INDEX_SIZE_ABORT, estimatedMemory);

            throw new RowIndexOversizeException(estimatedMemory);
        }

        // TODO: warnings are emitted here
        private void maybeLogLargePartitionIndexWarning(ByteBuffer key, List<IndexHelper.IndexInfo> columnsIndex)
        {
            int entries = columnsIndex.size();
            if (entries == 0)
                return;
            // only check when command is present that way only reads are captured
            ReadCommand command = ReadCommand.getCommand();
            if (command == null)
                return;

            // compute a estimate of heap cost for the IndexInfo list and warn if too large
            long memoryEstimate = columnsIndex.stream().mapToLong(IndexHelper.IndexInfo::unsharedHeapSize).sum();
            if (memoryEstimate < DatabaseDescriptor.getLargePartitionIndexWarningThreshold())
                return;

            byte[] currentParam = MessageParams.get(ReadCommand.INDEX_SIZE_WARNING);
            if (currentParam == null || ByteArrayUtil.getLong(currentParam) < memoryEstimate)
                MessageParams.add(ReadCommand.INDEX_SIZE_WARNING, memoryEstimate);

            Keyspace.open(descriptor.ksname)
                    .getColumnFamilyStore(descriptor.cfname)
                    .metric.largePartitionIndexBytes.update(memoryEstimate);

            // Column index offsets are relative to position, so don't care about position.
            // Find the last column which stores offset and how large the data range is;
            // the sum of offset and width should represent the size of the partition
            IndexHelper.IndexInfo lastIdexInfo = columnsIndex.get(columnsIndex.size() - 1);
            long expectedPartitionSize = lastIdexInfo.getOffset() + lastIdexInfo.getWidth();

            // only dedup on the ks/cf/key triplet
            String keyStr;
            try
            {
                keyStr = command.metadata().getKeyValidator().getString(key);
            }
            catch (Exception e)
            {
                // if parsing the key fails, fall back to normal .toString()
                // This isn't expected to happen, but trying to be defensive so the behavior isn't changed for large
                // partition queries.
                LARGE_PARTITION_LOGGER.error("Partition key failed to parse with {}", command.metadata().getKeyValidator(), e);
                keyStr = ByteBufferUtil.bytesToHex(key);
            }
            LARGE_PARTITION_LOGGER.warn("Reading large partition index "
                                        + descriptor.ksname + "/" + descriptor.cfname + ":" + keyStr
                                        + " (partition bytes {}, index memory {}) from sstable {} with {} index info entries; query: {}",
                                        expectedPartitionSize, memoryEstimate, descriptor.generation, entries,
                                        LazyToString.of(() -> extractQuery(command)));
        }

        // Reads only the data 'position' of the index entry and returns it. Note that this left 'in' in the middle
        // of reading an entry, so this is only useful if you know what you are doing and in most case 'deserialize'
        // should be used instead.
        public static long readPosition(DataInputPlus in, Version version) throws IOException
        {
            return version.storeRows() ? in.readUnsignedVInt() : in.readLong();
        }

        public static void skip(DataInputPlus in, Version version) throws IOException
        {
            readPosition(in, version);
            skipPromotedIndex(in, version);
        }

        private static void skipPromotedIndex(DataInputPlus in, Version version) throws IOException
        {
            int size = version.storeRows() ? (int)in.readUnsignedVInt() : in.readInt();
            if (size <= 0)
                return;

            in.skipBytesFully(size);
        }

        public int serializedSize(RowIndexEntry<IndexHelper.IndexInfo> rie)
        {
            assert descriptor.version.storeRows() : "We read old index files but we should never write them";

            int indexedSize = 0;
            if (rie.isIndexed())
            {
                List<IndexHelper.IndexInfo> index = rie.columnsIndex();

                indexedSize += TypeSizes.sizeofUnsignedVInt(rie.headerLength());
                indexedSize += DeletionTime.serializer.serializedSize(rie.deletionTime());
                indexedSize += TypeSizes.sizeofUnsignedVInt(index.size());

                for (IndexHelper.IndexInfo info : index)
                    indexedSize += idxSerializer.serializedSize(info);

                indexedSize += index.size() * TypeSizes.sizeof(0);
            }

            return TypeSizes.sizeofUnsignedVInt(rie.position) + TypeSizes.sizeofUnsignedVInt(indexedSize) + indexedSize;
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed.
     */
    private static class IndexedEntry extends RowIndexEntry<IndexHelper.IndexInfo>
    {
        private final DeletionTime deletionTime;

        // The offset in the file when the index entry end
        private final long headerLength;
        private final List<IndexHelper.IndexInfo> columnsIndex;
        private static final long BASE_SIZE =
                ObjectSizes.measure(new IndexedEntry(0, DeletionTime.LIVE, 0, Arrays.<IndexHelper.IndexInfo>asList(null, null)))
              + ObjectSizes.measure(new ArrayList<>(1));

        private IndexedEntry(long position, DeletionTime deletionTime, long headerLength, List<IndexHelper.IndexInfo> columnsIndex)
        {
            super(position);
            assert deletionTime != null;
            assert columnsIndex != null && columnsIndex.size() > 1;
            this.deletionTime = deletionTime;
            this.headerLength = headerLength;
            this.columnsIndex = columnsIndex;
        }

        @Override
        public DeletionTime deletionTime()
        {
            return deletionTime;
        }

        @Override
        public long headerLength()
        {
            return headerLength;
        }

        @Override
        public List<IndexHelper.IndexInfo> columnsIndex()
        {
            return columnsIndex;
        }

        @Override
        protected int promotedSize(IndexHelper.IndexInfo.Serializer idxSerializer)
        {
            long size = TypeSizes.sizeofUnsignedVInt(headerLength)
                      + DeletionTime.serializer.serializedSize(deletionTime)
                      + TypeSizes.sizeofUnsignedVInt(columnsIndex.size()); // number of entries
            for (IndexHelper.IndexInfo info : columnsIndex)
                size += idxSerializer.serializedSize(info);

            size += columnsIndex.size() * TypeSizes.sizeof(0);

            return Ints.checkedCast(size);
        }

        @Override
        public long unsharedHeapSize()
        {
            long entrySize = 0;
            for (IndexHelper.IndexInfo idx : columnsIndex)
                entrySize += idx.unsharedHeapSize();

            return BASE_SIZE
                   + entrySize
                   + deletionTime.unsharedHeapSize()
                   + ObjectSizes.sizeOfReferenceArray(columnsIndex.size());
        }
    }
}
