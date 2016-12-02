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
package org.apache.cassandra.io.sstable;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.*;

/**
 * Provides helper to serialize, deserialize and use column indexes.
 */
public final class IndexHelper
{
    private IndexHelper()
    {
    }

    /**
     * The index of the IndexInfo in which a scan starting with @name should begin.
     *
     * @param name name to search for
     * @param indexList list of the indexInfo objects
     * @param comparator the comparator to use
     * @param reversed whether or not the search is reversed, i.e. we scan forward or backward from name
     * @param lastIndex where to start the search from in indexList
     *
     * @return int index
     */
    public static int indexFor(ClusteringPrefix name, List<IndexInfo> indexList, ClusteringComparator comparator, boolean reversed, int lastIndex)
    {
        IndexInfo target = new IndexInfo(name, 0, 0, comparator);
        /*
        Take the example from the unit test, and say your index looks like this:
        [0..5][10..15][20..25]
        and you look for the slice [13..17].

        When doing forward slice, we are doing a binary search comparing 13 (the start of the query)
        to the lastName part of the index slot. You'll end up with the "first" slot, going from left to right,
        that may contain the start.

        When doing a reverse slice, we do the same thing, only using as a start column the end of the query,
        i.e. 17 in this example, compared to the firstName part of the index slots.  bsearch will give us the
        first slot where firstName > start ([20..25] here), so we subtract an extra one to get the slot just before.
        */
        int startIdx = 0;
        List<IndexInfo> toSearch = indexList;
        if (reversed)
        {
            if (lastIndex < indexList.size() - 1)
            {
                toSearch = indexList.subList(0, lastIndex + 1);
            }
        }
        else
        {
            if (lastIndex > 0)
            {
                startIdx = lastIndex;
                toSearch = indexList.subList(lastIndex, indexList.size());
            }
        }
        int index = Collections.binarySearch(toSearch, target, comparator.indexComparator(reversed));
        return startIdx + (index < 0 ? -index - (reversed ? 2 : 1) : index);
    }

    public static class IndexInfo
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new IndexInfo(null, 0, 0, null));

        // see <rdar://problem/27205556> Cass: Composite abstraction in 2.1 creates significant GC pressure (especially on wide rows)
        // for details on why we're doing all this ClusteringPrefix -> ByteBuffer -> ByteBuffer[] -> ClusteringPrefix stuff
        private final long offset;
        private final long width;
        private final ByteBuffer firstName;
        private final ByteBuffer lastName;
        private final ClusteringPrefix.Kind firstNameKind;
        private final ClusteringPrefix.Kind lastNameKind;
        private final ClusteringPrefix clusteringName;
        private final ClusteringComparator comparator;

        // If at the end of the index block there is an open range tombstone marker, this marker
        // deletion infos. null otherwise.
        public final DeletionTime endOpenMarker;

        public IndexInfo(ClusteringPrefix firstName,
                         ClusteringPrefix lastName,
                         long offset,
                         long width,
                         DeletionTime endOpenMarker,
                         ClusteringComparator comparator)
        {
            this.firstName = ByteBufferUtil.merge(firstName.getRawValues());
            this.firstNameKind = firstName.kind();
            this.lastName = ByteBufferUtil.merge(lastName.getRawValues());
            this.lastNameKind = lastName.kind();
            this.clusteringName = null;
            this.offset = offset;
            this.width = width;
            this.endOpenMarker = endOpenMarker;
            this.comparator = comparator;
        }

        private IndexInfo(ClusteringPrefix name,
                         long offset,
                         long width,
                         ClusteringComparator comparator)
        {
            this.clusteringName = name;
            this.firstName = null;
            this.lastName = null;
            this.firstNameKind = null;
            this.lastNameKind = null;
            this.offset = offset;
            this.width = width;
            this.endOpenMarker = null;
            this.comparator = comparator;
        }

        public static class Serializer
        {
            // This is the default index size that we use to delta-encode width when serializing so we get better vint-encoding.
            // This is imperfect as user can change the index size and ideally we would save the index size used with each index file
            // to use as base. However, that's a bit more involved a change that we want for now and very seldom do use change the index
            // size so using the default is almost surely better than using no base at all.
            public static final long WIDTH_BASE = 64 * 1024;

            private final ISerializer<ClusteringPrefix> clusteringSerializer;
            private final Version version;
            private final ClusteringComparator comparator;

            public Serializer(CFMetaData metadata, Version version, SerializationHeader header)
            {
                this.clusteringSerializer = metadata.serializers().indexEntryClusteringPrefixSerializer(version, header);
                this.version = version;
                this.comparator = metadata.comparator;
            }

            public void serialize(IndexInfo info, DataOutputPlus out) throws IOException
            {
                assert version.storeRows() : "We read old index files but we should never write them";

                clusteringSerializer.serialize(info.getFirstName(), out);
                clusteringSerializer.serialize(info.getLastName(), out);
                out.writeUnsignedVInt(info.offset);
                out.writeVInt(info.width - WIDTH_BASE);

                out.writeBoolean(info.endOpenMarker != null);
                if (info.endOpenMarker != null)
                    DeletionTime.serializer.serialize(info.endOpenMarker, out);
            }

            public IndexInfo deserialize(DataInputPlus in) throws IOException
            {
                ClusteringPrefix firstName = clusteringSerializer.deserialize(in);
                ClusteringPrefix lastName = clusteringSerializer.deserialize(in);
                long offset;
                long width;
                DeletionTime endOpenMarker = null;
                if (version.storeRows())
                {
                    offset = in.readUnsignedVInt();
                    width = in.readVInt() + WIDTH_BASE;
                    if (in.readBoolean())
                        endOpenMarker = DeletionTime.serializer.deserialize(in);
                }
                else
                {
                    offset = in.readLong();
                    width = in.readLong();
                }
                return new IndexInfo(firstName, lastName, offset, width, endOpenMarker, comparator);
            }

            public long serializedSize(IndexInfo info)
            {
                assert version.storeRows() : "We read old index files but we should never write them";

                long size = clusteringSerializer.serializedSize(info.getFirstName())
                          + clusteringSerializer.serializedSize(info.getLastName())
                          + TypeSizes.sizeofUnsignedVInt(info.offset)
                          + TypeSizes.sizeofVInt(info.width - WIDTH_BASE)
                          + TypeSizes.sizeof(info.endOpenMarker != null);

                if (info.endOpenMarker != null)
                    size += DeletionTime.serializer.serializedSize(info.endOpenMarker);
                return size;
            }
        }

        public long unsharedHeapSize()
        {
            return EMPTY_SIZE
                   + (clusteringName != null ? clusteringName.unsharedHeapSize() : firstName.remaining() + lastName.remaining())
                   + (endOpenMarker == null ? 0 : endOpenMarker.unsharedHeapSize());
        }

        public ClusteringPrefix getFirstName()
        {
            if (clusteringName != null)
                return clusteringName;

            ByteBuffer[] splitSegments = ByteBufferUtil.splitSegmentedByteBuffer(firstName.duplicate());
            return (firstNameKind == ClusteringPrefix.Kind.CLUSTERING)
                   ? comparator.make((Object[]) splitSegments)
                   : new RangeTombstone.Bound(firstNameKind, splitSegments);
        }

        public ClusteringPrefix getLastName()
        {
            if (clusteringName != null)
                return clusteringName;

            ByteBuffer[] splitSegments = ByteBufferUtil.splitSegmentedByteBuffer(lastName.duplicate());
            return (lastNameKind == ClusteringPrefix.Kind.CLUSTERING)
                   ? comparator.make((Object[]) splitSegments)
                   : new RangeTombstone.Bound(lastNameKind, splitSegments);
        }

        public ByteBuffer getFirstNameAsByteBuffer()
        {
            return (firstName == null) ? ByteBufferUtil.merge(clusteringName.getRawValues()) : firstName.duplicate();
        }

        public ByteBuffer getLastNameAsByteBuffer()
        {
            return (lastName == null) ? ByteBufferUtil.merge(clusteringName.getRawValues()) : lastName.duplicate();
        }

        public long getOffset()
        {
            return offset;
        }

        public long getWidth()
        {
            return width;
        }
    }
}
