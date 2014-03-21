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
package org.apache.cassandra.db.data;

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public interface Cell extends OnDiskAtom
{
    public static final int MAX_NAME_LENGTH = FBUtilities.MAX_UNSIGNED_SHORT;

    public static class Impl
    {
        public static Iterator<OnDiskAtom> onDiskIterator(final DataInput in,
                                                          final ColumnSerializer.Flag flag,
                                                          final int expireBefore,
                                                          final Descriptor.Version version,
                                                          final CellNameType type)
        {
            return new AbstractIterator<OnDiskAtom>()
            {
                protected OnDiskAtom computeNext()
                {
                    OnDiskAtom atom;
                    try
                    {
                        atom = type.onDiskAtomSerializer().deserializeFromSSTable(in, flag, expireBefore, version);
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                    if (atom == null)
                        return endOfData();

                    return atom;
                }
            };
        }
    }

    Cell withUpdatedName(CellName newName);

    Cell withUpdatedTimestamp(long newTimestamp);

    CellName name();

    ByteBuffer value();

    long timestamp();

    boolean isMarkedForDelete(long now);

    boolean isLive(long now);

    // Don't call unless the column is actually marked for delete.
    long getMarkedForDeleteAt();

    int dataSize();

    // returns the size of the Cell and all references on the heap, excluding any costs associated with byte arrays
    // that would be allocated by a localCopy, as these will be accounted for by the allocator
    long excessHeapSizeExcludingData();

    int serializedSize(CellNameType type, TypeSizes typeSizes);

    int serializationFlags();

    Cell diff(Cell cell);

    void updateDigest(MessageDigest digest);

    int getLocalDeletionTime();

    Cell reconcile(Cell cell);

    @Override
    boolean equals(Object o);

    @Override
    int hashCode();

    Cell localCopy(ColumnFamilyStore cfs, AbstractAllocator allocator);

    String getString(CellNameType comparator);

    void validateFields(CFMetaData metadata) throws MarshalException;
}
