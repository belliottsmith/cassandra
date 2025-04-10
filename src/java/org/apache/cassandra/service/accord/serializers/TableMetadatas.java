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
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;

import accord.utils.Invariants;
import accord.utils.SortedArrays;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;

import static accord.utils.SortedArrays.Search.FAST;

public abstract class TableMetadatas extends AbstractList<TableMetadata>
{
    private static final Comparator<Object> comparingId = Comparator.comparing(v -> ((TableMetadata) v).id);
    public static class Collector extends AbstractSortedCollector<TableMetadata, TableMetadatas>
    {
        @Override
        Comparator<Object> comparator()
        {
            return comparingId;
        }

        @Override
        TableMetadatas empty()
        {
            return TableMetadatas.none();
        }

        @Override
        TableMetadatas of(TableMetadata one)
        {
            return TableMetadatas.of(one);
        }

        @Override
        TableMetadatas copy(Object[] array, int count)
        {
            TableMetadata[] result = new TableMetadata[count];
            System.arraycopy(array, 0, result, 0, count);
            return TableMetadatas.of(result);
        }

        @Override
        TableMetadatas copyBtree(Object[] btree, int count)
        {
            TableMetadata[] result = new TableMetadata[count];
            int i = 0;
            for (TableMetadata v : BTree.<TableMetadata>iterable(btree))
                result[i++] = v;
            return TableMetadatas.of(result);
        }
    }

    public abstract int indexOf(TableMetadata find);
    public abstract int indexOf(TableId find);
    public abstract TableMetadata get(TableId tableId);

    public abstract void serialize(TableMetadata table, DataOutputPlus out) throws IOException;
    public abstract TableMetadata deserialize(DataInputPlus in) throws IOException;
    public abstract long serializedSize(TableMetadata table);

    public abstract void serializeSelf(DataOutputPlus out) throws IOException;
    public abstract long serializedSelfSize();

    public static TableMetadatas none()
    {
        return Multi.NONE;
    }

    public static TableMetadatas of(TableMetadata metadata)
    {
        return new One(metadata);
    }

    public static TableMetadatas of(TableMetadata ... metadatas)
    {
        if (metadatas.length == 0)
            return none();
        if (metadatas.length == 1)
            return new One(metadatas[0]);
        return new Multi(metadatas);
    }

    static class One extends TableMetadatas
    {
        final TableMetadata table;

        One(TableMetadata table)
        {
            this.table = table;
        }

        @Override
        public TableMetadata get(int index)
        {
            Invariants.require(index == 0);
            return table;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public int indexOf(TableMetadata find)
        {
            if (find.id.equals(table.id))
                return 0;
            return -1;
        }

        @Override
        public void serialize(TableMetadata table, DataOutputPlus out) throws IOException
        {
        }

        @Override
        public void serializeSelf(DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(1);
            table.id.serializeCompactComparable(out);
        }

        @Override
        public TableMetadata deserialize(DataInputPlus in) throws IOException
        {
            return table;
        }

        @Override
        public long serializedSize(TableMetadata table)
        {
            return 0;
        }

        @Override
        public long serializedSelfSize()
        {
            return TypeSizes.sizeofUnsignedVInt(1) + table.id.serializedCompactComparableSize();
        }

        @Override
        public int indexOf(TableId tableId)
        {
            if (tableId.equals(table.id))
                return 0;
            return -1;
        }

        @Override
        public TableMetadata get(TableId tableId)
        {
            if (tableId.equals(table.id))
                return table;
            return null;
        }
    }

    static class Multi extends TableMetadatas
    {
        static final TableMetadatas NONE = new Multi();

        final TableMetadata[] tables;

        Multi(TableMetadata ... tables)
        {
            this.tables = tables;
        }

        @Override
        public TableMetadata get(int index)
        {
            return tables[index];
        }

        @Override
        public int size()
        {
            return tables.length;
        }

        @Override
        public int indexOf(TableMetadata find)
        {
            return Arrays.binarySearch(tables, find, comparingId);
        }

        @Override
        public int indexOf(TableId find)
        {
            return SortedArrays.binarySearch(tables, 0, tables.length, find, (id, metadata) -> id.compareTo(metadata.id), FAST);
        }

        @Override
        public void serialize(TableMetadata table, DataOutputPlus out) throws IOException
        {
            int i = indexOf(table);
            if (i < 0)
                throw new IllegalStateException("TableMetadata for " + table + " not found in " + this);
            out.writeUnsignedVInt32(i);
        }

        @Override
        public void serializeSelf(DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(tables.length);
            for (TableMetadata table : tables)
                table.id.serializeCompactComparable(out);
        }

        @Override
        public TableMetadata deserialize(DataInputPlus in) throws IOException
        {
            return get(in.readUnsignedVInt32());
        }

        @Override
        public long serializedSize(TableMetadata table)
        {
            int i = indexOf(table);
            if (i < 0)
                throw new IllegalStateException("TableMetadata for " + table + " not found in " + this);
            return TypeSizes.sizeofUnsignedVInt(indexOf(table));
        }

        @Override
        public long serializedSelfSize()
        {
            long size = TypeSizes.sizeofUnsignedVInt(tables.length);
            for (TableMetadata table : tables)
                size += table.id.serializedCompactComparableSize();
            return size;
        }

        @Override
        public TableMetadata get(TableId tableId)
        {
            int i = indexOf(tableId);
            return i >= 0 ? tables[i] : null;
        }
    }

    public static TableMetadatas deserializeSelf(DataInputPlus in) throws IOException
    {
        int count = in.readUnsignedVInt32();
        if (count == 0)
            return none();
        if (count == 1)
            return new One(Schema.instance.getTableMetadata(TableId.deserializeCompactComparable(in)));
        TableMetadata[] array = new TableMetadata[count];
        for (int i = 0 ; i < count ; ++i)
            array[i] = Schema.instance.getTableMetadata(TableId.deserializeCompactComparable(in));
        return new Multi(array);
    }
}
