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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

public abstract class TxnQuery implements Query
{
    public static final TxnQuery ALL = new TxnQuery()
    {
        @Override
        public Result compute(TxnId txnId, Data data, @Nullable Read read, @Nullable Update update)
        {
            return data != null ? (TxnData) data : new TxnData();
        }
    };

    public static final TxnQuery NONE = new TxnQuery()
    {
        @Override
        public Result compute(TxnId txnId, Data data, @Nullable Read read, @Nullable Update update)
        {
            return new TxnData();
        }
    };

    private static final long SIZE = ObjectSizes.measure(ALL);

    private TxnQuery() {}

    public long estimatedSizeOnHeap()
    {
        return SIZE;
    }

    public static final IVersionedSerializer<TxnQuery> serializer = new IVersionedSerializer<TxnQuery>()
    {
        @Override
        public void serialize(TxnQuery query, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(query == null || query == ALL || query == NONE);
            out.writeByte(query == null ? 0 : query == ALL ? 1 : 2);
        }

        @Override
        public TxnQuery deserialize(DataInputPlus in, int version) throws IOException
        {
            switch (in.readByte())
            {
                default: throw new AssertionError();
                case 0: return null;
                case 1: return ALL;
                case 2: return NONE;
            }
        }

        @Override
        public long serializedSize(TxnQuery query, int version)
        {
            Preconditions.checkArgument(query == null || query == ALL || query == NONE);
            return TypeSizes.sizeof((byte)2);
        }
    };
}