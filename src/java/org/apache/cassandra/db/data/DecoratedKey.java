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

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface DecoratedKey extends RowPosition
{
    public static final Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
    {
        public int compare(DecoratedKey o1, DecoratedKey o2)
        {
            return o1.compareTo(o2);
        }
    };

    public ByteBuffer key();

    public static class Impl
    {
        public static int compareTo(IPartitioner partitioner, ByteBuffer key, RowPosition position)
        {
            // delegate to Token.KeyBound if needed
            if (!(position instanceof DecoratedKey))
                return -position.compareTo(partitioner.decorateKey(key));

            DecoratedKey otherKey = (DecoratedKey) position;
            int cmp = partitioner.getToken(key).compareTo(otherKey.token());
            return cmp == 0 ? ByteBufferUtil.compareUnsigned(key, otherKey.key()) : cmp;
        }

        public static int hashCode(DecoratedKey me)
        {
            return me.key().hashCode(); // hash of key is enough
        }

        public static boolean equals(DecoratedKey me, Object obj)
        {
            if (me == obj)
                return true;
            if (!(obj instanceof DecoratedKey))
                return false;

            DecoratedKey other = (DecoratedKey)obj;
            // TODO : optimise this for native comparison
            return ByteBufferUtil.compareUnsigned(me.key(), other.key()) == 0; // we compare faster than BB.equals for array backed BB
        }

        public static int compareTo(DecoratedKey me, RowPosition pos)
        {
            if (me == pos)
                return 0;

            // delegate to Token.KeyBound if needed
            if (!(pos instanceof DecoratedKey))
                return -pos.compareTo(me);

            // TODO : optimise this for native comparison
            DecoratedKey otherKey = (DecoratedKey) pos;
            int cmp = me.token().compareTo(otherKey.token());
            return cmp == 0 ? ByteBufferUtil.compareUnsigned(me.key(), otherKey.key()) : cmp;
        }

        public static boolean isMinimum(IPartitioner partitioner)
        {
            // A DecoratedKey can never be the minimum position on the ring
            return false;
        }

        public static boolean isMinimum()
        {
            return false;
        }

        public static Kind kind()
        {
            return Kind.ROW_KEY;
        }

        public static String toString(DecoratedKey me)
        {
            String keystring = me.key() == null ? "null" : ByteBufferUtil.bytesToHex(me.key());
            return "DecoratedKey(" + me.token() + ", " + keystring + ")";
        }
    }

}
