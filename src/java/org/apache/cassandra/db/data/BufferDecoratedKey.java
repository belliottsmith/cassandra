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

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

/**
 * Represents a decorated key, handy for certain operations
 * where just working with strings gets slow.
 *
 * We do a lot of sorting of DecoratedKeys, so for speed, we assume that tokens correspond one-to-one with keys.
 * This is not quite correct in the case of RandomPartitioner (which uses MD5 to hash keys to tokens);
 * if this matters, you can subclass RP to use a stronger hash, or use a non-lossy tokenization scheme (as in the
 * OrderPreservingPartitioner classes).
 */
public class BufferDecoratedKey implements DecoratedKey
{
    private final Token token;
    private final ByteBuffer key;

    public BufferDecoratedKey(Token token, ByteBuffer key)
    {
        assert token != null && key != null;
        this.key = key;
        this.token = token;
    }

    public Token token()
    {
        return token;
    }

    public ByteBuffer key()
    {
        return key;
    }

    public boolean isMinimum(IPartitioner partitioner)
    {
        return Impl.isMinimum(partitioner);
    }
    public Kind kind()
    {
        return Impl.kind();
    }
    public boolean isMinimum()
    {
        return Impl.isMinimum();
    }
    public int compareTo(RowPosition that)
    {
        return Impl.compareTo(this, that);
    }
    public int hashCode()
    {
        return Impl.hashCode(this);
    }
    public boolean equals(Object obj)
    {
        return Impl.equals(this, obj);
    }
    public String toString()
    {
        return Impl.toString(this);
    }
}
