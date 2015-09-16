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

package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

/**
 * This is the same as DataInputBuffer, i.e. a stream for a fixed byte buffer,
 * except that we also implement FileDataInput by using an offset and a file path.
 */
public class OffsetDataInput extends DataInputBuffer
{
    private final long offset;

    public OffsetDataInput(ByteBuffer buffer, long offset)
    {
        super(buffer, false);
        this.offset = offset;
    }

    private long size()
    {
        return offset + buffer.capacity();
    }

    public void seek(long pos)
    {
        if (pos < offset || pos > offset + size())
            throw new IndexOutOfBoundsException();
        buffer.position((int) (pos - offset));
    }

    public long getFilePointer()
    {
        return offset + buffer.position();
    }
}
