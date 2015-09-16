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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Extension to DataInput that provides support for reading varints
 */
public interface DataInputPlus extends DataInput
{

    long readVInt() throws IOException;
    long readUnsignedVInt() throws IOException;

    /**
     * Wrapper around an InputStream that provides no buffering but can decode varints
     */
    public class DataInputStreamPlus extends RebufferingInputStream
    {
        final InputStream in;
        public DataInputStreamPlus(InputStream in, int bufferSize)
        {
            super(ByteBuffer.allocate(bufferSize));
            buffer.limit(0);
            this.in = in;
        }

        protected void reBuffer() throws IOException
        {
            buffer.clear();
            int count = Math.min(in.available(), buffer.capacity());
            if (count == 0)
            {
                int v = in.read();
                if (v >= 0)
                    buffer.put((byte) v);
                buffer.flip();
            }
            else
            {
                int read = in.read(buffer.array(), 0, count);
                assert read == count;
                buffer.limit(read);
            }
        }

        public int available() throws IOException
        {
            return buffer.remaining() + in.available();
        }
    }
}
