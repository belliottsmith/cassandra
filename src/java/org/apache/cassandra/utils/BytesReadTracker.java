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
package org.apache.cassandra.utils;

import java.io.InputStream;

import org.apache.cassandra.io.util.DataInputStreamPlus;
import org.apache.cassandra.io.util.LimitInputStream;

/**
 * This class is to track bytes read from given DataInput
 */
public class BytesReadTracker extends DataInputStreamPlus
{
    final LimitInputStream in;
    final long totalLength;

    public BytesReadTracker(InputStream source, long totalLength)
    {
        this(new LimitInputStream(source, totalLength));
    }

    public BytesReadTracker(LimitInputStream in)
    {
        super(in, 8 << 10);
        this.totalLength = in.remainingBytes();
        this.in = in;
    }

    public long getBytesRead()
    {
        return totalLength - in.remainingBytes();
    }
}