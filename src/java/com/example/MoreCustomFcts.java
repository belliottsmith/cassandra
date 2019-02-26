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

package com.example;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.NativeScalarFunction;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class MoreCustomFcts
{
    private static final Function questionFct = new NativeScalarFunction("question", UTF8Type.instance, UTF8Type.instance)
    {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer bb = ByteBuffer.allocate(parameters.get(0).remaining() + 1);
            ByteBufferUtil.put(parameters.get(0), bb);
            bb.put((byte)'?');
            bb.flip();
            return bb;
        }
    };

    public static Collection<Function> all()
    {
        return ImmutableList.of(questionFct);
    }
}
