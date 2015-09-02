/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.stress.generate.values;

import java.util.Random;

import org.apache.cassandra.db.marshal.AbstractType;

public abstract class ByteString<T> extends Generator<T>
{
    protected final int byteCount;
    private final Random random = new Random();

    public ByteString(AbstractType<T> type, GeneratorConfig config, String name, Class<T> clazz)
    {
        super(type, config, name, clazz);
        byteCount = dataDistribution == null
                    ? 8
                    : (64 - Long.numberOfLeadingZeros(dataDistribution.maxValue())) / 8;
    }

    protected void setId(long id)
    {
        sizeDistribution.setSeed(id);
        if (dataDistribution != null)
            dataDistribution.setSeed(~id);
        else
            random.setSeed(~id);
    }

    protected long nextBytes()
    {
        return dataDistribution != null ? dataDistribution.next()
                                        : random.nextLong();
    }
}
