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

package org.apache.cassandra.distributed;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessage;

public class MultiVersionCluster extends AbstractCluster<IVersionedInstance> implements ICluster, AutoCloseable
{
    MultiVersionCluster(File root, Versions.Version version, List<InstanceConfig> configs, ClassLoader sharedClassLoader)
    {
        super(root, version, configs, sharedClassLoader);
    }

    protected IVersionedInstance newInstanceWrapper(Versions.Version version, InstanceConfig config)
    {
        return new Wrapper(version, config);
    }

    public static MultiVersionCluster create(int nodeCount) throws Throwable
    {
        return create(nodeCount, MultiVersionCluster::new);
    }
    public static MultiVersionCluster create(int nodeCount, File root)
    {
        return create(nodeCount, Versions.CURRENT, root, MultiVersionCluster::new);
    }

    public static MultiVersionCluster create(int nodeCount, Versions.Version version) throws IOException
    {
        return create(nodeCount, version, Files.createTempDirectory("dtests").toFile(), MultiVersionCluster::new);
    }
    public static MultiVersionCluster create(int nodeCount, Versions.Version version, File root)
    {
        return create(nodeCount, version, root, MultiVersionCluster::new);
    }

}

