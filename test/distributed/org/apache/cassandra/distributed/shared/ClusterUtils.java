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

package org.apache.cassandra.distributed.shared;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;

/**
 * Utilities for working with jvm-dtest clusters.
 *
 * This class is marked as Isolated as it relies on lambdas, which are in a package that is marked as shared, so need to
 * tell jvm-dtest to not share this class.
 *
 * This class should never be called from within the cluster, always in the App ClassLoader.
 */
// partial fork of https://issues.apache.org/jira/browse/CASSANDRA-16213 / https://github.geo.apple.com/cie/cie-cassandra/pull/1969
// once that is added, this class will be filled out more
public class ClusterUtils
{
    /**
     * Stop an instance in a blocking manner.
     *
     * The main difference between this and {@link IInstance#shutdown()} is that the wait on the future will catch
     * the exceptions and throw as runtime.
     */
    public static void stopUnchecked(IInstance i)
    {
        Futures.getUnchecked(i.shutdown());
    }

    /**
     * Stop all the instances in the cluster.  This function is differe than {@link ICluster#close()} as it doesn't
     * clean up the cluster state, it only stops all the instances.
     */
    public static <I extends IInstance> void stopAll(ICluster<I> cluster)
    {
        cluster.stream().forEach(ClusterUtils::stopUnchecked);
    }

    /**
     * Get all data directories for the given instance.
     *
     * @param instance to get data directories for
     * @return data directories
     */
    public static List<File> getDataDirectories(IInstance instance)
    {
        IInstanceConfig conf = instance.config();
        // this isn't safe as it assumes the implementation of InstanceConfig
        // might need to get smarter... some day...
        String[] ds = (String[]) conf.get("data_file_directories");
        List<File> files = new ArrayList<>(ds.length);
        for (int i = 0; i < ds.length; i++)
            files.add(new File(ds[i]));
        return files;
    }

    /**
     * Get the commit log directory for the given instance.
     *
     * @param instance to get the commit log directory for
     * @return commit log directory
     */
    public static File getCommitLogDirectory(IInstance instance)
    {
        IInstanceConfig conf = instance.config();
        // this isn't safe as it assumes the implementation of InstanceConfig
        // might need to get smarter... some day...
        String d = (String) conf.get("commitlog_directory");
        return new File(d);
    }

    /**
     * Gets the name of the Partitioner for the given instance.
     *
     * @param instance to get partitioner from
     * @return partitioner name
     */
    public static String getPartitionerName(IInstance instance)
    {
        IInstanceConfig conf = instance.config();
        return (String) conf.get("partitioner");
    }
}
