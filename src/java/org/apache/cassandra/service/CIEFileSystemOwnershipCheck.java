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

package org.apache.cassandra.service;

import java.util.function.Supplier;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.exceptions.StartupException;

public class CIEFileSystemOwnershipCheck extends FileSystemOwnershipCheck
{
    static final String APP_NAME_PROPERTY                       = "ISApplicationName";
    static final String CLUSTER_NAME_PROPERTY                   = "ISClusterName";
    static final String INSTANCE_ID_PROPERTY                    = "ISInstanceId";

    public CIEFileSystemOwnershipCheck()
    {
        super();
    }

    @VisibleForTesting
    CIEFileSystemOwnershipCheck(Supplier<Iterable<String>> dirs)
    {
        super(dirs);
    }

    @Override
    protected String constructTokenFromProperties() throws StartupException
    {
        String app = System.getProperty(APP_NAME_PROPERTY);
        if (null == app || app.isEmpty())
            throw exception(String.format(MISSING_SYSTEM_PROPERTY, APP_NAME_PROPERTY));

        String cluster = System.getProperty(CLUSTER_NAME_PROPERTY);
        if (null == cluster || cluster.isEmpty())
            throw exception(String.format(MISSING_SYSTEM_PROPERTY, CLUSTER_NAME_PROPERTY));

        String instance = System.getProperty(INSTANCE_ID_PROPERTY);
        if (null == instance || instance.isEmpty())
            throw exception(String.format(MISSING_SYSTEM_PROPERTY, INSTANCE_ID_PROPERTY));

        return String.format("%s/%s/%s", app, cluster, instance);
    }

    private StartupException exception(String message)
    {
        return new StartupException(StartupException.ERR_WRONG_DISK_STATE, ERROR_PREFIX + message);
    }
}
