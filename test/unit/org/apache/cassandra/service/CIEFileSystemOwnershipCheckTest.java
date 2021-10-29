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

import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.config.StartupChecksOptions.ENABLED_PROPERTY;
import static org.apache.cassandra.service.CIEFileSystemOwnershipCheck.APP_NAME_PROPERTY;
import static org.apache.cassandra.service.CIEFileSystemOwnershipCheck.CLUSTER_NAME_PROPERTY;
import static org.apache.cassandra.service.CIEFileSystemOwnershipCheck.INSTANCE_ID_PROPERTY;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.*;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.filesystem_ownership;


public class CIEFileSystemOwnershipCheckTest extends AbstractFilesystemOwnershipCheckTest
{
    @Before
    public void setup()
    {
        super.setup();
        options.getConfig(filesystem_ownership).put(ENABLED_PROPERTY, "true");
        String tokenPart1 = makeRandomString(10);
        String tokenPart2 = makeRandomString(10);
        String tokenPart3 = makeRandomString(10);

        System.setProperty(APP_NAME_PROPERTY, tokenPart1);
        System.setProperty(CLUSTER_NAME_PROPERTY, tokenPart2);
        System.setProperty(INSTANCE_ID_PROPERTY, tokenPart3);
        token = String.format("%s/%s/%s", tokenPart1, tokenPart2, tokenPart3);
    }

    protected FileSystemOwnershipCheck checker(Supplier<Iterable<String>> dirs)
    {
        return new CIEFileSystemOwnershipCheck(dirs);
    }

    @Test
    public void checkEnabledButAppPropertyIsEmpty() throws Exception
    {
        System.setProperty(APP_NAME_PROPERTY, "");
        executeAndFail(checker(tempDir), options, MISSING_PROPERTY, APP_NAME_PROPERTY);
    }

    @Test
    public void checkEnabledButAppPropertyIsUnset() throws Exception
    {
        System.clearProperty(APP_NAME_PROPERTY);
        executeAndFail(checker(tempDir), options, MISSING_PROPERTY, APP_NAME_PROPERTY);
    }

    @Test
    @Override
    public void checkEnabledButClusterPropertyIsEmpty()
    {
        System.setProperty(CLUSTER_NAME_PROPERTY, "");
        executeAndFail(checker(tempDir), options, MISSING_PROPERTY, CLUSTER_NAME_PROPERTY);
    }

    @Test
    @Override
    public void checkEnabledButClusterPropertyIsUnset()
    {
        System.clearProperty(CLUSTER_NAME_PROPERTY);
        executeAndFail(checker(tempDir), options, MISSING_PROPERTY, CLUSTER_NAME_PROPERTY);
    }

    @Test
    public void checkEnabledButInstanceIdPropertyIsEmpty() throws Exception
    {
        System.setProperty(INSTANCE_ID_PROPERTY, "");
        executeAndFail(checker(tempDir), options, MISSING_PROPERTY, INSTANCE_ID_PROPERTY);
    }

    @Test
    public void checkEnabledButInstanceIdPropertyIsUnset() throws Exception
    {
        System.clearProperty(INSTANCE_ID_PROPERTY);
        executeAndFail(checker(tempDir), options, MISSING_PROPERTY, INSTANCE_ID_PROPERTY);
    }
}
