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

package org.apache.cassandra.distributed.impl;

import java.util.Objects;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * Used by log4j to find/define property value, see log4j2-dtest.xml
 */
@Plugin(name="cluster", category = StrLookup.CATEGORY)
public class ClusterIDDefiner implements StrLookup
{
    private static volatile String ID = "<main>";

    public static void setId(String id)
    {
        ID = Objects.requireNonNull(id);
    }

    public static String getId()
    {
        return ID;
    }

    public String lookup(String s)
    {
        return lookup();
    }

    public String lookup(LogEvent logEvent, String s)
    {
        return lookup();
    }

    private String lookup()
    {
        return ID;
    }
}
