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

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * Used by log4j to find/define property value, see log4j2-dtest.xml
 */
@Plugin(name="instance", category = StrLookup.CATEGORY)
public class InstanceIDDefiner implements StrLookup
{
    // Instantiated per classloader, set by Instance
    private static final String INSTANCE_ID_OBJECT = "instance_id";

    public static void setInstanceId(int id, LoggerContext loggerContext)
    {
        loggerContext.putObject(INSTANCE_ID_OBJECT, "node" + id);
        NamedThreadFactory.setGlobalPrefix("node" + id + "_");
    }

    public static String getInstanceId()
    {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        final LoggerContext loggerContext = LoggerContext.getContext(contextClassLoader, false, null);
        final Object id = loggerContext.getObject(INSTANCE_ID_OBJECT);
        return Objects.toString(id, "<main>");
    }

    public String lookup(String s)
    {
        return getInstanceId();
    }

    public String lookup(LogEvent logEvent, String s)
    {
        return getInstanceId();
    }
}
