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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;

import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.profiler.AsyncProfiler;

public class AsyncProfilerUtil
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncProfilerUtil.class);

    private static final String NATIVE_RESOURCE_HOME = "META-INF/native/";
    private static boolean INITIALIZED = false;

    static
    {
        if (!Boolean.getBoolean("cassandra.async_profiler.disabled"))
        {
            AsyncProfiler profiler = null;
            try
            {
                profiler = AsyncProfiler.getInstance();
                INITIALIZED = true;
            }
            catch (Throwable t)
            {
                String libname = System.mapLibraryName("asyncProfiler");
                String path = NATIVE_RESOURCE_HOME + libname;
                URL url = AsyncProfilerUtil.class.getClassLoader().getResource(path);
                if (url == null)
                {
                    logger.warn("Unable to find library asyncProfiler at {}", path);
                }
                else
                {
                    int index = libname.lastIndexOf('.');
                    String prefix = libname.substring(0, index);
                    String suffix = libname.substring(index);
                    try
                    {
                        File tmpFile = File.createTempFile(prefix, suffix);

                        try (InputStream in = url.openStream();
                             FileOutputStream out = new FileOutputStream(tmpFile))
                        {
                            ByteStreams.copy(in, out);
                        }
                        profiler = AsyncProfiler.getInstance(tmpFile.getAbsolutePath());
                        INITIALIZED = true;
                    }
                    catch (Throwable e)
                    {
                        logger.warn("Unable to add AsyncProfiler mbean", t);
                    }
                }
            }
            if (profiler != null)
                MBeanWrapper.instance.registerMBean(profiler, "one.profiler:type=AsyncProfiler");
        }
    }

    private AsyncProfilerUtil()
    {
    }

    public static void init()
    {
        if (!INITIALIZED)
            logger.warn("AsyncProfiler was not setup");
    }
}
