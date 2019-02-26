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
package org.apache.cassandra.cql3.functions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

public abstract class CustomFcts
{
    private static final Logger logger = LoggerFactory.getLogger(CustomFcts.class);

    static private Collection<Function> loadCustomFunctions(String fctsClassName)
    {

        String className = fctsClassName.contains(".") ? fctsClassName : "org.apache.cassandra.cql3.functions." + fctsClassName;
        try
        {
            Class<?> typeClass = FBUtilities.<AbstractType<?>>classForName(className, "custom-fcts");
            Method allMethod = typeClass.getMethod("all");
            //noinspection unchecked
            return (Collection<Function>) allMethod.invoke(null);
        }
        catch (NoSuchMethodException | IllegalAccessException e)
        {
            throw new ConfigurationException("Inaccessible all() with custom fcts in " + className, e);
        }
        catch (InvocationTargetException e)
        {
            throw new ConfigurationException("Invalid definition for custom fcts " + className, e.getTargetException());
        }
        catch (ExceptionInInitializerError e)
        {
            throw new ConfigurationException("Invalid initializer for custom fcts " + fctsClassName, e);
        }
    }

    static public Collection<Function> all() {

        Collection<Function> result = new HashSet<>();

        for (String fctClass : DatabaseDescriptor.getCustomFunctions())
        {
            Collection<Function> all = loadCustomFunctions(fctClass);
            logger.info("Loaded custom functions from {}", fctClass);
            all.forEach(fn -> logger.debug("Loaded custom function {}", fn));
            result.addAll(all);
        }

        return result;
    }
}
