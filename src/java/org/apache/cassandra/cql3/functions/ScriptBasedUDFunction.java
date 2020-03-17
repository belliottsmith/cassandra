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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.ExecutorService;
import javax.script.*;

import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngine;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

final class ScriptBasedUDFunction extends UDFunction
{
    private static final ProtectionDomain protectionDomain;
    private static final AccessControlContext accessControlContext;

    //
    // For scripted UDFs we have to rely on the security mechanisms of the scripting engine and
    // SecurityManager - especially SecurityManager.checkPackageAccess(). Unlike Java-UDFs, strict checking
    // of class access via the UDF class loader is not possible, since e.g. Nashorn builds its own class loader
    // (jdk.nashorn.internal.runtime.ScriptLoader / jdk.nashorn.internal.runtime.NashornLoader) configured with
    // a system class loader.
    //
    private static final String[] allowedPackagesArray =
    {
    // following required by jdk.nashorn.internal.objects.Global.initJavaAccess()
    "",
    "com",
    "edu",
    "java",
    "javax",
    "javafx",
    "org",
    // following required by Nashorn runtime
    "java.lang",
    "java.lang.invoke",
    "java.lang.reflect",
    "java.nio.charset",
    "java.util",
    "java.util.concurrent",
    "javax.script",
    "sun.reflect",
    "jdk.internal.org.objectweb.asm.commons",
    "jdk.nashorn.internal.runtime",
    "jdk.nashorn.internal.runtime.linker",
    // Nashorn / Java 11
    "java.lang.ref",
    "java.io",
    "java.util.function",
    "jdk.dynalink.linker",
    "jdk.internal.org.objectweb.asm",
    "jdk.internal.reflect",
    "jdk.nashorn.internal.scripts",
    // following required by Java Driver
    "java.math",
    "java.nio",
    "java.text",
    "com.google.common.base",
    "com.google.common.collect",
    "com.google.common.reflect",
    "org.slf4j.ext",
    // following required by UDF
    "com.datastax.driver.core",
    "com.datastax.driver.core.utils",
    // CIE NOTE - trunk (4.0) was upgraded to driver 3.6 but is different so doesn't hit the driver code which touches IntObjectHashMap. This line below is not needed in 4.0
    "com.datastax.shaded.netty.util.collection"
    };

    // use a JVM standard ExecutorService as DebuggableThreadPoolExecutor references internal
    // classes, which triggers AccessControlException from the UDF sandbox
    private static final UDFExecutorService executor =
        new UDFExecutorService(new NamedThreadFactory("UserDefinedScriptFunctions",
                                                      Thread.MIN_PRIORITY,
                                                      udfClassLoader,
                                                      new SecurityThreadGroup("UserDefinedScriptFunctions",
                                                                              Collections.unmodifiableSet(new HashSet<>(Arrays.asList(allowedPackagesArray))),
                                                                              UDFunction::initializeThread)),
                               "userscripts");

    private static final ClassFilter classFilter = clsName -> secureResource(clsName.replace('.', '/') + ".class");

    private static final NashornScriptEngine scriptEngine;


    static
    {
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        ScriptEngine engine = scriptEngineManager.getEngineByName("nashorn");
        NashornScriptEngineFactory factory = engine != null ? (NashornScriptEngineFactory) engine.getFactory() : null;
        scriptEngine = factory != null ? (NashornScriptEngine) factory.getScriptEngine(new String[]{}, udfClassLoader, classFilter) : null;

        try
        {
            protectionDomain = new ProtectionDomain(new CodeSource(new URL("udf", "localhost", 0, "/script", new URLStreamHandler()
            {
                protected URLConnection openConnection(URL u)
                {
                    return null;
                }
            }), (Certificate[]) null), ThreadAwareSecurityManager.noPermissions);
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException(e);
        }
        accessControlContext = new AccessControlContext(new ProtectionDomain[]{ protectionDomain });
    }

    private final CompiledScript script;

    ScriptBasedUDFunction(FunctionName name,
                          List<ColumnIdentifier> argNames,
                          List<AbstractType<?>> argTypes,
                          AbstractType<?> returnType,
                          boolean calledOnNullInput,
                          String language,
                          String body)
    {
        super(name, argNames, argTypes, returnType, calledOnNullInput, language, body);

        if (!"JavaScript".equalsIgnoreCase(language) || scriptEngine == null)
            throw new InvalidRequestException(String.format("Invalid language '%s' for function '%s'", language, name));

        // execute compilation with no-permissions to prevent evil code e.g. via "static code blocks" / "class initialization"
        try
        {
            this.script = AccessController.doPrivileged((PrivilegedExceptionAction<CompiledScript>) () -> scriptEngine.compile(body),
                                                        accessControlContext);
        }
        catch (PrivilegedActionException x)
        {
            Throwable e = x.getCause();
            logger.info("Failed to compile function '{}' for language {}: ", name, language, e);
            throw new InvalidRequestException(
                                             String.format("Failed to compile function '%s' for language %s: %s", name, language, e));
        }
    }

    protected ExecutorService executor()
    {
        return executor;
    }

    public ByteBuffer executeUserDefined(int protocolVersion, List<ByteBuffer> parameters)
    {
        Object[] params = new Object[argTypes.size()];
        for (int i = 0; i < params.length; i++)
            params[i] = compose(protocolVersion, i, parameters.get(i));

        ScriptContext scriptContext = new SimpleScriptContext();
        scriptContext.setAttribute("javax.script.filename", this.name.toString(), ScriptContext.ENGINE_SCOPE);
        Bindings bindings = scriptContext.getBindings(ScriptContext.ENGINE_SCOPE);
        for (int i = 0; i < params.length; i++)
            bindings.put(argNames.get(i).toString(), params[i]);

        Object result;
        try
        {
            // How to prevent Class.forName() _without_ "help" from the script engine ?
            // NOTE: Nashorn enforces a special permission to allow class-loading, which is not granted - so it's fine.

            result = script.eval(scriptContext);
        }
        catch (ScriptException e)
        {
            throw new RuntimeException(e);
        }
        if (result == null)
            return null;

        Class<?> javaReturnType = UDHelper.asJavaClass(returnCodec);
        Class<?> resultType = result.getClass();
        if (!javaReturnType.isAssignableFrom(resultType))
        {
            if (result instanceof Number)
            {
                Number rNumber = (Number) result;
                if (javaReturnType == Integer.class)
                    result = rNumber.intValue();
                else if (javaReturnType == Long.class)
                    result = rNumber.longValue();
                else if (javaReturnType == Short.class)
                    result = rNumber.shortValue();
                else if (javaReturnType == Byte.class)
                    result = rNumber.byteValue();
                else if (javaReturnType == Float.class)
                    result = rNumber.floatValue();
                else if (javaReturnType == Double.class)
                    result = rNumber.doubleValue();
                else if (javaReturnType == BigInteger.class)
                {
                    if (javaReturnType == Integer.class)
                        result = rNumber.intValue();
                    else if (javaReturnType == Short.class)
                        result = rNumber.shortValue();
                    else if (javaReturnType == Byte.class)
                        result = rNumber.byteValue();
                    else if (javaReturnType == Long.class)
                        result = rNumber.longValue();
                    else if (javaReturnType == Float.class)
                        result = rNumber.floatValue();
                    else if (javaReturnType == Double.class)
                        result = rNumber.doubleValue();
                    else if (javaReturnType == BigInteger.class)
                    {
                        if (rNumber instanceof BigDecimal)
                            result = ((BigDecimal) rNumber).toBigInteger();
                        else if (rNumber instanceof Double || rNumber instanceof Float)
                            result = new BigDecimal(rNumber.toString()).toBigInteger();
                        else
                            result = BigInteger.valueOf(rNumber.longValue());
                    }
                    else if (javaReturnType == BigDecimal.class)
                        // String c'tor of BigDecimal is more accurate than valueOf(double)
                        result = new BigDecimal(rNumber.toString());
                }
                else if (javaReturnType == BigDecimal.class)
                    // String c'tor of BigDecimal is more accurate than valueOf(double)
                    result = new BigDecimal(rNumber.toString());
            }
        }

        return decompose(protocolVersion, result);
    }
}
