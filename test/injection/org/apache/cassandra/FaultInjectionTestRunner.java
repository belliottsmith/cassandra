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
package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Throwables;
import org.junit.Ignore;
import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import org.apache.cassandra.transport.Frame;
import org.jboss.byteman.agent.submit.ScriptText;
import org.jboss.byteman.agent.submit.Submit;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitConfigState;

import static java.lang.String.format;

public class FaultInjectionTestRunner extends BlockJUnit4ClassRunner
{

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.TYPE })
    public static @interface Faults
    {
        String[] scripts();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.PARAMETER })
    public static @interface Param
    {
        String[] value();
        String name();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.METHOD })
    public static @interface Debug
    {
        String[] params();
        String method();
        String location();
    }

    public static boolean executed = false;

    public FaultInjectionTestRunner(Class aClass) throws InitializationError
    {
        super(aClass);
        try
        {
            BMUnitConfigState.pushConfigurationState((BMUnitConfig) aClass.getAnnotation(BMUnitConfig.class), aClass);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void validateInstanceMethods(List<Throwable> errors)
    {
    }

    protected void runChild(FrameworkMethod method, RunNotifier notifier)
    {
        String methodName = testName(method);
        Faults faults = getTestClass().getJavaClass().getAnnotation(Faults.class);
        Debug debug = getTestClass().getJavaClass().getAnnotation(Debug.class);
        Object object;
        try
        {
            object = createTest();
            for (Params params : generateParams(method))
            {
                for (ScriptDef def : generateBMScripts(faults, debug))
                {
                    executed = false;
                    Submit submit = new Submit("localhost", 9091);
                    System.err.println(def.build().getText());
                    EachTestNotifier eachNotifier = new EachTestNotifier(notifier, def.description(params.name(methodName)));
                    try
                    {

                        try
                        {
                            submit.addScripts(Collections.singletonList(def.build()));
                        }
                        catch (Throwable t)
                        {
                            eachNotifier.addFailure(t);
                            continue;
                        }

                        if(method.getAnnotation(Ignore.class) != null)
                        {
                            eachNotifier.fireTestIgnored();
                        }
                        else
                        {
                            eachNotifier.fireTestStarted();

                            try
                            {
                                methodBlock(method, object, params.params).evaluate();
                            }
                            catch (AssumptionViolatedException var9)
                            {
                                eachNotifier.addFailedAssumption(var9);
                            }
                            catch (Throwable var10)
                            {
                                eachNotifier.addFailure(var10);
                            }
                        }
                    }
                    finally
                    {
                        try
                        {
                            submit.deleteScripts(Collections.singletonList(def.build()));
                            if (!executed)
                                throw new AssertionError("Fault not executed!");
                        }
                        catch (Throwable t)
                        {
                            eachNotifier.addFailure(t);
                        }
                        eachNotifier.fireTestFinished();
                    }
                }
            }
        }
        catch (Throwable t)
        {
            EachTestNotifier eachNotifier = this.makeNotifier(method, notifier);
            eachNotifier.fireTestStarted();
            eachNotifier.addFailure(t);
            eachNotifier.fireTestFinished();
            return;
        }

        EachTestNotifier eachNotifier = this.makeNotifier(method, notifier);
        eachNotifier.fireTestStarted();
        eachNotifier.fireTestFinished();
    }

    protected Statement methodBlock(FrameworkMethod method, Object test, Object[] params)
    {
        Statement statement = new Invoke(method, test, params);
        statement = this.possiblyExpectingExceptions(method, test, statement);
        statement = this.withPotentialTimeout(method, test, statement);
        statement = this.withBefores(method, test, statement);
        statement = this.withAfters(method, test, statement);
        return statement;
    }

    private class Invoke extends Statement
    {
        private final FrameworkMethod method;
        private final Object testObject;
        private final Object[] params;

        public Invoke(FrameworkMethod method, Object testObject, Object[] params)
        {
            this.method = method;
            this.testObject = testObject;
            this.params = params;
        }

        public void evaluate() throws Throwable
        {
            this.method.invokeExplosively(this.testObject, params);
        }
    }

    private List<Params> generateParams(FrameworkMethod method)
    {
        Debug debug = method.getMethod().getAnnotation(Debug.class);
        List<Params> result = new ArrayList<>();
        Parameter[] params = method.getMethod().getParameters();
        Object[] values = new Object[params.length];
        String[] names = new String[params.length];
        for (int p = 0 ; p < params.length ; p++)
        {
            if (!params[p].getType().equals(String.class))
                throw new IllegalStateException("Init method may only accept string values");
            if (params[p].getAnnotation(Param.class) == null)
                throw new IllegalStateException("Init method must have a @Param annotation on each parameter");
            names[p] = params[p].getAnnotation(Param.class).name();
        }
        if (debug != null)
        {
            assert values.length == debug.params().length;
            for (int i = 0 ; i < values.length ; i++)
                values[i] = debug.params()[i];
            result.add(new Params(values, names));
        }
        else
        {
            fill(result, params, names, values, 0);
        }
        return result;
    }

    private void fill(List<Params> result, Parameter[] params, String[] names, Object[] values, int param)
    {
        if (param == params.length)
        {
            result.add(new Params(values.clone(), names));
            return;
        }
        for (String v : params[param].getAnnotation(Param.class).value())
        {
            values[param] = v;
            fill(result, params, names, values, param + 1);
        }
    }

    class Params
    {
        final Object[] params;
        final String[] names;
        Params(Object[] params, String[] names)
        {
            this.params = params;
            this.names = names;
        }
        String name(String methodName)
        {
            StringBuilder sb = new StringBuilder(methodName);
            sb.append("{");
            for (int i = 0 ; i < names.length ; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(names[i]);
                sb.append("=");
                sb.append(params[i]);
            }
            sb.append("}");
            return sb.toString();
        }
        Description description(String basename)
        {
            return Description.createTestDescription(getTestClass().getJavaClass(), basename);
        }
    }

    private List<ScriptDef> generateBMScripts(Faults faults, Debug debug)
    {
        Pattern pattern = Pattern.compile("( *)(IF|BIND|DO|CLASS|METHOD|LOCATION): (.*)");
        List<ScriptDef> scripts = new ArrayList<>();
        int i = 0;
        for (String script : faults.scripts())
        {
            try
            {
                BufferedReader reader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream(script)));
                Stack<StackEntry> stack = new Stack<>();
                EnumMap<Type, String> clauses = new EnumMap<>(Type.class);
                String line;
                while ( null != (line = reader.readLine()) )
                {
                    if (line.equals("EXIT"))
                        break;
                    Matcher m = pattern.matcher(line);
                    assert m.matches() || line.trim().isEmpty();
                    if (!m.matches())
                        continue;
                    int indent = m.group(1).length();
                    while (!stack.isEmpty() && indent <= stack.peek().indent)
                        clauses.remove(stack.pop().type);
                    if (!clauses.containsKey(Type.IF))
                        clauses.put(Type.IF, "TRUE");
                    Type type = Type.valueOf(m.group(2));
                    clauses.put(type, m.group(3));
                    stack.push(new StackEntry(type, indent));
                    if (sufficient(clauses))
                    {
                        ScriptDef def = new ScriptDef(i++, clauses.clone());
                        if (debug == null || def.matches(debug))
                            scripts.add(def);
                    }
                }
            }
            catch (java.io.IOException e)
            {
                Throwables.propagate(e);
            }
        }
        return scripts;
    }

    private class ScriptDef
    {
        final int id;
        final EnumMap<Type, String> clauses;

        private ScriptDef(int id, EnumMap<Type, String> clauses)
        {
            this.id = id;
            this.clauses = clauses;
        }

        public ScriptText build()
        {
            return new ScriptText("script" + id,
            format(
                  "RULE rule%s\n" +
                  "CLASS %s\n" +
                  "METHOD %s\n" +
                  "%s\n" +
                  "%s" +
                  "IF %s\n" +
                  "DO org.apache.cassandra.FaultInjectionTestRunner.executed = true; %s\n" +
                  "ENDRULE",
                  Integer.toString(id),
                  clauses.get(Type.CLASS), clauses.get(Type.METHOD), clauses.get(Type.LOCATION),
                  clauses.containsKey(Type.BIND) ? "BIND " + clauses.get(Type.BIND) + "\n" : "",
                  clauses.get(Type.IF), clauses.get(Type.DO)));
        }

        public Description description(String basename)
        {
            String name = basename + ":" + method() + ":" + clauses.get(Type.LOCATION);
            return Description.createTestDescription(getTestClass().getJavaClass(), name);
        }

        String method()
        {
            String clazz = clauses.get(Type.CLASS);
            clazz = clazz.substring(clazz.lastIndexOf('.') + 1);
            return clazz + "." + clauses.get(Type.METHOD);
        }

        boolean matches(Debug debug)
        {
            return debug.method().equals(method()) && debug.location().equals(clauses.get(Type.LOCATION));
        }
    }

    private static class StackEntry
    {
        final Type type;
        final int indent;

        private StackEntry(Type type, int indent)
        {
            this.type = type;
            this.indent = indent;
        }
    }

    private static enum Type
    {
        CLASS, METHOD, LOCATION, DO, IF, BIND
    }

    static boolean sufficient(EnumMap<Type, String> clauses)
    {
        return clauses.size() == 6 || (clauses.size() == 5 && !clauses.containsKey(Type.BIND));
    }
}
