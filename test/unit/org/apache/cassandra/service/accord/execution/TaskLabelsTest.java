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

package org.apache.cassandra.service.accord.execution;

import java.util.function.Supplier;

import org.junit.Test;

import accord.local.ExecutionContext;

import static org.apache.cassandra.service.accord.execution.TaskLabels.Prefix.HEAD;
import static org.apache.cassandra.service.accord.execution.TaskLabels.Prefix.QUEUED;
import static org.apache.cassandra.service.accord.execution.TaskLabels.Prefix.RUN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TaskLabelsTest
{
    static class Nested {}

    @Test
    public void testLabels()
    {
        assertEquals("TaskLabelsTest$Nested", TaskLabels.label(Nested.class, -1, RUN));
        assertEquals("TaskLabelsTest$Nested s3", TaskLabels.label(Nested.class, 3, RUN));
        assertEquals("Queued TaskLabelsTest$Nested s3", TaskLabels.label(Nested.class, 3, QUEUED));
        assertEquals("Head TaskLabelsTest$Nested", TaskLabels.label(Nested.class, -1, HEAD));
        // grows to accommodate larger store ids, retaining earlier labels
        assertEquals("TaskLabelsTest$Nested s100", TaskLabels.label(Nested.class, 100, RUN));
        assertEquals("TaskLabelsTest$Nested s3", TaskLabels.label(Nested.class, 3, RUN));
    }

    @Test
    public void testCached()
    {
        assertSame(TaskLabels.label(Nested.class, 7, QUEUED), TaskLabels.label(Nested.class, 7, QUEUED));
        assertSame(TaskLabels.label(Nested.class, -1, HEAD), TaskLabels.label(Nested.class, -1, HEAD));
    }

    @Test
    public void testAnonymousContextsUseReason()
    {
        ExecutionContext context = ExecutionContext.unsequenced(null, "Some Reason");
        assertEquals("Some Reason", TaskLabels.kind(context));
        // an empty reason is no label at all, so fall back to the (anonymous) class
        assertTrue(TaskLabels.kind(ExecutionContext.unsequenced(null, "")).startsWith("ExecutionContext$"));
        // a named context class is labelled by its class, whatever its reason
        assertEquals("TaskLabelsTest$NamedContext", TaskLabels.kind(new NamedContext()));
    }

    static class NamedContext implements ExecutionContext.Empty
    {
        @Override public String reason() { return "a reason"; }
    }

    @Test
    public void testLambda()
    {
        Supplier<String> lambda = () -> "";
        assertEquals("TaskLabelsTest s1", TaskLabels.label(lambda.getClass(), 1, RUN));
    }
}
