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

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import accord.local.ExecutionContext;

/**
 * Short, low-cardinality labels for tasks, used to tag profiling spans (see
 * {@link org.apache.cassandra.service.accord.debug.DebugExecution}): the kind of work, and the command store
 * the task is exclusive to, if any. For example {@code "Accept s24"}, {@code "PlainChain s24"} or
 * {@code "PlainRunnable"}. {@code s<n>} is the command store id, i.e. the first number of
 * {@code [table|tid|<id>,<executor>,<node>]} as logged by {@code AccordCommandStore.toString()}.
 *
 * <p>The kind of a {@link SafeTask} is the class of its {@link ExecutionContext} (usually the request, e.g.
 * {@code PreAccept}, {@code Accept}, {@code Commit}), as {@code reason()} may embed TxnIds. Anonymous contexts
 * (e.g. those built by {@code ExecutionContext.contextFor}) are labelled by their {@code reason()} instead, as long
 * as it is short and we have seen few enough distinct reasons. For other tasks the kind is the task's own class.
 *
 * <p>Labels are cached, so labelling a task does not allocate once each (kind, store, prefix) has been seen.
 */
public final class TaskLabels
{
    public enum Prefix
    {
        RUN(""), QUEUED("Queued "), HEAD("Head ");

        final String text;
        final ConcurrentHashMap<String, ByStore> byKind = new ConcurrentHashMap<>();

        Prefix(String text)
        {
            this.text = text;
        }
    }

    private static final class ByStore
    {
        volatile String withoutStore;
        volatile String[] withStore = new String[0];
    }

    private static final ClassValue<String> KIND = new ClassValue<>()
    {
        @Override
        protected String computeValue(Class<?> type)
        {
            String name = type.getName();
            name = name.substring(name.lastIndexOf('.') + 1);
            int lambda = name.indexOf("$$Lambda");
            return lambda < 0 ? name : name.substring(0, lambda);
        }
    };

    private static final int MAX_REASON_LENGTH = 48;
    private static final int MAX_REASONS = 256;
    private static final ConcurrentHashMap<String, String> REASONS = new ConcurrentHashMap<>();

    private TaskLabels() {}

    static String kind(ExecutionContext context)
    {
        Class<?> type = context.getClass();
        if (type.isAnonymousClass())
        {
            String reason = context.reason();
            if (reason != null && !reason.isEmpty() && reason.length() <= MAX_REASON_LENGTH)
            {
                String kind = REASONS.get(reason);
                if (kind != null)
                    return kind;
                if (REASONS.size() < MAX_REASONS)
                    return REASONS.computeIfAbsent(reason, r -> r);
            }
        }
        return KIND.get(type);
    }

    public static String label(Task task, Prefix prefix)
    {
        if (task instanceof SafeTask<?>)
        {
            SafeTask<?> safeTask = (SafeTask<?>) task;
            ExecutionContext context = safeTask.executionContext();
            while (context instanceof ExecutionContext.Wrapped)
                context = ((ExecutionContext.Wrapped) context).wrapped();
            return label(context == null ? KIND.get(task.getClass()) : kind(context), safeTask.commandStore().id(), prefix);
        }
        if (task instanceof Plain)
        {
            ExclusiveExecutor exclusive = ((Plain) task).exclusiveExecutor();
            return label(KIND.get(task.getClass()), exclusive == null ? -1 : exclusive.commandStoreId, prefix);
        }
        if (task instanceof ExclusiveExecutor.ExclusiveExecutorTask)
            return label(KIND.get(task.getClass()), ((ExclusiveExecutor.ExclusiveExecutorTask) task).queue.commandStoreId, prefix);
        return label(KIND.get(task.getClass()), -1, prefix);
    }

    static String label(Class<?> kind, int commandStoreId, Prefix prefix)
    {
        return label(KIND.get(kind), commandStoreId, prefix);
    }

    static String label(String kind, int commandStoreId, Prefix prefix)
    {
        ByStore byStore = prefix.byKind.computeIfAbsent(kind, k -> new ByStore());
        if (commandStoreId < 0)
        {
            String label = byStore.withoutStore;
            if (label == null)
                byStore.withoutStore = label = prefix.text + kind;
            return label;
        }

        String[] labels = byStore.withStore;
        if (commandStoreId < labels.length && labels[commandStoreId] != null)
            return labels[commandStoreId];

        synchronized (byStore)
        {
            labels = byStore.withStore;
            if (commandStoreId >= labels.length)
                labels = Arrays.copyOf(labels, Math.max(commandStoreId + 1, labels.length * 2));
            else
                labels = labels.clone();
            if (labels[commandStoreId] == null)
                labels[commandStoreId] = prefix.text + kind + " s" + commandStoreId;
            byStore.withStore = labels;
            return labels[commandStoreId];
        }
    }
}
