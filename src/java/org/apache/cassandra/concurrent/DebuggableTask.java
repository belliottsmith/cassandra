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
package org.apache.cassandra.concurrent;

public interface DebuggableTask
{
    public long startTimeNanos();
    public String debug();

    public static class ThreadedDebuggableTask implements DebuggableTask
    {
        final DebuggableTask task;
        final String threadId;

        public ThreadedDebuggableTask(String threadId, DebuggableTask task)
        {
            this.task = task;
            this.threadId = threadId;
        }

        public String threadId()
        {
            return threadId;
        }

        public boolean hasTask()
        {
            return task != null;
        }

        public long startTimeNanos()
        {
            return task == null ? System.nanoTime() : task.startTimeNanos();
        }

        public String debug()
        {
            return task == null ? "" : task.debug();
        }
    }
}
