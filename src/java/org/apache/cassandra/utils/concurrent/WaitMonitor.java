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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.TimeUnit;

public interface WaitMonitor
{
    public static WaitMonitor NONE = new WaitMonitor()
    {
        public void waitUntil(long deadlineNanos) throws InterruptedException
        {
            long waitNanos = System.nanoTime() - deadlineNanos;
            if (waitNanos > 0)
                TimeUnit.NANOSECONDS.sleep(waitNanos);
        }

        public boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException
        {
            long wait = deadlineNanos - System.nanoTime();
            if (wait <= 0)
                return false;

            monitor.wait((wait + 999999) / 1000000);
            return true;
        }

        public void wait(Object monitor) throws InterruptedException
        {
            monitor.wait();
        }

        public boolean awaitUntil(Condition condition, long deadlineNanos) throws InterruptedException
        {
            return condition.awaitUntil(deadlineNanos);
        }

        public void signal(Condition condition)
        {
            condition.signalAll();
        }

        public void notify(Object monitor)
        {
            monitor.notifyAll();
        }
    };

    void waitUntil(long deadlineNanos) throws InterruptedException;
    void wait(Object monitor) throws InterruptedException;
    boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException;
    boolean awaitUntil(Condition condition, long deadlineNanos) throws InterruptedException;

    void signal(Condition condition);
    void notify(Object monitor);

}
