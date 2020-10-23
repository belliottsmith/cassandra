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

import com.google.common.base.Preconditions;

public interface WaitManager
{
    public static final class Global
    {
        // deliberately not volatile to ensure zero overhead outside of testing;
        // depend on other memory visibility primitives to ensure visibility
        private static WaitManager INSTANCE = NONE;
        public static WaitManager waits()
        {
            return INSTANCE;
        }
        public static void unsafeSet(WaitManager waits)
        {
            Preconditions.checkNotNull(waits);
            INSTANCE = waits;
        }
    }

    public static WaitManager NONE = new WaitManager()
    {
        public void waitUntil(long deadlineNanos) throws InterruptedException
        {
            long waitNanos = deadlineNanos - System.nanoTime();
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

        public void nemesis() { }
    };

    void waitUntil(long deadlineNanos) throws InterruptedException;
    void wait(Object monitor) throws InterruptedException;
    boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException;
    boolean awaitUntil(Condition condition, long deadlineNanos) throws InterruptedException;

    void signal(Condition condition);
    void notify(Object monitor);

    // used only for testing purposes, to reorder thread events
    void nemesis();
}
