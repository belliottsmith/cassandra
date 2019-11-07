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

import java.util.function.Supplier;

/**
 * Defers a functions conversion to strings until {@link #toString()} is needed.  This behavior is mostly usful with
 * loggers since <pre>{}</pre> will call {@link #toString()} in a different thread and only if the log level is enabled.
 *
 * Can also use with {@link NoSpamLogger}.
 */
public class LazyToString
{
    private final Supplier<String> fn;

    public LazyToString(Supplier<String> fn)
    {
        this.fn = fn;
    }

    public static LazyToString of(Supplier<String> fn)
    {
        return new LazyToString(fn);
    }

    public String toString()
    {
        return fn.get();
    }
}
