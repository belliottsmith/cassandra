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

package org.apache.cassandra.auth;

import java.util.HashMap;
import java.util.Map;

public interface Cacheable<K, V>
{
    /**
     * Default no-op impl of cache warming method.
     * Implementors can expect to be called after setup is complete, but before
     * native transport is opened, allowing datasets to be pre-computed and inserted
     * into the provided auth cache before any client connections are attempted.
     */
    default Map<K, V> getInitialEntriesForCache() {
        return new HashMap<>();
    }
}
