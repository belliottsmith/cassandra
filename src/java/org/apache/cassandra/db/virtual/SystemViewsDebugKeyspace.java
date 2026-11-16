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

package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableList;

import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS_DEBUG;

/**
 * Debug-only tables exposing internal state to aid developers; enabled by {@code enable_virtual_debug_only_keyspace}.
 * <p/>
 * These tables can and will change and/or go away at any point, including in a minor release, are not to be
 * considered part of the API, and are NOT to be relied on for anything.
 */
public final class SystemViewsDebugKeyspace extends VirtualKeyspace
{
    public static final SystemViewsDebugKeyspace instance = new SystemViewsDebugKeyspace();

    private SystemViewsDebugKeyspace()
    {
        super(VIRTUAL_VIEWS_DEBUG, ImmutableList.of(new LinkLatenciesTable(VIRTUAL_VIEWS_DEBUG)));
    }
}
