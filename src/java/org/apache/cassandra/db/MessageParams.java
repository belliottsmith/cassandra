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

package org.apache.cassandra.db;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.utils.ByteArrayUtil;

public class MessageParams
{
    private static final FastThreadLocal<Map<String, byte[]>> local = new FastThreadLocal<>();

    private static Map<String, byte[]> get()
    {
        Map<String, byte[]> instance = local.get();
        if (instance == null)
        {
            instance = new HashMap<>();
            local.set(instance);
        }

        return instance;
    }

    public static ImmutableMap<String, byte[]> getParams()
    {
        return ImmutableMap.copyOf(get());
    }

    public static void add(String key, byte[] bytes)
    {
        get().put(key, bytes);
    }

    public static void add(String key, int v)
    {
        add(key, ByteArrayUtil.bytes(v));
    }

    public static void add(String key, long v)
    {
        add(key, ByteArrayUtil.bytes(v));
    }

    public static byte[] get(String key)
    {
        return get().get(key);
    }

    public static void remove(String key)
    {
        get().remove(key);
    }

    public static void reset()
    {
        get().clear();
    }

    public static <T> MessageOut<T> addToMessage(MessageOut<T> message)
    {
        return message.withParameters(get());
    }
}
