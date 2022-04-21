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

package org.apache.cassandra.service.accord;

import java.util.concurrent.TimeUnit;

import accord.txn.Timestamp;

public class AccordTimestamps
{
    public static long uniqueNow()
    {
        return TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
    }

    public static long timestampToMicros(Timestamp timestamp)
    {
        /*
        FIXME: since we can't fit the node.id in the timestamp, we need to track the most recent timestamp for each key
         and take the max(real+logical, prev+1) to deal with real+logical collisions. Not sure if this will require
         coordinating with each key for all writes. It may be inferable from deps. We could also just track this locally,
         with the expectation that all operations on a table will be through accord and we can correct any collision
         related timestamp drift locally
         */
        return timestamp.real + timestamp.logical;
    }

    public static int timestampToSeconds(Timestamp timestamp)
    {
        return (int) TimeUnit.MICROSECONDS.toSeconds(timestampToMicros(timestamp));
    }
}