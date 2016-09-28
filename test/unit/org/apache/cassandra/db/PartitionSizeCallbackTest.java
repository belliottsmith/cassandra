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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PartitionSizeCallbackTest
{
    private final static String KS = "ks";
    private final static String TBL = "tbl";

    private final static InetAddress EP1;
    private final static InetAddress EP2;
    private final static InetAddress EP3;

    static
    {
        try
        {
            EP1 = InetAddress.getByName("10.0.0.1");
            EP2 = InetAddress.getByName("10.0.0.2");
            EP3 = InetAddress.getByName("10.0.0.3");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void successCase() throws Exception
    {
        PartitionSizeCommand command = new PartitionSizeCommand(KS, TBL, ByteBufferUtil.bytes(1));
        PartitionSizeCallback callback = new PartitionSizeCallback(command, ConsistencyLevel.ALL, 3, System.nanoTime());

        Assert.assertFalse(callback.isFinished());
        callback.handleResponse(EP1, 1);
        callback.handleResponse(EP2, 2);
        Assert.assertFalse(callback.isFinished());
        callback.handleResponse(EP3, 3);
        Assert.assertTrue(callback.isFinished());

        Map<InetAddress, Long> expected = new HashMap<>();
        expected.put(EP1, 1L);
        expected.put(EP2, 2L);
        expected.put(EP3, 3L);
        Map<InetAddress, Long> actual = callback.get();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void timeout() throws Exception
    {
        PartitionSizeCommand command = new PartitionSizeCommand(KS, TBL, ByteBufferUtil.bytes(1)) {
            public long getTimeout()
            {
                return 1;  // 1 millisecond
            }
        };
        PartitionSizeCallback callback = new PartitionSizeCallback(command, ConsistencyLevel.ALL, 3, System.nanoTime());

        Assert.assertFalse(callback.isFinished());
        callback.handleResponse(EP1, 1);
        callback.handleResponse(EP2, 2);
        Assert.assertFalse(callback.isFinished());

        try
        {
            callback.get();
            Assert.fail("expecting ReadTimeoutException");
        }
        catch (ReadTimeoutException e)
        {
            Assert.assertEquals(2, e.received);
            Assert.assertEquals(3, e.blockFor);
            Assert.assertTrue(e.dataPresent);
        }
    }
}
