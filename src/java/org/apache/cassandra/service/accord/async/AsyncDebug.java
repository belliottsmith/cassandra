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

package org.apache.cassandra.service.accord.async;

import accord.primitives.TxnId;

public class AsyncDebug
{
    private static final TxnId TARGET = TxnId.fromValues(11, 8, 9, 1);
    private static final TxnId OTHER = TxnId.fromValues(11,76,9,1);

    public static void check(TxnId id)
    {
        if (TARGET.equals(id))
            System.out.println();
        if (OTHER.equals(id))
            System.out.println();
    }
}
