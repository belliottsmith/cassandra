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

package com.datastax.driver.core;

// Unfortunately, MD5Digest and fields in PreparedStatement are package-private, so the easiest way to test these
// things while still using the driver was to create a class in DS package.
public class PreparedStatementHelper
{
    private static MD5Digest id(PreparedStatement statement)
    {
        return statement.getPreparedId().boundValuesMetadata.id;
    }

    public static void assertStable(PreparedStatement first, PreparedStatement subsequent)
    {
        if (!id(first).equals(id(subsequent)))
        {
            throw new AssertionError(String.format("Subsequent id (%s) is different from the first one (%s)",
                                                   id(first),
                                                   id(subsequent)));
        }
    }
}
