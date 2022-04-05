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

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;

public class DisabledRepairsTest extends CQLTester
{
    @Test
    public void testSchema() throws Throwable
    {
        String name = createTable("CREATE TABLE %s (id int primary key, t text) WITH disable_repairs = true");
        boolean foundSchema = false;
        for (Row x : executeNetWithPaging("DESCRIBE SCHEMA", 100))
        {
            foundSchema |= x.toString().contains(name) && x.toString().contains("disable_repairs = true");
        }
        Assert.assertTrue(foundSchema);
    }

    @Test
    public void testSchemaRepairsTrue() throws Throwable
    {
        String name = createTable("CREATE TABLE %s (id int primary key, t text) WITH disable_repairs = false");
        boolean foundSchema = false;
        for (Row x : executeNetWithPaging("DESCRIBE SCHEMA", 100))
        {
            foundSchema |= x.toString().contains(name) && x.toString().contains("disable_repairs = false");
        }
        Assert.assertTrue(foundSchema);
    }

    @Test
    public void testSchemaDefaultTrue() throws Throwable
    {
        String name = createTable("CREATE TABLE %s (id int primary key, t text)");
        boolean foundSchema = false;
        for (Row x : executeNetWithPaging("DESCRIBE SCHEMA", 100))
        {
            foundSchema |= x.toString().contains(name) && x.toString().contains("disable_repairs = false");
        }
        Assert.assertTrue(foundSchema);
    }
}
