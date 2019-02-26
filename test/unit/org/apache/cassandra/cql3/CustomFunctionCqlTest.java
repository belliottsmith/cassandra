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
package org.apache.cassandra.cql3;

import org.junit.Test;

public class CustomFunctionCqlTest extends CQLTester
{
    @Test
    public void selectAndUpdateTest() throws Throwable
    {
        // SomeCustomFcts and MoreCustomFcts must be in test/cassandra.yaml
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("INSERT INTO %s(k,v) VALUES (1, 'hello')");
        assertRows(execute("SELECT exclaim(v) FROM %s WHERE k = 1"),
                   row("hello!"));

        execute("UPDATE %s SET v = exclaim('goodbye') WHERE k = 2");
        assertRows(execute("SELECT v FROM %s WHERE k = 2"),
                   row("goodbye!"));

        execute("UPDATE %s SET v = exclaim(question(exclaim('argh'))) WHERE k = 3");
        assertRows(execute("SELECT v FROM %s WHERE k = 3 AND v = 'argh!?!' ALLOW FILTERING"),
                   row("argh!?!"));
    }
}
