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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VerifyEmptyBooleanTest extends CQLTester
{
    @Test
    public void inPartitionKeyTest() throws Throwable
    {
        emptyBooleanHelper("create table %s (id int, b boolean, primary key ((id, b)))", "Empty boolean in partition key=[2:]");
    }

    @Test
    public void inClusteringTest() throws Throwable
    {
        emptyBooleanHelper("create table %s (id int, b boolean, primary key (id, b))", "Empty boolean in clustering key=[2]");
    }

    @Test
    public void inStaticRowTest() throws Throwable
    {
        createTable("create table %s (id int, ck int, b boolean static, PRIMARY KEY (id, ck))");
        execute("insert into %s (id, ck, b) values (1, 1, true)");
        try (Breaker ignored = new Breaker())
        {
            execute("insert into %s (id, ck, b) values (2, 1, true)");
            flush();
        }
        Output output = new Output();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            try (Verifier verifier = new Verifier(getCurrentColumnFamilyStore(), sstable, output, false, Verifier.options().validateData(true).build()))
            {
                verifier.verify();
            }
        }
        assertEquals(1, output.everything.size());
        assertTrue(output.everything.get(0).contains("Empty boolean in row key=[2]") && output.everything.get(0).contains("STATIC"));
    }

    @Test
    public void inRowTest() throws Throwable
    {
        emptyBooleanHelper("create table %s (id int primary key, b boolean)", "Empty boolean in row key=[2]");
    }

    @Test
    public void collectionTestBrokenValues() throws Throwable
    {
        createTable("create table %s (id int primary key, x map<int, boolean>)");
        execute("insert into %s (id, x) values (1, {1 : true})");
        try (Breaker ignored = new Breaker())
        {
            execute("insert into %s (id, x) values (2, {1 : true})");
            flush();
        }
        Output output = new Output();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            try (Verifier verifier = new Verifier(getCurrentColumnFamilyStore(), sstable, output, false, Verifier.options().validateData(true).build()))
            {
                verifier.verify();
            }
        }
        assertEquals(1, output.everything.size());
        assertTrue(output.everything.get(0).contains("Empty boolean in row key=[2]"));
    }

    @Test
    public void collectionTestBrokenKeys() throws Throwable
    {
        createTable("create table %s (id int primary key, x map<boolean, int>)");
        execute("insert into %s (id, x) values (1, {true : 1})");
        try (Breaker ignored = new Breaker())
        {
            execute("insert into %s (id, x) values (2, {true : 2})");
            flush();
        }
        Output output = new Output();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            try (Verifier verifier = new Verifier(getCurrentColumnFamilyStore(), sstable, output, false, Verifier.options().validateData(true).build()))
            {
                verifier.verify();
            }
        }
        assertEquals(1, output.everything.size());
        assertTrue(output.everything.get(0).contains("Empty boolean in row key=[2]"));
    }

    private void emptyBooleanHelper(String createTable, String assertMessage) throws Throwable
    {
        createTable(createTable);
        execute("insert into %s (id, b) values (1, true)");

        try (Breaker ignored = new Breaker())
        {
            execute("insert into %s (id, b) values (2, true)");
            flush();
        }
        Output output = new Output();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            try (Verifier verifier = new Verifier(getCurrentColumnFamilyStore(), sstable, output, false, Verifier.options().validateData(true).build()))
            {
                verifier.verify();
            }
        }

        assertEquals(1, output.everything.size());
        assertTrue(output.everything.get(0).contains(assertMessage));
    }

    private static class Breaker implements AutoCloseable
    {
        public Breaker()
        {
            BooleanSerializer.instance.serialize(true).get();
            BooleanSerializer.instance.serialize(false).get();
        }
        public void close() throws Exception
        {
            BooleanSerializer.instance.serialize(true).position(0);
            BooleanSerializer.instance.serialize(false).position(0);
        }
    }

    private static class Output implements OutputHandler
    {
        List<String> everything = new ArrayList<>();
        public void output(String msg)
        {
        }

        public void debug(String msg)
        {
        }

        public void warn(String msg)
        {
            if (msg.contains("Empty boolean"))
                everything.add(msg);
        }

        public void warn(String msg, Throwable th)
        {
        }
    }
}
