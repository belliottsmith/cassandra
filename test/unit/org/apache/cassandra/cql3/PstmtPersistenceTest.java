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

import java.net.InetSocketAddress;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;

import static java.util.Collections.emptyMap;
import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PstmtPersistenceTest extends CQLTester
{
    @Before
    public void setUp()
    {
        QueryProcessor.clearPreparedStatements(false);
    }
 
    @Test
    public void testCachedPreparedStatements() throws Throwable
    {
        // need this for pstmt execution/validation tests
        requireNetwork();

        assertEquals(0, numberOfStatementsOnDisk((a,b) -> {}));

        execute("CREATE KEYSPACE IF NOT EXISTS foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        execute("CREATE TABLE foo.bar (key text PRIMARY KEY, val int)");

        ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 1234));

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val text)");

        List<MD5Digest> stmtIds = new ArrayList<>();
        String statement0 = "SELECT * FROM %s WHERE keyspace_name = ?";
        String statement1 = "SELECT * FROM %s WHERE key = ?";
        String statement2 = "SELECT * FROM %s WHERE pk = ?";
        String statement3 = "SELECT * FROM %S WHERE key = ?";
        // gets stored once, fully qualified, without keyspace (no USE)
        stmtIds.add(prepareStatement(statement0, SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES, clientState));
        // gets stored once, fully qualified, without keyspace (no USE)
        stmtIds.add(prepareStatement(statement1, "foo", "bar", clientState));
        assertEquals(2, QueryProcessor.preparedStatementsCount());
        clientState.setKeyspace("foo");
        // gets stored twice, after USE
        stmtIds.add(prepareStatement(statement2, clientState));
        // gets stored twice, after USE
        stmtIds.add(prepareStatement(statement3, "foo", "bar", clientState));

        assertEquals(4, stmtIds.size());
        assertEquals(6, QueryProcessor.preparedStatementsCount());
        assertEquals(6, numberOfStatementsOnDisk((ks, stmt) -> {}));

        QueryHandler handler = ClientState.getCQLQueryHandler();
        validatePstmts(stmtIds, handler);
        List<Pair<String, String>> beforeClear = StorageService.instance.getPreparedStatements();
        for (Pair<String, String> p : beforeClear)
            assertEquals(ClientState.getCQLQueryHandler().getPrepared(MD5Digest.wrap(Hex.hexToBytes(p.left))).rawCQLStatement, p.right);

        assertTrue(beforeClear.size() > 0);
        assertEquals(QueryProcessor.preparedStatementsCount(), beforeClear.size());
        // clear prepared statements cache
        QueryProcessor.clearPreparedStatements(true);

        assertEquals(0, StorageService.instance.getPreparedStatements().size());
        assertEquals(0, QueryProcessor.preparedStatementsCount());
        for (MD5Digest stmtId : stmtIds)
            Assert.assertNull(handler.getPrepared(stmtId));

        // load prepared statements and validate that these still execute fine
        QueryProcessor.instance.preloadPreparedStatements();

        assertEquals(beforeClear, StorageService.instance.getPreparedStatements());
        validatePstmts(stmtIds, handler);

        // validate that the prepared statements are in the system table
        String queryAll = "SELECT * FROM " + SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS;
        for (UntypedResultSet.Row row : QueryProcessor.executeOnceInternal(queryAll))
        {
            MD5Digest digest = MD5Digest.wrap(ByteBufferUtil.getArray(row.getBytes("prepared_id")));
            QueryHandler.Prepared prepared = QueryProcessor.instance.getPrepared(digest);
            Assert.assertNotNull(String.format("Could not find prepared statement for id %s", digest), prepared);
        }

        // add anther prepared statement and sync it to table
        prepareStatement(statement1, "foo", "bar", clientState);

        assertEquals(7, numberOfStatementsInMemory());
        assertEquals(7, numberOfStatementsOnDisk((ks, stmt) -> {}));

        // drop a keyspace (prepared statements are removed - syncPreparedStatements() remove should the rows, too)
        execute("DROP KEYSPACE foo");
        assertEquals(3, numberOfStatementsInMemory());
        assertEquals(3, numberOfStatementsOnDisk((ks, stmt) -> Assert.assertFalse(stmt.contains("foo"))));
    }

    private void validatePstmts(List<MD5Digest> stmtIds, QueryHandler handler)
    {
        QueryOptions optionsStr = QueryOptions.forInternalCalls(Collections.singletonList(UTF8Type.instance.fromString("foobar")));
        QueryOptions optionsInt = QueryOptions.forInternalCalls(Collections.singletonList(Int32Type.instance.decompose(42)));
        validatePstmt(handler, stmtIds.get(0), optionsStr);
        validatePstmt(handler, stmtIds.get(1), optionsStr);
        validatePstmt(handler, stmtIds.get(2), optionsInt);
        validatePstmt(handler, stmtIds.get(3), optionsStr);
    }

    private static void validatePstmt(QueryHandler handler, MD5Digest stmtId, QueryOptions options)
    {
        QueryProcessor.Prepared prepared = handler.getPrepared(stmtId);
        Assert.assertNotNull(prepared);
        handler.processPrepared(prepared.statement, forInternalCalls(), options, emptyMap(), nanoTime());
    }

    @Test
    public void testPstmtInvalidation() throws Throwable
    {
        ClientState clientState = ClientState.forInternalCalls();

        createTable("CREATE TABLE %s (key int primary key, val int)");

        for (int cnt = 1; cnt < 10000; cnt++)
        {
            prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + cnt, clientState);

            if (numberOfEvictedStatements() > 0)
            {
                assertEquals("Number of statements in table and in cache don't match", numberOfStatementsInMemory(), numberOfStatementsOnDisk((ks, stmt) -> {}));

                // prepare a more statements to trigger more evictions
                for (int cnt2 = 1; cnt2 < 10; cnt2++)
                    prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + cnt2, clientState);

                // each new prepared statement should have caused an eviction
                assertEquals("eviction count didn't increase by the expected number", numberOfEvictedStatements(), 10);
                assertEquals("Number of statements in table and in cache don't match", numberOfStatementsInMemory(), numberOfStatementsOnDisk((ks, stmt) -> {}));

                return;
            }
        }

        fail("Prepared statement eviction does not work");
    }

    private long numberOfStatementsOnDisk(BiConsumer<String, String> check) throws Throwable
    {
        UntypedResultSet resultSet = execute("SELECT * FROM " + SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS);
        for (UntypedResultSet.Row row : resultSet)
        {
            check.accept((row.has("logged_keyspace") ? row.getString("logged_keyspace") : null),
                         row.getString("query_string"));
        }
        return resultSet.size();
    }

    private long numberOfStatementsInMemory()
    {
        return QueryProcessor.preparedStatementsCount();
    }

    private long numberOfEvictedStatements()
    {
        return QueryProcessor.metrics.preparedStatementsEvicted.getCount();
    }

    private MD5Digest prepareStatement(String format, ClientState clientState)
    {
        return prepareStatement(format, keyspace(), currentTable(), clientState);
    }

    private MD5Digest prepareStatement(String format, String keyspace, String table, ClientState clientState)
    {
        String statement = String.format(format, keyspace + "." + table);
        return QueryProcessor.instance.prepare(statement, clientState).statementId;
    }
}
