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

package org.apache.cassandra.cql3.statements;

import java.util.Collections;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionSizeCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SelectSizeStatementTest
{
    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        CFMetaData[] cfms = {
            CFMetaData.compile("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "ks"),
            CFMetaData.compile("CREATE TABLE tbl2 (k1 INT, k2 INT , v INT, PRIMARY KEY ((k1, k2)))", "ks"),
        };
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1), cfms);
    }

    @Test
    public void successCase() throws Exception
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1").prepare();
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl2 WHERE k1=1 AND k2=2").prepare();
    }

    /**
     * Check proper commands are created for both parameterized and un-parameterized queries
     */
    @Test
    public void commandCreation() throws Exception
    {
        SelectSizeStatement statement;
        PartitionSizeCommand command;

        // parameterized
        statement = (SelectSizeStatement) QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=?").prepare().statement;
        command = statement.createCommand(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Lists.newArrayList(ByteBufferUtil.bytes(1))));
        Assert.assertEquals("ks", command.keyspace);
        Assert.assertEquals("tbl", command.table);
        Assert.assertEquals(ByteBufferUtil.bytes(1), command.key);

        // not parameterized
        statement = (SelectSizeStatement) QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1").prepare().statement;
        command = statement.createCommand(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Lists.newArrayList()));
        Assert.assertEquals("ks", command.keyspace);
        Assert.assertEquals("tbl", command.table);
        Assert.assertEquals(ByteBufferUtil.bytes(1), command.key);
    }

    /**
     * Non '=' where terms should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void nonEqOp() throws Exception
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k>1").prepare();
    }

    /**
     * Specifying only part of a compound partition key should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void incompletePartitionKey() throws Exception
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl2 WHERE k1=1").prepare();
    }

    @Test(expected = InvalidRequestException.class)
    public void nonExistantTable() throws Exception
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.notable WHERE k1=1").prepare();
    }

    @Test(expected = SyntaxException.class)
    public void noWhereClause() throws Exception
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl").prepare();
    }

    /**
     * Including a non-partition key in the where clause should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void nonPartitionKeyWhere() throws Exception
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1 AND v=1").prepare();
    }

    @Test(expected = InvalidRequestException.class)
    public void repeatedColumns() throws Exception
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1 AND k=2").prepare();
    }

    /**
     * Attempting to execute with serial consistency should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void serialConsistency() throws Exception
    {
        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1").prepare();
        Assert.assertSame(SelectSizeStatement.class, prepared.statement.getClass());
        CQLStatement stmt = prepared.statement;
        stmt.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(ConsistencyLevel.SERIAL, Collections.emptyList()));
    }
}
