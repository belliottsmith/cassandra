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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.PartitionSizeCommand;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * Provides client with a method determining the size of a partition on disk for a given row
 *
 * syntax is: SELECT_SIZE FROM <keyspace>.<table> WHERE <partition_key>=<some_val>;
 *
 * see: <rdar://problem/26203712> Add API to request the size of a row
 */
public class SelectSizeStatement implements CQLStatement
{
    private static final ColumnIdentifier ENDPOINT_IDENTIFIER = new ColumnIdentifier("endpoint", true);
    private static final ColumnIdentifier SIZE_IDENTIFIER = new ColumnIdentifier("size (bytes)", true);

    private final CFMetaData cfm;
    private final StatementRestrictions restrictions;
    private final int boundTerms;

    public SelectSizeStatement(CFMetaData cfm, StatementRestrictions restrictions, int boundTerms)
    {
        this.cfm = cfm;
        this.restrictions = restrictions;
        this.boundTerms = boundTerms;
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(cfm.ksName, cfm.cfName, Permission.SELECT);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
    }

    private ByteBuffer getKey(QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> keys = restrictions.getPartitionKeys(options);
        checkTrue(keys.size() == 1, "SELECT SIZE statements can only be restricted to a single partition key.");
        return keys.get(0);
    }

    @VisibleForTesting
    PartitionSizeCommand createCommand(QueryOptions options) throws InvalidRequestException
    {
        return new PartitionSizeCommand(cfm.ksName, cfm.cfName, getKey(options));
    }

    @Override
    public ResultMessage.Rows execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        if (options.getConsistency().isSerialConsistency())
        {
            throw new InvalidRequestException("SELECT_SIZE statements cannot use SERIAL consistency");
        }
        Map<InetAddress, Long> sizes = StorageProxy.fetchPartitionSize(createCommand(options), options.getConsistency(), System.nanoTime());
        return convertToRowsResultSet(sizes);
    }

    @Override
    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return execute(state, options);
    }

    private ResultMessage.Rows convertToRowsResultSet(Map<InetAddress, Long> sizesPerReplica)
    {
        Set<Map.Entry<InetAddress, Long>> resultEntries = sizesPerReplica.entrySet();
        List<List<ByteBuffer>> rows = new ArrayList<>(resultEntries.size());
        for (Map.Entry<InetAddress, Long> entry : resultEntries)
        {
            rows.add(getSerializedRowEntry(entry));
        }
        return new ResultMessage.Rows(new ResultSet(new ResultSet.ResultMetadata(getColumnSpecifications()), rows));
    }

    private List<ByteBuffer> getSerializedRowEntry(Map.Entry<InetAddress, Long> entry)
    {
        List<ByteBuffer> row = new ArrayList<>(2);
        row.add(InetAddressSerializer.instance.serialize(entry.getKey()));
        row.add(LongSerializer.instance.serialize(entry.getValue()));
        return row;
    }

    private List<ColumnSpecification> getColumnSpecifications()
    {
        List<ColumnSpecification> specifications = new ArrayList<>(2);
        specifications.add(new ColumnSpecification(cfm.ksName, cfm.cfName, ENDPOINT_IDENTIFIER, InetAddressType.instance));
        specifications.add(new ColumnSpecification(cfm.ksName, cfm.cfName, SIZE_IDENTIFIER, LongType.instance));
        return specifications;
    }

    public Iterable<org.apache.cassandra.cql3.functions.Function> getFunctions()
    {
        return Collections.emptyList();
    }

    public static class RawStatement extends CFStatement
    {
        private final WhereClause whereClause;

        public RawStatement(CFName cfName, WhereClause whereClause)
        {
            super(cfName);
            this.whereClause = whereClause;
        }

        public Prepared prepare(ClientState clientState) throws RequestValidationException
        {
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            VariableSpecifications boundNames = getBoundVariables();
            StatementRestrictions restrictions = parseAndValidateRestrictions(cfm, boundNames);
            SelectSizeStatement statement = new SelectSizeStatement(cfm, restrictions, boundNames.size());
            return new ParsedStatement.Prepared(statement, boundNames, boundNames.getPartitionKeyBindIndexes(cfm), cfm.ksName);
        }

        private StatementRestrictions parseAndValidateRestrictions(CFMetaData cfm, VariableSpecifications boundNames)
        {
            StatementRestrictions restrictions = new StatementRestrictions(StatementType.SELECT, cfm, whereClause, boundNames, false, false, false, false);
            // The WHERE clause can only restrict the query to a single partition (nothing more restrictive, nothing less restrictive).
            checkTrue(restrictions.hasPartitionKeyRestrictions(), "SELECT SIZE statements must be restricted to a partition.");
            checkTrue(!restrictions.hasClusteringColumnsRestriction(), "SELECT SIZE statements can only have partition key restrictions.");
            checkTrue(!restrictions.hasNonPrimaryKeyRestrictions(), "SELECT SIZE statements can only have partition key restrictions.");
            return restrictions;
        }
    }
}

