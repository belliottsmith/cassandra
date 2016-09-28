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

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionSizeCommand;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

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

    public final VariableSpecifications bindVariables;
    private final TableMetadata table;
    private final StatementRestrictions restrictions;

    public SelectSizeStatement(TableMetadata table,
                               VariableSpecifications bindVariables,
                               StatementRestrictions restrictions)
    {
        this.table = table;
        this.bindVariables = bindVariables;
        this.restrictions = restrictions;
    }

    @Override
    public List<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getBindVariables();
    }

    public void validate(ClientState state) throws RequestValidationException
    {
    }

    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.SELECT_SIZE, keyspace(), table.name);
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
        return new PartitionSizeCommand(table.keyspace, table.name, getKey(options));
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");

        cl.validateForRead();

        return executeInternal(state, options, queryStartNanoTime);
    }

    public ResultMessage.Rows executeLocally(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return executeInternal(state, options, nanoTime());
    }

    private ResultMessage.Rows executeInternal(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        if (options.getConsistency().isSerialConsistency())
        {
            throw new InvalidRequestException("SELECT_SIZE statements cannot use SERIAL consistency");
        }
        Map<InetAddressAndPort, Long> sizes = StorageProxy.fetchPartitionSize(createCommand(options), options.getConsistency(), queryStartNanoTime);
        return convertToRowsResultSet(sizes);
    }

    public String keyspace()
    {
        return table.keyspace;
    }

    public String columnFamily()
    {
        return table.name;
    }

    private ResultMessage.Rows convertToRowsResultSet(Map<InetAddressAndPort, Long> sizesPerReplica)
    {
        Set<Map.Entry<InetAddressAndPort, Long>> resultEntries = sizesPerReplica.entrySet();
        List<List<ByteBuffer>> rows = new ArrayList<>(resultEntries.size());
        for (Map.Entry<InetAddressAndPort, Long> entry : resultEntries)
        {
            rows.add(getSerializedRowEntry(entry));
        }
        return new ResultMessage.Rows(new ResultSet(new ResultSet.ResultMetadata(getColumnSpecifications()), rows));
    }

    private List<ByteBuffer> getSerializedRowEntry(Map.Entry<InetAddressAndPort, Long> entry)
    {
        List<ByteBuffer> row = new ArrayList<>(2);
        row.add(InetAddressSerializer.instance.serialize(entry.getKey().getAddress()));
        row.add(LongSerializer.instance.serialize(entry.getValue()));
        return row;
    }

    private List<ColumnSpecification> getColumnSpecifications()
    {
        List<ColumnSpecification> specifications = new ArrayList<>(2);
        specifications.add(new ColumnSpecification(table.keyspace, table.name, ENDPOINT_IDENTIFIER, InetAddressType.instance));
        specifications.add(new ColumnSpecification(table.keyspace, table.name, SIZE_IDENTIFIER, LongType.instance));
        return specifications;
    }

    public void authorize(ClientState state)
    {
        if (table.isView())
        {
            TableMetadataRef baseTable = View.findBaseTable(keyspace(), columnFamily());
            if (baseTable != null)
                state.ensureTablePermission(baseTable, Permission.SELECT);
        }
        else
        {
            state.ensureTablePermission(table, Permission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensurePermission(Permission.EXECUTE, function);
    }

    public static class RawStatement extends QualifiedStatement
    {
        private final WhereClause whereClause;

        public RawStatement(QualifiedName cfName,
                            WhereClause whereClause)
        {
            super(cfName);
            this.whereClause = whereClause;
        }

        public SelectSizeStatement prepare(ClientState state) throws RequestValidationException
        {
            TableMetadata table = Schema.instance.validateTable(keyspace(), name());
            checkTrue(!table.isView(), "SELECT SIZE statement can not be used on views");

            StatementRestrictions restrictions = parseAndValidateRestrictions(table, bindVariables);
            return new SelectSizeStatement(table, bindVariables, restrictions);
        }

        private StatementRestrictions parseAndValidateRestrictions(TableMetadata table, VariableSpecifications boundNames)
        {
            StatementRestrictions restrictions = new StatementRestrictions(StatementType.SELECT, table, whereClause, boundNames, false, false, false, false);
            // The WHERE clause can only restrict the query to a single partition (nothing more restrictive, nothing less restrictive).
            checkTrue(restrictions.hasPartitionKeyRestrictions(), "SELECT SIZE statements must be restricted to a partition.");
            checkTrue(!restrictions.hasClusteringColumnsRestrictions(), "SELECT SIZE statements can only have partition key restrictions.");
            checkTrue(!restrictions.hasNonPrimaryKeyRestrictions(), "SELECT SIZE statements can only have partition key restrictions.");
            return restrictions;
        }
    }
}

