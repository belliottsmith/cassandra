/*
 * Copyright (c) 2018-2022 Apple, Inc. All rights reserved.
 */

package com.apple.cie.db.virtual;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.REPAIR_HISTORY_CF;

/**
 * An Apple Internal {@link VirtualTable} that returns repair information in tabular format.
 */
public final class RepairInfoTable extends AbstractVirtualTable
{
    static final String TABLE_COMMENT = "lists the repair information";
    final static String KEYSPACE_NAME = "keyspace_name";
    final static String TABLE_NAME = "table_name";
    static final String START_TOKEN = "start_token";
    static final String END_TOKEN = "end_token";
    static final String LAST_REPAIR_TIME = "last_repair_time";

    public RepairInfoTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "xmas_repair_info")
                           .comment(TABLE_COMMENT)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance)))
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE_NAME, UTF8Type.instance)
                           .addClusteringColumn(START_TOKEN, UTF8Type.instance)
                           .addClusteringColumn(END_TOKEN, UTF8Type.instance)
                           .addRegularColumn(LAST_REPAIR_TIME, TimestampType.instance).build());
    }

    @Override
    public AbstractVirtualTable.DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        populateDataSet(result, null, null);
        return result;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        ByteBuffer[] keyspaceAndColumnFamilyBytes = ((CompositeType) metadata().partitionKeyType).split(partitionKey.getKey());
        String keyspace = UTF8Type.instance.compose(keyspaceAndColumnFamilyBytes[0]);
        String columnFamily = UTF8Type.instance.compose(keyspaceAndColumnFamilyBytes[1]);

        SimpleDataSet result = new SimpleDataSet(metadata());
        populateDataSet(result, keyspace, columnFamily);
        return result;
    }

    private void populateDataSet(SimpleDataSet ds, String keyspace, String columnFamily)
    {
        UntypedResultSet resultSet;
        if (keyspace != null && columnFamily != null)
        {
            String req = "SELECT * FROM system.%s WHERE keyspace_name = ? AND columnfamily_name = ?";
            resultSet = executeInternal(String.format(req, REPAIR_HISTORY_CF), keyspace, columnFamily);
        }
        else
        {
            String req = "SELECT * FROM system.%s LIMIT 2000";
            resultSet = executeInternal(String.format(req, REPAIR_HISTORY_CF));
        }

        if (resultSet == null || resultSet.isEmpty())
            return;

        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Token.TokenFactory tokenFactory = partitioner.getTokenFactory();
        for (UntypedResultSet.Row row : resultSet)
        {
            String keyspaceName = row.getString("keyspace_name");
            String columnFamilyName = row.getString("columnfamily_name");
            Date lastRepairTime = TimestampType.instance.compose(row.getBytes("succeed_at"));
            Range<Token> range = SystemKeyspace.byteBufferToRange(row.getBytes("range"), partitioner);

            // handle the wrap-around case
            if (range.left.compareTo(range.right) > 0)
            {
                ds.row(keyspaceName, columnFamilyName, tokenFactory.toString(range.left), tokenFactory.toString(partitioner.getMaximumToken()))
                  .column(LAST_REPAIR_TIME, lastRepairTime);

                ds.row(keyspaceName, columnFamilyName, tokenFactory.toString(partitioner.getMinimumToken()), tokenFactory.toString(range.right))
                  .column(LAST_REPAIR_TIME, lastRepairTime);
            }
            else
            {
                ds.row(keyspaceName, columnFamilyName, tokenFactory.toString(range.left), tokenFactory.toString(range.right))
                  .column(LAST_REPAIR_TIME, lastRepairTime);
            }
        }
    }
}
