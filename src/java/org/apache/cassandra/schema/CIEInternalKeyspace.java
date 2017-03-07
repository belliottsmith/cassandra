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

package org.apache.cassandra.schema;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.service.KeyspaceQuota;

public class CIEInternalKeyspace
{
    private CIEInternalKeyspace()
    {
    }

    public static final String NAME = "cie_internal";

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen 1577836800000000: original modern definition in 3.0.19; maps to Jan 1 2020, the date it's assumed we
     *                       will have no more fresh 2.1 or 3.0.17 clusters going up, or upgrades from 2.1 to 3.0.17.
     *
     * gen                1: downgrading the generation to 1 in order to prevent the override of table id for
     *                       partition_blacklist table - which used to be created manually in 2.1 initially, and
     *                       as such, doesn't have a deterministic id. See rdar://56101440. Until we find a good way
     *                       to resolve that issue permanently, the tables in this keyspace can only be evolved manually.
     * gen                2: compression chunk length reduced to 16KiB, memtable_flush_period_in_ms now unset on all tables in 4.0
     */
    public static final long GENERATION = 2;

    public static final String SCHEMA_DROP_LOG = "schema_drop_log";

    private static final TableMetadata SchemaDropLog =
        parse(SCHEMA_DROP_LOG,
          "Store all dropped tables for apple internal patch",
            "CREATE TABLE %s ("
            + "ks_name text,"
            + "cf_name text,"
            + "time timestamp,"
            + "PRIMARY KEY((ks_name), cf_name))")
        .build();

    public static final TableMetadata KeyspaceQuotaCf =
        parse(KeyspaceQuota.KS_QUOTA_CF,
              "Table containing keyspace quotas, for QA",
              "CREATE TABLE %s ("
              + "keyspace_name text PRIMARY KEY,"
              + "max_ks_size_mb int)")
        .build();

    private static TableMetadata.Builder parse(String tableName, String description, String schema)
    {
        return CreateTableStatement.parse(String.format(schema, tableName), NAME)
                                   .id(TableId.forSystemTable(NAME, tableName))
                                   .comment(description)
                                   .compaction(CompactionParams.DEFAULT_SYSTEM)
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(10));

    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.simple(Integer.getInteger("cie_internal_rf", 3)), Tables.of(SchemaDropLog, KeyspaceQuotaCf));
    }
}
