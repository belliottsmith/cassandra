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

package org.apache.cassandra.service;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;

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
     */
    public static final long GENERATION = 1577836800000000L;

    public static final String SCHEMA_DROP_LOG = "schema_drop_log";

    private static final CFMetaData SchemaDropLog =
    compile(SCHEMA_DROP_LOG,
            "Store all dropped tables for apple internal patch",
            "CREATE TABLE %s ("
            + "ks_name text,"
            + "cf_name text,"
            + "time timestamp,"
            + "PRIMARY KEY((ks_name), cf_name))");

    public static final CFMetaData PartitionBlacklistCf = compile(PartitionBlacklist.PARTITION_BLACKLIST_CF,
            "Partition keys which have been blacklisted",
            "CREATE TABLE %s ("
            + "ks_name text,"
            + "cf_name text,"
            + "key blob,"
            + "PRIMARY KEY ((ks_name, cf_name), key))");

    public static final CFMetaData KeyspaceQuotaCf = compile(KeyspaceQuota.KS_QUOTA_CF,
            "Table containing keyspace quotas, for QA",
            "CREATE TABLE %s ("
            + "keyspace_name text PRIMARY KEY,"
            + "max_ks_size_mb int)");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), NAME)
                         .comment(description)
                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(10));
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.simple(Integer.getInteger("cie_internal_rf", 3)), Tables.of(SchemaDropLog, PartitionBlacklistCf, KeyspaceQuotaCf));
    }
}
