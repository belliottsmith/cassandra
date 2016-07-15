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

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;

public class CIEInternalLocalKeyspace
{
    private CIEInternalLocalKeyspace()
    {
    }

    public static final String NAME = "cie_internal_local";

    public static final String HEALTH_CHECK_LOCAL_CF = "health_check_local";

    private static final TableMetadata HealthCheckLocal =
        parse(HEALTH_CHECK_LOCAL_CF,
              "Used by C* Mgr health checks. Apple internal",
                "CREATE TABLE %s ("
                + "key blob,"
                + "column1 blob,"
                + "value blob,"
                + "PRIMARY KEY((key), column1))")
        .build();

    private static TableMetadata.Builder parse(String tableName, String description, String schema)
    {
        // Table parameters matched with SystemKeyspace
        return CreateTableStatement.parse(String.format(schema, tableName), NAME)
                                   .id(TableId.forSystemTable(NAME, tableName))
                                   .gcGraceSeconds(0)
                                   .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.local(), Tables.of(HealthCheckLocal));
    }


}
