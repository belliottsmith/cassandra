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

import java.util.*;
import org.slf4j.*;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.UTF8Type;

import static org.apache.cassandra.cql3.QueryProcessor.process;

public class SchemaDropLog
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaDropLog.class);

    public static volatile boolean disableCheckForTest = Boolean.parseBoolean(System.getProperty("cie-cassandra.disable_schema_drop_log", "false"));

    public static void initialize()
    {
        Schema.instance.registerListener(new SchemaChangeListener()
        {
            public void onDropTable(TableMetadata table)
            {
                addSchemaDrop(table.keyspace, table.name);
            }
        });
    }

    private static void addSchemaDrop(final String ks_name, String cf_name)
    {
        if (disableCheckForTest || SchemaConstants.isInternalKeyspace(ks_name))
            return;

        String query = "INSERT INTO " + CIEInternalKeyspace.NAME + "." + CIEInternalKeyspace.SCHEMA_DROP_LOG + " (ks_name, cf_name, time) VALUES (?, ?, ?)";
        try
        {
            process(query, ConsistencyLevel.QUORUM,
                    Arrays.asList(UTF8Type.instance.decompose(ks_name.trim()),
                                  UTF8Type.instance.decompose(cf_name.trim()),
                                  DateType.instance.decompose(new Date())));
            logger.info("Writing schema drop to log for keyspace='{}' and table='{}'", ks_name, cf_name);
        }
        catch (Exception e)
        {
            logger.error(String.format("Could not insert from schema drop log for ks_name='%s' and cf_name='%s'",
                                       ks_name, cf_name), e);
        }

    }

    public static boolean schemaDropExists(final String ks_name, String cf_name)
    {
        if (DatabaseDescriptor.isSchemaDropCheckDisabled())
            return false;

        if(disableCheckForTest || SchemaConstants.isInternalKeyspace(ks_name))
            return false;

        String query = "SELECT * FROM " + CIEInternalKeyspace.NAME + "." + CIEInternalKeyspace.SCHEMA_DROP_LOG +
                       "  WHERE ks_name=? AND cf_name=?";
        try
        {
            UntypedResultSet resultSet = process(query, ConsistencyLevel.QUORUM,
                    Arrays.asList(UTF8Type.instance.decompose(ks_name.trim()),
                                  UTF8Type.instance.decompose(cf_name.trim())));
            return !resultSet.isEmpty();
        }
        catch (Exception e)
        {
            logger.error(String.format("Could not select from schema drop log for ks_name='%s' and cf_name='%s'",
                                       ks_name, cf_name), e);
        }

        //In case of error, we will allow create.
        return false;
    }
}
