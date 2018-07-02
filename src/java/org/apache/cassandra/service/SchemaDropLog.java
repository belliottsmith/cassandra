package org.apache.cassandra.service;


import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;

import org.slf4j.*;

import java.text.*;
import java.util.*;

import static org.apache.cassandra.cql3.QueryProcessor.process;

public class SchemaDropLog
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaDropLog.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static volatile boolean disableCheckForTest = Boolean.parseBoolean(System.getProperty("cie-cassandra.disable_schema_drop_log", "false"));

    public static void addSchemaDrop(final String ks_name, String cf_name)
    {
        if (disableCheckForTest || Schema.isInternalKeyspace(ks_name))
            return;

        String query = "INSERT INTO " + CIEInternalKeyspace.NAME + "." + CIEInternalKeyspace.SCHEMA_DROP_LOG + " (ks_name, cf_name, time) VALUES (\'"
                + ks_name.trim() + "\',\'" + cf_name.trim() + "\',\'" + sdf.format(new Date()) +  "\')";
        try
        {
            process(query, ConsistencyLevel.QUORUM);
            logger.info("Writing schema drop to log for keyspace=" + ks_name + " and table=" + cf_name);
        }
        catch (Exception e)
        {
            logger.error("Could not insert into schema drop log for ks_name=" + ks_name + " and cf_name=" + cf_name, e);
        }

    }

    public static boolean schemaDropExists(final String ks_name, String cf_name)
    {
        if (DatabaseDescriptor.isSchemaDropCheckDisabled())
            return false;

        if(disableCheckForTest || Schema.isInternalKeyspace(ks_name))
            return false;

        String query = "SELECT * FROM " + CIEInternalKeyspace.NAME + "." + CIEInternalKeyspace.SCHEMA_DROP_LOG + "  WHERE ks_name=\'"
                + ks_name.trim() + "\' AND cf_name=\'" + cf_name.trim() + "\'";
        try
        {
            UntypedResultSet resultSet = process(query, ConsistencyLevel.QUORUM);
            if(resultSet.isEmpty())
                return false;
            else
                return true;
        }
        catch (Exception e)
        {
            logger.error("Could not select from schema drop log for ks_name=" + ks_name + " and cf_name=" + cf_name, e);
        }

        //In case of error, we will allow create.
        return false;
    }
}