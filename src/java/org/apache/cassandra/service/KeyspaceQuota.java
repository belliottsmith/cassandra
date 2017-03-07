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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.CIEInternalKeyspace;

/**
 * Sets a quota on a keyspace, this is only used in QA environments
 */
public class KeyspaceQuota
{
    private static final Logger logger = LoggerFactory.getLogger(KeyspaceQuota.class);

    public static final String KS_QUOTA_CF = "ks_quota";

    /**
     * Return the amount of bytes this keyspace is allowed to use
     * @param ksname name of the keyspace
     * @return the number of bytes on disk allowed
     */
    public static long getQuotaForKS(String ksname)
    {
        if (!DatabaseDescriptor.getEnableKeyspaceQuotas())
            return -1;
        String query = String.format("SELECT max_ks_size_mb FROM %s.%s WHERE keyspace_name='%s'", CIEInternalKeyspace.NAME, KS_QUOTA_CF, ksname);
        try
        {
            UntypedResultSet res = QueryProcessor.process(query, ConsistencyLevel.ONE);
            if (res.isEmpty())
            {
                return DatabaseDescriptor.getDefaultKeyspaceQuotaBytes();
            }

            return ((long)res.one().getInt("max_ks_size_mb")) * 1024 * 1024;
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException("Got error querying quota keyspace", e);
        }

    }

    static void scheduleQuotaCheck()
    {
        ScheduledExecutors.optionalTasks.scheduleAtFixedRate(new Runnable()
        {
            public void run()
            {
                if (!DatabaseDescriptor.getEnableKeyspaceQuotas())
                {
                    return; // note that we don't need to set ks.disableForWrites to false since we check the enabled flag in ModificationStatement#checkAccess
                }

                for (Keyspace ks : Keyspace.nonSystem())
                {
                    try
                    {
                        long ksQuota = KeyspaceQuota.getQuotaForKS(ks.getName());
                        logger.info("{} ksQuota = {}", ks.getName(), ksQuota);
                        long ksSize = ks.metric.liveDiskSpaceUsed.getValue();
                        boolean disabledForWritesBefore = ks.disabledForWrites;
                        ks.disabledForWrites = ksQuota >= 0 && ksSize > ksQuota;
                        if (disabledForWritesBefore != ks.disabledForWrites)
                            logger.info((ks.disabledForWrites ? "Disabling" : "Enabling") + " keyspace {} for writes, keyspace size = {}, keyspace quota = {}", ks.getName(), ksSize, ksQuota);
                    }
                    catch (Throwable t)
                    {
                        logger.error("Could not query quota keyspace", t);
                    }
                }
            }
        }, 0, DatabaseDescriptor.getKeyspaceQuotaRefreshTimeInSec(), TimeUnit.SECONDS);
    }
}
