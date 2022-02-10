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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.range.RangeCommands;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * PartitionDenylist uses the system_distributed.partition_denylist table to maintain a list of denylisted partition keys
 * for each keyspace/table.
 *
 * Keys can be entered manually into the partition_denylist table or via the JMX operation StorageProxyMBean.denylistKey
 *
 * The denylist is stored as one CQL partition per table, and the denylisted keys are column names in that partition. The denylisted
 * keys for each table are cached in memory, and reloaded from the partition_denylist table every 10 minutes (default) or when the
 * StorageProxyMBean.loadPartitionDenylist is called via JMX.
 *
 * Concurrency of the cache is provided by the concurrency semantics of the guava LoadingCache. All values (DenylistEntry) are
 * immutable collections of keys/tokens which are replaced in whole when the cache refreshes from disk.
 *
 * The CL for the denylist is used on initial node load as well as on timer instigated cache refreshes. A JMX call by the
 * operator to load the denylist cache will warn on CL unavailability but go through with the denylist load. This is to
 * allow operators flexibility in the face of degraded cluster state and still grant them the ability to mutate the denylist
 * cache and bring it up if there are things they need to block on startup.
 *
 * Notably, in the current design it's possible for a table *cache expiration instigated* reload to end up violating the
 * contract on total denylisted keys allowed in the case where it initially loads with a value less than the DBD
 * allowable max per table limit due to global constraint enforcement on initial load. Our load and reload function
 * simply enforce the *per table* limit without consideration to what that entails at the global key level. While we
 * could track the constrained state and count in DenylistEntry, for now the complexity doesn't seem to justify the
 * protection against that edge case. The enforcement should take place on a user-instigated full reload as well as
 * error messaging about count violations, so this only applies to situations in which someone adds a key and doesn't
 * actively tell the cache to fully reload to take that key into consideration, which one could reasonably expect to be
 * an antipattern.
 */
public class PartitionDenylist
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionDenylist.class);
    private static final NoSpamLogger AVAILABILITY_LOGGER = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private final ExecutorService executor = executorFactory().pooled("DenylistCache", 2);

    /** We effectively don't use our initial empty cache to denylist until the {@link #load()} call which will replace it */
    private volatile LoadingCache<TableId, DenylistEntry> denylist = buildEmptyCache();

    /** Denylist entry is never mutated once constructed, only replaced with a new entry when the cache is refreshed */
    private static class DenylistEntry
    {
        public final ImmutableSet<ByteBuffer> keys;
        public final ImmutableSortedSet<Token> tokens;

        public DenylistEntry()
        {
            keys = ImmutableSet.of();
            tokens = ImmutableSortedSet.of();
        }

        public DenylistEntry(final ImmutableSet<ByteBuffer> keys, final ImmutableSortedSet<Token> tokens)
        {
            this.keys = keys;
            this.tokens = tokens;
        }
    }

    /** synchronized on this */
    private int loadAttempts = 0;

    /** synchronized on this */
    private int loadSuccesses = 0;

    public synchronized int getLoadAttempts()
    {
        return loadAttempts;
    }
    public synchronized int getLoadSuccesses()
    {
        return loadSuccesses;
    }

    /**
     * Performs initial load of the partition denylist. Should be called at startup and only loads if the operation
     * is expected to succeed.  If it is not possible to load at call time, a timer is set to retry.
     */
    public void initialLoad()
    {
        if (!DatabaseDescriptor.getPartitionDenylistEnabled())
            return;

        synchronized (this)
        {
            loadAttempts++;
        }

        // Check if there are sufficient nodes to attempt reading all the denylist partitions before issuing the query.
        // The pre-check prevents definite range-slice unavailables being marked and triggering an alert.  Nodes may still change
        // state between the check and the query, but it should significantly reduce the alert volume.
        String retryReason = "Insufficient nodes";
        try
        {
            if (checkDenylistNodeAvailability())
            {
                load();
                return;
            }
        }
        catch (Throwable tr)
        {
            logger.error("Failed to load partition denylist", tr);
            retryReason = "Exception";
        }

        // This path will also be taken on other failures other than UnavailableException,
        // but seems like a good idea to retry anyway.
        int retryInSeconds = DatabaseDescriptor.getDenylistInitialLoadRetrySeconds();
        logger.info("{} while loading partition denylist cache. Scheduled retry in {} seconds.", retryReason, retryInSeconds);
        ScheduledExecutors.optionalTasks.schedule(this::initialLoad, retryInSeconds, TimeUnit.SECONDS);
    }

    private boolean checkDenylistNodeAvailability()
    {
        boolean sufficientNodes = RangeCommands.sufficientLiveNodesForSelectStar(SystemDistributedKeyspace.PartitionDenylistTable,
                                                                                 DatabaseDescriptor.getDenylistConsistencyLevel());
        if (!sufficientNodes)
        {
            AVAILABILITY_LOGGER.warn("Attempting to load denylist and not enough nodes are available for a {} refresh. Reload the denylist when unavailable nodes are recovered to ensure your denylist remains in sync.",
                                     DatabaseDescriptor.getDenylistConsistencyLevel());
        }
        return sufficientNodes;
    }

    /** Helper method as we need to both build cache on initial init but also on reload of cache contents and params */
    private LoadingCache<TableId, DenylistEntry> buildEmptyCache()
    {
        // We rely on details of .refreshAfterWrite to reload this async in the background when it's hit:
        // https://github.com/ben-manes/caffeine/wiki/Refresh
        return Caffeine.newBuilder()
                       .refreshAfterWrite(DatabaseDescriptor.getDenylistRefreshSeconds(), TimeUnit.SECONDS)
                       .executor(executor)
                       .build(new CacheLoader<TableId, DenylistEntry>()
                       {
                           @Override
                           public DenylistEntry load(final TableId tid)
                           {
                               // We load whether or not the CL required count are available as the alternative is an
                               // empty denylist. This allows operators to intervene in the event they need to deny or
                               // undeny a specific partition key around a node recovery.
                               checkDenylistNodeAvailability();
                               return getDenylistForTableFromCQL(tid);
                           }

                           // The synchronous reload method defaults to being wrapped with a supplyAsync in CacheLoader.asyncReload
                           @Override
                           public DenylistEntry reload(final TableId tid, final DenylistEntry oldValue)
                           {
                               // Only process when we can hit the user specified CL for the denylist consistency on a timer prompted reload
                               if (checkDenylistNodeAvailability())
                               {
                                   final DenylistEntry newEntry = getDenylistForTableFromCQL(tid);
                                   if (newEntry != null)
                                       return newEntry;
                               }
                               if (oldValue != null)
                                   return oldValue;
                               return new DenylistEntry();
                           }
                       });
    }

    /**
     * We need to fully rebuild a new cache to accommodate deleting items from the denylist and potentially shrinking
     * the max allowable size in the list. We do not serve queries out of this denylist until it is populated
     * so as not to introduce a window of having a partially filled cache allow denylisted entries.
     */
    public void load()
    {
        final long start = currentTimeMillis();

        final Map<TableId, DenylistEntry> allDenylists = getDenylistForAllTablesFromCQL();

        // On initial load we have the slight overhead of GC'ing our initial empty cache
        LoadingCache<TableId, DenylistEntry> newDenylist = buildEmptyCache();
        newDenylist.putAll(allDenylists);

        synchronized (this)
        {
            loadSuccesses++;
        }
        denylist = newDenylist;
        logger.info("Loaded partition denylist cache in {}ms", currentTimeMillis() - start);
    }

    /**
     * We expect the caller to confirm that we are working with a valid keyspace and table. Further, we expect the usage
     * pattern of this to be one-off key by key, not in a bulk process, so we reload the entire table's deny list entry
     * on an addition or removal.
     */
    public boolean addKeyToDenylist(final String keyspace, final String table, final ByteBuffer key)
    {
        int successfulAdds = 0;
        if (!canDenylistKeyspace(keyspace))
            return false;

        for (DenylistParams params : DenylistParams.active())
        {
            String insert = params.addKeyToDenylistQuery(keyspace, table, ByteBufferUtil.bytesToHex(key));

            try
            {
                process(insert, DatabaseDescriptor.getDenylistConsistencyLevel());
                successfulAdds++;
            }
            catch (final Exception e) // 3.0.x nodes throw a RuntimeException (at least in in-JVM dtests)
            {
                logger.error("Failed to {} key [{}] in {}/{}", params.description, ByteBufferUtil.bytesToHex(key), keyspace, table, e);
            }
        }
        if (successfulAdds > 0)
            return refreshTableDenylist(keyspace, table);
        else
            return false;
    }

    /**
     * We expect the caller to confirm that we are working with a valid keyspace and table.
     */
    public boolean removeKeyFromDenylist(final String keyspace, final String table, final ByteBuffer key)
    {
        int successfulRemoves = 0;

        for (DenylistParams params : DenylistParams.active())
        {
            String delete = params.removeKeyFromDenylistQuery(keyspace, table, ByteBufferUtil.bytesToHex(key));

            try
            {
                process(delete, DatabaseDescriptor.getDenylistConsistencyLevel());
                successfulRemoves++;
            }
            catch (final RequestExecutionException e)
            {
                logger.error("Failed to remove key from {}: [{}] in {}/{}", params.description, ByteBufferUtil.bytesToHex(key), keyspace, table, e);
            }
        }
        if (successfulRemoves > 0)
            return refreshTableDenylist(keyspace, table);
        else
            return false;
    }

    /**
     * We disallow denylisting partitions in certain critical keyspaces to prevent users from making their clusters
     * inoperable.
     */
    private boolean canDenylistKeyspace(final String keyspace)
    {
        return !SchemaConstants.DISTRIBUTED_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.TRACE_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.VIRTUAL_SCHEMA.equals(keyspace) &&
               !SchemaConstants.VIRTUAL_VIEWS.equals(keyspace) &&
               !SchemaConstants.AUTH_KEYSPACE_NAME.equals(keyspace) &&
               !CIEInternalKeyspace.NAME.equals(keyspace);
    }

    public boolean isKeyPermitted(final String keyspace, final String table, final ByteBuffer key)
    {
        return isKeyPermitted(getTableId(keyspace, table), key);
    }

    public boolean isKeyPermitted(final TableId tid, final ByteBuffer key)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);

        // We have a few quick state checks to get out of the way first; this is hot path so we want to do these first if possible.
        if (!DatabaseDescriptor.getPartitionDenylistEnabled() || tid == null || tmd == null || !canDenylistKeyspace(tmd.keyspace))
            return true;

        try
        {
            // If we don't have an entry for this table id, nothing in it is denylisted.
            DenylistEntry entry = denylist.get(tid);
            if (entry == null)
                return true;
            return !entry.keys.contains(key);
        }
        catch (final Exception e)
        {
            // In the event of an error accessing or populating the cache, assume it's not denylisted
            logAccessFailure(tid, e);
            return true;
        }
    }

    private void logAccessFailure(final TableId tid, Throwable e)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (tmd == null)
            logger.debug("Failed to access partition denylist cache for unknown table id {}", tid.toString(), e);
        else
            logger.debug("Failed to access partition denylist cache for {}/{}", tmd.keyspace, tmd.name, e);
    }

    /**
     * @return number of denylisted keys in range
     */
    public int getDeniedKeysInRangeCount(final String keyspace, final String table, final AbstractBounds<PartitionPosition> range)
    {
        return getDeniedKeysInRangeCount(getTableId(keyspace, table), range);
    }

    /**
     * @return number of denylisted keys in range
     */
    public int getDeniedKeysInRangeCount(final TableId tid, final AbstractBounds<PartitionPosition> range)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (!DatabaseDescriptor.getPartitionDenylistEnabled() || tid == null || tmd == null || !canDenylistKeyspace(tmd.keyspace))
            return 0;

        try
        {
            final DenylistEntry denylistEntry = denylist.get(tid);
            if (denylistEntry == null || denylistEntry.tokens.size() == 0)
                return 0;
            final Token startToken = range.left.getToken();
            final Token endToken = range.right.getToken();

            // Normal case
            if (startToken.compareTo(endToken) <= 0 || endToken.isMinimum())
            {
                NavigableSet<Token> subSet = denylistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND == range.left.kind());
                if (!endToken.isMinimum())
                    subSet = subSet.headSet(endToken, PartitionPosition.Kind.MAX_BOUND == range.right.kind());
                return subSet.size();
            }

            // Wrap around case
            return denylistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND == range.left.kind()).size()
                   + denylistEntry.tokens.headSet(endToken, PartitionPosition.Kind.MAX_BOUND == range.right.kind()).size();
        }
        catch (final Exception e)
        {
            logAccessFailure(tid, e);
            return 0;
        }
    }


    @VisibleForTesting
    public static void setActiveDenylistParamsToUpgraded()
    {
        DenylistParams.overrideList = DenylistParams.upgraded;
    }

    enum DenylistParams
    {
        DENYLIST("OSS denylist", "table_name", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.PARTITION_DENYLIST_TABLE),
        LEGACY_DENYLIST("CIE denylist", "cf_name", CIEInternalKeyspace.NAME, "partition_blacklist");

        final String description;
        final String tableColumn;
        final String denylistKeyspace;
        final String denylistTable;
        final String allDeniedTables;

        DenylistParams(String description, String tableColumn, String denylistKeyspace, String denylistTable)
        {
            this.description = description;
            this.tableColumn = tableColumn;
            this.denylistKeyspace = denylistKeyspace;
            this.denylistTable = denylistTable;

            allDeniedTables = String.format("SELECT DISTINCT ks_name, %s FROM %s.%s",
                                            tableColumn, denylistKeyspace, denylistTable);
        }

        private static List<DenylistParams> overrideList = null;
        private static final List<DenylistParams> legacyMode = ImmutableList.of(LEGACY_DENYLIST); // during major upgrade
        private static final List<DenylistParams> mixedMode = ImmutableList.of(DENYLIST, LEGACY_DENYLIST); // during minor upgrade
        private static final List<DenylistParams> upgraded = ImmutableList.of(DENYLIST); // after major/minor upgrade completes

        static List<DenylistParams> active()
        {
            if (overrideList != null)
                return overrideList;

            if (Gossiper.instance.hasMajorVersion3Nodes())
            {
                // The 3.0 nodes will not pick up the distributed schema change as a different major version,
                // so do not even attempt dual writes
                logger.debug("active in legacy mode");
                return legacyMode;
            }
            else if (Schema.instance.getTableMetadata(LEGACY_DENYLIST.denylistKeyspace, LEGACY_DENYLIST.denylistTable) != null)
            {
                // If legacy and denylist tables exist, use both until the legacy one is removed. Otherwise if
                // keys are removed from the denylist they could still be loaded/migrated from the legacy table.
                logger.debug("active in mixed mode");
                return mixedMode;
            }
            else // Cluster created without the legacy tables or they have been removed
            {
                // Move over to the new schema.  Rely on per-instance upgrades having migrated
                // from the original tables and some read repair. Worst case will have to re-issue some denylist requests.
                logger.debug("active in upgraded mode");
                return upgraded;
            }
        }

        String readDenyListQuery(TableMetadata tmd, int limit)
        {
            // We attempt to query just over our allowable max keys in order to check whether we have configured data beyond that limit and alert the user if so
            return String.format("SELECT * FROM %s.%s WHERE ks_name='%s' AND %s='%s' LIMIT %d",
                                 denylistKeyspace,
                                 denylistTable,
                                 tmd.keyspace,
                                 tableColumn,
                                 tmd.name,
                                 limit + 1);
        }

        String addKeyToDenylistQuery(String keyspace, String table, String keyAsHex)
        {
            return String.format("INSERT INTO %s.%s (ks_name, %s, key) VALUES ('%s', '%s', 0x%s)",
                                 denylistKeyspace, denylistTable, tableColumn, keyspace, table, keyAsHex);
        }

        String removeKeyFromDenylistQuery(String keyspace, String table, String keyAsHex)
        {
            return String.format("DELETE FROM %s.%s WHERE ks_name='%s' AND %s='%s' AND key=0x%s",
                                 denylistKeyspace, denylistTable, keyspace, tableColumn, table, keyAsHex);
        }
    }

    /**
     * Get up to the configured allowable limit per table of denylisted keys
     */
    private DenylistEntry getDenylistForTableFromCQL(final TableId tid)
    {
        return getDenylistForTableFromCQL(tid, DatabaseDescriptor.getDenylistMaxKeysPerTable());
    }

    /**
     * Attempts to reload the DenylistEntry data from CQL for the given TableId and key count.
     * @return null if we do not find the data; allows cache reloader to preserve old value
     */
    private DenylistEntry getDenylistForTableFromCQL(final TableId tid, int limit)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (tmd == null)
            return null;

        // If our limit is < the standard per table we know we're at a global violation because we've constrained that request limit already.
        boolean globalLimit = limit != DatabaseDescriptor.getDenylistMaxKeysPerTable();
        String violationType = globalLimit ? "global" : "per-table";
        int errorLimit = Math.min(limit, DatabaseDescriptor.getDenylistMaxKeysPerTable());

        final Set<ByteBuffer> keys = new HashSet<>();
        final NavigableSet<Token> tokens = new TreeSet<>();
        int processed = 0;

        for (DenylistParams params : DenylistParams.active())
        {
            // We attempt to query just over our allowable max keys in order to check whether we have configured data beyond that limit and alert the user if so
            // Will over-read on the legacy loop, but not worth worrying about
            try
            {
                final UntypedResultSet results = process(params.readDenyListQuery(tmd, limit), DatabaseDescriptor.getDenylistConsistencyLevel());

                for (final UntypedResultSet.Row row : results)
                {
                    final ByteBuffer key = row.getBlob("key");
                    tokens.add(StorageService.instance.getTokenMetadata().partitioner.getToken(key));
                    if (keys.add(key))
                    {
                        processed++;
                        if (processed >= limit)
                        {
                            logger.error("Partition denylist for {}/{} has exceeded the {} allowance of ({}). Remaining keys were ignored; " +
                                         "please reduce the total number of keys denied or increase the denylist_max_keys_per_table param in " +
                                         "cassandra.yaml to avoid inconsistency in denied partitions across nodes.",
                                         tmd.keyspace,
                                         tmd.name,
                                         violationType,
                                         errorLimit);
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.warn("Reading from {} failed. Denylist functionality will be degraded until query succeeds", params.description, ex);
            }
        }
        return new DenylistEntry(ImmutableSet.copyOf(keys), ImmutableSortedSet.copyOf(tokens));
    }

    void findTableIdsWithDenylist(DenylistParams params, Set<TableId> deniedTableIds)
    {
        final UntypedResultSet deniedTableResults = process(params.allDeniedTables, DatabaseDescriptor.getDenylistConsistencyLevel());
        if (deniedTableResults == null || deniedTableResults.isEmpty())
            return;
        for (final UntypedResultSet.Row row : deniedTableResults)
        {
            final String ks = row.getString("ks_name");
            final String table = row.getString(params.tableColumn);
            final TableId tid = getTableId(ks, table);
            deniedTableIds.add(tid);
        }
    }

    /**
     * This method relies on {@link #getDenylistForTableFromCQL(TableId, int)} to pull a limited amount of keys
     * on a per-table basis from CQL to load into the cache. We need to navigate both respecting the max cache size limit
     * as well as respecting the per-table limit.
     * @return non-null mapping of TableId to DenylistEntry
     */
    private Map<TableId, DenylistEntry> getDenylistForAllTablesFromCQL()
    {
        String denyListTableDescription = "system_distributed.partition_denylist and/or cie_internal.partition_blacklist";
        // While we warn the user in this case, we continue with the reload anyway.
        checkDenylistNodeAvailability();

        try
        {
            // LinkedHashSet to keep the same ordering as the upstream version for unit tests
            Set<TableId> deniedTableIds = new LinkedHashSet<TableId>();
            for (DenylistParams params : DenylistParams.active())
            {
                findTableIdsWithDenylist(params, deniedTableIds);
            }

            int totalProcessed = 0 ;
            final Map<TableId, DenylistEntry> results = new HashMap<>();
            for (final TableId tid : deniedTableIds)
            {
                if (DatabaseDescriptor.getDenylistMaxKeysTotal() - totalProcessed <= 0)
                {
                    TableMetadata tmd = Schema.instance.getTableMetadata(tid);
                    logger.error("Hit limit on allowable denylisted keys in total. Processed {} total entries. Not adding all entries to denylist for {}/{}." +
                                 " Remove denylist entries in " + denyListTableDescription + " or increase your denylist_max_keys_total param in cassandra.yaml.",
                                 totalProcessed,
                                 tmd == null ? "<missing table metadata>" : tmd.keyspace,
                                 tmd == null ? "<missing table metadata>" : tmd.name,
                                 SystemDistributedKeyspace.PARTITION_DENYLIST_TABLE);
                    results.put(tid, new DenylistEntry());
                }
                else
                {
                    // Determine whether we can get up to table max or we need a subset at edge condition of max overflow.
                    int allowedTableRecords = Math.min(DatabaseDescriptor.getDenylistMaxKeysPerTable(), DatabaseDescriptor.getDenylistMaxKeysTotal() - totalProcessed);
                    DenylistEntry tableDenylist = getDenylistForTableFromCQL(tid, allowedTableRecords);
                    if (tableDenylist != null)
                        totalProcessed += tableDenylist.keys.size();
                    results.put(tid, tableDenylist);
                }
            }
            return results;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading full partition denylist from "
                         + denyListTableDescription +
                         ". Partition Denylisting will be compromised. Exception: " + e);
            return Collections.emptyMap();
        }
    }

    private boolean refreshTableDenylist(String keyspace, String table)
    {
        checkDenylistNodeAvailability();
        final TableId tid = getTableId(keyspace, table);
        if (tid == null)
        {
            logger.warn("Got denylist mutation for unknown ks/cf: {}/{}. Skipping refresh.", keyspace, table);
            return false;
        }

        DenylistEntry newEntry = getDenylistForTableFromCQL(tid);
        denylist.put(tid, newEntry);
        return true;
    }

    private static TableId getTableId(final String keyspace, final String table)
    {
        TableMetadata tmd = Schema.instance.getTableMetadata(keyspace, table);
        return tmd == null ? null : tmd.id;
    }

    /* Migrate from the internal table used before the upstream and rename in CASSANDRA-12106
       The partition key for the old and new tables is the same, so we can just migrate locally one instance at a
       time.  There could be some read repair, but that's ok. No attempt is made to preserve timestamps either.
    */
    static public void maybeMigrate()
    {
        // Only migrate once new denylist is supported by the cluster. Otherwise calls to remove denylist
        // entries will not be removed from the migrated data and get resurrected.
        if (!DenylistParams.active().contains(DenylistParams.DENYLIST))
        {
            logger.info("Skipped denylist migration as pre-4.0 instances present in the cluster");
            return;
        }

        try
        {
            // Check if there's anything to migrate
            if (getTableId(DenylistParams.LEGACY_DENYLIST.denylistKeyspace, DenylistParams.LEGACY_DENYLIST.denylistTable) == null)
                return;

            String selectQuery = String.format("SELECT ks_name, cf_name, key FROM %s.%s LIMIT 50000",
                                               DenylistParams.LEGACY_DENYLIST.denylistKeyspace,
                                               DenylistParams.LEGACY_DENYLIST.denylistTable);
            UntypedResultSet legacyEntries = executeInternal(selectQuery);
            legacyEntries.forEach(row -> {
                String ks = row.getString("ks_name");
                String tn = row.getString("cf_name");
                ByteBuffer key = row.getBlob("key");
                String addKeyToDenylistQuery = DenylistParams.DENYLIST.addKeyToDenylistQuery(ks, tn, ByteBufferUtil.bytesToHex(key));
                executeInternal(addKeyToDenylistQuery);
            });

            // Once the fleet is upgraded to 4.0.0.50 and above and all 3.0 upgrades are complete,
            // the cie_internal keyspace table can be removed. Leaving for now so that legacy denylist
            // queries can succeed in a mixed version cluster.
            logger.info("Migrated {} denylist entries. Leaving originals in place in case of downgrade.", legacyEntries.size());
        }
        catch (Exception e)
        {
            logger.error("Error migrating partition denylist from legacy CIE version. " +
                         "Partition Denylisting will be compromised. Exception: " + e);
        }
    }
}
