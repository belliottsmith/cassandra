package org.apache.cassandra.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.cql3.QueryProcessor.process;

/**
 * PartitionBlacklist uses the cie_internal.partition_blacklist CF to maintain a list of blacklisted partition keys
 * for each KS/CF.
 *
 * Keys can be entered manually into the partition_blacklist table or via the JMX operation StorageProxyMBean.blacklistKey
 *
 * The blacklist is stored as one CQL partition per CF, and the blacklisted keys are column names in that partition. The blacklisted
 * keys for each CF are cached in memory, and reloaded from the partition_blacklist table every 24 hours (default) or when the
 * StorageProxyMBean.loadPartitionBlacklist is called via JMX.
 *
 * Concurrency of the cache is provided by the concurrency semantics of the guava LoadingCache. All values (BlacklistEntry) are
 * immutable collections of keys/tokens which are replaced in whole when the cache refreshes from disk.
 */
public class PartitionBlacklist
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionBlacklist.class);
    public static final String PARTITION_BLACKLIST_CF = "partition_blacklist";

    private final ExecutorService executor = new ThreadPoolExecutor(2, 2, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    private final LoadingCache<UUID, BlacklistEntry> blacklist = CacheBuilder.newBuilder()
            .refreshAfterWrite(DatabaseDescriptor.getBlacklistRefreshPeriodSeconds(), TimeUnit.SECONDS)
            .build(new CacheLoader<UUID, BlacklistEntry>()
            {
                @Override
                public BlacklistEntry load(final UUID cfid) throws Exception
                {
                    return readBlacklist(cfid);
                }

                @Override
                public ListenableFuture<BlacklistEntry> reload(final UUID cfid, final BlacklistEntry oldValue) throws Exception
                {
                    ListenableFutureTask<BlacklistEntry> task = ListenableFutureTask.create(new Callable<BlacklistEntry>()
                    {
                        @Override
                        public BlacklistEntry call() throws Exception
                        {
                            final BlacklistEntry newEntry = readBlacklist(cfid);
                            if (newEntry != null)
                                return newEntry;
                            if (oldValue != null)
                                return oldValue;
                            return new BlacklistEntry();
                        }
                    });
                    executor.execute(task);
                    return task;
                }
            });

    /**
     * Blacklist entry is never mutated once constructed, only replaced with a new entry when the cache is refreshed
     */
    private static class BlacklistEntry
    {
        public BlacklistEntry()
        {
            keys = ImmutableSet.of();
            tokens = ImmutableSortedSet.of();
        }

        public BlacklistEntry(final ImmutableSet<ByteBuffer> keys, final ImmutableSortedSet<Token> tokens)
        {
            this.keys = keys;
            this.tokens = tokens;
        }

        public final ImmutableSet<ByteBuffer> keys;
        public final ImmutableSortedSet<Token> tokens;
    }

    // synchronized on this
    private int loadAttempts = 0;
    private int loadSuccesses = 0;

    public synchronized int getLoadAttempts()
    {
        return loadAttempts;
    }
    public synchronized int getLoadSuccesses()
    {
        return loadSuccesses;
    }

    /* Perform initial load of the partition blacklist.  Should
     * be called at startup and only loads if the operation
     * is expected to succeed.  If it is not possible to load
     * at call time, a timer is set to retry.
     */
    public void initialLoad()
    {
        if (!DatabaseDescriptor.enablePartitionBlacklist())
            return;

        synchronized (this)
        {
            loadAttempts++;
        }

        // Check if there are sufficient nodes to attempt reading
        // all the blacklist partitions before issuing the query.
        // The pre-check prevents definite range-slice unavailables
        // being marked and triggering an alert.  Nodes may still change
        // state between the check and the query, but it should significantly
        // reduce the alert volume.
        String retryReason = "Insufficient nodes";
        try
        {
            if (readAllHasSufficientNodes() && load())
            {
                return;
            }
        }
        catch (Throwable tr)
        {
            logger.error("Failed to load partition blacklist", tr);
            retryReason = "Exception";
        }

        // This path will also be taken on other failures other than UnavailableException,
        // but seems like a good idea to retry anyway.
        int retryInSeconds = DatabaseDescriptor.getBlacklistInitialLoadRetrySeconds();
        logger.info(retryReason + " while loading partition blacklist cache.  Scheduled retry in {} seconds.",
                    retryInSeconds);
        ScheduledExecutors.optionalTasks.schedule(this::initialLoad, retryInSeconds, TimeUnit.SECONDS);
    }

    private boolean readAllHasSufficientNodes()
    {
        return StorageProxy.sufficientLiveNodesForSelectStar(CIEInternalKeyspace.PartitionBlacklistCf,
                                                             DatabaseDescriptor.blacklistConsistencyLevel());
    }

    public boolean load()
    {
        final long start = System.currentTimeMillis();
        final Map<UUID, BlacklistEntry> allBlacklists = readAll();
        if (allBlacklists == null)
            return false;

        // cache iterators are weakly-consistent
        for (final Iterator<UUID> it = blacklist.asMap().keySet().iterator(); it.hasNext(); )
        {
            final UUID cfid = it.next();
            if (!allBlacklists.containsKey(cfid))
                allBlacklists.put(cfid, new BlacklistEntry());
        }
        blacklist.putAll(allBlacklists);
        logger.info("Loaded partition blacklist cache in {}ms", System.currentTimeMillis() - start);

        synchronized (this)
        {
            loadSuccesses++;
        }
        return true;
    }

    public boolean blacklist(final String keyspace, final String cf, final ByteBuffer key)
    {
        if (!isPermitted(keyspace))
            return false;

        final byte[] keyBytes = new byte[key.remaining()];
        key.slice().get(keyBytes);

        final String insert = String.format("INSERT INTO cie_internal.partition_blacklist (ks_name, cf_name, key) VALUES ('%s', '%s', 0x%s)",
                keyspace, cf, Hex.bytesToHex(keyBytes));

        try
        {
            process(insert, DatabaseDescriptor.blacklistConsistencyLevel());
            return true;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Failed to blacklist key [{}] in {}/{}", Hex.bytesToHex(keyBytes), keyspace, cf, e);
        }
        return false;
    }

    private boolean isPermitted(final String keyspace)
    {
        return !CIEInternalLocalKeyspace.NAME.equals(keyspace) && !CIEInternalKeyspace.NAME.equals(keyspace) && !SystemKeyspace.NAME.equals(keyspace)
                && !TraceKeyspace.NAME.equals(keyspace) && !AuthKeyspace.NAME.equals(keyspace);
    }

    private boolean isPermitted(final UUID cfid)
    {
        final Pair<String, String> kscf = Schema.instance.getCF(cfid);
        if (kscf == null)
            return false;
        return isPermitted(kscf.left);
    }

    public boolean validateKey(final String keyspace, final String cf, final ByteBuffer key)
    {
        return validateKey(getCFID(keyspace, cf), key);
    }

    public boolean validateKey(final UUID cfid, final ByteBuffer key)
    {
        if (!DatabaseDescriptor.enablePartitionBlacklist() || cfid == null || !isPermitted(cfid))
            return true;

        try
        {
            return !blacklist.get(cfid).keys.contains(key);
        }
        catch (final ExecutionException e)
        {
            // in the event of an error accessing or populating the cache, assume it's not blacklisted
            final Pair<String, String> kscf = Schema.instance.getCF(cfid);
            if (kscf == null)
                logger.debug("Failed to access partition blacklist cache for unknown CFID {}", cfid.toString(), e);
            else
                logger.debug("Failed to access partition blacklist cache for {}/{}", kscf.left, kscf.right, e);
            return true;
        }
    }

    /**
     * @return number of blacklisted keys in range
     */
    public int validateRange(final String keyspace, final String cf, final AbstractBounds<PartitionPosition> range)
    {
        return validateRange(getCFID(keyspace, cf), range);
    }

    /**
     * @return number of blacklisted keys in range
     */
    public int validateRange(final UUID cfid, final AbstractBounds<PartitionPosition> range)
    {
        if (!DatabaseDescriptor.enablePartitionBlacklist() || cfid == null || !isPermitted(cfid))
            return 0;

        try
        {
            final BlacklistEntry blacklistEntry = blacklist.get(cfid);
            final Token startToken = range.left.getToken();
            final Token endToken = range.right.getToken();
            // normal case
            if (startToken.compareTo(endToken) <= 0 || endToken.isMinimum())
            {
                NavigableSet<Token> subSet = blacklistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND.equals(range.left.kind()));
                if (!endToken.isMinimum())
                    subSet = subSet.headSet(endToken, PartitionPosition.Kind.MAX_BOUND.equals(range.right.kind()));
                return subSet.size();
            }
            // wrap around case
            return blacklistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND.equals(range.left.kind())).size()
                    + blacklistEntry.tokens.headSet(endToken, PartitionPosition.Kind.MAX_BOUND.equals(range.right.kind())).size();
        }
        catch (final ExecutionException e)
        {
            final Pair<String, String> kscf = Schema.instance.getCF(cfid);
            if (kscf == null)
                logger.debug("Failed to access partition blacklist cache for unknown CFID {}", cfid.toString(), e);
            else
                logger.debug("Failed to access partition blacklist cache for {}/{}", kscf.left, kscf.right, e);
            return 0;
        }
    }

    private BlacklistEntry readBlacklist(final UUID cfid)
    {
        final Pair<String, String> kscf = Schema.instance.getCF(cfid);
        if (kscf == null)
            return new BlacklistEntry();

        final String readBlacklist = String.format("SELECT * FROM %s.%s WHERE ks_name='%s' AND cf_name='%s' LIMIT %d", CIEInternalKeyspace.NAME, PARTITION_BLACKLIST_CF,
                kscf.left, kscf.right, DatabaseDescriptor.maxBlacklistKeysPerCf() + 1);

        try
        {
            final UntypedResultSet results = process(readBlacklist, DatabaseDescriptor.blacklistConsistencyLevel());
            if (results == null || results.isEmpty())
                return new BlacklistEntry();

            if (results.size() > DatabaseDescriptor.maxBlacklistKeysPerCf())
                logger.error("Partition blacklist for {}/{} has exceeded the maximum allowable size ({}). Remaining keys were ignored.", kscf.left, kscf.right, DatabaseDescriptor.maxBlacklistKeysPerCf());

            final Set<ByteBuffer> keys = new HashSet<>();
            final NavigableSet<Token> tokens = new TreeSet<>();
            for (final UntypedResultSet.Row row : results)
            {
                final ByteBuffer key = row.getBlob("key");
                keys.add(key);
                tokens.add(StorageService.instance.getTokenMetadata().partitioner.getToken(key));
            }
            return new BlacklistEntry(ImmutableSet.copyOf(keys), ImmutableSortedSet.copyOf(tokens));
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading partition_blacklist table for {}/{}", kscf.left, kscf.right, e);
            return null;
        }
    }

    private Map<UUID, BlacklistEntry> readAll()
    {
        final String readBlacklist = String.format("SELECT * FROM %s.%s LIMIT %d", CIEInternalKeyspace.NAME, PARTITION_BLACKLIST_CF, DatabaseDescriptor.maxBlacklistKeysTotal() + 1);
        try
        {
            final UntypedResultSet results = process(readBlacklist, DatabaseDescriptor.blacklistConsistencyLevel());
            if (results == null || results.isEmpty())
                return new HashMap<>();

            if (results.size() > DatabaseDescriptor.maxBlacklistKeysTotal())
                logger.error("Partition blacklist has exceeded the maximum allowable total size ({}). Remaining keys were ignored.", DatabaseDescriptor.maxBlacklistKeysTotal());

            final Map<UUID, Pair<Set<ByteBuffer>, NavigableSet<Token>>> allBlacklists = new HashMap<>();
            for (final UntypedResultSet.Row row : results)
            {
                final UUID cfid = getCFID(row.getString("ks_name"), row.getString("cf_name"));
                if (cfid == null)
                    continue;
                if (!allBlacklists.containsKey(cfid))
                    allBlacklists.put(cfid, Pair.create(new HashSet<>(), new TreeSet<>()));
                final ByteBuffer key = row.getBlob("key");
                allBlacklists.get(cfid).left.add(key);
                allBlacklists.get(cfid).right.add(StorageService.instance.getTokenMetadata().partitioner.getToken(key));
            }

            final Map<UUID, BlacklistEntry> ret = new HashMap<>();
            for (final UUID cfid : allBlacklists.keySet())
            {
                final Pair<Set<ByteBuffer>, NavigableSet<Token>> entries = allBlacklists.get(cfid);
                ret.put(cfid, new BlacklistEntry(ImmutableSet.copyOf(entries.left), ImmutableSortedSet.copyOf(entries.right)));
            }
            return ret;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading partition_blacklist table on startup", e);
            return null;
        }
    }

    private UUID getCFID(final String keyspace, final String cf)
    {
        return Schema.instance.getId(keyspace, cf);
    }
}
