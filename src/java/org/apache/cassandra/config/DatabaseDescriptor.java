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
package org.apache.cassandra.config;

import java.io.IOException;
import java.net.*;
import java.nio.file.FileStore;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.auth.AuthConfig;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.auth.INetworkAuthorizer;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerCDC;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStandard;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.io.util.SpinningDiskOptimizationStrategy;
import org.apache.cassandra.io.util.SsdDiskOptimizationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.CacheService.CacheType;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.OS_ARCH;
import static org.apache.cassandra.config.CassandraRelevantProperties.SUN_ARCH_DATA_MODEL;
import static org.apache.cassandra.io.util.FileUtils.ONE_GB;
import static org.apache.cassandra.io.util.FileUtils.ONE_MB;
import static org.apache.cassandra.utils.Clock.Global.logInitializationOutcome;

public class DatabaseDescriptor
{
    static
    {
        // This static block covers most usages
        FBUtilities.preventIllegalAccessWarnings();
        System.setProperty("io.netty.transport.estimateSizeOnSubmit", "false");
    }

    private static final Logger logger = LoggerFactory.getLogger(DatabaseDescriptor.class);

    /**
     * Tokens are serialized in a Gossip VersionedValue String.  VV are restricted to 64KB
     * when we send them over the wire, which works out to about 1700 tokens.
     */
    private static final int MAX_NUM_TOKENS = 1536;

    private static Config conf;

    /**
     * Request timeouts can not be less than below defined value (see CASSANDRA-9375)
     */
    static final long LOWEST_ACCEPTED_TIMEOUT = 10L;

    private static Supplier<IFailureDetector> newFailureDetector;
    private static IEndpointSnitch snitch;
    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress broadcastAddress;
    private static InetAddress rpcAddress;
    private static InetAddress broadcastRpcAddress;
    private static SeedProvider seedProvider;
    private static IInternodeAuthenticator internodeAuthenticator = new AllowAllInternodeAuthenticator();

    /* Hashing strategy Random or OPHF */
    private static IPartitioner partitioner;
    private static String paritionerName;

    private static Config.DiskAccessMode indexAccessMode;

    private static IAuthenticator authenticator;
    private static IAuthorizer authorizer;
    private static INetworkAuthorizer networkAuthorizer;
    // Don't initialize the role manager until applying config. The options supported by CassandraRoleManager
    // depend on the configured IAuthenticator, so defer creating it until that's been set.
    private static IRoleManager roleManager;

    private static long preparedStatementsCacheSizeInMB;

    private static long keyCacheSizeInMB;
    private static long counterCacheSizeInMB;
    private static long indexSummaryCapacityInMB;

    private static String localDC;
    private static Comparator<Replica> localComparator;
    private static EncryptionContext encryptionContext;
    private static boolean hasLoggedConfig;

    private static DiskOptimizationStrategy diskOptimizationStrategy;

    private static boolean clientInitialized;
    private static boolean toolInitialized;
    private static boolean daemonInitialized;

    private static final int searchConcurrencyFactor = Integer.parseInt(System.getProperty(Config.PROPERTY_PREFIX + "search_concurrency_factor", "1"));

    private static volatile boolean disableSTCSInL0 = Boolean.getBoolean(Config.PROPERTY_PREFIX + "disable_stcs_in_l0");
    private static final boolean unsafeSystem = Boolean.getBoolean(Config.PROPERTY_PREFIX + "unsafesystem");

    // turns some warnings into exceptions for testing
    private static final boolean strictRuntimeChecks = Boolean.getBoolean("cassandra.strict.runtime.checks");

    public static volatile boolean allowUnlimitedConcurrentValidations = Boolean.getBoolean("cassandra.allow_unlimited_concurrent_validations");

    private static Function<CommitLog, AbstractCommitLogSegmentManager> commitLogSegmentMgrProvider = c -> DatabaseDescriptor.isCDCEnabled()
                                       ? new CommitLogSegmentManagerCDC(c, DatabaseDescriptor.getCommitLogLocation())
                                       : new CommitLogSegmentManagerStandard(c, DatabaseDescriptor.getCommitLogLocation());

    private static volatile boolean aggressiveGC = Boolean.getBoolean("cassandra.aggressivegcls.enabled");
    private static volatile long scheduledCompactionCycleTimeSeconds;

    private static long repairHistorySyncTimeoutSeconds;

    public static void daemonInitialization() throws ConfigurationException
    {
        daemonInitialization(DatabaseDescriptor::loadConfig);
    }

    public static void daemonInitialization(Supplier<Config> config) throws ConfigurationException
    {
        if (toolInitialized)
            throw new AssertionError("toolInitialization() already called");
        if (clientInitialized)
            throw new AssertionError("clientInitialization() already called");

        // Some unit tests require this :(
        if (daemonInitialized)
            return;
        daemonInitialized = true;

        setConfig(config.get());
        applyAll();
        AuthConfig.applyAuth();
    }

    /**
     * Equivalent to {@link #toolInitialization(boolean) toolInitialization(true)}.
     */
    public static void toolInitialization()
    {
        toolInitialization(true);
    }

    /**
     * Initializes this class as a tool, which means that the configuration is loaded
     * using {@link #loadConfig()} and all non-daemon configuration parts will be setup.
     *
     * @param failIfDaemonOrClient if {@code true} and a call to {@link #daemonInitialization()} or
     *                             {@link #clientInitialization()} has been performed before, an
     *                             {@link AssertionError} will be thrown.
     */
    public static void toolInitialization(boolean failIfDaemonOrClient)
    {
        if (!failIfDaemonOrClient && (daemonInitialized || clientInitialized))
        {
            return;
        }
        else
        {
            if (daemonInitialized)
                throw new AssertionError("daemonInitialization() already called");
            if (clientInitialized)
                throw new AssertionError("clientInitialization() already called");
        }

        if (toolInitialized)
            return;
        toolInitialized = true;

        setConfig(loadConfig());

        applySimpleConfig();

        applyPartitioner();

        applySnitch();

        applyEncryptionContext();
    }

    /**
     * Equivalent to {@link #clientInitialization(boolean) clientInitialization(true)}.
     */
    public static void clientInitialization()
    {
        clientInitialization(true);
    }

    /**
     * Initializes this class as a client, which means that just an empty configuration will
     * be used.
     *
     * @param failIfDaemonOrTool if {@code true} and a call to {@link #daemonInitialization()} or
     *                           {@link #toolInitialization()} has been performed before, an
     *                           {@link AssertionError} will be thrown.
     */
    public static void clientInitialization(boolean failIfDaemonOrTool)
    {
        if (!failIfDaemonOrTool && (daemonInitialized || toolInitialized))
        {
            return;
        }
        else
        {
            if (daemonInitialized)
                throw new AssertionError("daemonInitialization() already called");
            if (toolInitialized)
                throw new AssertionError("toolInitialization() already called");
        }

        if (clientInitialized)
            return;
        clientInitialized = true;

        Config.setClientMode(true);
        conf = new Config();
        diskOptimizationStrategy = new SpinningDiskOptimizationStrategy();
    }

    public static boolean isClientInitialized()
    {
        return clientInitialized;
    }

    public static boolean isToolInitialized()
    {
        return toolInitialized;
    }

    public static boolean isClientOrToolInitialized()
    {
        return clientInitialized || toolInitialized;
    }

    public static boolean isDaemonInitialized()
    {
        return daemonInitialized;
    }

    public static Config getRawConfig()
    {
        return conf;
    }

    @VisibleForTesting
    public static Config loadConfig() throws ConfigurationException
    {
        if (Config.getOverrideLoadConfig() != null)
            return Config.getOverrideLoadConfig().get();

        String loaderClass = System.getProperty(Config.PROPERTY_PREFIX + "config.loader");
        ConfigurationLoader loader = loaderClass == null
                                     ? new YamlConfigurationLoader()
                                     : FBUtilities.construct(loaderClass, "configuration loading");
        Config config = loader.loadConfig();

        if (!hasLoggedConfig)
        {
            hasLoggedConfig = true;
            Config.log(config);
        }

        return config;
    }

    private static InetAddress getNetworkInterfaceAddress(String intf, String configName, boolean preferIPv6) throws ConfigurationException
    {
        try
        {
            NetworkInterface ni = NetworkInterface.getByName(intf);
            if (ni == null)
                throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" could not be found", false);
            Enumeration<InetAddress> addrs = ni.getInetAddresses();
            if (!addrs.hasMoreElements())
                throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" was found, but had no addresses", false);

            /*
             * Try to return the first address of the preferred type, otherwise return the first address
             */
            InetAddress retval = null;
            while (addrs.hasMoreElements())
            {
                InetAddress temp = addrs.nextElement();
                if (preferIPv6 && temp instanceof Inet6Address) return temp;
                if (!preferIPv6 && temp instanceof Inet4Address) return temp;
                if (retval == null) retval = temp;
            }
            return retval;
        }
        catch (SocketException e)
        {
            throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" caused an exception", e);
        }
    }

    @VisibleForTesting
    public static void setConfig(Config config)
    {
        conf = config;
    }

    private static void applyAll() throws ConfigurationException
    {
        //InetAddressAndPort cares that applySimpleConfig runs first
        applySimpleConfig();

        applyPartitioner();

        applyAddressConfig();

        applySnitch();

        applyTokensConfig();

        applySeedProvider();

        applyEncryptionContext();

        applySslContext();

        applyGuardrails();
    }

    private static void applySimpleConfig()
    {
        //Doing this first before all other things in case other pieces of config want to construct
        //InetAddressAndPort and get the right defaults
        InetAddressAndPort.initializeDefaultPort(getStoragePort());

        if (conf.commitlog_sync == null)
        {
            throw new ConfigurationException("Missing required directive CommitLogSync", false);
        }

        if (conf.commitlog_sync == Config.CommitLogSync.batch)
        {
            if (conf.commitlog_sync_period_in_ms != 0)
            {
                throw new ConfigurationException("Batch sync specified, but commitlog_sync_period_in_ms found. Only specify commitlog_sync_batch_window_in_ms when using batch sync", false);
            }
            logger.debug("Syncing log with batch mode");
        }
        else if (conf.commitlog_sync == CommitLogSync.group)
        {
            if (Double.isNaN(conf.commitlog_sync_group_window_in_ms) || conf.commitlog_sync_group_window_in_ms <= 0d)
            {
                throw new ConfigurationException("Missing value for commitlog_sync_group_window_in_ms: positive double value expected.", false);
            }
            else if (conf.commitlog_sync_period_in_ms != 0)
            {
                throw new ConfigurationException("Group sync specified, but commitlog_sync_period_in_ms found. Only specify commitlog_sync_group_window_in_ms when using group sync", false);
            }
            logger.debug("Syncing log with a group window of {}", conf.commitlog_sync_period_in_ms);
        }
        else
        {
            if (conf.commitlog_sync_period_in_ms <= 0)
            {
                throw new ConfigurationException("Missing value for commitlog_sync_period_in_ms: positive integer expected", false);
            }
            else if (!Double.isNaN(conf.commitlog_sync_batch_window_in_ms))
            {
                throw new ConfigurationException("commitlog_sync_period_in_ms specified, but commitlog_sync_batch_window_in_ms found.  Only specify commitlog_sync_period_in_ms when using periodic sync.", false);
            }
            logger.debug("Syncing log with a period of {}", conf.commitlog_sync_period_in_ms);
        }

        /* evaluate the DiskAccessMode Config directive, which also affects indexAccessMode selection */
        if (conf.disk_access_mode == Config.DiskAccessMode.auto)
        {
            conf.disk_access_mode = hasLargeAddressSpace() ? Config.DiskAccessMode.mmap : Config.DiskAccessMode.standard;
            indexAccessMode = conf.disk_access_mode;
            logger.info("DiskAccessMode 'auto' determined to be {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
        }
        else if (conf.disk_access_mode == Config.DiskAccessMode.mmap_index_only)
        {
            conf.disk_access_mode = Config.DiskAccessMode.standard;
            indexAccessMode = Config.DiskAccessMode.mmap;
            logger.info("DiskAccessMode is {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
        }
        else
        {
            indexAccessMode = conf.disk_access_mode;
            logger.info("DiskAccessMode is {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
        }

        if (conf.gc_warn_threshold_in_ms < 0)
        {
            throw new ConfigurationException("gc_warn_threshold_in_ms must be a positive integer");
        }

        /* phi convict threshold for FailureDetector */
        if (conf.phi_convict_threshold < 5 || conf.phi_convict_threshold > 16)
        {
            throw new ConfigurationException("phi_convict_threshold must be between 5 and 16, but was " + conf.phi_convict_threshold, false);
        }

        /* Thread per pool */
        if (conf.concurrent_reads < 2)
        {
            throw new ConfigurationException("concurrent_reads must be at least 2, but was " + conf.concurrent_reads, false);
        }

        if (conf.concurrent_writes < 2 && System.getProperty("cassandra.test.fail_mv_locks_count", "").isEmpty())
        {
            throw new ConfigurationException("concurrent_writes must be at least 2, but was " + conf.concurrent_writes, false);
        }

        if (conf.concurrent_counter_writes < 2)
            throw new ConfigurationException("concurrent_counter_writes must be at least 2, but was " + conf.concurrent_counter_writes, false);

        if (conf.concurrent_replicates != null)
            logger.warn("concurrent_replicates has been deprecated and should be removed from cassandra.yaml");

        if (conf.networking_cache_size_in_mb == null)
            conf.networking_cache_size_in_mb = Math.min(128, (int) (Runtime.getRuntime().maxMemory() / (16 * 1048576)));

        if (conf.file_cache_size_in_mb == null)
            conf.file_cache_size_in_mb = Math.min(512, (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)));

        // round down for SSDs and round up for spinning disks
        if (conf.file_cache_round_up == null)
            conf.file_cache_round_up = conf.disk_optimization_strategy == Config.DiskOptimizationStrategy.spinning;

        if (conf.memtable_offheap_space_in_mb == null)
            conf.memtable_offheap_space_in_mb = (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576));
        if (conf.memtable_offheap_space_in_mb < 0)
            throw new ConfigurationException("memtable_offheap_space_in_mb must be positive, but was " + conf.memtable_offheap_space_in_mb, false);
        // for the moment, we default to twice as much on-heap space as off-heap, as heap overhead is very large
        if (conf.memtable_heap_space_in_mb == null)
            conf.memtable_heap_space_in_mb = (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576));
        if (conf.memtable_heap_space_in_mb <= 0)
            throw new ConfigurationException("memtable_heap_space_in_mb must be positive, but was " + conf.memtable_heap_space_in_mb, false);
        logger.info("Global memtable on-heap threshold is enabled at {}MB", conf.memtable_heap_space_in_mb);
        if (conf.memtable_offheap_space_in_mb == 0)
            logger.info("Global memtable off-heap threshold is disabled, HeapAllocator will be used instead");
        else
            logger.info("Global memtable off-heap threshold is enabled at {}MB", conf.memtable_offheap_space_in_mb);

        if (conf.repair_session_max_tree_depth != null)
        {
            logger.warn("repair_session_max_tree_depth has been deprecated and should be removed from cassandra.yaml. Use repair_session_space_in_mb instead");
            if (conf.repair_session_max_tree_depth < 10)
                throw new ConfigurationException("repair_session_max_tree_depth should not be < 10, but was " + conf.repair_session_max_tree_depth);
            if (conf.repair_session_max_tree_depth > 20)
                logger.warn("repair_session_max_tree_depth of " + conf.repair_session_max_tree_depth + " > 20 could lead to excessive memory usage");
        }
        else
        {
            conf.repair_session_max_tree_depth = 20;
        }

        if (conf.repair_session_space_in_mb == null)
            conf.repair_session_space_in_mb = Math.max(1, (int) (Runtime.getRuntime().maxMemory() / (16 * 1048576)));

        if (conf.repair_session_space_in_mb < 1)
            throw new ConfigurationException("repair_session_space_in_mb must be > 0, but was " + conf.repair_session_space_in_mb);
        else if (conf.repair_session_space_in_mb > (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)))
            logger.warn("A repair_session_space_in_mb of " + conf.repair_session_space_in_mb + " megabytes is likely to cause heap pressure");

        checkForLowestAcceptedTimeouts(conf);

        checkValidForByteConversion(conf.native_transport_max_frame_size_in_mb,
                                    "native_transport_max_frame_size_in_mb", ByteUnit.MEBI_BYTES);

        checkValidForByteConversion(conf.column_index_size_in_kb,
                                    "column_index_size_in_kb", ByteUnit.KIBI_BYTES);

        checkValidForByteConversion(conf.column_index_cache_size_in_kb,
                                    "column_index_cache_size_in_kb", ByteUnit.KIBI_BYTES);

        checkValidForByteConversion(conf.batch_size_warn_threshold_in_kb,
                                    "batch_size_warn_threshold_in_kb", ByteUnit.KIBI_BYTES);

        if (conf.native_transport_max_negotiable_protocol_version != null)
            logger.warn("The configuration option native_transport_max_negotiable_protocol_version has been deprecated " +
                        "and should be removed from cassandra.yaml as it has no longer has any effect.");

        // if data dirs, commitlog dir, or saved caches dir are set in cassandra.yaml, use that.  Otherwise,
        // use -Dcassandra.storagedir (set in cassandra-env.sh) as the parent dir for data/, commitlog/, and saved_caches/
        if (conf.commitlog_directory == null)
        {
            conf.commitlog_directory = storagedirFor("commitlog");
        }

        if (conf.hints_directory == null)
        {
            conf.hints_directory = storagedirFor("hints");
        }

        if (conf.native_transport_max_concurrent_requests_in_bytes <= 0)
        {
            conf.native_transport_max_concurrent_requests_in_bytes = Runtime.getRuntime().maxMemory() / 10;
        }

        if (conf.native_transport_max_concurrent_requests_in_bytes_per_ip <= 0)
        {
            conf.native_transport_max_concurrent_requests_in_bytes_per_ip = Runtime.getRuntime().maxMemory() / 40;
        }
        
        if (conf.native_transport_rate_limiting_enabled)
            logger.info("Native transport rate-limiting enabled at {} requests/second.", conf.native_transport_max_requests_per_second);
        else
            logger.info("Native transport rate-limiting disabled.");

        if (conf.commitlog_total_space_in_mb == null)
        {
            final int preferredSizeInMB = 8192;
            // use 1/4 of available space.  See discussion on #10013 and #10199
            final long totalSpaceInBytes = tryGetSpace(conf.commitlog_directory, FileStore::getTotalSpace);
            conf.commitlog_total_space_in_mb = calculateDefaultSpaceInMB("commitlog",
                                                                         conf.commitlog_directory,
                                                                         "commitlog_total_space_in_mb",
                                                                         preferredSizeInMB,
                                                                         totalSpaceInBytes, 1, 4);
        }

        if (conf.cdc_enabled)
        {
            // Windows memory-mapped CommitLog files is incompatible with CDC as we hard-link files in cdc_raw. Confirm we don't have both enabled.
            if (FBUtilities.isWindows && conf.commitlog_compression == null)
                throw new ConfigurationException("Cannot enable cdc on Windows with uncompressed commitlog.");

            if (conf.cdc_raw_directory == null)
            {
                conf.cdc_raw_directory = storagedirFor("cdc_raw");
            }

            if (conf.cdc_total_space_in_mb == 0)
            {
                final int preferredSizeInMB = 4096;
                // use 1/8th of available space.  See discussion on #10013 and #10199 on the CL, taking half that for CDC
                final long totalSpaceInBytes = tryGetSpace(conf.cdc_raw_directory, FileStore::getTotalSpace);
                conf.cdc_total_space_in_mb = calculateDefaultSpaceInMB("cdc",
                                                                       conf.cdc_raw_directory,
                                                                       "cdc_total_space_in_mb",
                                                                       preferredSizeInMB,
                                                                       totalSpaceInBytes, 1, 8);
            }

            logger.info("cdc_enabled is true. Starting casssandra node with Change-Data-Capture enabled.");
        }

        if (conf.saved_caches_directory == null)
        {
            conf.saved_caches_directory = storagedirFor("saved_caches");
        }
        if (conf.data_file_directories == null || conf.data_file_directories.length == 0)
        {
            conf.data_file_directories = new String[]{ storagedir("data_file_directories") + File.pathSeparator() + "data" };
        }

        long dataFreeBytes = 0;
        /* data file and commit log directories. they get created later, when they're needed. */
        for (String datadir : conf.data_file_directories)
        {
            if (datadir == null)
                throw new ConfigurationException("data_file_directories must not contain empty entry", false);
            if (datadir.equals(conf.local_system_data_file_directory))
                throw new ConfigurationException("local_system_data_file_directory must not be the same as any data_file_directories", false);
            if (datadir.equals(conf.commitlog_directory))
                throw new ConfigurationException("commitlog_directory must not be the same as any data_file_directories", false);
            if (datadir.equals(conf.hints_directory))
                throw new ConfigurationException("hints_directory must not be the same as any data_file_directories", false);
            if (datadir.equals(conf.saved_caches_directory))
                throw new ConfigurationException("saved_caches_directory must not be the same as any data_file_directories", false);

            dataFreeBytes = saturatedSum(dataFreeBytes, tryGetSpace(datadir, FileStore::getUnallocatedSpace));
        }
        if (dataFreeBytes < 64 * ONE_GB) // 64 GB
            logger.warn("Only {} free across all data volumes. Consider adding more capacity to your cluster or removing obsolete snapshots",
                        FBUtilities.prettyPrintMemory(dataFreeBytes));

        if (conf.local_system_data_file_directory != null)
        {
            if (conf.local_system_data_file_directory.equals(conf.commitlog_directory))
                throw new ConfigurationException("local_system_data_file_directory must not be the same as the commitlog_directory", false);
            if (conf.local_system_data_file_directory.equals(conf.saved_caches_directory))
                throw new ConfigurationException("local_system_data_file_directory must not be the same as the saved_caches_directory", false);
            if (conf.local_system_data_file_directory.equals(conf.hints_directory))
                throw new ConfigurationException("local_system_data_file_directory must not be the same as the hints_directory", false);

            long freeBytes = tryGetSpace(conf.local_system_data_file_directory, FileStore::getUnallocatedSpace);

            if (freeBytes < ONE_GB)
                logger.warn("Only {} free in the system data volume. Consider adding more capacity or removing obsolete snapshots",
                            FBUtilities.prettyPrintMemory(freeBytes));
        }

        if (conf.commitlog_directory.equals(conf.saved_caches_directory))
            throw new ConfigurationException("saved_caches_directory must not be the same as the commitlog_directory", false);
        if (conf.commitlog_directory.equals(conf.hints_directory))
            throw new ConfigurationException("hints_directory must not be the same as the commitlog_directory", false);
        if (conf.hints_directory.equals(conf.saved_caches_directory))
            throw new ConfigurationException("saved_caches_directory must not be the same as the hints_directory", false);

        if (conf.memtable_flush_writers == 0)
        {
            conf.memtable_flush_writers = conf.data_file_directories.length == 1 ? 2 : 1;
        }

        if (conf.memtable_flush_writers < 1)
            throw new ConfigurationException("memtable_flush_writers must be at least 1, but was " + conf.memtable_flush_writers, false);

        if (conf.memtable_cleanup_threshold == null)
        {
            conf.memtable_cleanup_threshold = (float) (1.0 / (1 + conf.memtable_flush_writers));
        }
        else
        {
            logger.warn("memtable_cleanup_threshold has been deprecated and should be removed from cassandra.yaml");
        }

        if (conf.memtable_cleanup_threshold < 0.01f)
            throw new ConfigurationException("memtable_cleanup_threshold must be >= 0.01, but was " + conf.memtable_cleanup_threshold, false);
        if (conf.memtable_cleanup_threshold > 0.99f)
            throw new ConfigurationException("memtable_cleanup_threshold must be <= 0.99, but was " + conf.memtable_cleanup_threshold, false);
        if (conf.memtable_cleanup_threshold < 0.1f)
            logger.warn("memtable_cleanup_threshold is set very low [{}], which may cause performance degradation", conf.memtable_cleanup_threshold);

        if (conf.concurrent_compactors == null)
            conf.concurrent_compactors = Math.min(8, Math.max(2, Math.min(FBUtilities.getAvailableProcessors(), conf.data_file_directories.length)));

        if (conf.concurrent_compactors <= 0)
            throw new ConfigurationException("concurrent_compactors should be strictly greater than 0, but was " + conf.concurrent_compactors, false);

        applyConcurrentValidations(conf);
        applyRepairCommandPoolSize(conf);
        applyTrackWarningsValidations(conf);

        if (conf.concurrent_materialized_view_builders <= 0)
            throw new ConfigurationException("concurrent_materialized_view_builders should be strictly greater than 0, but was " + conf.concurrent_materialized_view_builders, false);

        if (conf.num_tokens != null && conf.num_tokens > MAX_NUM_TOKENS)
            throw new ConfigurationException(String.format("A maximum number of %d tokens per node is supported", MAX_NUM_TOKENS), false);

        try
        {
            // if prepared_statements_cache_size_mb option was set to "auto" then size of the cache should be "max(1/256 of Heap (in MB), 10MB)"
            preparedStatementsCacheSizeInMB = (conf.prepared_statements_cache_size_mb == null)
                                              ? Math.max(10, (int) (Runtime.getRuntime().maxMemory() / 1024 / 1024 / 256))
                                              : conf.prepared_statements_cache_size_mb;

            if (preparedStatementsCacheSizeInMB <= 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("prepared_statements_cache_size_mb option was set incorrectly to '"
                                             + conf.prepared_statements_cache_size_mb + "', supported values are <integer> >= 0.", false);
        }

        try
        {
            // if key_cache_size_in_mb option was set to "auto" then size of the cache should be "min(5% of Heap (in MB), 100MB)
            keyCacheSizeInMB = (conf.key_cache_size_in_mb == null)
                               ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.05 / 1024 / 1024)), 100)
                               : conf.key_cache_size_in_mb;

            if (keyCacheSizeInMB < 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("key_cache_size_in_mb option was set incorrectly to '"
                                             + conf.key_cache_size_in_mb + "', supported values are <integer> >= 0.", false);
        }

        try
        {
            // if counter_cache_size_in_mb option was set to "auto" then size of the cache should be "min(2.5% of Heap (in MB), 50MB)
            counterCacheSizeInMB = (conf.counter_cache_size_in_mb == null)
                                   ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.025 / 1024 / 1024)), 50)
                                   : conf.counter_cache_size_in_mb;

            if (counterCacheSizeInMB < 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("counter_cache_size_in_mb option was set incorrectly to '"
                                             + conf.counter_cache_size_in_mb + "', supported values are <integer> >= 0.", false);
        }

        // if set to empty/"auto" then use 5% of Heap size
        indexSummaryCapacityInMB = (conf.index_summary_capacity_in_mb == null)
                                   ? Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.05 / 1024 / 1024))
                                   : conf.index_summary_capacity_in_mb;

        if (indexSummaryCapacityInMB < 0)
            throw new ConfigurationException("index_summary_capacity_in_mb option was set incorrectly to '"
                                             + conf.index_summary_capacity_in_mb + "', it should be a non-negative integer.", false);

        if (conf.user_defined_function_fail_timeout < 0)
            throw new ConfigurationException("user_defined_function_fail_timeout must not be negative", false);
        if (conf.user_defined_function_warn_timeout < 0)
            throw new ConfigurationException("user_defined_function_warn_timeout must not be negative", false);

        if (conf.user_defined_function_fail_timeout < conf.user_defined_function_warn_timeout)
            throw new ConfigurationException("user_defined_function_warn_timeout must less than user_defined_function_fail_timeout", false);

        if (conf.commitlog_segment_size_in_mb <= 0)
            throw new ConfigurationException("commitlog_segment_size_in_mb must be positive, but was "
                    + conf.commitlog_segment_size_in_mb, false);
        else if (conf.commitlog_segment_size_in_mb >= 2048)
            throw new ConfigurationException("commitlog_segment_size_in_mb must be smaller than 2048, but was "
                    + conf.commitlog_segment_size_in_mb, false);

        if (conf.max_mutation_size_in_kb == null)
            conf.max_mutation_size_in_kb = conf.commitlog_segment_size_in_mb * 1024 / 2;
        else if (conf.commitlog_segment_size_in_mb * 1024 < 2 * conf.max_mutation_size_in_kb)
            throw new ConfigurationException("commitlog_segment_size_in_mb must be at least twice the size of max_mutation_size_in_kb / 1024", false);

        // native transport encryption options
        if (conf.client_encryption_options != null)
        {
            conf.client_encryption_options.applyConfig();

            if (conf.native_transport_port_ssl != null
                && conf.native_transport_port_ssl != conf.native_transport_port
                && conf.client_encryption_options.tlsEncryptionPolicy() == EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
            {
                throw new ConfigurationException("Encryption must be enabled in client_encryption_options for native_transport_port_ssl", false);
            }
        }

        if (conf.snapshot_links_per_second < 0)
            throw new ConfigurationException("snapshot_links_per_second must be >= 0");

        if (conf.max_value_size_in_mb <= 0)
            throw new ConfigurationException("max_value_size_in_mb must be positive", false);
        else if (conf.max_value_size_in_mb >= 2048)
            throw new ConfigurationException("max_value_size_in_mb must be smaller than 2048, but was "
                    + conf.max_value_size_in_mb, false);

        switch (conf.disk_optimization_strategy)
        {
            case ssd:
                diskOptimizationStrategy = new SsdDiskOptimizationStrategy(conf.disk_optimization_page_cross_chance);
                break;
            case spinning:
                diskOptimizationStrategy = new SpinningDiskOptimizationStrategy();
                break;
        }

        if (conf.server_encryption_options != null)
        {
            conf.server_encryption_options.applyConfig();

            if (conf.server_encryption_options.enable_legacy_ssl_storage_port &&
                conf.server_encryption_options.tlsEncryptionPolicy() == EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
            {
                throw new ConfigurationException("enable_legacy_ssl_storage_port is true (enabled) with internode encryption disabled (none). Enable encryption or disable the legacy ssl storage port.");
            }
        }
        Integer maxMessageSize = conf.internode_max_message_size_in_bytes;
        if (maxMessageSize != null)
        {
            if (maxMessageSize > conf.internode_application_receive_queue_reserve_endpoint_capacity_in_bytes)
                throw new ConfigurationException("internode_max_message_size_in_mb must no exceed internode_application_receive_queue_reserve_endpoint_capacity_in_bytes", false);

            if (maxMessageSize > conf.internode_application_receive_queue_reserve_global_capacity_in_bytes)
                throw new ConfigurationException("internode_max_message_size_in_mb must no exceed internode_application_receive_queue_reserve_global_capacity_in_bytes", false);

            if (maxMessageSize > conf.internode_application_send_queue_reserve_endpoint_capacity_in_bytes)
                throw new ConfigurationException("internode_max_message_size_in_mb must no exceed internode_application_send_queue_reserve_endpoint_capacity_in_bytes", false);

            if (maxMessageSize > conf.internode_application_send_queue_reserve_global_capacity_in_bytes)
                throw new ConfigurationException("internode_max_message_size_in_mb must no exceed internode_application_send_queue_reserve_global_capacity_in_bytes", false);
        }
        else
        {
            conf.internode_max_message_size_in_bytes =
                Math.min(conf.internode_application_receive_queue_reserve_endpoint_capacity_in_bytes,
                         conf.internode_application_send_queue_reserve_endpoint_capacity_in_bytes);
        }

        validateMaxConcurrentAutoUpgradeTasksConf(conf.max_concurrent_automatic_sstable_upgrades);

        // ACI Cassandra - preserve property from earlier internal patch
        // rdar://60088220 p27729987 PRO/RST Restrict replication strategy and factor
        // that was partially replaced by CASSANDRA-14557.
        if (CassandraRelevantProperties.MINIMUM_ALLOWED_REPLICATION_FACTOR.isPresent())
        {
            int property_min = CassandraRelevantProperties.MINIMUM_ALLOWED_REPLICATION_FACTOR.getInt();
            if (property_min != conf.minimum_keyspace_rf)
            {
                logger.info("Overriding configured default_keyspace_rf of {} with value from system property {}",
                        conf.minimum_keyspace_rf, property_min);
                conf.minimum_keyspace_rf = property_min;
            }
        }

        if (conf.default_keyspace_rf < conf.minimum_keyspace_rf)
        {
            throw new ConfigurationException(String.format("default_keyspace_rf (%d) cannot be less than minimum_keyspace_rf (%d)",
                                                           conf.default_keyspace_rf, conf.minimum_keyspace_rf));
        }

        applyDenylistConfiguration(conf);

        if (conf.paxos_variant == Config.PaxosVariant.v1_without_linearizable_reads)
        {
            logger.warn("This node was started with paxos_variant config option set to v1_norrl. " +
                        "SERIAL (and LOCAL_SERIAL) reads coordinated by this node " +
                        "will not offer linearizability (see CASSANDRA-12126 for details on what this mean) with " +
                        "respect to other SERIAL operations. Please note that, with this option, SERIAL reads will be " +
                        "slower than QUORUM reads, yet offer no more guarantee. This flag should only be used in " +
                        "the restricted case of upgrading from a pre-CASSANDRA-12126 version, and only if you " +
                        "understand the tradeoff.");
        }

        Paxos.setPaxosVariant(conf.paxos_variant);

        logInitializationOutcome(logger);

        repairHistorySyncTimeoutSeconds = parseScheduledCycleTimeSeconds(conf.repair_history_sync_timeout);

        if (conf.max_space_usable_for_compactions_in_percentage < 0 || conf.max_space_usable_for_compactions_in_percentage > 1)
            throw new ConfigurationException("max_space_usable_for_compactions_in_percentage must be between 0 and 1", false);

        scheduledCompactionCycleTimeSeconds = parseScheduledCycleTimeSeconds(conf.scheduled_compaction_cycle_time);

        if (conf.column_index_max_target_size_in_kb != null || conf.column_index_max_target_index_objects != null)
        {
            logger.warn("The configuration options column_index_max_target_size_in_kb and " +
                        "column_index_max_target_index_objects have been deprecated in 4.0 " +
                        "and should be removed from cie-db-conf as it has no longer has any effect.");
        }

        if (conf.start_rpc != null) // ACI Cassandra - warn about start_rpc configuration, see rdar://74580514
        {
            logger.info("start_rpc is set {} in configuration. Thrift has been removed from ACI Cassandra 4.0 and " +
                        "cannot be started. Please remove from configuration.",
                        conf.start_rpc);
        }
    }

    @VisibleForTesting
    static void applyConcurrentValidations(Config config)
    {
        if (config.concurrent_validations < 1)
        {
            config.concurrent_validations = config.concurrent_compactors;
        }
        else if (config.concurrent_validations > config.concurrent_compactors && !allowUnlimitedConcurrentValidations)
        {
            throw new ConfigurationException("To set concurrent_validations > concurrent_compactors, " +
                                             "set the system property cassandra.allow_unlimited_concurrent_validations=true");
        }
    }

    @VisibleForTesting
    static void applyRepairCommandPoolSize(Config config)
    {
        if (config.repair_command_pool_size < 1)
            config.repair_command_pool_size = config.concurrent_validations;
    }

    @VisibleForTesting
    static void applyTrackWarningsValidations(Config config)
    {
        config.track_warnings.validate("track_warnings");
    }

    public static GuardrailsOptions getGuardrailsConfig()
    {
        return conf.guardrails;
    }

    private static void applyGuardrails()
    {
        try
        {
            conf.guardrails.validate();
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Invalid guardrails configuration: " + e.getMessage(), e);
        }
    }

    private static <X> X applyDenylistSetting(X currentValue, X legacyValue, X defaultValue, String name)
    {
        if (legacyValue != null && currentValue != null)
        {
            if (legacyValue.equals(currentValue))
                logger.info("Legacy denylist configuration '{}' set same as - please remove", name);
            else
                logger.info("Conflict with legacy denylist configuration. Using {}={} (legacy was {})",
                            name, currentValue, legacyValue);
            return currentValue;
        }
        else if (legacyValue == null && currentValue != null)
        {
            return currentValue;
        }
        else if (legacyValue != null && currentValue == null)
        {
            logger.info("Legacy denylist configuration for '{}' used - please upgrade", name);
            return legacyValue;
        }
        else
        {
            return defaultValue;
        }
    }

    private static void applyDenylistConfiguration(Config config)
    {
        config.partition_denylist_enabled = applyDenylistSetting(config.partition_denylist_enabled,
                                                                 config.enable_partition_blacklist,
                                                                 false, "partition_denylist_enabled");
        config.denylist_writes_enabled = applyDenylistSetting(config.denylist_writes_enabled,
                                                              config.enable_blacklist_writes,
                                                              true, "denylist_writes_enabled");
        config.denylist_reads_enabled = applyDenylistSetting(config.denylist_reads_enabled,
                                                             config.enable_blacklist_reads,
                                                             true, "denylist_reads_enabled");
        config.denylist_range_reads_enabled = applyDenylistSetting(config.denylist_range_reads_enabled,
                                                                   config.enable_blacklist_range_reads,
                                                                   false, "denylist_range_reads_enabled");
        config.denylist_refresh_seconds = applyDenylistSetting(config.denylist_refresh_seconds,
                                                               config.blacklist_refresh_period_seconds,
                                                               600, "denylist_refresh_seconds");
        config.denylist_initial_load_retry_seconds = applyDenylistSetting(config.denylist_initial_load_retry_seconds,
                                                                          config.blacklist_initial_load_retry_seconds,
                                                                          5, "denylist_initial_load_retry_seconds");
        config.denylist_max_keys_per_table = applyDenylistSetting(config.denylist_max_keys_per_table,
                                                                  config.blacklist_max_keys_per_table,
                                                                  1000, "denylist_max_keys_per_table");
        config.denylist_max_keys_total = applyDenylistSetting(config.denylist_max_keys_total,
                                                              config.blacklist_max_keys_total,
                                                              10000, "denylist_max_keys_total");
        config.denylist_consistency_level = applyDenylistSetting(config.denylist_consistency_level,
                                                                 config.blacklist_consistency_level,
                                                                 ConsistencyLevel.QUORUM, "denylist_consistency_level");
    }

    private static String storagedirFor(String type)
    {
        return storagedir(type + "_directory") + File.pathSeparator() + type;
    }

    private static String storagedir(String errMsgType)
    {
        String storagedir = System.getProperty(Config.PROPERTY_PREFIX + "storagedir", null);
        if (storagedir == null)
            throw new ConfigurationException(errMsgType + " is missing and -Dcassandra.storagedir is not set", false);
        return storagedir;
    }

    static int calculateDefaultSpaceInMB(String type, String path, String setting, int preferredSizeInMB, long totalSpaceInBytes, long totalSpaceNumerator, long totalSpaceDenominator)
    {
        final long totalSizeInMB = totalSpaceInBytes / ONE_MB;
        final int minSizeInMB = Ints.saturatedCast(totalSpaceNumerator * totalSizeInMB / totalSpaceDenominator);

        if (minSizeInMB < preferredSizeInMB)
        {
            logger.warn("Small {} volume detected at '{}'; setting {} to {}.  You can override this in cassandra.yaml",
                        type, path, setting, minSizeInMB);
            return minSizeInMB;
        }
        else
        {
            return preferredSizeInMB;
        }
    }

    public static void applyAddressConfig() throws ConfigurationException
    {
        applyAddressConfig(conf);
    }

    public static void applyAddressConfig(Config config) throws ConfigurationException
    {
        listenAddress = null;
        rpcAddress = null;
        broadcastAddress = null;
        broadcastRpcAddress = null;

        /* Local IP, hostname or interface to bind services to */
        if (config.listen_address != null && config.listen_interface != null)
        {
            throw new ConfigurationException("Set listen_address OR listen_interface, not both", false);
        }
        else if (config.listen_address != null)
        {
            try
            {
                listenAddress = InetAddress.getByName(config.listen_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown listen_address '" + config.listen_address + '\'', false);
            }

            if (listenAddress.isAnyLocalAddress())
                throw new ConfigurationException("listen_address cannot be a wildcard address (" + config.listen_address + ")!", false);
        }
        else if (config.listen_interface != null)
        {
            listenAddress = getNetworkInterfaceAddress(config.listen_interface, "listen_interface", config.listen_interface_prefer_ipv6);
        }

        /* Gossip Address to broadcast */
        if (config.broadcast_address != null)
        {
            try
            {
                broadcastAddress = InetAddress.getByName(config.broadcast_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown broadcast_address '" + config.broadcast_address + '\'', false);
            }

            if (broadcastAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_address cannot be a wildcard address (" + config.broadcast_address + ")!", false);
        }

        /* Local IP, hostname or interface to bind RPC server to */
        if (config.rpc_address != null && config.rpc_interface != null)
        {
            throw new ConfigurationException("Set rpc_address OR rpc_interface, not both", false);
        }
        else if (config.rpc_address != null)
        {
            try
            {
                rpcAddress = InetAddress.getByName(config.rpc_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown host in rpc_address " + config.rpc_address, false);
            }
        }
        else if (config.rpc_interface != null)
        {
            rpcAddress = getNetworkInterfaceAddress(config.rpc_interface, "rpc_interface", config.rpc_interface_prefer_ipv6);
        }
        else
        {
            rpcAddress = FBUtilities.getJustLocalAddress();
        }

        /* RPC address to broadcast */
        if (config.broadcast_rpc_address != null)
        {
            try
            {
                broadcastRpcAddress = InetAddress.getByName(config.broadcast_rpc_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown broadcast_rpc_address '" + config.broadcast_rpc_address + '\'', false);
            }

            if (broadcastRpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_rpc_address cannot be a wildcard address (" + config.broadcast_rpc_address + ")!", false);
        }
        else
        {
            if (rpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("If rpc_address is set to a wildcard address (" + config.rpc_address + "), then " +
                                                 "you must set broadcast_rpc_address to a value other than " + config.rpc_address, false);
        }
    }

    public static void applyEncryptionContext()
    {
        // always attempt to load the cipher factory, as we could be in the situation where the user has disabled encryption,
        // but has existing commitlogs and sstables on disk that are still encrypted (and still need to be read)
        encryptionContext = new EncryptionContext(conf.transparent_data_encryption_options);
    }

    public static void applySslContext()
    {
        try
        {
            SSLFactory.validateSslContext("Internode messaging", conf.server_encryption_options, true, true);
            SSLFactory.validateSslContext("Native transport", conf.client_encryption_options, conf.client_encryption_options.require_client_auth, true);
            SSLFactory.initHotReloading(conf.server_encryption_options, conf.client_encryption_options, false);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Failed to initialize SSL", e);
        }
    }

    public static void applySeedProvider()
    {
        // load the seeds for node contact points
        if (conf.seed_provider == null)
        {
            throw new ConfigurationException("seeds configuration is missing; a minimum of one seed is required.", false);
        }
        try
        {
            Class<?> seedProviderClass = Class.forName(conf.seed_provider.class_name);
            seedProvider = (SeedProvider)seedProviderClass.getConstructor(Map.class).newInstance(conf.seed_provider.parameters);
        }
        // there are about 5 checked exceptions that could be thrown here.
        catch (Exception e)
        {
            throw new ConfigurationException(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.", true);
        }
        if (seedProvider.getSeeds().size() == 0)
            throw new ConfigurationException("The seed provider lists no seeds.", false);
    }

    @VisibleForTesting
    static void checkForLowestAcceptedTimeouts(Config conf)
    {
        if(conf.read_request_timeout_in_ms < LOWEST_ACCEPTED_TIMEOUT)
        {
           logInfo("read_request_timeout_in_ms", conf.read_request_timeout_in_ms, LOWEST_ACCEPTED_TIMEOUT);
           conf.read_request_timeout_in_ms = LOWEST_ACCEPTED_TIMEOUT;
        }

        if(conf.range_request_timeout_in_ms < LOWEST_ACCEPTED_TIMEOUT)
        {
           logInfo("range_request_timeout_in_ms", conf.range_request_timeout_in_ms, LOWEST_ACCEPTED_TIMEOUT);
           conf.range_request_timeout_in_ms = LOWEST_ACCEPTED_TIMEOUT;
        }

        if(conf.request_timeout_in_ms < LOWEST_ACCEPTED_TIMEOUT)
        {
           logInfo("request_timeout_in_ms", conf.request_timeout_in_ms, LOWEST_ACCEPTED_TIMEOUT);
           conf.request_timeout_in_ms = LOWEST_ACCEPTED_TIMEOUT;
        }

        if(conf.write_request_timeout_in_ms < LOWEST_ACCEPTED_TIMEOUT)
        {
           logInfo("write_request_timeout_in_ms", conf.write_request_timeout_in_ms, LOWEST_ACCEPTED_TIMEOUT);
           conf.write_request_timeout_in_ms = LOWEST_ACCEPTED_TIMEOUT;
        }

        if(conf.cas_contention_timeout_in_ms < LOWEST_ACCEPTED_TIMEOUT)
        {
           logInfo("cas_contention_timeout_in_ms", conf.cas_contention_timeout_in_ms, LOWEST_ACCEPTED_TIMEOUT);
           conf.cas_contention_timeout_in_ms = LOWEST_ACCEPTED_TIMEOUT;
        }

        if(conf.counter_write_request_timeout_in_ms < LOWEST_ACCEPTED_TIMEOUT)
        {
           logInfo("counter_write_request_timeout_in_ms", conf.counter_write_request_timeout_in_ms, LOWEST_ACCEPTED_TIMEOUT);
           conf.counter_write_request_timeout_in_ms = LOWEST_ACCEPTED_TIMEOUT;
        }

        if(conf.truncate_request_timeout_in_ms < LOWEST_ACCEPTED_TIMEOUT)
        {
           logInfo("truncate_request_timeout_in_ms", conf.truncate_request_timeout_in_ms, LOWEST_ACCEPTED_TIMEOUT);
           conf.truncate_request_timeout_in_ms = LOWEST_ACCEPTED_TIMEOUT;
        }
    }

    private static void logInfo(String property, long actualValue, long lowestAcceptedValue)
    {
        logger.info("found {}::{} less than lowest acceptable value {}, continuing with {}", property, actualValue, lowestAcceptedValue, lowestAcceptedValue);
    }

    public static void applyTokensConfig()
    {
        applyTokensConfig(conf);
    }

    static void applyTokensConfig(Config conf)
    {
        if (conf.initial_token != null)
        {
            Collection<String> tokens = tokensFromString(conf.initial_token);
            if (conf.num_tokens == null)
            {
                if (tokens.size() == 1)
                    conf.num_tokens = 1;
                else
                    throw new ConfigurationException("initial_token was set but num_tokens is not!", false);
            }

            if (tokens.size() != conf.num_tokens)
            {
                throw new ConfigurationException(String.format("The number of initial tokens (by initial_token) specified (%s) is different from num_tokens value (%s)",
                                                               tokens.size(),
                                                               conf.num_tokens),
                                                 false);
            }

            for (String token : tokens)
                partitioner.getTokenFactory().validate(token);
        }
        else if (conf.num_tokens == null)
        {
            conf.num_tokens = 1;
        }
    }

    // definitely not safe for tools + clients - implicitly instantiates StorageService
    public static void applySnitch()
    {
        /* end point snitch */
        if (conf.endpoint_snitch == null)
        {
            throw new ConfigurationException("Missing endpoint_snitch directive", false);
        }
        snitch = createEndpointSnitch(conf.dynamic_snitch, conf.endpoint_snitch);
        EndpointSnitchInfo.create();

        localDC = snitch.getLocalDatacenter();
        localComparator = (replica1, replica2) -> {
            boolean local1 = localDC.equals(snitch.getDatacenter(replica1));
            boolean local2 = localDC.equals(snitch.getDatacenter(replica2));
            if (local1 && !local2)
                return -1;
            if (local2 && !local1)
                return 1;
            return 0;
        };
        newFailureDetector = () -> createFailureDetector(conf.failure_detector);
    }

    // definitely not safe for tools + clients - implicitly instantiates schema
    public static void applyPartitioner()
    {
        applyPartitioner(conf);
    }

    public static void applyPartitioner(Config conf)
    {
        /* Hashing strategy */
        if (conf.partitioner == null)
        {
            throw new ConfigurationException("Missing directive: partitioner", false);
        }
        String name = conf.partitioner;
        try
        {
            name = System.getProperty(Config.PROPERTY_PREFIX + "partitioner", conf.partitioner);
            partitioner = FBUtilities.newPartitioner(name);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Invalid partitioner class " + name, e);
        }

        paritionerName = partitioner.getClass().getCanonicalName();
    }

    /**
     * Computes the sum of the 2 specified positive values returning {@code Long.MAX_VALUE} if the sum overflow.
     *
     * @param left the left operand
     * @param right the right operand
     * @return the sum of the 2 specified positive values of {@code Long.MAX_VALUE} if the sum overflow.
     */
    private static long saturatedSum(long left, long right)
    {
        assert left >= 0 && right >= 0;
        long sum = left + right;
        return sum < 0 ? Long.MAX_VALUE : sum;
    }

    private static long tryGetSpace(String dir, PathUtils.IOToLongFunction<FileStore> getSpace)
    {
        return PathUtils.tryGetSpace(new File(dir).toPath(), getSpace, e -> { throw new ConfigurationException("Unable check disk space in '" + dir + "'. Perhaps the Cassandra user does not have the necessary permissions"); });
    }

    public static IEndpointSnitch createEndpointSnitch(boolean dynamic, String snitchClassName) throws ConfigurationException
    {
        if (!snitchClassName.contains("."))
            snitchClassName = "org.apache.cassandra.locator." + snitchClassName;
        IEndpointSnitch snitch = FBUtilities.construct(snitchClassName, "snitch");
        return dynamic ? new DynamicEndpointSnitch(snitch) : snitch;
    }

    private static IFailureDetector createFailureDetector(String detectorClassName) throws ConfigurationException
    {
        if (!detectorClassName.contains("."))
            detectorClassName = "org.apache.cassandra.gms." + detectorClassName;
        IFailureDetector detector = FBUtilities.construct(detectorClassName, "failure detector");
        return detector;
    }

    public static IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public static void setAuthenticator(IAuthenticator authenticator)
    {
        DatabaseDescriptor.authenticator = authenticator;
    }

    public static IAuthorizer getAuthorizer()
    {
        return authorizer;
    }

    public static void setAuthorizer(IAuthorizer authorizer)
    {
        DatabaseDescriptor.authorizer = authorizer;
    }

    public static INetworkAuthorizer getNetworkAuthorizer()
    {
        return networkAuthorizer;
    }

    public static void setNetworkAuthorizer(INetworkAuthorizer networkAuthorizer)
    {
        DatabaseDescriptor.networkAuthorizer = networkAuthorizer;
    }

    public static void setAuthFromRoot(boolean fromRoot)
    {
        conf.traverse_auth_from_root = fromRoot;
    }

    public static boolean getAuthFromRoot()
    {
        return conf.traverse_auth_from_root;
    }

    public static IRoleManager getRoleManager()
    {
        return roleManager;
    }

    public static void setRoleManager(IRoleManager roleManager)
    {
        DatabaseDescriptor.roleManager = roleManager;
    }

    public static int getPermissionsValidity()
    {
        return conf.permissions_validity_in_ms;
    }

    public static void setPermissionsValidity(int timeout)
    {
        conf.permissions_validity_in_ms = timeout;
    }

    public static int getPermissionsUpdateInterval()
    {
        return conf.permissions_update_interval_in_ms == -1
             ? conf.permissions_validity_in_ms
             : conf.permissions_update_interval_in_ms;
    }

    public static void setPermissionsUpdateInterval(int updateInterval)
    {
        conf.permissions_update_interval_in_ms = updateInterval;
    }

    public static int getPermissionsCacheMaxEntries()
    {
        return conf.permissions_cache_max_entries;
    }

    public static int setPermissionsCacheMaxEntries(int maxEntries)
    {
        return conf.permissions_cache_max_entries = maxEntries;
    }

    public static boolean getPermissionsCacheActiveUpdate()
    {
        return conf.permissions_cache_active_update;
    }

    public static void setPermissionsCacheActiveUpdate(boolean update)
    {
        conf.permissions_cache_active_update = update;
    }

    public static int getRolesValidity()
    {
        return conf.roles_validity_in_ms;
    }

    public static void setRolesValidity(int validity)
    {
        conf.roles_validity_in_ms = validity;
    }

    public static int getRolesUpdateInterval()
    {
        return conf.roles_update_interval_in_ms == -1
             ? conf.roles_validity_in_ms
             : conf.roles_update_interval_in_ms;
    }

    public static void setRolesCacheActiveUpdate(boolean update)
    {
        conf.roles_cache_active_update = update;
    }

    public static boolean getRolesCacheActiveUpdate()
    {
        return conf.roles_cache_active_update;
    }

    public static void setRolesUpdateInterval(int interval)
    {
        conf.roles_update_interval_in_ms = interval;
    }

    public static int getRolesCacheMaxEntries()
    {
        return conf.roles_cache_max_entries;
    }

    public static int setRolesCacheMaxEntries(int maxEntries)
    {
        return conf.roles_cache_max_entries = maxEntries;
    }

    public static int getCredentialsValidity()
    {
        return conf.credentials_validity_in_ms;
    }

    public static void setCredentialsValidity(int timeout)
    {
        conf.credentials_validity_in_ms = timeout;
    }

    public static int getCredentialsUpdateInterval()
    {
        return conf.credentials_update_interval_in_ms == -1
               ? conf.credentials_validity_in_ms
               : conf.credentials_update_interval_in_ms;
    }

    public static void setCredentialsUpdateInterval(int updateInterval)
    {
        conf.credentials_update_interval_in_ms = updateInterval;
    }

    public static int getCredentialsCacheMaxEntries()
    {
        return conf.credentials_cache_max_entries;
    }

    public static int setCredentialsCacheMaxEntries(int maxEntries)
    {
        return conf.credentials_cache_max_entries = maxEntries;
    }

    public static boolean getCredentialsCacheActiveUpdate()
    {
        return conf.credentials_cache_active_update;
    }

    public static void setCredentialsCacheActiveUpdate(boolean update)
    {
        conf.credentials_cache_active_update = update;
    }

    public static boolean getAuthHashCacheEnabled()
    {
        return conf.auth_hash_cache;
    }

    public static void setAuthHashCacheEnabled(boolean authHashCacheEnabled)
    {
        conf.auth_hash_cache = authHashCacheEnabled;
    }

    public static int getMaxValueSize()
    {
        return conf.max_value_size_in_mb * 1024 * 1024;
    }

    public static void setMaxValueSize(int maxValueSizeInBytes)
    {
        conf.max_value_size_in_mb = maxValueSizeInBytes / 1024 / 1024;
    }

    /**
     * Creates all storage-related directories.
     */
    public static void createAllDirectories()
    {
        try
        {
            if (conf.data_file_directories.length == 0)
                throw new ConfigurationException("At least one DataFileDirectory must be specified", false);

            for (String dataFileDirectory : conf.data_file_directories)
                FileUtils.createDirectory(dataFileDirectory);

            if (conf.local_system_data_file_directory != null)
                FileUtils.createDirectory(conf.local_system_data_file_directory);

            if (conf.commitlog_directory == null)
                throw new ConfigurationException("commitlog_directory must be specified", false);
            FileUtils.createDirectory(conf.commitlog_directory);

            if (conf.hints_directory == null)
                throw new ConfigurationException("hints_directory must be specified", false);
            FileUtils.createDirectory(conf.hints_directory);

            if (conf.saved_caches_directory == null)
                throw new ConfigurationException("saved_caches_directory must be specified", false);
            FileUtils.createDirectory(conf.saved_caches_directory);

            if (conf.cdc_enabled)
            {
                if (conf.cdc_raw_directory == null)
                    throw new ConfigurationException("cdc_raw_directory must be specified", false);
                FileUtils.createDirectory(conf.cdc_raw_directory);
            }
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException("Bad configuration; unable to start server: "+e.getMessage());
        }
        catch (FSWriteError e)
        {
            throw new IllegalStateException(e.getCause().getMessage() + "; unable to start server");
        }
    }

    public static IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public static String getPartitionerName()
    {
        return paritionerName;
    }

    /* For tests ONLY, don't use otherwise or all hell will break loose. Tests should restore value at the end. */
    public static IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner old = partitioner;
        partitioner = newPartitioner;
        return old;
    }

    public static IEndpointSnitch getEndpointSnitch()
    {
        return snitch;
    }
    public static void setEndpointSnitch(IEndpointSnitch eps)
    {
        snitch = eps;
    }

    public static IFailureDetector newFailureDetector()
    {
        return newFailureDetector.get();
    }

    public static void setDefaultFailureDetector()
    {
        newFailureDetector = () -> createFailureDetector("FailureDetector");
    }

    public static int getColumnIndexSize()
    {
        return (int) ByteUnit.KIBI_BYTES.toBytes(conf.column_index_size_in_kb);
    }

    public static int getColumnIndexSizeInKB()
    {
        return conf.column_index_size_in_kb;
    }

    public static void setColumnIndexSize(int val)
    {
        checkValidForByteConversion(val, "column_index_size_in_kb", ByteUnit.KIBI_BYTES);
        conf.column_index_size_in_kb = val;
    }

    public static int getColumnIndexCacheSize()
    {
        return (int) ByteUnit.KIBI_BYTES.toBytes(conf.column_index_cache_size_in_kb);
    }

    public static int getColumnIndexCacheSizeInKB()
    {
        return conf.column_index_cache_size_in_kb;
    }

    public static void setColumnIndexCacheSize(int val)
    {
        checkValidForByteConversion(val, "column_index_cache_size_in_kb", ByteUnit.KIBI_BYTES);
        conf.column_index_cache_size_in_kb = val;
    }

    public static int getBatchSizeWarnThreshold()
    {
        return (int) ByteUnit.KIBI_BYTES.toBytes(conf.batch_size_warn_threshold_in_kb);
    }

    public static int getBatchSizeWarnThresholdInKB()
    {
        return conf.batch_size_warn_threshold_in_kb;
    }

    public static long getBatchSizeFailThreshold()
    {
        return ByteUnit.KIBI_BYTES.toBytes(conf.batch_size_fail_threshold_in_kb);
    }

    public static int getBatchSizeFailThresholdInKB()
    {
        return conf.batch_size_fail_threshold_in_kb;
    }

    public static int getUnloggedBatchAcrossPartitionsWarnThreshold()
    {
        return conf.unlogged_batch_across_partitions_warn_threshold;
    }

    public static void setBatchSizeWarnThresholdInKB(int threshold)
    {
        checkValidForByteConversion(threshold, "batch_size_warn_threshold_in_kb", ByteUnit.KIBI_BYTES);
        conf.batch_size_warn_threshold_in_kb = threshold;
    }

    public static void setBatchSizeFailThresholdInKB(int threshold)
    {
        conf.batch_size_fail_threshold_in_kb = threshold;
    }

    public static Collection<String> getInitialTokens()
    {
        return tokensFromString(System.getProperty(Config.PROPERTY_PREFIX + "initial_token", conf.initial_token));
    }

    public static String getAllocateTokensForKeyspace()
    {
        return System.getProperty(Config.PROPERTY_PREFIX + "allocate_tokens_for_keyspace", conf.allocate_tokens_for_keyspace);
    }

    public static Integer getAllocateTokensForLocalRf()
    {
        return conf.allocate_tokens_for_local_replication_factor;
    }

    public static Collection<String> tokensFromString(String tokenString)
    {
        List<String> tokens = new ArrayList<>();
        if (tokenString != null)
            for (String token : StringUtils.split(tokenString, ','))
                tokens.add(token.trim());
        return tokens;
    }

    public static int getNumTokens()
    {
        return conf.num_tokens;
    }

    public static InetAddressAndPort getReplaceAddress()
    {
        try
        {
            if (System.getProperty(Config.PROPERTY_PREFIX + "replace_address", null) != null)
                return InetAddressAndPort.getByName(System.getProperty(Config.PROPERTY_PREFIX + "replace_address", null));
            else if (System.getProperty(Config.PROPERTY_PREFIX + "replace_address_first_boot", null) != null)
                return InetAddressAndPort.getByName(System.getProperty(Config.PROPERTY_PREFIX + "replace_address_first_boot", null));
            return null;
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Replacement host name could not be resolved or scope_id was specified for a global IPv6 address", e);
        }
    }

    public static Collection<String> getReplaceTokens()
    {
        return tokensFromString(System.getProperty(Config.PROPERTY_PREFIX + "replace_token", null));
    }

    public static UUID getReplaceNode()
    {
        try
        {
            return UUID.fromString(System.getProperty(Config.PROPERTY_PREFIX + "replace_node", null));
        } catch (NullPointerException e)
        {
            return null;
        }
    }

    public static String getClusterName()
    {
        return conf.cluster_name;
    }

    public static int getStoragePort()
    {
        return Integer.parseInt(System.getProperty(Config.PROPERTY_PREFIX + "storage_port", Integer.toString(conf.storage_port)));
    }

    public static int getSSLStoragePort()
    {
        return Integer.parseInt(System.getProperty(Config.PROPERTY_PREFIX + "ssl_storage_port", Integer.toString(conf.ssl_storage_port)));
    }

    public static long nativeTransportIdleTimeout()
    {
        return conf.native_transport_idle_timeout_in_ms;
    }

    public static void setNativeTransportIdleTimeout(long nativeTransportTimeout)
    {
        conf.native_transport_idle_timeout_in_ms = nativeTransportTimeout;
    }

    public static long getRpcTimeout(TimeUnit unit)
    {
        return unit.convert(conf.request_timeout_in_ms, MILLISECONDS);
    }

    public static void setRpcTimeout(long timeOutInMillis)
    {
        conf.request_timeout_in_ms = timeOutInMillis;
    }

    public static long getReadRpcTimeout(TimeUnit unit)
    {
        return unit.convert(conf.read_request_timeout_in_ms, MILLISECONDS);
    }

    public static void setReadRpcTimeout(long timeOutInMillis)
    {
        conf.read_request_timeout_in_ms = timeOutInMillis;
    }

    public static long getRangeRpcTimeout(TimeUnit unit)
    {
        return unit.convert(conf.range_request_timeout_in_ms, MILLISECONDS);
    }

    public static void setRangeRpcTimeout(long timeOutInMillis)
    {
        conf.range_request_timeout_in_ms = timeOutInMillis;
    }

    public static long getWriteRpcTimeout(TimeUnit unit)
    {
        return unit.convert(conf.write_request_timeout_in_ms, MILLISECONDS);
    }

    public static void setWriteRpcTimeout(long timeOutInMillis)
    {
        conf.write_request_timeout_in_ms = timeOutInMillis;
    }

    public static long getCounterWriteRpcTimeout(TimeUnit unit)
    {
        return unit.convert(conf.counter_write_request_timeout_in_ms, MILLISECONDS);
    }

    public static void setCounterWriteRpcTimeout(long timeOutInMillis)
    {
        conf.counter_write_request_timeout_in_ms = timeOutInMillis;
    }

    public static long getCasContentionTimeout(TimeUnit unit)
    {
        return unit.convert(conf.cas_contention_timeout_in_ms, MILLISECONDS);
    }

    public static void setCasContentionTimeout(long timeOutInMillis)
    {
        conf.cas_contention_timeout_in_ms = timeOutInMillis;
    }

    public static long getTruncateRpcTimeout(TimeUnit unit)
    {
        return unit.convert(conf.truncate_request_timeout_in_ms, MILLISECONDS);
    }

    public static void setTruncateRpcTimeout(long timeOutInMillis)
    {
        conf.truncate_request_timeout_in_ms = timeOutInMillis;
    }

    public static boolean hasCrossNodeTimeout()
    {
        return conf.cross_node_timeout;
    }

    public static void setCrossNodeTimeout(boolean crossNodeTimeout)
    {
        conf.cross_node_timeout = crossNodeTimeout;
    }

    public static long getSlowQueryTimeout(TimeUnit units)
    {
        return units.convert(conf.slow_query_log_timeout_in_ms, MILLISECONDS);
    }

    /**
     * @return the minimum configured {read, write, range, truncate, misc} timeout
     */
    public static long getMinRpcTimeout(TimeUnit unit)
    {
        return Longs.min(getRpcTimeout(unit),
                         getReadRpcTimeout(unit),
                         getRangeRpcTimeout(unit),
                         getWriteRpcTimeout(unit),
                         getCounterWriteRpcTimeout(unit),
                         getTruncateRpcTimeout(unit));
    }

    public static long getPingTimeout(TimeUnit unit)
    {
        return unit.convert(getBlockForPeersTimeoutInSeconds(), TimeUnit.SECONDS);
    }

    public static double getPhiConvictThreshold()
    {
        return conf.phi_convict_threshold;
    }

    public static void setPhiConvictThreshold(double phiConvictThreshold)
    {
        conf.phi_convict_threshold = phiConvictThreshold;
    }

    public static int getConcurrentReaders()
    {
        return conf.concurrent_reads;
    }

    public static void setConcurrentReaders(int concurrent_reads)
    {
        if (concurrent_reads < 0)
        {
            throw new IllegalArgumentException("Concurrent reads must be non-negative");
        }
        conf.concurrent_reads = concurrent_reads;
    }

    public static int getConcurrentWriters()
    {
        return conf.concurrent_writes;
    }

    public static void setConcurrentWriters(int concurrent_writers)
    {
        if (concurrent_writers < 0)
        {
            throw new IllegalArgumentException("Concurrent reads must be non-negative");
        }
        conf.concurrent_writes = concurrent_writers;
    }

    public static int getConcurrentCounterWriters()
    {
        return conf.concurrent_counter_writes;
    }

    public static void setConcurrentCounterWriters(int concurrent_counter_writes)
    {
        if (concurrent_counter_writes < 0)
        {
            throw new IllegalArgumentException("Concurrent reads must be non-negative");
        }
        conf.concurrent_counter_writes = concurrent_counter_writes;
    }

    public static int getConcurrentViewWriters()
    {
        return conf.concurrent_materialized_view_writes;
    }

    public static void setConcurrentViewWriters(int concurrent_materialized_view_writes)
    {
        if (concurrent_materialized_view_writes < 0)
        {
            throw new IllegalArgumentException("Concurrent reads must be non-negative");
        }
        conf.concurrent_materialized_view_writes = concurrent_materialized_view_writes;
    }

    public static int getFlushWriters()
    {
        return conf.memtable_flush_writers;
    }

    public static int getAvailableProcessors()
    {
        return conf == null ? -1 : conf.available_processors;
    }

    public static int getConcurrentCompactors()
    {
        return conf.concurrent_compactors;
    }

    public static void setConcurrentCompactors(int value)
    {
        conf.concurrent_compactors = value;
    }

    public static int getCompactionThroughputMbPerSec()
    {
        return conf.compaction_throughput_mb_per_sec;
    }

    public static void setCompactionThroughputMbPerSec(int value)
    {
        conf.compaction_throughput_mb_per_sec = value;
    }

    public static long getCompactionLargePartitionWarningThreshold() { return ByteUnit.MEBI_BYTES.toBytes(conf.compaction_large_partition_warning_threshold_mb); }

    public static int getCompactionTombstoneWarningThreshold()
    {
        return conf.compaction_tombstone_warning_threshold;
    }

    public static void setCompactionTombstoneWarningThreshold(int count)
    {
        conf.compaction_tombstone_warning_threshold = count;
    }

    public static int getConcurrentIndexBuilds()
    {
        return conf.concurrent_index_builds;
    }

    public static int getConcurrentValidations()
    {
        return conf.concurrent_validations;
    }

    public static void setConcurrentValidations(int value)
    {
        value = value > 0 ? value : Integer.MAX_VALUE;
        conf.concurrent_validations = value;
    }

    public static int getConcurrentViewBuilders()
    {
        return conf.concurrent_materialized_view_builders;
    }

    public static void setConcurrentViewBuilders(int value)
    {
        conf.concurrent_materialized_view_builders = value;
    }

    public static long getMinFreeSpacePerDriveInBytes()
    {
        return ByteUnit.MEBI_BYTES.toBytes(conf.min_free_space_per_drive_in_mb);
    }

    @VisibleForTesting
    public static long setMinFreeSpacePerDriveInBytes(long value)
    {
        return conf.min_free_space_per_drive_in_mb = (int)(value >> 20);
    }

    public static double getMaxSpaceForCompactionsPerDrive()
    {
        return conf.max_space_usable_for_compactions_in_percentage;
    }

    public static void setMaxSpaceForCompactionsPerDrive(double percentage)
    {
        conf.max_space_usable_for_compactions_in_percentage = percentage;
    }

    public static boolean getDisableSTCSInL0()
    {
        return disableSTCSInL0;
    }

    public static void setDisableSTCSInL0(boolean disabled)
    {
        disableSTCSInL0 = disabled;
    }

    public static int getStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.stream_throughput_outbound_megabits_per_sec;
    }

    public static void setStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.stream_throughput_outbound_megabits_per_sec = value;
    }

    public static int getEntireSSTableStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.entire_sstable_stream_throughput_outbound_megabits_per_sec;
    }

    public static void setEntireSSTableStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.entire_sstable_stream_throughput_outbound_megabits_per_sec = value;
    }

    public static int getInterDCStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.inter_dc_stream_throughput_outbound_megabits_per_sec;
    }

    public static void setInterDCStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.inter_dc_stream_throughput_outbound_megabits_per_sec = value;
    }

    public static int getEntireSSTableInterDCStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.entire_sstable_inter_dc_stream_throughput_outbound_megabits_per_sec;
    }

    public static void setEntireSSTableInterDCStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.entire_sstable_inter_dc_stream_throughput_outbound_megabits_per_sec = value;
    }

    /**
     * Checks if the local system data must be stored in a specific location which supports redundancy.
     *
     * @return {@code true} if the local system keyspaces data must be stored in a different location,
     * {@code false} otherwise.
     */
    public static boolean useSpecificLocationForLocalSystemData()
    {
        return conf.local_system_data_file_directory != null;
    }

    /**
     * Returns the locations where the local system keyspaces data should be stored.
     *
     * <p>If the {@code local_system_data_file_directory} was unspecified, the local system keyspaces data should be stored
     * in the first data directory. This approach guarantees that the server can tolerate the lost of all the disks but the first one.</p>
     *
     * @return the locations where should be stored the local system keyspaces data
     */
    public static String[] getLocalSystemKeyspacesDataFileLocations()
    {
        if (useSpecificLocationForLocalSystemData())
            return new String[] {conf.local_system_data_file_directory};

        return conf.data_file_directories.length == 0  ? conf.data_file_directories
                                                       : new String[] {conf.data_file_directories[0]};
    }

    /**
     * Returns the locations where the non local system keyspaces data should be stored.
     *
     * @return the locations where the non local system keyspaces data should be stored.
     */
    public static String[] getNonLocalSystemKeyspacesDataFileLocations()
    {
        return conf.data_file_directories;
    }

    /**
     * Returns the list of all the directories where the data files can be stored (for local system and non local system keyspaces).
     *
     * @return the list of all the directories where the data files can be stored.
     */
    public static String[] getAllDataFileLocations()
    {
        if (conf.local_system_data_file_directory == null)
            return conf.data_file_directories;

        return ArrayUtils.addFirst(conf.data_file_directories, conf.local_system_data_file_directory);
    }

    public static String getCommitLogLocation()
    {
        return conf.commitlog_directory;
    }

    @VisibleForTesting
    public static void setCommitLogLocation(String value)
    {
        conf.commitlog_directory = value;
    }

    public static ParameterizedClass getCommitLogCompression()
    {
        return conf.commitlog_compression;
    }

    public static void setCommitLogCompression(ParameterizedClass compressor)
    {
        conf.commitlog_compression = compressor;
    }

    public static Config.FlushCompression getFlushCompression()
    {
        return conf.flush_compression;
    }

    public static void setFlushCompression(Config.FlushCompression compression)
    {
        conf.flush_compression = compression;
    }

   /**
    * Maximum number of buffers in the compression pool. The default value is 3, it should not be set lower than that
    * (one segment in compression, one written to, one in reserve); delays in compression may cause the log to use
    * more, depending on how soon the sync policy stops all writing threads.
    */
    public static int getCommitLogMaxCompressionBuffersInPool()
    {
        return conf.commitlog_max_compression_buffers_in_pool;
    }

    public static void setCommitLogMaxCompressionBuffersPerPool(int buffers)
    {
        conf.commitlog_max_compression_buffers_in_pool = buffers;
    }

    public static int getMaxMutationSize()
    {
        return (int) ByteUnit.KIBI_BYTES.toBytes(conf.max_mutation_size_in_kb);
    }

    public static int getTombstoneWarnThreshold()
    {
        return conf.tombstone_warn_threshold;
    }

    public static void setTombstoneWarnThreshold(int threshold)
    {
        conf.tombstone_warn_threshold = threshold;
    }

    public static int getTombstoneFailureThreshold()
    {
        return conf.tombstone_failure_threshold;
    }

    public static void setTombstoneFailureThreshold(int threshold)
    {
        conf.tombstone_failure_threshold = threshold;
    }

    public static int getCachedReplicaRowsWarnThreshold()
    {
        return conf.replica_filtering_protection.cached_rows_warn_threshold;
    }

    public static void setCachedReplicaRowsWarnThreshold(int threshold)
    {
        conf.replica_filtering_protection.cached_rows_warn_threshold = threshold;
    }

    public static int getCachedReplicaRowsFailThreshold()
    {
        return conf.replica_filtering_protection.cached_rows_fail_threshold;
    }

    public static void setCachedReplicaRowsFailThreshold(int threshold)
    {
        conf.replica_filtering_protection.cached_rows_fail_threshold = threshold;
    }

    /**
     * size of commitlog segments to allocate
     */
    public static int getCommitLogSegmentSize()
    {
        return (int) ByteUnit.MEBI_BYTES.toBytes(conf.commitlog_segment_size_in_mb);
    }

    /**
     * Update commitlog_segment_size_in_mb in the tests.
     * {@link CommitLogSegmentManagerCDC} uses the CommitLogSegmentSize to estimate the file size on allocation.
     * It is important to keep the value unchanged for the estimation to be correct.
     * @param sizeMegabytes
     */
    @VisibleForTesting /* Only for testing */
    public static void setCommitLogSegmentSize(int sizeMegabytes)
    {
        conf.commitlog_segment_size_in_mb = sizeMegabytes;
    }

    public static String getSavedCachesLocation()
    {
        return conf.saved_caches_directory;
    }

    public static Set<InetAddressAndPort> getSeeds()
    {
        return ImmutableSet.<InetAddressAndPort>builder().addAll(seedProvider.getSeeds()).build();
    }

    public static SeedProvider getSeedProvider()
    {
        return seedProvider;
    }

    public static void setSeedProvider(SeedProvider newSeedProvider)
    {
        seedProvider = newSeedProvider;
    }

    public static InetAddress getListenAddress()
    {
        return listenAddress;
    }

    public static void setListenAddress(InetAddress newlistenAddress)
    {
        listenAddress = newlistenAddress;
    }

    public static InetAddress getBroadcastAddress()
    {
        return broadcastAddress;
    }

    public static boolean shouldListenOnBroadcastAddress()
    {
        return conf.listen_on_broadcast_address;
    }

    public static void setShouldListenOnBroadcastAddress(boolean shouldListenOnBroadcastAddress)
    {
        conf.listen_on_broadcast_address = shouldListenOnBroadcastAddress;
    }

    public static void setListenOnBroadcastAddress(boolean listen_on_broadcast_address)
    {
        conf.listen_on_broadcast_address = listen_on_broadcast_address;
    }

    public static IInternodeAuthenticator getInternodeAuthenticator()
    {
        return internodeAuthenticator;
    }

    public static void setInternodeAuthenticator(IInternodeAuthenticator internodeAuthenticator)
    {
        Preconditions.checkNotNull(internodeAuthenticator);
        DatabaseDescriptor.internodeAuthenticator = internodeAuthenticator;
    }

    public static void setBroadcastAddress(InetAddress broadcastAdd)
    {
        broadcastAddress = broadcastAdd;
    }

    /**
     * This is the address used to bind for the native protocol to communicate with clients. Most usages in the code
     * refer to it as native address although some places still call it RPC address. It's not thrift RPC anymore
     * so native is more appropriate. The address alone is not enough to uniquely identify this instance because
     * multiple instances might use the same interface with different ports.
     */
    public static InetAddress getRpcAddress()
    {
        return rpcAddress;
    }

    public static void setBroadcastRpcAddress(InetAddress broadcastRPCAddr)
    {
        broadcastRpcAddress = broadcastRPCAddr;
    }

    /**
     * This is the address used to reach this instance for the native protocol to communicate with clients. Most usages in the code
     * refer to it as native address although some places still call it RPC address. It's not thrift RPC anymore
     * so native is more appropriate. The address alone is not enough to uniquely identify this instance because
     * multiple instances might use the same interface with different ports.
     *
     * May be null, please use {@link FBUtilities#getBroadcastNativeAddressAndPort()} instead.
     */
    public static InetAddress getBroadcastRpcAddress()
    {
        return broadcastRpcAddress;
    }

    public static boolean getRpcKeepAlive()
    {
        return conf.rpc_keepalive;
    }

    public static int getInternodeSocketSendBufferSizeInBytes()
    {
        return conf.internode_socket_send_buffer_size_in_bytes;
    }

    public static int getInternodeSocketReceiveBufferSizeInBytes()
    {
        return conf.internode_socket_receive_buffer_size_in_bytes;
    }

    public static int getInternodeApplicationSendQueueCapacityInBytes()
    {
        return conf.internode_application_send_queue_capacity_in_bytes;
    }

    public static int getInternodeApplicationSendQueueReserveEndpointCapacityInBytes()
    {
        return conf.internode_application_send_queue_reserve_endpoint_capacity_in_bytes;
    }

    public static int getInternodeApplicationSendQueueReserveGlobalCapacityInBytes()
    {
        return conf.internode_application_send_queue_reserve_global_capacity_in_bytes;
    }

    public static int getInternodeApplicationReceiveQueueCapacityInBytes()
    {
        return conf.internode_application_receive_queue_capacity_in_bytes;
    }

    public static int getInternodeApplicationReceiveQueueReserveEndpointCapacityInBytes()
    {
        return conf.internode_application_receive_queue_reserve_endpoint_capacity_in_bytes;
    }

    public static int getInternodeApplicationReceiveQueueReserveGlobalCapacityInBytes()
    {
        return conf.internode_application_receive_queue_reserve_global_capacity_in_bytes;
    }

    public static int getInternodeTcpConnectTimeoutInMS()
    {
        return conf.internode_tcp_connect_timeout_in_ms;
    }

    public static void setInternodeTcpConnectTimeoutInMS(int value)
    {
        conf.internode_tcp_connect_timeout_in_ms = value;
    }

    public static int getInternodeTcpUserTimeoutInMS()
    {
        return conf.internode_tcp_user_timeout_in_ms;
    }

    public static void setInternodeTcpUserTimeoutInMS(int value)
    {
        conf.internode_tcp_user_timeout_in_ms = value;
    }

    public static int getInternodeStreamingTcpUserTimeoutInMS()
    {
        return conf.internode_streaming_tcp_user_timeout_in_ms;
    }

    public static void setInternodeStreamingTcpUserTimeoutInMS(int value)
    {
        conf.internode_streaming_tcp_user_timeout_in_ms = value;
    }

    public static int getInternodeMaxMessageSizeInBytes()
    {
        return conf.internode_max_message_size_in_bytes;
    }

    @VisibleForTesting
    public static void setInternodeMaxMessageSizeInBytes(int value)
    {
        conf.internode_max_message_size_in_bytes = value;
    }

    public static boolean startNativeTransport()
    {
        return conf.start_native_transport;
    }

    /**
     *  This is the port used with RPC address for the native protocol to communicate with clients. Now that thrift RPC
     *  is no longer in use there is no RPC port.
     */
    public static int getNativeTransportPort()
    {
        return Integer.parseInt(System.getProperty(Config.PROPERTY_PREFIX + "native_transport_port", Integer.toString(conf.native_transport_port)));
    }

    @VisibleForTesting
    public static void setNativeTransportPort(int port)
    {
        conf.native_transport_port = port;
    }

    public static int getNativeTransportPortSSL()
    {
        return conf.native_transport_port_ssl == null ? getNativeTransportPort() : conf.native_transport_port_ssl;
    }

    @VisibleForTesting
    public static void setNativeTransportPortSSL(Integer port)
    {
        conf.native_transport_port_ssl = port;
    }

    public static int getNativeTransportMaxThreads()
    {
        return conf.native_transport_max_threads;
    }

    public static void setNativeTransportMaxThreads(int max_threads)
    {
        conf.native_transport_max_threads = max_threads;
    }

    public static Integer getNativeTransportMaxAuthThreads()
    {
        return conf.native_transport_max_auth_threads;
    }

    /**
     * this will not resize the thread pool but would change the value for purpose of doing auth requests in normal
     * request pool
     * see {@link org.apache.cassandra.transport.Dispatcher#dispatch(Channel, Message.Request, Dispatcher.FlushItemConverter)})
     */
    public static void setNativeTransportMaxAuthThreads(int threads)
    {
        conf.native_transport_max_auth_threads = threads;
    }

    public static int getNativeTransportMaxFrameSize()
    {
        return (int) ByteUnit.MEBI_BYTES.toBytes(conf.native_transport_max_frame_size_in_mb);
    }

    public static void setNativeTransportMaxFrameSize(int bytes)
    {
        conf.native_transport_max_frame_size_in_mb = (int) ByteUnit.MEBI_BYTES.fromBytes(bytes);
    }

    public static long getNativeTransportMaxConcurrentConnections()
    {
        return conf.native_transport_max_concurrent_connections;
    }

    public static void setNativeTransportMaxConcurrentConnections(long nativeTransportMaxConcurrentConnections)
    {
        conf.native_transport_max_concurrent_connections = nativeTransportMaxConcurrentConnections;
    }

    public static long getNativeTransportMaxConcurrentConnectionsPerIp()
    {
        return conf.native_transport_max_concurrent_connections_per_ip;
    }

    public static void setNativeTransportMaxConcurrentConnectionsPerIp(long native_transport_max_concurrent_connections_per_ip)
    {
        conf.native_transport_max_concurrent_connections_per_ip = native_transport_max_concurrent_connections_per_ip;
    }

    public static boolean useNativeTransportLegacyFlusher()
    {
        return conf.native_transport_flush_in_batches_legacy;
    }

    public static boolean getNativeTransportAllowOlderProtocols()
    {
        return conf.native_transport_allow_older_protocols;
    }

    public static void setNativeTransportAllowOlderProtocols(boolean isEnabled)
    {
        conf.native_transport_allow_older_protocols = isEnabled;
    }

    public static double getCommitLogSyncGroupWindow()
    {
        return conf.commitlog_sync_group_window_in_ms;
    }

    public static void setCommitLogSyncGroupWindow(double windowMillis)
    {
        conf.commitlog_sync_group_window_in_ms = windowMillis;
    }

    public static int getNativeTransportReceiveQueueCapacityInBytes()
    {
        return conf.native_transport_receive_queue_capacity_in_bytes;
    }

    public static void setNativeTransportReceiveQueueCapacityInBytes(int queueSize)
    {
        conf.native_transport_receive_queue_capacity_in_bytes = queueSize;
    }

    public static long getNativeTransportMaxConcurrentRequestsInBytesPerIp()
    {
        return conf.native_transport_max_concurrent_requests_in_bytes_per_ip;
    }

    public static Config.PaxosVariant getPaxosVariant()
    {
        return conf.paxos_variant;
    }

    public static void setPaxosVariant(Config.PaxosVariant variant)
    {
        conf.paxos_variant = variant;
    }

    public static void setNativeTransportMaxConcurrentRequestsInBytesPerIp(long maxConcurrentRequestsInBytes)
    {
        conf.native_transport_max_concurrent_requests_in_bytes_per_ip = maxConcurrentRequestsInBytes;
    }

    public static long getNativeTransportMaxConcurrentRequestsInBytes()
    {
        return conf.native_transport_max_concurrent_requests_in_bytes;
    }

    public static void setNativeTransportMaxConcurrentRequestsInBytes(long maxConcurrentRequestsInBytes)
    {
        conf.native_transport_max_concurrent_requests_in_bytes = maxConcurrentRequestsInBytes;
    }

    public static int getNativeTransportMaxRequestsPerSecond()
    {
        return conf.native_transport_max_requests_per_second;
    }

    public static void setNativeTransportMaxRequestsPerSecond(int perSecond)
    {
        Preconditions.checkArgument(perSecond > 0, "native_transport_max_requests_per_second must be greater than zero");
        conf.native_transport_max_requests_per_second = perSecond;
    }

    public static void setNativeTransportRateLimitingEnabled(boolean enabled)
    {
        logger.info("native_transport_rate_limiting_enabled set to {}", enabled);
        conf.native_transport_rate_limiting_enabled = enabled;
    }

    public static boolean getNativeTransportRateLimitingEnabled()
    {
        return conf.native_transport_rate_limiting_enabled;
    }

    public static int getCommitLogSyncPeriod()
    {
        return conf.commitlog_sync_period_in_ms;
    }

    public static long getPeriodicCommitLogSyncBlock()
    {
        Integer blockMillis = conf.periodic_commitlog_sync_lag_block_in_ms;
        return blockMillis == null
               ? (long)(getCommitLogSyncPeriod() * 1.5)
               : blockMillis;
    }

    public static void setCommitLogSyncPeriod(int periodMillis)
    {
        conf.commitlog_sync_period_in_ms = periodMillis;
    }

    public static Config.CommitLogSync getCommitLogSync()
    {
        return conf.commitlog_sync;
    }

    public static void setCommitLogSync(CommitLogSync sync)
    {
        conf.commitlog_sync = sync;
    }

    public static Config.DiskAccessMode getDiskAccessMode()
    {
        return conf.disk_access_mode;
    }

    // Do not use outside unit tests.
    @VisibleForTesting
    public static void setDiskAccessMode(Config.DiskAccessMode mode)
    {
        conf.disk_access_mode = mode;
    }

    public static Config.DiskAccessMode getIndexAccessMode()
    {
        return indexAccessMode;
    }

    // Do not use outside unit tests.
    @VisibleForTesting
    public static void setIndexAccessMode(Config.DiskAccessMode mode)
    {
        indexAccessMode = mode;
    }

    public static void setDiskFailurePolicy(Config.DiskFailurePolicy policy)
    {
        conf.disk_failure_policy = policy;
    }

    public static Config.DiskFailurePolicy getDiskFailurePolicy()
    {
        return conf.disk_failure_policy;
    }

    public static void setCommitFailurePolicy(Config.CommitFailurePolicy policy)
    {
        conf.commit_failure_policy = policy;
    }

    public static Config.CommitFailurePolicy getCommitFailurePolicy()
    {
        return conf.commit_failure_policy;
    }

    public static boolean isSnapshotBeforeCompaction()
    {
        return conf.snapshot_before_compaction;
    }

    public static boolean isAutoSnapshot()
    {
        return conf.auto_snapshot;
    }

    @VisibleForTesting
    public static void setAutoSnapshot(boolean autoSnapshot)
    {
        conf.auto_snapshot = autoSnapshot;
    }
    @VisibleForTesting
    public static boolean getAutoSnapshot()
    {
        return conf.auto_snapshot;
    }

    public static long getSnapshotLinksPerSecond()
    {
        return conf.snapshot_links_per_second == 0 ? Long.MAX_VALUE : conf.snapshot_links_per_second;
    }

    public static void setSnapshotLinksPerSecond(long throttle)
    {
        if (throttle < 0)
            throw new IllegalArgumentException("Invalid throttle for snapshot_links_per_second: must be positive");

        conf.snapshot_links_per_second = throttle;
    }

    public static RateLimiter getSnapshotRateLimiter()
    {
        return RateLimiter.create(getSnapshotLinksPerSecond());
    }

    public static boolean isAutoBootstrap()
    {
        return Boolean.parseBoolean(System.getProperty(Config.PROPERTY_PREFIX + "auto_bootstrap", Boolean.toString(conf.auto_bootstrap)));
    }

    public static void setHintedHandoffEnabled(boolean hintedHandoffEnabled)
    {
        conf.hinted_handoff_enabled = hintedHandoffEnabled;
    }

    public static boolean hintedHandoffEnabled()
    {
        return conf.hinted_handoff_enabled;
    }

    public static Set<String> hintedHandoffDisabledDCs()
    {
        return conf.hinted_handoff_disabled_datacenters;
    }

    public static boolean useDeterministicTableID()
    {
        return conf != null && conf.use_deterministic_table_id;
    }

    public static void useDeterministicTableID(boolean value)
    {
        conf.use_deterministic_table_id = value;
    }

    public static void enableHintsForDC(String dc)
    {
        conf.hinted_handoff_disabled_datacenters.remove(dc);
    }

    public static void disableHintsForDC(String dc)
    {
        conf.hinted_handoff_disabled_datacenters.add(dc);
    }

    public static void setMaxHintWindow(int ms)
    {
        conf.max_hint_window_in_ms = ms;
    }

    public static int getMaxHintWindow()
    {
        return conf.max_hint_window_in_ms;
    }

    public static File getHintsDirectory()
    {
        return new File(conf.hints_directory);
    }

    public static boolean hintWindowPersistentEnabled()
    {
        return conf.hint_window_persistent_enabled;
    }

    public static File getSerializedCachePath(CacheType cacheType, String version, String extension)
    {
        String name = cacheType.toString()
                + (version == null ? "" : '-' + version + '.' + extension);
        return new File(conf.saved_caches_directory, name);
    }

    public static int getDynamicUpdateInterval()
    {
        return conf.dynamic_snitch_update_interval_in_ms;
    }
    public static void setDynamicUpdateInterval(int dynamicUpdateInterval)
    {
        conf.dynamic_snitch_update_interval_in_ms = dynamicUpdateInterval;
    }

    public static int getDynamicResetInterval()
    {
        return conf.dynamic_snitch_reset_interval_in_ms;
    }
    public static void setDynamicResetInterval(int dynamicResetInterval)
    {
        conf.dynamic_snitch_reset_interval_in_ms = dynamicResetInterval;
    }

    public static double getDynamicBadnessThreshold()
    {
        return conf.dynamic_snitch_badness_threshold;
    }

    public static void setDynamicBadnessThreshold(double dynamicBadnessThreshold)
    {
        conf.dynamic_snitch_badness_threshold = dynamicBadnessThreshold;
    }

    public static EncryptionOptions.ServerEncryptionOptions getInternodeMessagingEncyptionOptions()
    {
        return conf.server_encryption_options;
    }

    public static void setInternodeMessagingEncyptionOptions(EncryptionOptions.ServerEncryptionOptions encryptionOptions)
    {
        conf.server_encryption_options = encryptionOptions;
    }

    public static EncryptionOptions getNativeProtocolEncryptionOptions()
    {
        return conf.client_encryption_options;
    }

    @VisibleForTesting
    public static void updateNativeProtocolEncryptionOptions(Function<EncryptionOptions, EncryptionOptions> update)
    {
        conf.client_encryption_options = update.apply(conf.client_encryption_options);
    }

    public static int getHintedHandoffThrottleInKB()
    {
        return conf.hinted_handoff_throttle_in_kb;
    }

    public static void setHintedHandoffThrottleInKB(int throttleInKB)
    {
        conf.hinted_handoff_throttle_in_kb = throttleInKB;
    }

    public static int getBatchlogReplayThrottleInKB()
    {
        return conf.batchlog_replay_throttle_in_kb;
    }

    public static void setBatchlogReplayThrottleInKB(int throttleInKB)
    {
        conf.batchlog_replay_throttle_in_kb = throttleInKB;
    }

    public static int getMaxHintsDeliveryThreads()
    {
        return conf.max_hints_delivery_threads;
    }

    public static int getHintsFlushPeriodInMS()
    {
        return conf.hints_flush_period_in_ms;
    }

    public static long getMaxHintsFileSize()
    {
        return  ByteUnit.MEBI_BYTES.toBytes(conf.max_hints_file_size_in_mb);
    }

    public static ParameterizedClass getHintsCompression()
    {
        return conf.hints_compression;
    }

    public static void setHintsCompression(ParameterizedClass parameterizedClass)
    {
        conf.hints_compression = parameterizedClass;
    }

    public static boolean isAutoHintsCleanupEnabled()
    {
        return conf.auto_hints_cleanup_enabled;
    }

    public static void setAutoHintsCleanupEnabled(boolean value)
    {
        conf.auto_hints_cleanup_enabled = value;
    }

    public static boolean isIncrementalBackupsEnabled()
    {
        return conf.incremental_backups;
    }

    public static void setIncrementalBackupsEnabled(boolean value)
    {
        conf.incremental_backups = value;
    }

    public static boolean getFileCacheEnabled()
    {
        return conf.file_cache_enabled;
    }

    public static int getFileCacheSizeInMB()
    {
        if (conf.file_cache_size_in_mb == null)
        {
            // In client mode the value is not set.
            assert DatabaseDescriptor.isClientInitialized();
            return 0;
        }

        return conf.file_cache_size_in_mb;
    }

    public static int getNetworkingCacheSizeInMB()
    {
        if (conf.networking_cache_size_in_mb == null)
        {
            // In client mode the value is not set.
            assert DatabaseDescriptor.isClientInitialized();
            return 0;
        }
        return conf.networking_cache_size_in_mb;
    }

    public static boolean getFileCacheRoundUp()
    {
        if (conf.file_cache_round_up == null)
        {
            // In client mode the value is not set.
            assert DatabaseDescriptor.isClientInitialized();
            return false;
        }

        return conf.file_cache_round_up;
    }

    public static DiskOptimizationStrategy getDiskOptimizationStrategy()
    {
        return diskOptimizationStrategy;
    }

    public static double getDiskOptimizationEstimatePercentile()
    {
        return conf.disk_optimization_estimate_percentile;
    }

    public static long getTotalCommitlogSpaceInMB()
    {
        return conf.commitlog_total_space_in_mb;
    }

    public static boolean shouldMigrateKeycacheOnCompaction()
    {
        return conf.key_cache_migrate_during_compaction;
    }

    public static void setMigrateKeycacheOnCompaction(boolean migrateCacheEntry)
    {
        conf.key_cache_migrate_during_compaction = migrateCacheEntry;
    }

    public static int getSSTablePreemptiveOpenIntervalInMB()
    {
        return FBUtilities.isWindows ? -1 : conf.sstable_preemptive_open_interval_in_mb;
    }
    public static void setSSTablePreemptiveOpenIntervalInMB(int mb)
    {
        conf.sstable_preemptive_open_interval_in_mb = mb;
    }

    public static boolean getTrickleFsync()
    {
        return conf.trickle_fsync;
    }

    public static int getTrickleFsyncIntervalInKb()
    {
        return conf.trickle_fsync_interval_in_kb;
    }

    public static long getKeyCacheSizeInMB()
    {
        return keyCacheSizeInMB;
    }

    public static long getIndexSummaryCapacityInMB()
    {
        return indexSummaryCapacityInMB;
    }

    public static int getKeyCacheSavePeriod()
    {
        return conf.key_cache_save_period;
    }

    public static void setKeyCacheSavePeriod(int keyCacheSavePeriod)
    {
        conf.key_cache_save_period = keyCacheSavePeriod;
    }

    public static int getKeyCacheKeysToSave()
    {
        return conf.key_cache_keys_to_save;
    }

    public static void setKeyCacheKeysToSave(int keyCacheKeysToSave)
    {
        conf.key_cache_keys_to_save = keyCacheKeysToSave;
    }

    public static String getRowCacheClassName()
    {
        return conf.row_cache_class_name;
    }

    public static long getRowCacheSizeInMB()
    {
        return conf.row_cache_size_in_mb;
    }

    @VisibleForTesting
    public static void setRowCacheSizeInMB(long val)
    {
        conf.row_cache_size_in_mb = val;
    }

    public static int getRowCacheSavePeriod()
    {
        return conf.row_cache_save_period;
    }

    public static void setRowCacheSavePeriod(int rowCacheSavePeriod)
    {
        conf.row_cache_save_period = rowCacheSavePeriod;
    }

    public static int getRowCacheKeysToSave()
    {
        return conf.row_cache_keys_to_save;
    }

    public static long getCounterCacheSizeInMB()
    {
        return counterCacheSizeInMB;
    }

    public static void setRowCacheKeysToSave(int rowCacheKeysToSave)
    {
        conf.row_cache_keys_to_save = rowCacheKeysToSave;
    }

    public static int getCounterCacheSavePeriod()
    {
        return conf.counter_cache_save_period;
    }

    public static void setCounterCacheSavePeriod(int counterCacheSavePeriod)
    {
        conf.counter_cache_save_period = counterCacheSavePeriod;
    }

    public static int getCacheLoadTimeout()
    {
        return conf.cache_load_timeout_seconds;
    }

    @VisibleForTesting
    public static void setCacheLoadTimeout(int seconds)
    {
        conf.cache_load_timeout_seconds = seconds;
    }

    public static int getCounterCacheKeysToSave()
    {
        return conf.counter_cache_keys_to_save;
    }

    public static void setCounterCacheKeysToSave(int counterCacheKeysToSave)
    {
        conf.counter_cache_keys_to_save = counterCacheKeysToSave;
    }

    public static int getStreamingKeepAlivePeriod()
    {
        return conf.streaming_keep_alive_period_in_secs;
    }

    public static int getStreamingConnectionsPerHost()
    {
        return conf.streaming_connections_per_host;
    }

    public static boolean streamEntireSSTables()
    {
        return conf.stream_entire_sstables;
    }

    public static boolean getSkipStreamDiskSpaceCheck()
    {
        return conf.skip_stream_disk_space_check;
    }

    public static void setSkipStreamDiskSpaceCheck(boolean value)
    {
        conf.skip_stream_disk_space_check = value;
    }

    public static String getLocalDataCenter()
    {
        return localDC;
    }

    public static Comparator<Replica> getLocalComparator()
    {
        return localComparator;
    }

    public static Config.InternodeCompression internodeCompression()
    {
        return conf.internode_compression;
    }

    public static void setInternodeCompression(Config.InternodeCompression compression)
    {
        conf.internode_compression = compression;
    }

    public static boolean getInterDCTcpNoDelay()
    {
        return conf.inter_dc_tcp_nodelay;
    }

    public static long getMemtableHeapSpaceInMb()
    {
        return conf.memtable_heap_space_in_mb;
    }

    public static long getMemtableOffheapSpaceInMb()
    {
        return conf.memtable_offheap_space_in_mb;
    }

    public static Config.MemtableAllocationType getMemtableAllocationType()
    {
        return conf.memtable_allocation_type;
    }

    public static int getRepairSessionMaxTreeDepth()
    {
        return conf.repair_session_max_tree_depth;
    }

    public static void setRepairSessionMaxTreeDepth(int depth)
    {
        if (depth < 10)
            throw new ConfigurationException("Cannot set repair_session_max_tree_depth to " + depth +
                                             " which is < 10, doing nothing");
        else if (depth > 20)
            logger.warn("repair_session_max_tree_depth of " + depth + " > 20 could lead to excessive memory usage");

        conf.repair_session_max_tree_depth = depth;
    }

    public static int getRepairSessionSpaceInMegabytes()
    {
        return conf.repair_session_space_in_mb;
    }

    public static void setRepairSessionSpaceInMegabytes(int sizeInMegabytes)
    {
        if (sizeInMegabytes < 1)
            throw new ConfigurationException("Cannot set repair_session_space_in_mb to " + sizeInMegabytes +
                                             " < 1 megabyte");
        else if (sizeInMegabytes > (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)))
            logger.warn("A repair_session_space_in_mb of " + conf.repair_session_space_in_mb +
                        " megabytes is likely to cause heap pressure.");

        conf.repair_session_space_in_mb = sizeInMegabytes;
    }

    public static Float getMemtableCleanupThreshold()
    {
        return conf.memtable_cleanup_threshold;
    }

    public static int getIndexSummaryResizeIntervalInMinutes()
    {
        return conf.index_summary_resize_interval_in_minutes;
    }

    public static boolean hasLargeAddressSpace()
    {
        // currently we just check if it's a 64bit arch, but any we only really care if the address space is large
        String datamodel = SUN_ARCH_DATA_MODEL.getString();
        if (datamodel != null)
        {
            switch (datamodel)
            {
                case "64": return true;
                case "32": return false;
            }
        }
        String arch = OS_ARCH.getString();
        return arch.contains("64") || arch.contains("sparcv9");
    }

    public static int getTracetypeRepairTTL()
    {
        return conf.tracetype_repair_ttl;
    }

    public static int getTracetypeQueryTTL()
    {
        return conf.tracetype_query_ttl;
    }

    public static int getWindowsTimerInterval()
    {
        return conf.windows_timer_interval;
    }

    public static long getPreparedStatementsCacheSizeMB()
    {
        return preparedStatementsCacheSizeInMB;
    }

    public static boolean enableUserDefinedFunctions()
    {
        return conf.enable_user_defined_functions;
    }

    public static boolean enableScriptedUserDefinedFunctions()
    {
        return conf.enable_scripted_user_defined_functions;
    }

    public static void enableScriptedUserDefinedFunctions(boolean enableScriptedUserDefinedFunctions)
    {
        conf.enable_scripted_user_defined_functions = enableScriptedUserDefinedFunctions;
    }

    public static boolean enableUserDefinedFunctionsThreads()
    {
        return conf.enable_user_defined_functions_threads;
    }

    public static long getUserDefinedFunctionWarnTimeout()
    {
        return conf.user_defined_function_warn_timeout;
    }

    public static void setUserDefinedFunctionWarnTimeout(long userDefinedFunctionWarnTimeout)
    {
        conf.user_defined_function_warn_timeout = userDefinedFunctionWarnTimeout;
    }

    public static boolean getEnableMaterializedViews()
    {
        // CIE specific system property to override to disable materialized views
        return conf.enable_materialized_views && CassandraRelevantProperties.ALLOW_MATERIALIZEDVIEWS.getBoolean(false);
    }

    public static void setEnableMaterializedViews(boolean enableMaterializedViews)
    {
        conf.enable_materialized_views = enableMaterializedViews;
    }

    public static boolean getEnableSASIIndexes()
    {
        return conf.enable_sasi_indexes;
    }

    public static void setEnableSASIIndexes(boolean enableSASIIndexes)
    {
        conf.enable_sasi_indexes = enableSASIIndexes;
    }

    public static boolean isTransientReplicationEnabled()
    {
        return conf.enable_transient_replication;
    }

    public static void setTransientReplicationEnabledUnsafe(boolean enabled)
    {
        conf.enable_transient_replication = enabled;
    }

    public static boolean enableDropCompactStorage()
    {
        return conf.enable_drop_compact_storage;
    }

    @VisibleForTesting
    public static void setEnableDropCompactStorage(boolean enableDropCompactStorage)
    {
        conf.enable_drop_compact_storage = enableDropCompactStorage;
    }

    public static long getUserDefinedFunctionFailTimeout()
    {
        return conf.user_defined_function_fail_timeout;
    }

    public static void setUserDefinedFunctionFailTimeout(long userDefinedFunctionFailTimeout)
    {
        conf.user_defined_function_fail_timeout = userDefinedFunctionFailTimeout;
    }

    public static Config.UserFunctionTimeoutPolicy getUserFunctionTimeoutPolicy()
    {
        return conf.user_function_timeout_policy;
    }

    public static void setUserFunctionTimeoutPolicy(Config.UserFunctionTimeoutPolicy userFunctionTimeoutPolicy)
    {
        conf.user_function_timeout_policy = userFunctionTimeoutPolicy;
    }

    public static long getGCLogThreshold()
    {
        return conf.gc_log_threshold_in_ms;
    }

    public static EncryptionContext getEncryptionContext()
    {
        return encryptionContext;
    }

    public static long getGCWarnThreshold()
    {
        return conf.gc_warn_threshold_in_ms;
    }

    public static boolean isCDCEnabled()
    {
        return conf.cdc_enabled;
    }

    @VisibleForTesting
    public static void setCDCEnabled(boolean cdc_enabled)
    {
        conf.cdc_enabled = cdc_enabled;
    }

    public static boolean getCDCBlockWrites()
    {
        return conf.cdc_block_writes;
    }

    public static void setCDCBlockWrites(boolean val)
    {
        conf.cdc_block_writes = val;
    }

    public static String getCDCLogLocation()
    {
        return conf.cdc_raw_directory;
    }

    public static int getCDCSpaceInMB()
    {
        return conf.cdc_total_space_in_mb;
    }

    @VisibleForTesting
    public static void setCDCSpaceInMB(int input)
    {
        conf.cdc_total_space_in_mb = input;
    }

    public static int getCDCDiskCheckInterval()
    {
        return conf.cdc_free_space_check_interval_ms;
    }

    @VisibleForTesting
    public static void setEncryptionContext(EncryptionContext ec)
    {
        encryptionContext = ec;
    }

    public static int searchConcurrencyFactor()
    {
        return searchConcurrencyFactor;
    }

    public static boolean isUnsafeSystem()
    {
        return unsafeSystem;
    }

    public static boolean diagnosticEventsEnabled()
    {
        return conf.diagnostic_events_enabled;
    }

    public static void setDiagnosticEventsEnabled(boolean enabled)
    {
        conf.diagnostic_events_enabled = enabled;
    }

    public static ConsistencyLevel getIdealConsistencyLevel()
    {
        return conf.ideal_consistency_level;
    }

    public static void setIdealConsistencyLevel(ConsistencyLevel cl)
    {
        conf.ideal_consistency_level = cl;
    }

    public static int getRepairCommandPoolSize()
    {
        return conf.repair_command_pool_size;
    }

    public static Config.RepairCommandPoolFullStrategy getRepairCommandPoolFullStrategy()
    {
        return conf.repair_command_pool_full_strategy;
    }

    public static FullQueryLoggerOptions getFullQueryLogOptions()
    {
        if (conf.full_query_logging_options.log_dir.isEmpty() && conf.full_query_log_dir != null && !conf.full_query_log_dir.isEmpty())
            return new FullQueryLoggerOptions(conf.full_query_log_dir);
        else
            return conf.full_query_logging_options;
    }

    public static boolean getBlockForPeersInRemoteDatacenters()
    {
        return conf.block_for_peers_in_remote_dcs;
    }

    public static int getBlockForPeersTimeoutInSeconds()
    {
        return conf.block_for_peers_timeout_in_secs;
    }

    public static boolean automaticSSTableUpgrade()
    {
        return conf.automatic_sstable_upgrade;
    }

    public static void setAutomaticSSTableUpgradeEnabled(boolean enabled)
    {
        if (conf.automatic_sstable_upgrade != enabled)
            logger.debug("Changing automatic_sstable_upgrade to {}", enabled);
        conf.automatic_sstable_upgrade = enabled;
    }

    public static int maxConcurrentAutoUpgradeTasks()
    {
        return conf.max_concurrent_automatic_sstable_upgrades;
    }

    public static void setMaxConcurrentAutoUpgradeTasks(int value)
    {
        if (conf.max_concurrent_automatic_sstable_upgrades != value)
            logger.debug("Changing max_concurrent_automatic_sstable_upgrades to {}", value);
        validateMaxConcurrentAutoUpgradeTasksConf(value);
        conf.max_concurrent_automatic_sstable_upgrades = value;
    }

    private static void validateMaxConcurrentAutoUpgradeTasksConf(int value)
    {
        if (value < 0)
            throw new ConfigurationException("max_concurrent_automatic_sstable_upgrades can't be negative");
        if (value > getConcurrentCompactors())
            logger.warn("max_concurrent_automatic_sstable_upgrades ({}) is larger than concurrent_compactors ({})", value, getConcurrentCompactors());
    }
    
    public static AuditLogOptions getAuditLoggingOptions()
    {
        return conf.audit_logging_options;
    }

    public static void setAuditLoggingOptions(AuditLogOptions auditLoggingOptions)
    {
        conf.audit_logging_options = new AuditLogOptions.Builder(auditLoggingOptions).build();
    }

    public static Config.CorruptedTombstoneStrategy getCorruptedTombstoneStrategy()
    {
        return conf.corrupted_tombstone_strategy;
    }

    public static void setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy strategy)
    {
        conf.corrupted_tombstone_strategy = strategy;
    }

    public static boolean getRepairedDataTrackingForRangeReadsEnabled()
    {
        return conf.repaired_data_tracking_for_range_reads_enabled;
    }

    public static void setRepairedDataTrackingForRangeReadsEnabled(boolean enabled)
    {
        conf.repaired_data_tracking_for_range_reads_enabled = enabled;
    }

    public static boolean getRepairedDataTrackingForPartitionReadsEnabled()
    {
        return conf.repaired_data_tracking_for_partition_reads_enabled;
    }

    public static void setRepairedDataTrackingForPartitionReadsEnabled(boolean enabled)
    {
        conf.repaired_data_tracking_for_partition_reads_enabled = enabled;
    }

    public static boolean snapshotOnRepairedDataMismatch()
    {
        return conf.snapshot_on_repaired_data_mismatch;
    }

    public static void setSnapshotOnRepairedDataMismatch(boolean enabled)
    {
        conf.snapshot_on_repaired_data_mismatch = enabled;
    }

    public static boolean snapshotOnDuplicateRowDetection()
    {
        return conf.snapshot_on_duplicate_row_detection;
    }

    public static void setSnapshotOnDuplicateRowDetection(boolean enabled)
    {
        conf.snapshot_on_duplicate_row_detection = enabled;
    }

    public static boolean reportUnconfirmedRepairedDataMismatches()
    {
        return conf.report_unconfirmed_repaired_data_mismatches;
    }

    public static void reportUnconfirmedRepairedDataMismatches(boolean enabled)
    {
        conf.report_unconfirmed_repaired_data_mismatches = enabled;
    }

    public static boolean strictRuntimeChecks()
    {
        return strictRuntimeChecks;
    }

    public static boolean useOffheapMerkleTrees()
    {
        return conf.use_offheap_merkle_trees;
    }

    public static void useOffheapMerkleTrees(boolean value)
    {
        logger.info("Setting use_offheap_merkle_trees to {}", value);
        conf.use_offheap_merkle_trees = value;
    }

    public static Function<CommitLog, AbstractCommitLogSegmentManager> getCommitLogSegmentMgrProvider()
    {
        return commitLogSegmentMgrProvider;
    }

    public static void setCommitLogSegmentMgrProvider(Function<CommitLog, AbstractCommitLogSegmentManager> provider)
    {
        commitLogSegmentMgrProvider = provider;
    }

    /**
     * Class that primarily tracks overflow thresholds during conversions
     */
    private enum ByteUnit {
        KIBI_BYTES(2048 * 1024, 1024),
        MEBI_BYTES(2048, 1024 * 1024);

        private final int overflowThreshold;
        private final int multiplier;

        ByteUnit(int t, int m)
        {
            this.overflowThreshold = t;
            this.multiplier = m;
        }

        public int overflowThreshold()
        {
            return overflowThreshold;
        }

        public boolean willOverflowInBytes(int val)
        {
            return val >= overflowThreshold;
        }

        public long toBytes(int val)
        {
            return val * multiplier;
        }

        public long fromBytes(int val)
        {
            return val / multiplier;
        }
    }

    /**
     * Ensures passed in configuration value is positive and will not overflow when converted to Bytes
     */
    private static void checkValidForByteConversion(int val, final String name, final ByteUnit unit)
    {
        if (val < 0 || unit.willOverflowInBytes(val))
            throw new ConfigurationException(String.format("%s must be positive value < %d, but was %d",
                                                           name, unit.overflowThreshold(), val), false);
    }

    public static int getValidationPreviewPurgeHeadStartInSec()
    {
        int seconds = conf.validation_preview_purge_head_start_in_sec;
        return Math.max(seconds, 0);
    }

    public static boolean checkForDuplicateRowsDuringReads()
    {
        return conf.check_for_duplicate_rows_during_reads;
    }

    public static void setCheckForDuplicateRowsDuringReads(boolean enabled)
    {
        conf.check_for_duplicate_rows_during_reads = enabled;
    }

    public static boolean checkForDuplicateRowsDuringCompaction()
    {
        return conf.check_for_duplicate_rows_during_compaction;
    }

    public static void setCheckForDuplicateRowsDuringCompaction(boolean enabled)
    {
        conf.check_for_duplicate_rows_during_compaction = enabled;
    }

    public static int getRepairPendingCompactionRejectThreshold()
    {
        return conf.reject_repair_compaction_threshold;
    }

    public static void setRepairPendingCompactionRejectThreshold(int value)
    {
        conf.reject_repair_compaction_threshold = value;
    }

    public static int getInitialRangeTombstoneListAllocationSize()
    {
        return conf.initial_range_tombstone_list_allocation_size;
    }

    public static void setInitialRangeTombstoneListAllocationSize(int size)
    {
        conf.initial_range_tombstone_list_allocation_size = size;
    }

    public static double getRangeTombstoneListGrowthFactor()
    {
        return conf.range_tombstone_list_growth_factor;
    }

    public static void setRangeTombstoneListGrowthFactor(double resizeFactor)
    {
        conf.range_tombstone_list_growth_factor = resizeFactor;
    }

    public static boolean getAutocompactionOnStartupEnabled()
    {
        return conf.autocompaction_on_startup_enabled;
    }

    public static void setIgnorePkLivenessForRowCompletion(boolean ignore)
    {
        logger.info("Setting ignore_pk_liveness_for_row_completion to {}", ignore);
        conf.ignore_pk_liveness_for_row_completion = ignore;
    }

    public static boolean ignorePkLivenessForRowCompletion()
    {
        return conf.ignore_pk_liveness_for_row_completion;
    }

    public static boolean autoOptimiseIncRepairStreams()
    {
        return conf.auto_optimise_inc_repair_streams;
    }

    public static void setAutoOptimiseIncRepairStreams(boolean enabled)
    {
        if (enabled != conf.auto_optimise_inc_repair_streams)
            logger.info("Changing auto_optimise_inc_repair_streams from {} to {}", conf.auto_optimise_inc_repair_streams, enabled);
        conf.auto_optimise_inc_repair_streams = enabled;
    }

    public static boolean autoOptimiseFullRepairStreams()
    {
        return conf.auto_optimise_full_repair_streams;
    }

    public static void setAutoOptimiseFullRepairStreams(boolean enabled)
    {
        if (enabled != conf.auto_optimise_full_repair_streams)
            logger.info("Changing auto_optimise_full_repair_streams from {} to {}", conf.auto_optimise_full_repair_streams, enabled);
        conf.auto_optimise_full_repair_streams = enabled;
    }

    public static boolean autoOptimisePreviewRepairStreams()
    {
        return conf.auto_optimise_preview_repair_streams;
    }

    public static void setAutoOptimisePreviewRepairStreams(boolean enabled)
    {
        if (enabled != conf.auto_optimise_preview_repair_streams)
            logger.info("Changing auto_optimise_preview_repair_streams from {} to {}", conf.auto_optimise_preview_repair_streams, enabled);
        conf.auto_optimise_preview_repair_streams = enabled;
    }

    @Deprecated
    public static int tableCountWarnThreshold()
    {
        return conf.table_count_warn_threshold;
    }

    @Deprecated // this warning threshold will be replaced by an equivalent guardrail
    public static void setTableCountWarnThreshold(int value)
    {
        conf.table_count_warn_threshold = value;
    }

    @Deprecated // this warning threshold will be replaced by an equivalent guardrail
    public static int keyspaceCountWarnThreshold()
    {
        return conf.keyspace_count_warn_threshold;
    }

    @Deprecated // this warning threshold will be replaced by an equivalent guardrail
    public static void setKeyspaceCountWarnThreshold(int value)
    {
        conf.keyspace_count_warn_threshold = value;
    }

    @Deprecated // this warning threshold will be replaced by an equivalent guardrail
    public static ConsistencyLevel getAuthWriteConsistencyLevel()
    {
        return ConsistencyLevel.valueOf(conf.auth_write_consistency_level);
    }

    public static ConsistencyLevel getAuthReadConsistencyLevel()
    {
        return ConsistencyLevel.valueOf(conf.auth_read_consistency_level);
    }

    public static void setAuthWriteConsistencyLevel(ConsistencyLevel cl)
    {
        conf.auth_write_consistency_level = cl.toString();
    }

    public static void setAuthReadConsistencyLevel(ConsistencyLevel cl)
    {
        conf.auth_read_consistency_level = cl.toString();
    }

    public static int getConsecutiveMessageErrorsThreshold()
    {
        return conf.consecutive_message_errors_threshold;
    }

    public static void setConsecutiveMessageErrorsThreshold(int value)
    {
        conf.consecutive_message_errors_threshold = value;
    }

    public static boolean getPartitionDenylistEnabled()
    {
        return conf.partition_denylist_enabled;
    }

    public static void setPartitionDenylistEnabled(boolean enabled)
    {
        conf.partition_denylist_enabled = enabled;
    }

    public static boolean getDenylistWritesEnabled()
    {
        return conf.denylist_writes_enabled;
    }

    public static void setDenylistWritesEnabled(boolean enabled)
    {
        conf.denylist_writes_enabled = enabled;
    }

    public static boolean getDenylistReadsEnabled()
    {
        return conf.denylist_reads_enabled;
    }

    public static void setDenylistReadsEnabled(boolean enabled)
    {
        conf.denylist_reads_enabled = enabled;
    }

    public static boolean getDenylistRangeReadsEnabled()
    {
        return conf.denylist_range_reads_enabled;
    }

    public static void setDenylistRangeReadsEnabled(boolean enabled)
    {
        conf.denylist_range_reads_enabled = enabled;
    }

    public static int getDenylistRefreshSeconds()
    {
        return conf.denylist_refresh_seconds;
    }

    public static void setDenylistRefreshSeconds(int seconds)
    {
        if (seconds <= 0)
            throw new IllegalArgumentException("denylist_refresh_seconds must be a positive integer.");
        conf.denylist_refresh_seconds = seconds;
    }

    public static int getDenylistInitialLoadRetrySeconds()
    {
        return conf.denylist_initial_load_retry_seconds;
    }

    public static void setDenylistInitialLoadRetrySeconds(int seconds)
    {
        if (seconds <= 0)
            throw new IllegalArgumentException("denylist_initial_load_retry_seconds must be a positive integer.");
        conf.denylist_initial_load_retry_seconds = seconds;
    }

    public static ConsistencyLevel getDenylistConsistencyLevel()
    {
        return conf.denylist_consistency_level;
    }

    public static void setDenylistConsistencyLevel(ConsistencyLevel cl)
    {
        conf.denylist_consistency_level = cl;
    }

    public static int getDenylistMaxKeysPerTable()
    {
        return conf.denylist_max_keys_per_table;
    }

    public static void setDenylistMaxKeysPerTable(int value)
    {
        if (value <= 0)
            throw new IllegalArgumentException("denylist_max_keys_per_table must be a positive integer.");
        conf.denylist_max_keys_per_table = value;
    }

    public static int getDenylistMaxKeysTotal()
    {
        return conf.denylist_max_keys_total;
    }

    public static void setDenylistMaxKeysTotal(int value)
    {
        if (value <= 0)
            throw new IllegalArgumentException("denylist_max_keys_total must be a positive integer.");
        conf.denylist_max_keys_total = value;
    }

    public static boolean getAuthCacheWarmingEnabled()
    {
        return conf.auth_cache_warming_enabled;
    }

    public static SubnetGroups getClientErrorReportingExclusions()
    {
        return conf.client_error_reporting_exclusions;
    }

    public static SubnetGroups getInternodeErrorReportingExclusions()
    {
        return conf.internode_error_reporting_exclusions;
    }

    public static boolean getTrackWarningsEnabled()
    {
        return conf.track_warnings.enabled;
    }

    public static void setTrackWarningsEnabled(boolean value)
    {
        conf.track_warnings.enabled = value;
    }

    public static long getCoordinatorReadSizeWarnThresholdKB()
    {
        return conf.track_warnings.coordinator_read_size.getWarnThresholdKb();
    }

    public static void setCoordinatorReadSizeWarnThresholdKB(long threshold)
    {
        conf.track_warnings.coordinator_read_size.setWarnThresholdKb(threshold);
    }

    public static long getCoordinatorReadSizeAbortThresholdKB()
    {
        return conf.track_warnings.coordinator_read_size.getAbortThresholdKb();
    }

    public static void setCoordinatorReadSizeAbortThresholdKB(long threshold)
    {
        conf.track_warnings.coordinator_read_size.setAbortThresholdKb(threshold);
    }

    public static long getLocalReadSizeWarnThresholdKb()
    {
        return conf.track_warnings.local_read_size.getWarnThresholdKb();
    }

    public static void setLocalReadSizeWarnThresholdKb(long value)
    {
        conf.track_warnings.local_read_size.setWarnThresholdKb(value);
    }

    public static long getLocalReadSizeAbortThresholdKb()
    {
        return conf.track_warnings.local_read_size.getAbortThresholdKb();
    }

    public static void setLocalReadSizeAbortThresholdKb(long value)
    {
        conf.track_warnings.local_read_size.setAbortThresholdKb(value);
    }

    public static int getRowIndexSizeWarnThresholdKb()
    {
        return conf.track_warnings.row_index_size.getWarnThresholdKb();
    }

    public static void setRowIndexSizeWarnThresholdKb(int value)
    {
        conf.track_warnings.row_index_size.setWarnThresholdKb(value);
    }

    public static int getRowIndexSizeAbortThresholdKb()
    {
        return conf.track_warnings.row_index_size.getAbortThresholdKb();
    }

    public static void setRowIndexSizeAbortThresholdKb(int value)
    {
        conf.track_warnings.row_index_size.setAbortThresholdKb(value);
    }

    public static int getDefaultKeyspaceRF() { return conf.default_keyspace_rf; }

    public static void setDefaultKeyspaceRF(int value) throws ConfigurationException
    {
        if (value < 1)
        {
            throw new ConfigurationException("default_keyspace_rf cannot be less than 1");
        }

        if (value < getMinimumKeyspaceRF())
        {
            throw new ConfigurationException(String.format("default_keyspace_rf to be set (%d) cannot be less than minimum_keyspace_rf (%d)", value, getMinimumKeyspaceRF()));
        }

        conf.default_keyspace_rf = value;
    }

    public static int getMinimumKeyspaceRF() { return conf.minimum_keyspace_rf; }

    public static void setMinimumKeyspaceRF(int value) throws ConfigurationException
    {
        if (value < 0)
        {
            throw new ConfigurationException("minimum_keyspace_rf cannot be negative");
        }

        if (value > getDefaultKeyspaceRF())
        {
            throw new ConfigurationException(String.format("minimum_keyspace_rf to be set (%d) cannot be greater than default_keyspace_rf (%d)", value, getDefaultKeyspaceRF()));
        }

        conf.minimum_keyspace_rf = value;
    }

    public static void setEnableAggressiveGCCompaction(boolean enable) { aggressiveGC = enable; }

    public static boolean getEnableAggressiveGCCompaction() { return aggressiveGC; }

    public static boolean getAlterTableEnabled()
    {
        return conf.alter_table_enabled;
    }

    public static void setAlterTableEnabled(boolean alterTableEnabled)
    {
        conf.alter_table_enabled = alterTableEnabled;
    }

    public static void setChristmasPatchEnabled(boolean enabled)
    {
        conf.enable_christmas_patch = enabled;
    }

    public static boolean enableChristmasPatch()
    {
        return conf.enable_christmas_patch;
    }

    public static boolean enableShadowChristmasPatch()
    {
        //If christmas patch is enabled, shadow is also enabled.
        return conf.enable_christmas_patch || conf.enable_shadow_christmas_patch;
    }

    public static void setChristmasPatchEnabled()
    {
        conf.enable_christmas_patch = true;
    }

    public static void setChristmasPatchDisabled()
    {
        conf.enable_christmas_patch = false;
    }

    public static void setIncrementalUpdatesLastRepaired(boolean enabled)
    {
        conf.incremental_updates_last_repaired = enabled;
    }

    public static boolean getIncrementalUpdatesLastRepaired()
    {
        return conf.incremental_updates_last_repaired;
    }

    public static boolean getCompactBiggestSTCSBucketInL0()
    {
        return conf.compact_biggest_stcs_bucket_l0;
    }

    public static boolean topPartitionsEnabled()
    {
        return conf.top_partitions_enabled;
    }

    public static int getMaxTopSizePartitionCount()
    {
        return conf.max_top_size_partition_count;
    }

    public static void setMaxTopSizePartitionCount(int value)
    {
        conf.max_top_size_partition_count = value;
    }

    public static int getMaxTopTombstonePartitionCount()
    {
        return conf.max_top_tombstone_partition_count;
    }

    public static void setMaxTopTombstonePartitionCount(int value)
    {
        conf.max_top_tombstone_partition_count = value;
    }

    public static long getMinTrackedPartitionSize()
    {
        return conf.min_tracked_partition_size_bytes;
    }

    public static void setMinTrackedPartitionSize(long value)
    {
        conf.min_tracked_partition_size_bytes = value;
    }

    public static long getMinTrackedPartitionTombstoneCount()
    {
        return conf.min_tracked_partition_tombstone_count;
    }

    public static void setMinTrackedPartitionTombstoneCount(long value)
    {
        conf.min_tracked_partition_tombstone_count = value;
    }

    public static void setCompactBiggestSTCSBucketInL0(boolean value)
    {
        if (value != conf.compact_biggest_stcs_bucket_l0)
        {
            logger.info("Setting compact_biggest_stcs_bucket_in_l0 to {}", value);
            conf.compact_biggest_stcs_bucket_l0 = value;
        }
    }

    public static Set<String> getCustomFunctions()
    {
        return conf.custom_functions;
    }

    public static boolean disableIncrementalRepair()
    {
        return conf.disable_incremental_repair;
    }

    public static boolean getEnableKeyspaceQuotas()
    {
        return conf.enable_keyspace_quotas;
    }

    public static void setKeyspaceQuotasEnabled(boolean enabled)
    {
        conf.enable_keyspace_quotas = enabled;
    }

    public static long getDefaultKeyspaceQuotaBytes()
    {
        return conf.default_keyspace_quota_bytes;
    }

    public static void setDefaultKeyspaceQuotaBytes(long quotaInBytes)
    {
        conf.default_keyspace_quota_bytes = quotaInBytes;
    }

    public static int getKeyspaceQuotaRefreshTimeInSec()
    {
        return conf.keyspace_quota_refresh_time_in_sec;
    }
    public static void setKeyspaceQuotaRefreshTimeInSec(int refreshTime)
    {
        conf.keyspace_quota_refresh_time_in_sec = refreshTime;
    }

    public static int getMemtableClockShift()
    {
        return conf.memtable_clock_shift;
    }

    public static long getMemtableExcessWasteBytes()
    {
        return conf.memtable_excess_waste_bytes;
    }

    public static boolean getLogOutOfTokenRangeRequests()
    {
        return conf.log_out_of_token_range_requests;
    }

    public static void setLogOutOfTokenRangeRequests(boolean enabled)
    {
        conf.log_out_of_token_range_requests = enabled;
    }

    public static boolean getRejectOutOfTokenRangeRequests()
    {
        return conf.reject_out_of_token_range_requests;
    }

    public static void setRejectOutOfTokenRangeRequests(boolean enabled)
    {
        conf.reject_out_of_token_range_requests = enabled;
    }

    public static boolean getEnableScheduledCompactions()
    {
        return conf.enable_scheduled_compactions;
    }

    public static void setEnableScheduledCompactions(boolean enable)
    {
        conf.enable_scheduled_compactions = enable;
    }

    public static int getScheduledCompactionRangeSplits()
    {
        return conf.scheduled_compaction_range_splits;
    }

    public static void setScheduledCompactionRangeSplits(int val)
    {
        if (val <= 0)
            throw new RuntimeException("Range splits must be > 0");
        conf.scheduled_compaction_range_splits = val;
    }

    public static long getScheduledCompactionCycleTimeSeconds()
    {
        return scheduledCompactionCycleTimeSeconds;
    }

    public static boolean getSkipSingleSSTableScheduledCompactions()
    {
        return conf.skip_single_sstable_scheduled_compactions;
    }

    public static void setSkipSingleSSTableScheduledCompactions(boolean val)
    {
        conf.skip_single_sstable_scheduled_compactions = val;
    }

    public static void setScheduledCompactionCycleTime(String time)
    {
        try
        {
            scheduledCompactionCycleTimeSeconds = parseScheduledCycleTimeSeconds(time);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException("Could not set "+time+" as scheduled compaction cycle time");
        }
    }

    public static long getMaxScheduledCompactionSSTableSizeBytes()
    {
        return conf.max_scheduled_compaction_sstable_size_bytes;
    }

    public static void setMaxScheduledCompactionSSTableSizeBytes(long size)
    {
        if (size <= 0)
            throw new RuntimeException("Max sstable size must be > 0");
        conf.max_scheduled_compaction_sstable_size_bytes = size;
    }

    public static int getMaxScheduledCompactionSSTableCount()
    {
        return conf.max_scheduled_compaction_sstable_count;
    }

    public static void setMaxScheduledCompactionSSTableCount(int count)
    {
        if (count <= 1)
            throw new RuntimeException("Max sstable count must be > 1");
        conf.max_scheduled_compaction_sstable_count = count;
    }

    private static long parseScheduledCycleTimeSeconds(String rawValue) throws ConfigurationException
    {
        String trimmed = rawValue.trim().toLowerCase();
        char unitCharacter = trimmed.charAt(trimmed.length() - 1);
        int value;
        try
        {
            value = Integer.valueOf(trimmed.substring(0, trimmed.length() - 1));
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(rawValue + " is invalid configuration for scheduled_compaction_cycle_time");
        }
        TimeUnit timeUnit;
        switch (unitCharacter)
        {
            case 's':
                timeUnit = TimeUnit.SECONDS;
                break;
            case 'm':
                timeUnit = TimeUnit.MINUTES;
                break;
            case 'h':
                timeUnit = TimeUnit.HOURS;
                break;
            case 'd':
                timeUnit = TimeUnit.DAYS;
                break;
            default:
                throw new ConfigurationException("Could only supported time units are: s, h, d, got: "+unitCharacter);
        }
        return timeUnit.toSeconds(value);
    }

    public static long getRepairHistorySyncTimeoutSeconds()
    {
        return repairHistorySyncTimeoutSeconds;
    }

    public static boolean enableSecondaryIndex()
    {
        // see rdar://56795580 (Disable creating 2i by default but add a config to allow)
        return conf.enable_secondary_index;
    }

    public static void setEnableSecondaryIndex(boolean value)
    {
        logger.info("Setting enable_secondary_index to {}", value);
        conf.enable_secondary_index = value;
    }

    public static boolean isSchemaDropCheckDisabled()
    {
        return conf.disable_schema_drop_check;
    }

    public static void setIsSchemaDropCheckDisabled(boolean value)
    {
        conf.disable_schema_drop_check = value;
    }

    public static void setAllowUnsafeAggressiveSSTableExpiration(boolean allow)
    {
        logger.info("Setting allow_unsafe_aggressive_sstable_expiration to {}", allow);
        conf.allow_unsafe_aggressive_sstable_expiration = allow;
    }

    public static boolean allowUnsafeAggressiveSSTableExpiration()
    {
        return conf.allow_unsafe_aggressive_sstable_expiration;
    }
    
    public static boolean getDumpHeapOnUncaughtException()
    {
        return conf.dump_heap_on_uncaught_exception;
    }

    public static void setDumpHeapOnUncaughtException(boolean enabled)
    {
        logger.info("Setting dump_heap_on_uncaught_exception to {}", enabled);
        conf.dump_heap_on_uncaught_exception = enabled;
    }

    public static boolean allowCompactStorage()
    {
        return conf.allow_compact_storage;
    }

    public static void setAllowCompactStorage(boolean allowCompactStorage)
    {
        logger.info("Setting allow_compact_storage = " + allowCompactStorage);
        conf.allow_compact_storage = allowCompactStorage;
    }

    public static long getBiggestBucketMaxSizeBytes()
    {
        return conf.biggest_bucket_max_size_bytes;
    }

    public static int getBiggestBucketMaxSSTableCount()
    {
        return conf.biggest_bucket_max_sstable_count;
    }

    public static void setBiggestBucketMaxSizeBytes(long maxSizeBytes)
    {
        if (maxSizeBytes <= 0)
            throw new ConfigurationException("Invalid biggest_bucket_max_size_bytes: "+maxSizeBytes);
        if (maxSizeBytes != conf.biggest_bucket_max_size_bytes)
        {
            logger.info("Setting biggest_bucket_max_size_bytes to {}", maxSizeBytes);
            conf.biggest_bucket_max_size_bytes = maxSizeBytes;
        }
    }

    public static void setBiggestBucketMaxSSTableCount(int maxSSTableCount)
    {
        if (maxSSTableCount <= 0)
            throw new ConfigurationException("Invalid biggest_bucket_max_sstable_count " + maxSSTableCount);
        if (maxSSTableCount != conf.biggest_bucket_max_sstable_count)
        {
            logger.info("Setting biggest_bucket_max_sstable_count to {}", maxSSTableCount);
            conf.biggest_bucket_max_sstable_count = maxSSTableCount;
        }
    }

    public static boolean shouldGenerateSSTableDigestComponents()
    {
        return conf.generate_sstable_digest_components;
    }

    public static void setGenerateSSTableDigestComponents(boolean shouldGenerateDigests)
    {
        conf.generate_sstable_digest_components = shouldGenerateDigests;
    }

    public static void setAllowNonSuperUserSelectSaltedHash(boolean allow)
    {
        conf.allow_nonsuperuser_select_salted_hash = allow;
    }

    public static boolean getAllowNonSuperUserSelectSaltedHash()
    {
        return conf.allow_nonsuperuser_select_salted_hash;
    }
}
