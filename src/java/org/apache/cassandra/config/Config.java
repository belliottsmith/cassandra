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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;

/**
 * A class that contains configuration properties for the cassandra node it runs within.
 *
 * Properties declared as volatile can be mutated via JMX.
 */
public class Config
{
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    public static Set<String> splitCommaDelimited(String src)
    {
        if (src == null)
            return ImmutableSet.of();
        String[] split = src.split(",\\s*");
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String s : split)
        {
            s = s.trim();
            if (!s.isEmpty())
                builder.add(s);
        }
        return builder.build();
    }
    /*
     * Prefix for Java properties for internal Cassandra configuration options
     */
    public static final String PROPERTY_PREFIX = "cassandra.";


    public String cluster_name = "Test Cluster";
    public String authenticator;
    public String authorizer;
    public String role_manager;
    public String network_authorizer;
    public volatile int permissions_validity_in_ms = 2000;
    public int permissions_cache_max_entries = 1000;
    public volatile int permissions_update_interval_in_ms = -1;
    public volatile boolean permissions_cache_active_update = false;
    public volatile int roles_validity_in_ms = 2000;
    public int roles_cache_max_entries = 1000;
    public volatile int roles_update_interval_in_ms = -1;
    public volatile boolean roles_cache_active_update = false;

    public int authenticator_validity_in_ms = 2000;
    public int authenticator_cache_max_entries = 1000;
    public int authenticator_update_interval_in_ms = -1;
    public volatile boolean authenticator_cache_active_update = false;

    public boolean auth_cache_warming_enabled = true;

    /* Hashing strategy Random or OPHF */
    public String partitioner;

    public Boolean auto_bootstrap = true;
    public volatile boolean hinted_handoff_enabled = true;
    public Set<String> hinted_handoff_disabled_datacenters = Sets.newConcurrentHashSet();
    public volatile Integer max_hint_window_in_ms = 3 * 3600 * 1000; // three hours
    public String hints_directory;

    public ParameterizedClass seed_provider;
    public DiskAccessMode disk_access_mode = DiskAccessMode.auto;

    public DiskFailurePolicy disk_failure_policy = DiskFailurePolicy.ignore;
    public CommitFailurePolicy commit_failure_policy = CommitFailurePolicy.stop;

    public volatile boolean disable_schema_drop_check = false;
    public volatile boolean use_deterministic_table_id = false;

    public volatile Boolean enable_partition_blacklist = false;

    public volatile Boolean enable_blacklist_writes = true;

    public volatile Boolean enable_blacklist_reads = true;

    public volatile Boolean enable_blacklist_range_reads = false;

    public int blacklist_refresh_period_seconds = 86400;

    public int blacklist_initial_load_retry_seconds = 5;

    public int max_blacklist_keys_per_cf = 1000;

    public int max_blacklist_keys_total = 10000;

    public ConsistencyLevel blacklist_consistency_level = ConsistencyLevel.QUORUM;

    public Boolean enable_christmas_patch = true;

    public Boolean enable_shadow_christmas_patch = false;

    public volatile boolean enable_speculative_read_repair = true;

    /* initial token in the ring */
    public String initial_token;
    public Integer num_tokens = 1;
    /** Triggers automatic allocation of tokens if set, using the replication strategy of the referenced keyspace */
    public String allocate_tokens_for_keyspace = null;

    public volatile Long request_timeout_in_ms = 10000L;

    public volatile Long read_request_timeout_in_ms = 5000L;

    public volatile Long range_request_timeout_in_ms = 10000L;

    public volatile Long write_request_timeout_in_ms = 2000L;

    public volatile Long counter_write_request_timeout_in_ms = 5000L;

    public volatile Long cas_contention_timeout_in_ms = 1000L;

    public volatile Long truncate_request_timeout_in_ms = 60000L;

    public Integer streaming_socket_timeout_in_ms = 86400000; //24 hours

    public boolean cross_node_timeout = false;

    public volatile Double phi_convict_threshold = 8.0;

    public volatile Integer concurrent_reads = 32;
    public volatile Integer concurrent_writes = 32;
    public volatile Integer concurrent_counter_writes = 32;
    public volatile Integer concurrent_materialized_view_writes = 32;

    @Deprecated
    public Integer concurrent_replicates = null;

    public Integer memtable_flush_writers = null;
    public Integer memtable_heap_space_in_mb;
    public Integer memtable_offheap_space_in_mb;
    public Float memtable_cleanup_threshold = null;
    public Integer memtable_clock_shift = 17;
    public Long memtable_excess_waste_bytes = 10 * 1024 * 1024L;

    // Limit the maximum depth of repair session merkle trees
    public volatile Integer repair_session_max_tree_depth = 18;

    // default changed in request rdar://63403100 (Enable use_offheap_merkle_trees by default)
    public volatile boolean use_offheap_merkle_trees = true;

    public volatile int paxos_repair_parallelism = -1;

    public Integer storage_port = 7000;
    public Integer ssl_storage_port = 7001;
    public String listen_address;
    public String listen_interface;
    public Boolean listen_interface_prefer_ipv6 = false;
    public String broadcast_address;
    public Boolean listen_on_broadcast_address = false;
    public String internode_authenticator;

    /* intentionally left set to true, despite being set to false in stock 2.2 cassandra.yaml
       we don't want to surprise Thrift users who have the setting blank in the yaml during 2.1->2.2 upgrade */
    public Boolean start_rpc = true;
    public String rpc_address;
    public String rpc_interface;
    public Boolean rpc_interface_prefer_ipv6 = false;
    public String broadcast_rpc_address;
    public Integer rpc_port = 9160;
    public Integer rpc_listen_backlog = 50;
    public String rpc_server_type = "sync";
    public Boolean rpc_keepalive = true;
    public Integer rpc_min_threads = 16;
    public Integer rpc_max_threads = Integer.MAX_VALUE;
    public Integer rpc_send_buff_size_in_bytes;
    public Integer rpc_recv_buff_size_in_bytes;
    public Integer internode_send_buff_size_in_bytes;
    public Integer internode_recv_buff_size_in_bytes;
    public int internode_socket_timeout_ms = 0;

    public Boolean start_native_transport = false;
    public Integer native_transport_port = 9042;
    public Integer native_transport_port_ssl = null;
    public volatile Integer native_transport_max_threads = 128;
    /** do bcrypt hashing in a limited pool to prevent cpu load spikes **/
    public volatile Integer native_transport_max_auth_threads = 2;
    public Integer native_transport_max_frame_size_in_mb = 256;
    public volatile Long native_transport_max_concurrent_connections = -1L;
    public volatile Long native_transport_max_concurrent_connections_per_ip = -1L;
    public boolean native_transport_flush_in_batches_legacy = true;
    public volatile long native_transport_max_concurrent_requests_in_bytes_per_ip = -1L;
    public volatile long native_transport_max_concurrent_requests_in_bytes = -1L;
    public volatile boolean native_transport_allow_older_protocols = true;

    public volatile boolean force_paging_state_legacy_serialization = true;

    @Deprecated
    public Integer native_transport_max_protocol_version = null;

    @Deprecated
    public Integer thrift_max_message_length_in_mb = 16;
    /**
     * Max size of values in SSTables, in MegaBytes.
     * Default is the same as the native protocol frame limit: 256Mb.
     * See AbstractType for how it is used.
     */
    public Integer max_value_size_in_mb = 256;

    public Integer thrift_framed_transport_size_in_mb = 15;
    public Boolean snapshot_before_compaction = false;
    public Boolean auto_snapshot = true;

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    public Integer column_index_size_in_kb = 64;
    // We set the column index downsampling knobs very high to disable it by default
    public volatile int column_index_max_target_size_in_kb = Integer.MAX_VALUE / 1024; // downsample if we're > 2x this value
    public volatile int column_index_max_target_index_objects = Integer.MAX_VALUE; // downsample if we're > 2x this value
    public volatile int batch_size_warn_threshold_in_kb = 5;
    public volatile int batch_size_fail_threshold_in_kb = 50;
    public Integer unlogged_batch_across_partitions_warn_threshold = 10;
    public Integer concurrent_compactors;
    public volatile Integer compaction_throughput_mb_per_sec = 16;
    public volatile Integer compaction_large_partition_warning_threshold_mb = 100;
    public Integer min_free_space_per_drive_in_mb = 50;

    public ValidationPoolFullStrategy validation_pool_full_strategy = ValidationPoolFullStrategy.queue;
    public volatile int concurrent_validations;

    // The number of pending compactions at which we should reject new repair prepare requests
    public volatile int reject_repair_compaction_threshold = 2048;

    /**
     * @deprecated retry support removed on CASSANDRA-10992
     */
    @Deprecated
    public Integer max_streaming_retries = 3;

    public volatile Integer stream_throughput_outbound_megabits_per_sec = 200;
    public volatile Integer inter_dc_stream_throughput_outbound_megabits_per_sec = 200;

    public String[] data_file_directories = new String[0];

    public String saved_caches_directory;

    // Commit Log
    public String commitlog_directory;
    public Integer commitlog_total_space_in_mb;
    public CommitLogSync commitlog_sync;
    public Double commitlog_sync_batch_window_in_ms;
    public Integer commitlog_sync_period_in_ms;
    public int commitlog_segment_size_in_mb = 32;
    public ParameterizedClass commitlog_compression;
    public int commitlog_max_compression_buffers_in_pool = 3;
    public Integer periodic_commitlog_sync_lag_block_in_ms;

    public Integer max_mutation_size_in_kb;

    @Deprecated
    public int commitlog_periodic_queue_size = -1;

    public String endpoint_snitch;
    public Boolean dynamic_snitch = true;
    public volatile Integer dynamic_snitch_update_interval_in_ms = 100;
    public volatile Integer dynamic_snitch_reset_interval_in_ms = 600000;
    public volatile Double dynamic_snitch_badness_threshold = 0.1;
    public volatile Boolean dynamic_snitch_manual_severity_only = false;

    public String request_scheduler;
    public RequestSchedulerId request_scheduler_id;
    public RequestSchedulerOptions request_scheduler_options;

    public ServerEncryptionOptions server_encryption_options = new ServerEncryptionOptions();
    public EncryptionOptions client_encryption_options = new EncryptionOptions();
    // this encOptions is for backward compatibility (a warning is logged by DatabaseDescriptor)
    public ServerEncryptionOptions encryption_options;

    public InternodeCompression internode_compression = InternodeCompression.none;

    @Deprecated
    public Integer index_interval = null;

    public int hinted_handoff_throttle_in_kb = 1024;
    public int batchlog_replay_throttle_in_kb = 1024;
    public int max_hints_delivery_threads = 2;
    public int hints_flush_period_in_ms = 10000;
    public int max_hints_file_size_in_mb = 128;
    public ParameterizedClass hints_compression;

    public volatile boolean incremental_backups = false;
    public boolean trickle_fsync = false;
    public int trickle_fsync_interval_in_kb = 10240;

    public volatile int sstable_preemptive_open_interval_in_mb = 50;

    public volatile boolean key_cache_migrate_during_compaction = true;
    public Long key_cache_size_in_mb = null;
    public volatile int key_cache_save_period = 14400;
    public volatile int key_cache_keys_to_save = Integer.MAX_VALUE;

    public String row_cache_class_name = "org.apache.cassandra.cache.OHCProvider";
    public long row_cache_size_in_mb = 0;
    public volatile int row_cache_save_period = 0;
    public volatile int row_cache_keys_to_save = Integer.MAX_VALUE;

    public Long counter_cache_size_in_mb = null;
    public volatile int counter_cache_save_period = 7200;
    public volatile int counter_cache_keys_to_save = Integer.MAX_VALUE;

    public Long paxos_cache_size_in_mb = null;

    private static boolean isClientMode = false;
    private static Supplier<Config> overrideLoadConfig = null;

    public Integer file_cache_size_in_mb = 512;

    public boolean buffer_pool_use_heap_if_exhausted = true;

    public DiskOptimizationStrategy disk_optimization_strategy = DiskOptimizationStrategy.ssd;

    public double disk_optimization_estimate_percentile = 0.95;

    public double disk_optimization_page_cross_chance = 0.1;

    public boolean inter_dc_tcp_nodelay = true;

    public MemtableAllocationType memtable_allocation_type = MemtableAllocationType.heap_buffers;

    /**
     * @deprecated No longer needed for streaming protocol. See CASSANDRA-12673 for details.
     */
    @Deprecated
    protected static boolean outboundBindAny = false;

    public volatile int tombstone_warn_threshold = 1000;
    public volatile int tombstone_failure_threshold = 100000;
    public volatile boolean tombstone_count_gcable = false;

    public volatile boolean generate_sstable_digest_components = true;

    public final ReplicaFilteringProtectionOptions replica_filtering_protection = new ReplicaFilteringProtectionOptions();

    public volatile Long index_summary_capacity_in_mb;
    public volatile int index_summary_resize_interval_in_minutes = 60;

    public int gc_log_threshold_in_ms = 200;
    public int gc_warn_threshold_in_ms = 0;

    public Boolean disable_incremental_repair = Boolean.parseBoolean(System.getProperty("cassandra.disable_incremental_repair", "true"));

    /**
     * number of seconds to set nowInSec into the future when performing validation previews against repaired data
     * this (attempts) to prevent a race where validations on different machines are started on different sides of
     * a tombstone being compacted away
     */
    public volatile int validation_preview_purge_head_start_in_sec = 60 * 60;

    /**
     * If true, streams and merkle trees will be dumped for validation previews
     * that detected differences in repaired data sets
     */
    public volatile boolean debug_validation_preview = false;

    // TTL for different types of trace events.
    public int tracetype_query_ttl = (int) TimeUnit.DAYS.toSeconds(1);
    public int tracetype_repair_ttl = (int) TimeUnit.DAYS.toSeconds(7);

    /**
     * Maintain statistics on whether writes achieve the ideal consistency level
     * before expiring and becoming hints
     */
    public volatile ConsistencyLevel ideal_consistency_level = null;
    public volatile boolean enable_keyspace_quotas = false;
    public volatile long default_keyspace_quota_bytes = -1;
    public int keyspace_quota_refresh_time_in_sec = 120;
    /*
     * Strategy to use for coalescing messages in OutboundTcpConnection.
     * Can be fixed, movingaverage, timehorizon, disabled. Setting is case and leading/trailing
     * whitespace insensitive. You can also specify a subclass of CoalescingStrategies.CoalescingStrategy by name.
     */
    public String otc_coalescing_strategy = "DISABLED";

    /*
     * How many microseconds to wait for coalescing. For fixed strategy this is the amount of time after the first
     * message is received before it will be sent with any accompanying messages. For moving average this is the
     * maximum amount of time that will be waited as well as the interval at which messages must arrive on average
     * for coalescing to be enabled.
     */
    public static final int otc_coalescing_window_us_default = 200;
    public int otc_coalescing_window_us = otc_coalescing_window_us_default;
    public int otc_coalescing_enough_coalesced_messages = 8;

    /**
     * Backlog expiration interval in milliseconds for the OutboundTcpConnection.
     */
    public static final int otc_backlog_expiration_interval_ms_default = 200;
    public volatile int otc_backlog_expiration_interval_ms = otc_backlog_expiration_interval_ms_default;
 
    public int windows_timer_interval = 0;

    public boolean enable_user_defined_functions = false;
    public boolean enable_scripted_user_defined_functions = false;

    public boolean enable_materialized_views = false;

    /**
     * Optionally disable asynchronous UDF execution.
     * Disabling asynchronous UDF execution also implicitly disables the security-manager!
     * By default, async UDF execution is enabled to be able to detect UDFs that run too long / forever and be
     * able to fail fast - i.e. stop the Cassandra daemon, which is currently the only appropriate approach to
     * "tell" a user that there's something really wrong with the UDF.
     * When you disable async UDF execution, users MUST pay attention to read-timeouts since these may indicate
     * UDFs that run too long or forever - and this can destabilize the cluster.
     */
    public boolean enable_user_defined_functions_threads = true;
    /**
     * Time in milliseconds after a warning will be emitted to the log and to the client that a UDF runs too long.
     * (Only valid, if enable_user_defined_functions_threads==true)
     */
    public long user_defined_function_warn_timeout = 500;
    /**
     * Time in milliseconds after a fatal UDF run-time situation is detected and action according to
     * user_function_timeout_policy will take place.
     * (Only valid, if enable_user_defined_functions_threads==true)
     */
    public long user_defined_function_fail_timeout = 1500;
    /**
     * Defines what to do when a UDF ran longer than user_defined_function_fail_timeout.
     * Possible options are:
     * - 'die' - i.e. it is able to emit a warning to the client before the Cassandra Daemon will shut down.
     * - 'die_immediate' - shut down C* daemon immediately (effectively prevent the chance that the client will receive a warning).
     * - 'ignore' - just log - the most dangerous option.
     * (Only valid, if enable_user_defined_functions_threads==true)
     */
    public UserFunctionTimeoutPolicy user_function_timeout_policy = UserFunctionTimeoutPolicy.die;

    public volatile int initial_range_tombstone_allocation_size = 64;
    public volatile double range_tombstone_resize_factor = 2.0;

    /*
     * Toggles to turn on the logging or rejection of operations for token ranges that the node does not own,
     * or is not about to acquire.
     * <rdar://problem/33279387> Don't allow writes for token range instance don't own
     * <rdar://problem/33279491> Don't accept reads for ranges which we dont own
     * <rdar://problem/33280054> Don't accept incoming out outgoing stream for token instance dont own.
     * <rdar://problem/33280066> Don't accept Merkle tree request for ranges instance dont own
     * <rdar://problem/33299827> Don't accepts hints for data instance dont own
     */
    public volatile boolean log_out_of_token_range_requests = true;
    public volatile boolean reject_out_of_token_range_requests = true;

    public volatile boolean enable_scheduled_compactions = false;
    public volatile int scheduled_compaction_range_splits = 100;
    public volatile String scheduled_compaction_cycle_time = "60d";
    public volatile boolean skip_single_sstable_scheduled_compactions = true;
    public volatile long max_scheduled_compaction_sstable_size_bytes = 10240 * 1024L * 1024L;
    public volatile int max_scheduled_compaction_sstable_count = 40;
    public volatile boolean incremental_updates_last_repaired = false;

    public volatile long large_partition_index_warning_threshold_kb = 4096L;

    /**
     * Class names to look for custom functions in.  They must define a static @code{Collection<Function> all()}
     * that returns the custom functions to add.
     */
    public Set<String> custom_functions = new HashSet<>();

    public static boolean getOutboundBindAny()
    {
        return outboundBindAny;
    }

    public static void setOutboundBindAny(boolean value)
    {
        outboundBindAny = value;
    }
    public volatile boolean automatic_sstable_upgrade = false;
    public volatile int max_concurrent_automatic_sstable_upgrades = 1;

    public RepairCommandPoolFullStrategy repair_command_pool_full_strategy = RepairCommandPoolFullStrategy.queue;
    public int repair_command_pool_size;

    public CorruptedTombstoneStrategy corrupted_tombstone_strategy = CorruptedTombstoneStrategy.disabled;

    // parameters to adjust how much to delay startup until a certain amount of the cluster is connect to and marked alive
    public int block_for_peers_percentage = 70;
    public int block_for_peers_timeout_in_secs = 10;

    /**
     * flags for enabling tracking repaired state of data during reads
     * separate flags for range & single partition reads as single partition reads are only tracked
     * when CL > 1 and a digest mismatch occurs. Currently, range queries don't use digests so if
     * enabled for range reads, all such reads will include repaired data tracking. As this adds
     * some overhead, operators may wish to disable it whilst still enabling it for partition reads
     */
    public volatile boolean repaired_data_tracking_for_range_reads_enabled = false;
    public volatile boolean repaired_data_tracking_for_partition_reads_enabled = false;

    /**
     * Allow keyspace, table or row level exclusions to repaired data tracking to be configured
     * and enabled. Configuration must be done via cassandra.yaml, but enabling/disabling is
     * exposed by JMX
     * See o.a.c.config.RepairedDataTrackingExclusions for details of the config format
     */
    public volatile boolean repaired_data_tracking_exclusions_enabled = false;

    /**
     * example:
     *
     * repaired_data_tracking_exclusions: ks1:tbl:02,ks1:tbl:03
     */
    public String repaired_data_tracking_exclusions;

    /* If true, unconfirmed mismatches (those which cannot be considered conclusive proof of out of
     * sync repaired data due to the presence of pending repair sessions, or unrepaired partition
     * deletes) will increment a metric, distinct from confirmed mismatches. If false, unconfirmed
     * mismatches are simply ignored by the coordinator.
     * This is purely to allow operators to avoid potential signal:noise issues as these types of
     * mismatches are considerably less actionable than their confirmed counterparts. Setting this
     * to true only disables the incrementing of the counters when an unconfirmed mismatch is found
     * and has no other effect on the collection or processing of the repaired data.
     */
    public volatile boolean report_unconfirmed_repaired_data_mismatches = false;

    public volatile boolean allow_drop_compact_storage = false;

    /**
     * If true, when a repaired data mismatch is detected at read time, a snapshot request will be
     * issued to each replica participating in the query. These are limited at the replica level
     * so that only a single snapshot per-day can be taken via this method.
     */
    public volatile boolean snapshot_on_repaired_data_mismatch = false;

    /**
     * If true, when rows with duplicate clustering keys are detected during a read or compaction
     * a snapshot will be taken. In the read case, a snapshot request will be issued to each
     * replica involved in the query, for compaction the snapshot will be created locally.
     * These are limited at the replica level so that only a single snapshot per-day can be taken
     * via this method.
     *
     * This requires check_for_duplicate_rows_during_reads and/or check_for_duplicate_rows_during_compaction
     * below to be enabled
     */
    public volatile boolean snapshot_on_duplicate_row_detection = false;
    /**
     * If these are enabled duplicate keys will get logged, and if snapshot_on_duplicate_row_detection
     * is enabled, the table will get snapshotted for offline investigation
     */
    public volatile boolean check_for_duplicate_rows_during_reads = true;
    public volatile boolean check_for_duplicate_rows_during_compaction = true;

    public volatile boolean enable_secondary_index = Boolean.getBoolean("cassandra.enable_secondary_index");

    public String repair_history_sync_timeout = "10m";

    public volatile boolean allow_unsafe_aggressive_sstable_expiration = false;

    public static boolean isClientMode()
    {
        return isClientMode;
    }

    public static void setClientMode(boolean clientMode)
    {
        isClientMode = clientMode;
    }

    public ConsistencyLevel auth_read_consistency_level = ConsistencyLevel.LOCAL_QUORUM;
    public ConsistencyLevel auth_write_consistency_level = ConsistencyLevel.EACH_QUORUM;

    public volatile boolean alter_table_enabled = true;

    public String full_query_log_dir = null;

    public volatile boolean compact_biggest_stcs_bucket_l0 = false;

    public boolean autocompaction_on_startup_enabled = Boolean.parseBoolean(System.getProperty("cassandra.autocompaction_on_startup_enabled", "true"));

    // see CASSANDRA-3200
    public volatile boolean auto_optimise_inc_repair_streams = false;
    public volatile boolean auto_optimise_full_repair_streams = false;
    public volatile boolean auto_optimise_preview_repair_streams = false;

    public enum PaxosVariant
    {
        legacy,
        legacy_cached,
        legacy_fixed,  // fixes bugs in legacy impl to support validating transition from legacy to apple paxos. Not intended for production
        apple_norrl, // with legacy semantics for read/read linearizability (i.e. not guaranteed)
        apple_norrfwl, // with legacy semantics for read/read and failed write linearizability (i.e. not guaranteed)
        apple_rrl2rt, // with read/read linearizability guaranteed but requiring an extra read round-trip
        apple_rrl // provides read/read linearizability, doesn't incur an extra round-trip if no contending paxos operation is detected
    }

    public volatile PaxosVariant paxos_variant = PaxosVariant.legacy;

    public volatile boolean skip_paxos_repair_on_topology_change = Boolean.getBoolean("cassandra.skip_paxos_repair_on_topology_change");

    /**
     * If true, paxos topology change repair only requires a global quorum of live nodes. If false,
     * it requires a global quorum as well as a local quorum for each dc (EACH_QUORUM), with the
     * exception explained in paxos_topology_repair_strict_each_quorum
     */
    public boolean paxos_topology_repair_no_dc_checks = false;

    /**
     * If true, a quorum will be required for the global and local quorum checks. If false, we will
     * accept a quorum OR n - 1 live nodes. This is to allow for topologies like 2:2:2, where paxos queries
     * always use SERIAL, and a single node down in a dc should not preclude a paxos repair
     */
    public boolean paxos_topology_repair_strict_each_quorum = false;
    public volatile Set<String> skip_paxos_repair_on_topology_change_keyspaces = splitCommaDelimited(System.getProperty("cassandra.skip_paxos_repair_on_topology_change_keyspaces"));

    public String paxos_contention_wait_randomizer;
    public String paxos_contention_min_wait;
    public String paxos_contention_max_wait;
    public String paxos_contention_min_delta;

    /**
     * The amount of disk space paxos uncommitted key files can consume before we begin automatically scheduling paxos repairs
     */
    public volatile int paxos_auto_repair_threshold_mb = 32;

    public volatile boolean allow_compact_storage = true;

    public static Supplier<Config> getOverrideLoadConfig()
    {
        return overrideLoadConfig;
    }

    public static void setOverrideLoadConfig(Supplier<Config> loadConfig)
    {
        overrideLoadConfig = loadConfig;
    }

    public enum CommitLogSync
    {
        periodic,
        batch
    }
    public enum InternodeCompression
    {
        all, none, dc
    }

    public enum DiskAccessMode
    {
        auto,
        mmap,
        mmap_index_only,
        standard,
    }

    public enum MemtableAllocationType
    {
        unslabbed_heap_buffers,
        heap_buffers,
        offheap_buffers,
        offheap_objects
    }

    public enum DiskFailurePolicy
    {
        best_effort,
        stop,
        ignore,
        stop_paranoid,
        die
    }

    public enum CommitFailurePolicy
    {
        stop,
        stop_commit,
        ignore,
        die,
    }

    public enum UserFunctionTimeoutPolicy
    {
        ignore,
        die,
        die_immediate
    }

    public enum RequestSchedulerId
    {
        keyspace
    }

    public enum DiskOptimizationStrategy
    {
        ssd,
        spinning
    }

    public enum RepairCommandPoolFullStrategy
    {
        queue,
        reject
    }

    public enum ValidationPoolFullStrategy
    {
        queue,
        reject
    }

    public enum CorruptedTombstoneStrategy
    {
        disabled,
        warn,
        exception
    }

    private static final List<String> SENSITIVE_KEYS = new ArrayList<String>() {{
        add("client_encryption_options");
        add("server_encryption_options");
    }};

    public static void log(Config config)
    {
        Map<String, String> configMap = new TreeMap<>();
        for (Field field : Config.class.getFields())
        {
            // ignore the constants
            if (Modifier.isFinal(field.getModifiers()))
                continue;

            String name = field.getName();
            if (SENSITIVE_KEYS.contains(name))
            {
                configMap.put(name, "<REDACTED>");
                continue;
            }

            String value;
            try
            {
                // Field.get() can throw NPE if the value of the field is null
                value = field.get(config).toString();
            }
            catch (NullPointerException | IllegalAccessException npe)
            {
                value = "null";
            }
            configMap.put(name, value);
        }

        logger.info("Node configuration:[" + Joiner.on("; ").join(configMap.entrySet()) + "]");
    }
}
