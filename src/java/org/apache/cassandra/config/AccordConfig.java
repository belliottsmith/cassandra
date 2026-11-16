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

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import accord.api.ProtocolModifiers.CleanCfkBefore;
import accord.api.ProtocolModifiers.CoordinatorBacklogExecution;
import accord.api.ProtocolModifiers.FastExecution;
import accord.api.ProtocolModifiers.ReplicaExecution;
import accord.api.ProtocolModifiers.SendMinimal;
import accord.api.ProtocolModifiers.SendStableMessages;
import accord.api.ProtocolModifiers.UniqueTimestampOnConflict;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.config.AccordConfig.QueueShardModel.THREAD_POOL_PER_SHARD;
import static org.apache.cassandra.config.AccordConfig.QueueSubmissionModel.SIGNAL;
import static org.apache.cassandra.config.AccordConfig.QueueSubmissionModel.SYNC;
import static org.apache.cassandra.config.AccordConfig.RangeIndexMode.in_memory;
import static org.apache.cassandra.config.AccordConfig.RangeIndexMode.journal_sai;

public class AccordConfig
{
    public volatile boolean enabled = false;

    public volatile String journal_directory;

    /**
     * Enables the virtual Accord debug-only keyspace with tables
     * that expose internal state to aid the developers working
     * on Accord implementation.
     * <p/>
     * These tables can and will change and/or go away at any point,
     * including in a minor release, are not to be considered part of the API,
     * and are NOT to be relied on for anything.
     * <p/>
     * Only enable this keyspace if you are working on Accord and
     * need to debug an issue with Accord implementation, or if an Accord
     * developer asked you to.
     */
    public boolean enable_virtual_debug_only_keyspace = false;

    public enum QueueShardModel
    {
        /**
         * Same number of threads as queue shards, but the shard lock is held only while managing the queue,
         * so that submitting threads may queue load/save work.
         *
         * This is incompatible with QueueSubmissionModel.SIGNAL
         */
        THREAD_PER_SHARD,

        /**
         * Same number of threads as shards, and the shard lock is held for the duration of serving requests.
         *
         * This is incompatible with QueueSubmissionModel.SIGNAL
         */
        THREAD_PER_SHARD_SYNC_QUEUE,

        /**
         * More threads than shards. Threads update transaction state as well as performing IO, minimising context switching.
         * Fewer shards is generally better, until queue-contention is encountered.
         */
        THREAD_POOL_PER_SHARD,
    }

    public enum QueueSubmissionModel
    {
        /**
         * The queue workers and all submissions require ownership of the lock.
         */
        SYNC,

        /**
         * The queue workers and some submissions require ownership of the lock.
         * That is, if the lock is available on submission we take it; if it is not we try to guarantee that
         * another thread will witness the work submission promptly, but if we cannot we wait for the lock
         * to ensure work is scheduled.
         */
        SEMI_SYNC,

        /**
         * The queue workers only require ownership of the lock, submissions happens fully asynchronously.
         *
         * NOTE: EXPERIMENTAL
         */
        ASYNC,

        /**
         * Queue workers try to avoid competing for the lock, with the lock owner distributing work to any waiting threads
         * and signalling them without them taking the lock
         *
         * NOTE: EXPERIMENTAL
         */
        SIGNAL,

        /**
         * The queue is backed by submission to a single-threaded plain executor.
         * This implementation does not honor the sharding model option.
         *
         * Note: this isn't intended to be used by real clusters.
         */
        EXEC_ST
    }

    public enum QueuePriorityModel
    {
        /**
         * All work is queued on a first-come first-serve basis.
         * Overload can lead to more rapid degradation, as later phases of the state machine are delayed
         * by the arrival of new work.
         */
        FIFO,

        /**
         * If the work has an associated TxnId or Ballot, prioritise by the newest HLC (and FIFO otherwise)
         */
        HLC_FIFO,

        /**
         * If the work has an associated TxnId, prioritise by its HLC (and FIFO otherwise).
         * In this case, all phases with recovery ballots enter a QoS pool shared with other progress tasks,
         * so that Accept/Commit do not compete. Recovery work is scheduled in age order.
         */
        ORIG_HLC_FIFO
    }

    public enum QueueBalancingModel
    {
        /**
         * Always pick the task by priority
         */
        PRIORITY_ONLY,

        /**
         * Pick by phase first, so the highest phase with work ALWAYS runs.
         * Within a phase, pick by priority.
         */
        PHASE_ONLY,

        /**
         * Pick by phase first, selecting the phase that has processed the least work recently relative to arrivals.
         * Tie-break between phases with round-robin.
         * Within a phase, pick by priority.
         */
        PHASE_FAIR,

        /**
         * A blended (1:1) mix of PRIORITY and PHASE_FAIR
         */
        BLENDED_PRIORITY_PHASE_FAIR,
    }

    public enum UniqueTimestampReservations
    {
        NONE,
        SMALL_SHARED,
        HISTOGRAM
    }

    public QueueShardModel queue_shard_model = THREAD_POOL_PER_SHARD;
    public QueueSubmissionModel queue_submission_model = SIGNAL;

    /**
     * The number of queue (and cache) shards.
     */
    public volatile OptionaldPositiveInt queue_shard_count = OptionaldPositiveInt.UNDEFINED;

    /**
     * The total number of threads to share between queue shards
     */
    public volatile OptionaldPositiveInt queue_thread_count = OptionaldPositiveInt.UNDEFINED;

    public QueuePriorityModel queue_priority_model = QueuePriorityModel.HLC_FIFO;
    public QueueBalancingModel queue_balancing_model = QueueBalancingModel.BLENDED_PRIORITY_PHASE_FAIR;

    public String queue_active_limits;

    public boolean queue_nonsync_enabled = true;
    public boolean queue_key_ordering_enabled = true;

    /**
     * Size at which we will begin processing a task that is ASYNC, INCR OR INCR_ATOMIC.
     * Note that a size of zero will effectively give implicit priority to INCR_ATOMIC tasks, as they may immediately
     * take a FIFO queue slot (which is processed preferentially).
     */
    public Integer queue_nonsync_min_batch_size = 16;

    /**
     * If there are more than min_batch_size keys ready for an ASYNC, INCR or INCR_ATOMIC task,
     * process up to this many keys at once.
     */
    public Integer queue_nonsync_max_batch_size = 64;

    /**
     * An ASYNC, INCR or INCR_ATOMIC task that is ready to run but waiting for batch_size work will proceed
     * once this number of tasks are blocked behind it, regardless of batch_size.
     */
    public Integer queue_nonsync_blocked_limit;

    /**
     * The maximum number of bounds each command store keeps on the latest transaction of the CommandsForKey not in its
     * cache, which let a sync point avoid scanning parts of its ranges on disk for keys it must load. Zero disables them,
     * as for {@link MaxUncachedTxnIdMode#IGNORE}.
     */
    public int max_uncached_txnid_entries = 4096;

    public enum MaxUncachedTxnIdMode
    {
        /**
         * Neither maintain nor consult the bounds: a sync point scans its ranges on disk in full.
         */
        IGNORE,

        /**
         * Maintain the bounds and plan each scan with them, but scan in full and load as without them, reporting via
         * {@link accord.utils.Invariants#expect} any key we must load that the bounds would have let us skip.
         */
        VALIDATE,

        /**
         * Scan on disk only those parts of our ranges whose bounds may be relevant, or that we know nothing about.
         */
        RELY
    }

    /**
     * How a command store uses its bounds on the CommandsForKey not in its cache (see max_uncached_txnid_entries).
     * Read when each command store is created.
     */
    public MaxUncachedTxnIdMode max_uncached_txnid_mode = MaxUncachedTxnIdMode.RELY;

    /**
     * The number of threads that may be used to execute distributed requests for migration tasks
     */
    public volatile OptionaldPositiveInt migration_concurrency = OptionaldPositiveInt.UNDEFINED;

    /**
     * If set, the signal loop does not match park/unpark pairs, but instead consumers perform timed-park spin waits
     */
    public DurationSpec.LongMicrosecondsBound queue_spin_interval;

    /**
     * If set, the signal loop reduces the number of threads it is using when the time spent parked exceeds real-time
     * by this interval.
     */
    public DurationSpec.LongMicrosecondsBound queue_stop_check_interval;

    /**
     * If set (and non-zero), each pass of the signal loop's dispatch (one acquisition of an executor's lock) spends at
     * most about this long completing tasks; further completions are deferred, in order, to later passes, so that new
     * work and ready tasks are not held up behind many expensive completions. Unset or zero: no budget.
     */
    public DurationSpec.LongMicrosecondsBound queue_completion_budget;

    /**
     * If the HLC is older than this, queue by next position instead
     */
    public DurationSpec.IntMillisecondsBound queue_priority_age_limit = new DurationSpec.IntMillisecondsBound(500);

    /**
     * Fifo claims on cache entries always run ahead of priority claims, so a task that becomes fifo (on its first run, at
     * setup, or by this mechanism) overtakes any claims queued ahead of it by priority, and a stream of such tasks on a
     * hot key could otherwise starve them. So on each key a task becomes fifo on, up to this many of the waiting tasks it
     * overtook (those with the lowest positions) are also made fifo, behind it, and transitively for the tasks so
     * promoted. One per key suffices to bound how often any waiter may be overtaken. Zero disables.
     */
    public Integer queue_fifo_promote_per_key;

    /**
     * EXPERIMENTAL (baseline for micro-batching): when a command store's exclusive executor finishes running a task and
     * has more work queued, the thread may try (without blocking) to take the executor lock, complete the task and
     * prepare the next, and run it immediately, without giving up ownership of the command store. This avoids a
     * hand-off between threads, but not the lock. Up to this many further tasks may be run per hand-off;
     * zero (the default) disables.
     */
    public Integer queue_exclusive_continue_limit;

    /**
     * EXPERIMENTAL: micro-batching. When a command store's next task is a SYNC task, other SYNC tasks queued behind it on
     * its keys and txnIds (and only behind tasks of the batch on any other entry they use), and any consequences of these
     * tasks whose entries the batch already owns, may be run immediately after it by the same thread, without taking the
     * executor lock, passing state between them directly. The entries used by the batch are frozen (owned) until it
     * completes. This is the budget of a batch, besides its head: each task costs the number of entries it uses (keys
     * plus txnIds). Zero (the default) disables.
     */
    public Integer queue_exclusive_batch_limit;

    /**
     * Deadline-ordered delivery of contended PreAccepts (AccordExecutorSignalLoop only). If set, a PreAccept that is
     * judged to be contended has its processing postponed until txnId.hlc() plus this pluggable
     * {@link org.apache.cassandra.service.accord.execution.deadline.PreAcceptPenalty}, and contended PreAccepts are
     * released strictly in txnId order.
     *
     * The standard implementation supplies Y, the maximum one-way link latency from any DC, estimated as half the
     * round-trip to each DC measured by {@link org.apache.cassandra.net.LinkLatencies} (so link_latency_probe_interval
     * must also be configured), e.g.
     *   class_name: LinkLatencyPreAcceptPenalty
     */
    public ParameterizedClass preaccept_deadline_penalty;

    /**
     * A loaded key is considered contended if it has witnessed a transaction with an HLC within this window of now.
     */
    public DurationSpec.LongMicrosecondsBound preaccept_deadline_contention_window = new DurationSpec.LongMicrosecondsBound("100ms");

    /**
     * The maximum penalty that may be applied to any PreAccept, irrespective of what the penalty implementation supplies.
     */
    public DurationSpec.LongMicrosecondsBound preaccept_deadline_max_penalty = new DurationSpec.LongMicrosecondsBound("200ms");
    /**
     * The target number of command stores to create per topology shard.
     * This determines the amount of execution parallelism possible for a given table/shard on the host.
     * More shards means more parallelism, but more state.
     *
     * TODO (expected): make this a table property
     * TODO (expected): adjust this by proportion of ring
     */
    public volatile OptionaldPositiveInt command_store_shard_count = OptionaldPositiveInt.UNDEFINED;

    public volatile OptionaldPositiveInt progress_log_concurrency = OptionaldPositiveInt.UNDEFINED;
    public DurationSpec.IntMillisecondsBound progress_log_query_fallback_timeout = new DurationSpec.IntMillisecondsBound("1m");

    public DataStorageSpec.LongMebibytesBound cache_size = null;
    public DataStorageSpec.LongMebibytesBound working_set_size = null;
    public boolean shrink_cache_entries_before_eviction = true;

    public DurationSpec.IntMillisecondsBound range_syncpoint_timeout = new DurationSpec.IntMillisecondsBound("3m");
    public DurationSpec.IntMillisecondsBound repair_timeout = new DurationSpec.IntMillisecondsBound("10m");
    public String recover_txn = "5s*attempts <= 60s";
    public StringRetryStrategy recover_syncpoint = new StringRetryStrategy("60s <= 30s*attempts...60s*attempts <= 600s");
    public String fetch_txn = "2s*attempts <= 60s";
    public String fetch_syncpoint = "5s*attempts <= 60s";
    /*
     * The expire_txn/syncpoint/durability, slow_txn_* and slow_syncpoint_* (and slow_read, slow_status_check)
     * specifications are evaluated separately for each kind of message and destination: their percentiles (pX) refer to
     * the recent round-trips of the message being sent (e.g. PreAccept) to the datacenter it is being sent to (or of
     * local delivery), as recorded by the AccordCoordinator <Request>Message-<destination> metrics (falling back to all
     * destinations until there are samples). For the syncpoint specifications these are the round-trips of sync
     * points; otherwise (r) selects those of read transactions, (w) writes, and (rw) the larger.
     */
    public String expire_txn = "5s*attempts <= 60s";
    public String expire_syncpoint = "15s*attempts<=300s";
    public String expire_epoch_wait = "10s";
    public String slow_syncpoint_preaccept = "10s";
    /*
     * For WaitUntilApplied and ApplyThenWaitUntilApplied, which are only sent for sync points, to await their application
     * (e.g. for durability). We don't want to wait ages for this, as it blocks other durability progress; even this
     * might be too long, as we can always retry.
     */
    @Replaces(oldName = "expire_durability", converter = Converters.IDENTITY, deprecated = true)
    public String expire_syncpoint_wait_until_applied = "10s*attempts <= 30s";
    // must be less than expire_syncpoint_wait_until_applied to have any effect
    public String slow_syncpoint_wait_until_applied = "1s <= p99*2 <= 10s";
    public String slow_txn_preaccept = "30ms <= p50*2 <= 1000ms";
    public String slow_read = "30ms <= p50*2 <= 1000ms";
    public String slow_status_check = "30ms <= p50*2 <= 1000ms";
    public StringRetryStrategy retry_syncpoint = new StringRetryStrategy("500ms*attempt <= 10s");
    public StringRetryStrategy retry_background_syncpoint = new StringRetryStrategy("10s*attempt <= 600s");
    public StringRetryStrategy retry_durability = new StringRetryStrategy("10s*attempt <= 600s");
    public StringRetryStrategy retry_bootstrap = new StringRetryStrategy("10s*attempt <= 600s");
    public StringRetryStrategy retry_join_bootstrap = new StringRetryStrategy("30s*attempt,attempts=5");
    public StringRetryStrategy retry_fetch_min_epoch = new StringRetryStrategy("200ms...1s*attempt <= 1s,retries=3");
    public StringRetryStrategy retry_fetch_topology = new StringRetryStrategy("200ms...1s*attempt <= 1s,retries=100");
    public StringRetryStrategy retry_journal_index_ready = new StringRetryStrategy("100ms");

    public volatile DurationSpec.IntMillisecondsBound coordinator_exclusive_time_slice = new DurationSpec.IntMillisecondsBound("250ms");
    public volatile DurationSpec.IntSecondsBound fast_path_update_delay = null;

    public volatile int shard_durability_target_splits = 8;
    public volatile int shard_durability_max_splits = 64;
    public volatile DurationSpec.IntSecondsBound durability_txnid_lag = new DurationSpec.IntSecondsBound(5);
    public volatile DurationSpec.IntSecondsBound shard_durability_cycle = new DurationSpec.IntSecondsBound(5, TimeUnit.MINUTES);
    public volatile DurationSpec.IntSecondsBound global_durability_cycle = new DurationSpec.IntSecondsBound(5, TimeUnit.MINUTES);
    /**
     * Flush tables that a durability report is waiting on with approximately this frequency; defaults to {@link #shard_durability_cycle}.
     */
    public volatile DurationSpec.IntSecondsBound durability_flush_interval;

    public volatile DurationSpec.IntSecondsBound topology_watermark_interval = new DurationSpec.IntSecondsBound(60);
    public volatile boolean topology_sync_propagator_enabled_pre_start = false;
    public volatile boolean topology_sync_propagator_enabled_post_startup = false;

    public enum TransactionalRangeMigration
    {
        auto, explicit
    }

    /**
     * Defines the behavior of range migration opt-in when changing transactional settings on a table. In auto,
     * all ranges are marked as migrating and no additional user action is needed aside from running repairs. In
     * explicit, no ranges are marked as migrating, and the user needs to explicitly mark ranges as migrating to
     * the target transactional mode via nodetool.
     */
    public volatile TransactionalRangeMigration range_migration = TransactionalRangeMigration.auto;

    public enum CatchupFallbackMode
    {
        IGNORE,
        EXIT,
        REBOOTSTRAP,
        REBOOTSTRAP_AND_CATCHUP
    }

    /**
     * default transactional mode for tables created by this node when no transactional mode has been specified in the DDL
     */
    public TransactionalMode default_transactional_mode = TransactionalMode.off;

    // ******** PROTOCOL MODIFIERS ***********

    public CoordinatorBacklogExecution coordinator_backlog_execution;
    public Boolean coordinator_backlog_execution_multi_key;
    public Boolean coordinator_backlog_execution_claims; // must not be enabled until all nodes support BacklogClaim
    public Boolean coordinator_backlog_execution_yield_to_nearby;
    public Boolean coordinator_backlog_execution_wait; // must not be enabled until all nodes support ReadTxnDataIfReady without NO_WAIT
    public DurationSpec.IntMillisecondsBound coordinator_backlog_execution_linger;
    public Boolean coordinator_backlog_execution_per_dc; // with coordinator_backlog_execution: DESIGNATED
    public DurationSpec.IntMillisecondsBound coordinator_backlog_execution_grace; // with coordinator_backlog_execution: DESIGNATED
    public Boolean relay_remote_apply; // must not be enabled until all nodes support ApplyRelay
    public Boolean coordinator_backlog_execution_persist_locally; // with coordinator_backlog_execution: DESIGNATED and per_dc; must not be enabled until all nodes support BacklogApplied
    // with coordinator_backlog_execution_persist_locally: how long the original coordinator waits for designated replicas in
    // other DCs to report they have applied a transaction before sending its own Apply; latencies are PreAccept latencies
    // (AccordCoordinator PreAcceptLatency), as a proxy for a round-trip to remote DCs
    public String coordinator_backlog_execution_remote_apply_delay = "50ms <= p99(rw) <= 1000ms";
    public Boolean permit_local_delivery;
    public Boolean permit_atomic_incremental_tasks;
    public Boolean permit_coordinator_local_execution; // if disabled, the privileged coordinator optimisation will be counter-productive and should also be disabled
    public TxnId.FastPath permit_fast_path;
    public Boolean permit_track_stable_medium_path;
    public Boolean permit_fast_quorum_medium_path;
    public Boolean always_inform_durable_single_key;
    public ReplicaExecution replica_execution;
    public Float replica_execution_distributed_persist_chance;
    public FastExecution fast_write_execution;
    public FastExecution fast_read_execution;
    public CleanCfkBefore clean_cfk_before;
    public SendStableMessages send_stable;
    /**
     * include the least information expected to be necessary in messages -
     * this is more efficient but may lead to some additional traffic and latency when earlier messages had not arrived
     * (MINIMAL_NO_DEPS must not be enabled until all nodes support it)
     */
    public SendMinimal send_minimal;
    // note: simulator incompatible (for now)
    public Boolean precise_micros;
    public UniqueTimestampReservations unique_timestamp_reservations = UniqueTimestampReservations.HISTOGRAM;
    public UniqueTimestampOnConflict unique_timestamp_on_conflict = UniqueTimestampOnConflict.STALE;
    public DurationSpec.IntMillisecondsBound unique_timestamp_reservation_range = new DurationSpec.IntMillisecondsBound(100);

    public boolean ephemeral_reads = true;

    // for compatibility during live upgrade of 6.0-alpha2; can be turned off in release
    public boolean alpha_compatibility_serialize_write_keys = true;

    public boolean state_cache_listener_jfr_enabled = false;

    public float hard_reject_ratio = 0.5f;
    public int min_soft_reject_count = 100;
    public int max_soft_reject_count = 1000;
    public DurationSpec.LongMicrosecondsBound soft_reject_age = new DurationSpec.LongMicrosecondsBound("10s");
    public DurationSpec.LongMicrosecondsBound soft_reject_cumulative_age = new DurationSpec.LongMicrosecondsBound("60s");


    public DurationSpec.IntSecondsBound commands_for_key_prune_delta = new DurationSpec.IntSecondsBound(1);
    public int commands_for_key_prune_interval = 64;
    public DurationSpec.IntSecondsBound max_conflicts_prune_delta = new DurationSpec.IntSecondsBound(1);

    // number of times we will try to catch up if the catchup was slow (we do not retry if we fail for some other reason)
    public int catchup_on_start_max_slow_attempts = 5;
    public boolean catchup_on_start = true;
    public DurationSpec.IntSecondsBound catchup_on_start_success_latency = new DurationSpec.IntSecondsBound(60);
    public DurationSpec.IntSecondsBound catchup_on_start_fail_latency = new DurationSpec.IntSecondsBound(900);
    // TODO (required): default this to EXIT or REBOOTSTRAP
    public CatchupFallbackMode catchup_on_start_on_timeout = CatchupFallbackMode.IGNORE;
    public CatchupFallbackMode catchup_on_start_on_error = CatchupFallbackMode.IGNORE;
    public CatchupFallbackMode catchup_on_start_on_rebootstrap_fallback = CatchupFallbackMode.IGNORE;
    public DurationSpec.IntSecondsBound shutdown_grace_period = new DurationSpec.IntSecondsBound(15 * 60);

    public boolean execute_waiting_on_start = true;
    public DurationSpec.IntSecondsBound execute_waiting_on_start_timeout = new DurationSpec.IntSecondsBound(0);
    public boolean execute_waiting_on_start_fail_on_timeout = false;

    public enum RangeIndexMode { in_memory, journal_sai }
    public RangeIndexMode range_index_mode = in_memory;

    /**
     * With journal_sai, replay rebuilds the in-memory index of journal segments not yet compacted into the journal table
     * only from the commands it replays (see Replay). Modes weaker than ALL skip commands durable to both the command
     * and data stores, whose routes would then be missing from range searches until their segments are compacted.
     * TODO (required): re-index skipped commands from their stored route, then remove this restriction
     */
    public static void validateReplayMode(RangeIndexMode rangeIndexMode, JournalConfig.ReplayMode replay) throws ConfigurationException
    {
        if (rangeIndexMode == journal_sai && (replay == JournalConfig.ReplayMode.PART_NON_DURABLE || replay == JournalConfig.ReplayMode.NON_DURABLE))
            throw new ConfigurationException("accord.range_index_mode journal_sai requires accord.journal.replay ALL or RESET, not " + replay, false);
    }

    public final JournalConfig journal = new JournalConfig();

    public enum MixedTimeSourceHandling
    {
        reject, log, ignore
    }

    public volatile MixedTimeSourceHandling mixedTimeSourceHandling = MixedTimeSourceHandling.reject;

    public static class JournalConfig implements Params
    {
        public enum ReplayMode
        {
            /**
             * Replay all journal entries and erase local state such as CommandsForKey that can be recreated
             */
            RESET,

            /**
             * Replay all journal entries
             */
            ALL,

            /**
             * Replay journal entries for commands that intersect a non-durable range.
             * Ordinarily it should be necessary to only replay commands that do not intersect any durable ranges.
             */
            PART_NON_DURABLE,

            /**
             * Replay journal entries for commands that are not durable to the data or command stores.
             * THIS MODE IS NOT YET SAFE TO RUN
             */
            NON_DURABLE,

            /**
             * Don't replay, simply rebootstrap, marking our log as incomplete.
             */
            REBOOTSTRAP_INCOMPLETE,

            /**
             * Don't replay, simply rebootstrap, marking our log as corrupted/unavailable.
             */
            REBOOTSTRAP_RESET
        }

        public enum ReplaySavePoint
        {
            NO,
            LATEST
        }

        public enum StopMarkerFailurePolicy
        {
            /**
             * If the start marker exceeds the stop marker then exit, since we cannot guarantee our consensus log is complete.
             */
            EXIT,

            /**
             * @deprecated since alpha release, replaced by ALLOW_UNSAFE_STARTUP for consistency with FailurePolicy.ALLOW_UNSAFE_STARTUP
             */
            @Deprecated(since="6.0")
            UNSAFE_STARTUP,

            /**
             * If the start marker exceeds the stop marker startup, assuming the consensus log has been determined complete externally.
             * Note this is VERY UNSAFE if you care about isolation guarantees.
             */
            ALLOW_UNSAFE_STARTUP,

            REBOOTSTRAP
        }

        public int segmentSize = 32 << 20;
        public int compactMaxSegments = 32;
        public FailurePolicy failurePolicy = FailurePolicy.STOP;
        public ReplayMode replay = ReplayMode.PART_NON_DURABLE;
        public ReplaySavePoint replaySavePoint = ReplaySavePoint.LATEST;
        public int retainSavePoints = 2;
        public StopMarkerFailurePolicy stopMarkerFailurePolicy = StopMarkerFailurePolicy.EXIT;
        public RecoverableCrcFailurePolicy crcFailureOnRebuildPolicy = RecoverableCrcFailurePolicy.FAIL;
        public FlushMode flushMode = FlushMode.PERIODIC;
        public volatile DurationSpec flushPeriod; // pulls default from 'commitlog_sync_period'
        public DurationSpec periodicFlushLagBlock = new DurationSpec.IntMillisecondsBound("1500ms");
        public DurationSpec.IntMillisecondsBound compactionPeriod = new DurationSpec.IntMillisecondsBound("60000ms");
        private volatile long flushCombinedBlockPeriod = Long.MIN_VALUE;
        public Version version = Version.DOWNGRADE_SAFE_VERSION;
        public boolean enable_compaction = true;

        public JournalConfig setFlushPeriod(DurationSpec newFlushPeriod)
        {
            flushPeriod = newFlushPeriod;
            flushCombinedBlockPeriod = Long.MIN_VALUE;
            return this;
        }

        public void setPeriodicFlushLagBlock(DurationSpec newPeriodicFlushLagBlock)
        {
            periodicFlushLagBlock = newPeriodicFlushLagBlock;
            flushCombinedBlockPeriod = Long.MIN_VALUE;
        }

        @Override
        public int segmentSize()
        {
            return segmentSize;
        }

        @Override
        public int compactMaxSegments()
        {
            return compactMaxSegments;
        }

        @Override
        public FailurePolicy failurePolicy()
        {
            return failurePolicy;
        }

        @Override
        public RecoverableCrcFailurePolicy crcFailureOnRebuildPolicy()
        {
            return crcFailureOnRebuildPolicy;
        }

        @Override
        public FlushMode flushMode()
        {
            return flushMode;
        }

        @Override
        public boolean enableCompaction()
        {
            return enable_compaction;
        }

        @Override
        public long compactionPeriod(TimeUnit unit)
        {
            return compactionPeriod.to(unit);
        }

        @JsonIgnore
        @Override
        public long flushPeriod(TimeUnit units)
        {
            return flushPeriod.to(units);
        }

        @JsonIgnore
        @Override
        public long periodicBlockPeriod(TimeUnit units)
        {
            long nanos = flushCombinedBlockPeriod;
            if (nanos >= 0)
                return units.convert(nanos, TimeUnit.NANOSECONDS);

            long flushPeriodNanos = flushPeriod(TimeUnit.NANOSECONDS);
            Invariants.require(flushPeriodNanos > 0);
            nanos = periodicFlushLagBlock.to(TimeUnit.NANOSECONDS) + flushPeriodNanos;
            // it is possible for this to race and cache the wrong value after an update
            flushCombinedBlockPeriod = nanos;
            return nanos;
        }

        /**
         * This is required by the journal, but we don't have multiple versions, so block it from showing up, so we don't need to worry about maintaining it
         */
        @JsonIgnore
        @Override
        public int userVersion()
        {
            return version.version;
        }
    }

    public int commandStoreShardCount()
    {
        return command_store_shard_count.or(DatabaseDescriptor::getAvailableProcessors);
    }
}
