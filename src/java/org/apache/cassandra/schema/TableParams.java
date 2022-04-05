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
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.reads.PercentileSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.utils.BloomCalculations;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

public final class TableParams
{
    public enum Option
    {
        BLOOM_FILTER_FP_CHANCE,
        CACHING,
        COMMENT,
        COMPACTION,
        COMPRESSION,
        DEFAULT_TIME_TO_LIVE,
        EXTENSIONS,
        GC_GRACE_SECONDS,
        MAX_INDEX_INTERVAL,
        MEMTABLE_FLUSH_PERIOD_IN_MS,
        MIN_INDEX_INTERVAL,
        SPECULATIVE_RETRY,
        ADDITIONAL_WRITE_POLICY,
        CRC_CHECK_CHANCE,
        CDC,
        READ_REPAIR,
        DISABLE_REPAIRS,
        DISABLE_CHRISTMAS_PATCH;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public final String comment;
    public final double bloomFilterFpChance;
    public final double crcCheckChance;
    public final int gcGraceSeconds;
    public final int defaultTimeToLive;
    public final int memtableFlushPeriodInMs;
    public final int minIndexInterval;
    public final int maxIndexInterval;
    public final SpeculativeRetryPolicy speculativeRetry;
    public final SpeculativeRetryPolicy additionalWritePolicy;
    public final CachingParams caching;
    public final CompactionParams compaction;
    public final CompressionParams compression;
    public final ImmutableMap<String, ByteBuffer> extensions;
    public final boolean cdc;
    public final ReadRepairStrategy readRepair;

    // Used to disable the feature for specific tables where it is enabled at the instance level, in cassandra.yaml.
    // By default the table level setting is false, i.e. the override is not in place for the table until explicitly
    // set. See extractXmasPatchParam(Map<String, ByteBuffer>)
    public final boolean disableChristmasPatch;
    public final boolean disableRepairs;

    private TableParams(Builder builder)
    {
        comment = builder.comment;
        bloomFilterFpChance = builder.bloomFilterFpChance == null
                            ? builder.compaction.defaultBloomFilterFbChance()
                            : builder.bloomFilterFpChance;
        crcCheckChance = builder.crcCheckChance;
        gcGraceSeconds = builder.gcGraceSeconds;
        defaultTimeToLive = builder.defaultTimeToLive;
        memtableFlushPeriodInMs = builder.memtableFlushPeriodInMs;
        minIndexInterval = builder.minIndexInterval;
        maxIndexInterval = builder.maxIndexInterval;
        speculativeRetry = builder.speculativeRetry;
        additionalWritePolicy = builder.additionalWritePolicy;
        caching = builder.caching;
        compaction = builder.compaction;
        compression = builder.compression;
        extensions = builder.extensions;
        cdc = builder.cdc;
        readRepair = builder.readRepair;
        disableChristmasPatch = extractExtensionOption(builder.extensions, Option.DISABLE_CHRISTMAS_PATCH);
        disableRepairs = extractExtensionOption(builder.extensions, Option.DISABLE_REPAIRS);
    }

    private boolean extractExtensionOption(Map<String, ByteBuffer> extensions, Option option)
    {
        // If the option is set at the instance level, it can be overridden and
        // turned off for specific tables. When this is done, the table level flag is
        // persisted as an entry in the table extensions, which is a map<text, blob>. The
        // default is for a table _not_ to override the instance setting, so a missing or
        // empty value in the map indicates the override is not in place for this table.
        // Conversely, the presence of _any_ value in the map can be read as the override
        // being turned on (i.e. christmas patch is disabled) for the table.
        ByteBuffer val = extensions.get(option.name());
        return null != val && val.hasRemaining();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(TableParams params)
    {
        return new Builder().bloomFilterFpChance(params.bloomFilterFpChance)
                            .caching(params.caching)
                            .comment(params.comment)
                            .compaction(params.compaction)
                            .compression(params.compression)
                            .crcCheckChance(params.crcCheckChance)
                            .defaultTimeToLive(params.defaultTimeToLive)
                            .gcGraceSeconds(params.gcGraceSeconds)
                            .maxIndexInterval(params.maxIndexInterval)
                            .memtableFlushPeriodInMs(params.memtableFlushPeriodInMs)
                            .minIndexInterval(params.minIndexInterval)
                            .speculativeRetry(params.speculativeRetry)
                            .additionalWritePolicy(params.additionalWritePolicy)
                            .extensions(params.extensions)
                            .cdc(params.cdc)
                            .readRepair(params.readRepair);
    }

    public Builder unbuild()
    {
        return builder(this);
    }

    public void validate()
    {
        compaction.validate();
        compression.validate();

        double minBloomFilterFpChanceValue = BloomCalculations.minSupportedBloomFilterFpChance();
        if (bloomFilterFpChance <=  minBloomFilterFpChanceValue || bloomFilterFpChance > 1)
        {
            fail("%s must be larger than %s and less than or equal to 1.0 (got %s)",
                 Option.BLOOM_FILTER_FP_CHANCE,
                 minBloomFilterFpChanceValue,
                 bloomFilterFpChance);
        }

        if (crcCheckChance < 0 || crcCheckChance > 1.0)
        {
            fail("%s must be larger than or equal to 0 and smaller than or equal to 1.0 (got %s)",
                 Option.CRC_CHECK_CHANCE,
                 crcCheckChance);
        }

        if (defaultTimeToLive < 0)
            fail("%s must be greater than or equal to 0 (got %s)", Option.DEFAULT_TIME_TO_LIVE, defaultTimeToLive);

        if (defaultTimeToLive > Attributes.MAX_TTL)
            fail("%s must be less than or equal to %d (got %s)", Option.DEFAULT_TIME_TO_LIVE, Attributes.MAX_TTL, defaultTimeToLive);

        if (gcGraceSeconds < 0)
            fail("%s must be greater than or equal to 0 (got %s)", Option.GC_GRACE_SECONDS, gcGraceSeconds);

        if (minIndexInterval < 1)
            fail("%s must be greater than or equal to 1 (got %s)", Option.MIN_INDEX_INTERVAL, minIndexInterval);

        if (maxIndexInterval < minIndexInterval)
        {
            fail("%s must be greater than or equal to %s (%s) (got %s)",
                 Option.MAX_INDEX_INTERVAL,
                 Option.MIN_INDEX_INTERVAL,
                 minIndexInterval,
                 maxIndexInterval);
        }

        if (memtableFlushPeriodInMs < 0)
            fail("%s must be greater than or equal to 0 (got %s)", Option.MEMTABLE_FLUSH_PERIOD_IN_MS, memtableFlushPeriodInMs);
    }

    private static void fail(String format, Object... args)
    {
        throw new ConfigurationException(format(format, args));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TableParams))
            return false;

        TableParams p = (TableParams) o;

        return comment.equals(p.comment)
            && bloomFilterFpChance == p.bloomFilterFpChance
            && crcCheckChance == p.crcCheckChance
            && gcGraceSeconds == p.gcGraceSeconds
            && defaultTimeToLive == p.defaultTimeToLive
            && memtableFlushPeriodInMs == p.memtableFlushPeriodInMs
            && minIndexInterval == p.minIndexInterval
            && maxIndexInterval == p.maxIndexInterval
            && speculativeRetry.equals(p.speculativeRetry)
            && caching.equals(p.caching)
            && compaction.equals(p.compaction)
            && compression.equals(p.compression)
            && extensions.equals(p.extensions)
            && cdc == p.cdc
            && readRepair == p.readRepair;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(comment,
                                bloomFilterFpChance,
                                crcCheckChance,
                                gcGraceSeconds,
                                defaultTimeToLive,
                                memtableFlushPeriodInMs,
                                minIndexInterval,
                                maxIndexInterval,
                                speculativeRetry,
                                caching,
                                compaction,
                                compression,
                                extensions,
                                cdc,
                                readRepair);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add(Option.COMMENT.toString(), comment)
                          .add(Option.BLOOM_FILTER_FP_CHANCE.toString(), bloomFilterFpChance)
                          .add(Option.CRC_CHECK_CHANCE.toString(), crcCheckChance)
                          .add(Option.GC_GRACE_SECONDS.toString(), gcGraceSeconds)
                          .add(Option.DEFAULT_TIME_TO_LIVE.toString(), defaultTimeToLive)
                          .add(Option.MEMTABLE_FLUSH_PERIOD_IN_MS.toString(), memtableFlushPeriodInMs)
                          .add(Option.MIN_INDEX_INTERVAL.toString(), minIndexInterval)
                          .add(Option.MAX_INDEX_INTERVAL.toString(), maxIndexInterval)
                          .add(Option.SPECULATIVE_RETRY.toString(), speculativeRetry)
                          .add(Option.CACHING.toString(), caching)
                          .add(Option.COMPACTION.toString(), compaction)
                          .add(Option.COMPRESSION.toString(), compression)
                          .add(Option.EXTENSIONS.toString(), extensions)
                          .add(Option.CDC.toString(), cdc)
                          .add(Option.READ_REPAIR.toString(), readRepair)
                          .add(Option.DISABLE_REPAIRS.toString(), disableRepairs)
                          .add(Option.DISABLE_CHRISTMAS_PATCH.toString(), disableChristmasPatch)
                          .toString();
    }

    public void appendCqlTo(CqlBuilder builder)
    {
        // option names should be in alphabetical order
        builder.append("additional_write_policy = ").appendWithSingleQuotes(additionalWritePolicy.toString())
               .newLine()
               .append("AND bloom_filter_fp_chance = ").append(bloomFilterFpChance)
               .newLine()
               .append("AND caching = ").append(caching.asMap())
               .newLine()
               .append("AND cdc = ").append(cdc)
               .newLine()
               .append("AND comment = ").appendWithSingleQuotes(comment)
               .newLine()
               .append("AND compaction = ").append(compaction.asMap())
               .newLine()
               .append("AND compression = ").append(compression.asMap())
               .newLine()
               .append("AND crc_check_chance = ").append(crcCheckChance)
               .newLine()
               .append("AND default_time_to_live = ").append(defaultTimeToLive)
               .newLine()
               .append("AND disable_christmas_patch = ").append(disableChristmasPatch)
               .newLine()
               .append("AND disable_repairs = ").append(disableRepairs)
               .newLine()
               .append("AND extensions = ").append(extensions.entrySet()
                                                             .stream()
                                                             .collect(toMap(Entry::getKey,
                                                                             e -> "0x" + ByteBufferUtil.bytesToHex(e.getValue()))),
                                                   false)
               .newLine()
               .append("AND gc_grace_seconds = ").append(gcGraceSeconds)
               .newLine()
               .append("AND max_index_interval = ").append(maxIndexInterval)
               .newLine()
               .append("AND memtable_flush_period_in_ms = ").append(memtableFlushPeriodInMs)
               .newLine()
               .append("AND min_index_interval = ").append(minIndexInterval)
               .newLine()
               .append("AND read_repair = ").appendWithSingleQuotes(readRepair.toString())
               .newLine()
               .append("AND speculative_retry = ").appendWithSingleQuotes(speculativeRetry.toString());

    }

    public static final class Builder
    {
        private String comment = "";
        private Double bloomFilterFpChance;
        private double crcCheckChance = 1.0;
        private int gcGraceSeconds = 864000; // 10 days
        private int defaultTimeToLive = 0;
        private int memtableFlushPeriodInMs = 0;
        private int minIndexInterval = 128;
        private int maxIndexInterval = 2048;
        private SpeculativeRetryPolicy speculativeRetry = PercentileSpeculativeRetryPolicy.NINETY_NINE_P;
        private SpeculativeRetryPolicy additionalWritePolicy = PercentileSpeculativeRetryPolicy.NINETY_NINE_P;
        private CachingParams caching = CachingParams.DEFAULT;
        private CompactionParams compaction = CompactionParams.DEFAULT;
        private CompressionParams compression = CompressionParams.DEFAULT;
        private ImmutableMap<String, ByteBuffer> extensions = ImmutableMap.of();
        private boolean cdc;
        private ReadRepairStrategy readRepair = ReadRepairStrategy.BLOCKING;

        public Builder()
        {
        }

        public TableParams build()
        {
            return new TableParams(this);
        }

        public Builder comment(String val)
        {
            comment = val;
            return this;
        }

        public Builder bloomFilterFpChance(double val)
        {
            bloomFilterFpChance = val;
            return this;
        }

        public Builder crcCheckChance(double val)
        {
            crcCheckChance = val;
            return this;
        }

        public Builder gcGraceSeconds(int val)
        {
            gcGraceSeconds = val;
            return this;
        }

        public Builder defaultTimeToLive(int val)
        {
            defaultTimeToLive = val;
            return this;
        }

        public Builder memtableFlushPeriodInMs(int val)
        {
            memtableFlushPeriodInMs = val;
            return this;
        }

        public Builder minIndexInterval(int val)
        {
            minIndexInterval = val;
            return this;
        }

        public Builder maxIndexInterval(int val)
        {
            maxIndexInterval = val;
            return this;
        }

        public Builder speculativeRetry(SpeculativeRetryPolicy val)
        {
            speculativeRetry = val;
            return this;
        }

        public Builder additionalWritePolicy(SpeculativeRetryPolicy val)
        {
            additionalWritePolicy = val;
            return this;
        }

        public Builder caching(CachingParams val)
        {
            caching = val;
            return this;
        }

        public Builder compaction(CompactionParams val)
        {
            compaction = val;
            return this;
        }

        public Builder compression(CompressionParams val)
        {
            compression = val;
            return this;
        }

        public Builder cdc(boolean val)
        {
            cdc = val;
            return this;
        }

        public Builder readRepair(ReadRepairStrategy val)
        {
            readRepair = val;
            return this;
        }

        private void maybeAdd(Map<String, ByteBuffer> val, Option option) {
            // Before replacing any existing extensions map, extract the options setting, if present,
            // so we can re-apply it as long as it doesn't conflict ith the supplied new map (this is
            // unlikely as there's no exposed way to actually set extensions).
            if (val.containsKey(option.name())
                || !extensions.containsKey(option.name()))
            {
                // either the option wasn't set previously, or it is explicitly set in
                // this new map, so we can just copy the supplied map wholesale
                extensions = ImmutableMap.copyOf(val);
            }
            else
            {
                // the entry is present in the existing map, but not the new one. Add it
                // to the supplied map. The value associated with the key is not important.
                extensions = ImmutableMap.<String, ByteBuffer>builder()
                                         .putAll(val)
                                         .put(option.name(), ByteBuffer.wrap(new byte[]{1}))
                                         .build();
            }
        }

        public Builder extensions(Map<String, ByteBuffer> val)
        {
            maybeAdd(val, Option.DISABLE_CHRISTMAS_PATCH);
            maybeAdd(val, Option.DISABLE_REPAIRS);
            return this;
        }

        private Builder excludeExtentionOption(Option opt, boolean val) {
            ImmutableMap.Builder<String, ByteBuffer> builder = ImmutableMap.builder();
            builder.putAll(Maps.filterKeys(extensions, k -> k != null && !k.equals(opt.name())));
            if (val)
                builder.put(opt.name(), ByteBuffer.wrap(new byte[]{1}));
            extensions = builder.build();
            return this;
        }

        public Builder disableRepairs(boolean val)
        {
            // like christmas patch option, this is in extension map for internal tooling to flag the table to
            // be ignored for repairs
            return excludeExtentionOption(Option.DISABLE_REPAIRS, val);
        }

        public Builder disableChristmasPatch(boolean val)
        {
            // Add/replace a serialized value for the xmas patch option in the
            // extensions map. We store this like so as we don't have to modify the
            // system_schema.tables schema to accomodate the xmas patch flag
            // Note: we only insert an entry into the map if val == true. If
            // the key is not present in the map when the TableParams are built,
            // the feature is not disabled at the table level.
            return excludeExtentionOption(Option.DISABLE_CHRISTMAS_PATCH, val);
        }
    }
}
