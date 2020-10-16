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

package org.apache.cassandra.service.paxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.config.CFMetaData;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleToLongFunction;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Math.*;
import static java.lang.Math.min;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.*;
import static org.apache.cassandra.service.StorageProxy.casReadMetrics;
import static org.apache.cassandra.service.StorageProxy.casWriteMetrics;
import static org.apache.cassandra.utils.concurrent.WaitManager.Global.waits;

/**
 * <p>A strategy for making back-off decisions for Paxos operations that fail to make progress because of other paxos operations.
 * The strategy is defined by three values: <ul>
 * <li> {@link #min}
 * <li> {@link #max}
 * <li> {@link #minDelta}
 * </ul>
 *
 * <p>All three may be defined dynamically based on a simple calculation over: <ul>
 * <li> {@code pX()} recent experienced latency distribution for successful operations,
 *                 e.g. {@code p50(rw)} the maximum of read and write median latencies,
 *                      {@code p999(r)} the 99.9th percentile of read latencies
 * <li> {@code attempts} the number of failed attempts made by the operation so far
 * <li> {@code constant} a user provided floating point constant
 * </ul>
 *
 * <p>The calculation may take any of these forms
 * <li> constant            {@code $constant$[mu]s}
 * <li> dynamic constant    {@code pX() * constant}
 * <li> dynamic linear      {@code pX() * constant * attempts}
 * <li> dynamic exponential {@code pX() * constant ^ attempts}
 *
 * <p>Furthermore, the dynamic calculations can be bounded with a min/max, like so:
 *  {@code min[mu]s <= dynamic expr <= max[mu]s}
 *
 * e.g.
 * <li> {@code 10ms <= p50(rw)*0.66}
 * <li> {@code 10ms <= p95(rw)*1.8^attempts <= 100ms}
 * <li> {@code 5ms <= p50(rw)*0.5}
 *
 * <p>These calculations are put together to construct a range from which we draw a random number.
 * The period we wait for {@code X} will be drawn so that {@code min <= X < max}.
 *
 * <p>With the constraint that {@code max} must be {@code minDelta} greater than {@code min},
 * but no greater than its expression-defined maximum. {@code max} will be increased up until
 * this point, after which {@code min} will be decreased until this gap is imposed.
 */
public class ContentionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(ContentionStrategy.class);

    private static final Pattern BOUND = Pattern.compile(
                "((?<min>0|[0-9]+[mu]s) *<= *)?" +
                    "p(?<perc>[0-9]+)\\((?<rw>r|w|rw|wr)\\)" +
                    "\\s*([*]\\s*(?<mod>[0-9.]+)?\\s*(?<modkind>[*^]\\s*attempts)?)?" +
                "( *<= *(?<max>0|[0-9]+[mu]s))?" +
                "|(?<const>0|[0-9]+[mu]s)");
    private static final Pattern TIME = Pattern.compile(
                "0|([0-9]+)ms|([0-9]+)us");
    private static final String DEFAULT_MIN = "0 <= p50(rw)*0.66"; // at least 0ms, and at least 66% of median latency
    private static final String DEFAULT_MAX = "10ms <= p95(rw)*1.8^attempts <= 100ms"; // p95 latency with exponential back-off at rate of 1.8^attempts
    private static final String DEFAULT_MIN_DELTA = "5ms <= p50(rw)*0.5"; // at least 5ms, and at least 50% of median latency

    private static volatile ContentionStrategy current;

    // Factories can be useful for testing purposes, to supply custom implementations of selectors and modifiers.
    final static LatencySelectorFactory selectors = new LatencySelectorFactory(){};
    final static LatencyModifierFactory modifiers = new LatencyModifierFactory(){};

    static
    {
        current = new ContentionStrategy(defaultMinWait(), defaultMaxWait(), defaultMinDelta(), Integer.MAX_VALUE);
    }

    static interface LatencySelector
    {
        abstract long select(DoubleToLongFunction readLatencyHistogram, DoubleToLongFunction writeLatencyHistogram);
    }

    static interface LatencySelectorFactory
    {
        default LatencySelector constant(long latency) { return (read, write) -> latency; }
        default LatencySelector read(double percentile) { return (read, write) -> read.applyAsLong(percentile); }
        default LatencySelector write(double percentile) { return (read, write) -> write.applyAsLong(percentile); }
        default LatencySelector maxReadWrite(double percentile) { return (read, write) -> max(read.applyAsLong(percentile), write.applyAsLong(percentile)); }
    }

    static interface LatencyModifier
    {
        long modify(long latency, int attempts);
    }

    static interface LatencyModifierFactory
    {
        default LatencyModifier identity() { return (l, a) -> l; }
        default LatencyModifier multiply(double constant) { return (l, a) -> saturatedCast(l * constant); }
        default LatencyModifier multiplyByAttempts(double multiply) { return (l, a) -> saturatedCast(l * multiply * a); }
        default LatencyModifier multiplyByAttemptsExp(double base) { return (l, a) -> saturatedCast(l * pow(base, a)); }
    }

    static class Bound
    {
        final long min, max, onFailure;
        final LatencyModifier modifier;
        final LatencySelector selector;

        Bound(long min, long max, long onFailure, LatencyModifier modifier, LatencySelector selector)
        {
            Preconditions.checkArgument(min<=max, "min (%s) must be less than or equal to max (%s)", min, max);
            this.min = min;
            this.max = max;
            this.onFailure = onFailure;
            this.modifier = modifier;
            this.selector = selector;
        }

        long get(int attempts)
        {
            try
            {
                long base = selector.select(casReadMetrics.recentLatencyHistogram, casWriteMetrics.recentLatencyHistogram);
                return max(min, min(max, modifier.modify(base, attempts)));
            }
            catch (Throwable t)
            {
                NoSpamLogger.getLogger(logger, 1L, MINUTES).info("", t);
                return onFailure;
            }
        }

        public String toString()
        {
            return "Bound{" +
                   "min=" + min +
                   ", max=" + max +
                   ", onFailure=" + onFailure +
                   ", modifier=" + modifier +
                   ", selector=" + selector +
                   '}';
        }
    }

    final Bound min, max, minDelta;
    final int traceAfterAttempts;

    public ContentionStrategy(String min, String max, String minDelta, int traceAfterAttempts)
    {
        this.min = parseBound(min, true);
        this.max = parseBound(max, false);
        this.minDelta = parseBound(minDelta, true);
        this.traceAfterAttempts = traceAfterAttempts;
    }

    private boolean doWaitForContention(long deadline, int attempts, CFMetaData table, DecoratedKey partitionKey, ConsistencyLevel consistency, boolean isWrite)
    {
        if (attempts >= traceAfterAttempts && !Tracing.isTracing())
        {
            Tracing.instance.newSession(Tracing.TraceType.QUERY);
            Tracing.instance.begin(isWrite ? "Contended Paxos Write" : "Contended Paxos Read",
                    ImmutableMap.of(
                            "keyspace", table.ksName,
                            "table", table.cfName,
                            "partitionKey", table.getKeyValidator().getString(partitionKey.getKey()),
                            "consistency", consistency.name(),
                            "kind", isWrite ? "write" : "read"
                            ));

            logger.info("Tracing contended paxos {} for key {} on {}.{} with trace id {}",
                        isWrite ? "write" : "read",
                        ByteBufferUtil.bytesToHex(partitionKey.getKey()),
                        table.ksName, table.cfName,
                        Tracing.instance.getSessionId());
        }

        long minWaitMicros = min.get(attempts);
        long maxWaitMicros = max.get(attempts);
        long minDeltaMicros = minDelta.get(attempts);

        if (minWaitMicros + minDeltaMicros > maxWaitMicros)
        {
            maxWaitMicros = minWaitMicros + minDeltaMicros;
            if (maxWaitMicros > this.max.max)
            {
                maxWaitMicros = this.max.max;
                minWaitMicros = max(this.min.min, min(this.min.max, maxWaitMicros - minDeltaMicros));
            }
        }

        long wait = MICROSECONDS.toNanos(ThreadLocalRandom.current().nextLong(minWaitMicros, maxWaitMicros));
        long until = System.nanoTime() + wait;
        if (until >= deadline)
            return false;

        try
        {
            waits().waitUntil(until);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    static boolean waitForContention(long deadline, int attempts, CFMetaData table, DecoratedKey partitionKey, ConsistencyLevel consistency, boolean isWrite)
    {
        return current.doWaitForContention(deadline, attempts, table, partitionKey, consistency, isWrite);
    }

    static class ParsedStrategy
    {
        final String min, max, minDelta;
        final ContentionStrategy strategy;

        ParsedStrategy(String min, String max, String minDelta, ContentionStrategy strategy)
        {
            this.min = min;
            this.max = max;
            this.minDelta = minDelta;
            this.strategy = strategy;
        }
    }

    @VisibleForTesting
    static ParsedStrategy parseStrategy(String spec)
    {
        String[] args = spec.split(",");
        String min = find(args, "min");
        String max = find(args, "max");
        String minDelta = find(args, "delta");
        String trace = find(args, "trace");

        if (min == null) min = defaultMinWait();
        if (max == null) max = defaultMaxWait();
        if (minDelta == null) minDelta = defaultMinDelta();
        int traceAfterAttempts = trace == null ? current.traceAfterAttempts: Integer.parseInt(trace);

        ContentionStrategy strategy = new ContentionStrategy(min, max, minDelta, traceAfterAttempts);
        return new ParsedStrategy(min, max, minDelta, strategy);
    }


    public static void setStrategy(String spec)
    {
        ParsedStrategy parsed = parseStrategy(spec);
        current = parsed.strategy;
        setPaxosContentionMinWait(parsed.min);
        setPaxosContentionMaxWait(parsed.max);
        setPaxosContentionMinDelta(parsed.minDelta);
    }

    public static String getStrategySpec()
    {
        return "min=" + defaultMinWait()
                + ",max=" + defaultMaxWait()
                + ",delta=" + defaultMinDelta()
                + ",trace=" + current.traceAfterAttempts;
    }

    private static String find(String[] args, String param)
    {
        return stream(args).filter(s -> s.startsWith(param + '='))
                .map(s -> s.substring(param.length() + 1))
                .findFirst().orElse(null);
    }

    private static LatencySelector parseLatencySelector(Matcher m, LatencySelectorFactory selectors)
    {
        double percentile = parseDouble("0." + m.group("perc"));
        String rw = m.group("rw");
        if (rw.length() == 2)
            return selectors.maxReadWrite(percentile);
        else if ("r".equals(rw))
            return selectors.read(percentile);
        else
            return selectors.write(percentile);
    }

    private static LatencyModifier parseLatencyModifier(Matcher m, LatencyModifierFactory modifiers)
    {
        String mod = m.group("mod");
        if (mod == null)
            return modifiers.identity();

        double modifier = parseDouble(mod);

        String modkind = m.group("modkind");
        if (modkind == null)
            return modifiers.multiply(modifier);

        if (modkind.startsWith("*"))
            return modifiers.multiplyByAttempts(modifier);
        else if (modkind.startsWith("^"))
            return modifiers.multiplyByAttemptsExp(modifier);
        else
            throw new IllegalArgumentException("Unrecognised attempt modifier: " + modkind);
    }

    static long saturatedCast(double v)
    {
        if (v > Long.MAX_VALUE)
            return Long.MAX_VALUE;
        return (long) v;
    }

    static Bound parseBound(String input, boolean isMin)
    {
        return parseBound(input, isMin, selectors, modifiers);
    }

    @VisibleForTesting
    static Bound parseBound(String input, boolean isMin, LatencySelectorFactory selectors, LatencyModifierFactory modifiers)
    {
        Matcher m = BOUND.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException(input + " does not match " + BOUND);

        String maybeConst = m.group("const");
        if (maybeConst != null)
        {
            long v = parseInMicros(maybeConst);
            return new Bound(v, v, v, modifiers.identity(), selectors.constant(v));
        }

        long min = parseInMicros(m.group("min"), 0);
        long max = parseInMicros(m.group("max"), maxQueryTimeout() / 2);
        return new Bound(min, max, isMin ? min : max, parseLatencyModifier(m, modifiers), parseLatencySelector(m, selectors));
    }

    private static long parseInMicros(String input, long orElse)
    {
        if (input == null)
            return orElse;

        return parseInMicros(input);
    }

    private static long parseInMicros(String input)
    {
        Matcher m = TIME.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException(input + " does not match " + TIME);

        String text;
        if (null != (text = m.group(1)))
            return parseInt(text) * 1000;
        else if (null != (text = m.group(2)))
            return parseInt(text);
        else
            return 0;
    }

    @VisibleForTesting
    static String defaultMinWait()
    {
        return orElse(DatabaseDescriptor::getPaxosContentionMinWait, DEFAULT_MIN);
    }

    @VisibleForTesting
    static String defaultMaxWait()
    {
        return orElse(DatabaseDescriptor::getPaxosContentionMaxWait, DEFAULT_MAX);
    }

    @VisibleForTesting
    static String defaultMinDelta()
    {
        return orElse(DatabaseDescriptor::getPaxosContentionMinDelta, DEFAULT_MIN_DELTA);
    }

    @VisibleForTesting
    static long maxQueryTimeout()
    {
        return MILLISECONDS.toMicros(max(max(getCasContentionTimeout(), getWriteRpcTimeout()), getReadRpcTimeout()));
    }

    private static String orElse(Supplier<String> get, String orElse)
    {
        String result = get.get();
        return result != null ? result : orElse;
    }
}
