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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Tracing;
import accord.coordinate.Coordination.CoordinationKind;
import accord.local.CommandStore;
import accord.primitives.Participants;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.service.accord.AccordTracing.BucketMode.LEAKY;
import static org.apache.cassandra.service.accord.AccordTracing.BucketMode.SAMPLE;

public class AccordTracing extends AccordCoordinatorMetrics.Listener
{
    private static final int MAX_EVENTS = 10000;
    private static final Logger logger = LoggerFactory.getLogger(AccordTracing.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    public enum BucketMode
    {
        LEAKY, SAMPLE, RING;

        int position(int permits, int total)
        {
            switch (this)
            {
                default: throw UnhandledEnum.unknown(this);
                case LEAKY: return Integer.MAX_VALUE;
                case RING: return total % permits;
                case SAMPLE: return ThreadLocalRandom.current().nextInt(total);
            }
        }
    }

    public interface ConsumeState
    {
        void accept(TxnId txnId, CoordinationKind eventType, TxnEvents state);
    }

    public static class Message
    {
        public final long atNanos;
        public final int commandStoreId;
        public final String message;

        Message(int commandStoreId, String message, long atLeastNanos)
        {
            this.commandStoreId = commandStoreId;
            this.message = message;
            this.atNanos = Math.max(atLeastNanos, Clock.Global.nanoTime());
        }

        @Override
        public String toString()
        {
            return message;
        }
    }

    public static class TxnEvent implements Tracing, Comparable<TxnEvent>
    {
        public final long idMicros = uniqueNowMicros();
        public final long atNanos = Clock.Global.nanoTime();
        final List<Message> messages = new ArrayList<>();

        @Override
        public void trace(CommandStore commandStore, String s)
        {
            long prevNanos = messages.isEmpty() ? 0 : messages.get(messages.size() - 1).atNanos;
            int id = commandStore == null ? -1 : commandStore.id();
            if (s.length() > 1000)
                s = s.substring(0, 1000);
            messages.add(new Message(id, s, prevNanos + 1));
        }

        @Override
        public int compareTo(TxnEvent that)
        {
            return Long.compareUnsigned(this.idMicros, that.idMicros);
        }

        public List<Message> messages()
        {
            return Collections.unmodifiableList(messages);
        }
    }

    public static class TxnEvents
    {
        private BucketMode mode = LEAKY;
        private TracePatternState owner;
        private int permits, size, total;
        private float chance = 1.0f;
        private TxnEvent[] events;

        void addInternal(TxnEvent event)
        {
            if (events == null) events = new TxnEvent[10];
            else if (size == events.length) events = Arrays.copyOf(events, events.length * 2);
            events[size++] = event;
        }

        void truncate(int eraseBefore)
        {
            System.arraycopy(events, eraseBefore, events, 0, size - eraseBefore);
            Arrays.fill(events, size - eraseBefore, size, null);
            size -= eraseBefore;
        }

        public boolean hasOwner()
        {
            return owner != null;
        }

        public int permits()
        {
            return permits;
        }

        public int total()
        {
            return total;
        }

        public float chance()
        {
            return chance;
        }

        public BucketMode mode()
        {
            return mode;
        }

        public void forEach(Consumer<TxnEvent> forEach)
        {
            for (int i = 0 ; i < size ; ++i)
                forEach.accept(events[i]);
        }

        public TxnEvent get(int index)
        {
            return events[index];
        }

        private void set(int index, TxnEvent e)
        {
            events[index] = e;
        }

        public boolean isEmpty()
        {
            return size == 0;
        }

        public int size()
        {
            return size;
        }
    }

    enum NewOrFailure
    {
        NEW, FAILURE
    }

    public static class CoordinationKinds extends TinyEnumSet<CoordinationKind>
    {
        final boolean printAsSubtraction;
        public CoordinationKinds(boolean printAsSubtraction, int bitset)
        {
            super(bitset);
            this.printAsSubtraction = printAsSubtraction;
        }

        @Override
        public String toString()
        {
            if (printAsSubtraction)
                return '-' + toString(CoordinationKind.ALL.not(this).bitset());
            return '+' + toString(bitset, CoordinationKind::forOrdinal);
        }

        public static CoordinationKinds parse(String input)
        {
            if (input.length() < 3 || input.charAt(1) != '{' || input.charAt(input.length() - 1) != '}' || (input.charAt(0) != '+' && input.charAt(0) != '-'))
                throw new IllegalArgumentException("Invalid CoordinationKinds specification: " + input);

            int bits = 0;
            for (String name : input.substring(2, input.length() - 1).split("\\s*,\\s*"))
                bits |= TinyEnumSet.encode(CoordinationKind.valueOf(name));

            if (input.charAt(0) == '-')
                return new CoordinationKinds(true, CoordinationKind.ALL.bitset() & ~bits);
            return new CoordinationKinds(false, bits);
        }

        private static String toString(int bitset)
        {
            return TinyEnumSet.toString(bitset, CoordinationKind::forOrdinal);
        }
    }

    public static class TxnKindsAndDomains
    {
        static final int ALL_KINDS = Txn.Kind.All.bitset();
        final boolean printAsSubtraction;
        final int keys, ranges;
        public TxnKindsAndDomains(boolean printAsSubtraction, int keys, int ranges)
        {
            this.printAsSubtraction = printAsSubtraction;
            this.keys = keys;
            this.ranges = ranges;
        }

        boolean matches(TxnId txnId)
        {
            int bits = txnId.is(Routable.Domain.Key) ? keys : ranges;
            return TinyEnumSet.contains(bits, txnId.kind());
        }

        @Override
        public String toString()
        {
            if (printAsSubtraction)
                return '-' + toString(ALL_KINDS & ~keys, ALL_KINDS & ~ranges);
            return '+' + toString(keys, ranges);
        }

        public static TxnKindsAndDomains parse(String input)
        {
            if (input.length() < 3 || input.charAt(1) != '{' || input.charAt(input.length() - 1) != '}' || (input.charAt(0) != '+' && input.charAt(0) != '-'))
                throw new IllegalArgumentException("Invalid TxnKindsAndDomain specification: " + input);

            int keys = 0, ranges = 0;
            for (String element : input.substring(2, input.length() - 1).split("\\s*,\\s*"))
            {
                if (element.length() != 2)
                    throw new IllegalArgumentException("Invalid TxnKindsAndDomain element: " + element);

                int kinds;
                if (element.charAt(1) == '*') kinds = ALL_KINDS;
                else
                {
                    Txn.Kind kind = Txn.Kind.forShortName(element.charAt(1));
                    if (kind == null) throw new IllegalArgumentException("Unknown Txn.Kind: " + element.charAt(1));
                    kinds = TinyEnumSet.encode(kind);
                }

                switch (element.charAt(0))
                {
                    default: throw new IllegalArgumentException("Invalid TxnKindsAndDomain element: " + element);
                    case '*': keys |= kinds; ranges |= kinds; break;
                    case 'K': keys |= kinds; break;
                    case 'R': ranges |= kinds; break;
                }
            }

            if (input.charAt(0) == '-')
                return new TxnKindsAndDomains(true, ALL_KINDS & ~keys, ALL_KINDS & ~ranges);
            return new TxnKindsAndDomains(false, keys, ranges);
        }

        private static String toString(int keys, int ranges)
        {
            StringBuilder out = new StringBuilder("{");
            if (keys != 0)
            {
                if (keys == ALL_KINDS) out.append("K*");
                else TinyEnumSet.append(keys, Txn.Kind::forOrdinal, k -> "K" + k.shortName(), out);
            }

            if (ranges != 0)
            {
                if (keys != 0) out.append(',');
                if (ranges == ALL_KINDS) out.append("R*");
                else TinyEnumSet.append(keys, Txn.Kind::forOrdinal, k -> "R" + k.shortName(), out);
            }
            out.append('}');
            return out.toString();
        }
    }

    public static class TracePattern
    {
        private static final TracePattern EMPTY = new TracePattern(null, null, null, null, null, 1.0f);

        public final TxnKindsAndDomains kinds;
        public final Boolean isCreator;
        public final Participants<?> intersects;
        public final CoordinationKinds traceNew;
        public final CoordinationKinds traceFailures;
        public final float chance;

        public TracePattern(TxnKindsAndDomains kinds, Boolean isCreator, @Nullable Participants<?> intersects, CoordinationKinds traceNew, CoordinationKinds traceFailures, float chance)
        {
            this.kinds = kinds;
            this.isCreator = isCreator;
            this.intersects = intersects;
            this.traceNew = traceNew;
            this.traceFailures = traceFailures;
            this.chance = chance;
        }

        public TracePattern withKinds(TxnKindsAndDomains kinds)
        {
            return new TracePattern(kinds, isCreator, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withIsCreator(Boolean isCreator)
        {
            return new TracePattern(kinds, isCreator, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withIntersects(Participants<?> intersects)
        {
            return new TracePattern(kinds, isCreator, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withTraceNew(CoordinationKinds traceNew)
        {
            return new TracePattern(kinds, isCreator, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withTraceFailures(CoordinationKinds traceFailures)
        {
            return new TracePattern(kinds, isCreator, intersects, traceNew, traceFailures, chance);
        }

        public TracePattern withChance(float chance)
        {
            return new TracePattern(kinds, isCreator, intersects, traceNew, traceFailures, chance);
        }

        boolean matches(TxnId txnId, @Nullable Participants<?> participants, CoordinationKind kind, NewOrFailure newOrFailure)
        {
            if (isCreator != null
                && isCreator != txnId.node.equals(AccordService.instance().nodeId()))
                return false;

            if (kinds != null && !kinds.matches(txnId))
                return false;

            TinyEnumSet<CoordinationKind> testKind = newOrFailure == NewOrFailure.NEW ? traceNew : traceFailures;
            if (testKind == null || !testKind.contains(kind))
                return false;

            if (intersects != null && (participants == null || !intersects.intersects(participants)))
                return false;

            return chance >= 1.0f || ThreadLocalRandom.current().nextFloat() > chance;
        }
    }

    public class TracePatternState
    {
        final int id;

        private volatile TracePattern pattern;
        private BucketMode bucketMode = SAMPLE;
        private int permits;
        private int total;
        private BucketMode childBucketMode = SAMPLE;
        private int childTypePermits;
        private CoordinationKinds childTypes = new CoordinationKinds(false, 0);

        private final List<TxnId> txnIds = new ArrayList<>();

        public TracePatternState(int id)
        {
            this.pattern = TracePattern.EMPTY;
            this.id = id;
        }

        public int id() { return id; }
        public TracePattern pattern() { return pattern; }
        public int permits() { return permits; }
        public BucketMode mode() { return bucketMode; }
        public int total() { return total; }
        public BucketMode childMode() { return childBucketMode; }
        public int childTypePermits() { return childTypePermits; }
        public CoordinationKinds childTypes() { return childTypes; }

        public int size()
        {
            return txnIds.size();
        }

        public TxnId get(int index)
        {
            return txnIds.get(index);
        }

        boolean maybeAdd(TxnId txnId, @Nullable Participants<?> participants, CoordinationKind kind, NewOrFailure newOrFailure)
        {
            return pattern.matches(txnId, participants, kind, newOrFailure) && maybeAdd(txnId);
        }

        private synchronized boolean maybeAdd(TxnId txnId)
        {
            if (permits == 0 || childTypePermits == 0 || childTypes.isEmpty())
                return false;

            ++total;
            if (permits > txnIds.size())
            {
                if (!trace(txnId))
                    return false;
                txnIds.add(txnId);
                return true;
            }

            if (++total < 0)
                total = Integer.MAX_VALUE;

            int position = bucketMode.position(permits, total);

            if (position >= permits || !trace(txnId))
                return false;

            untrace(txnIds.get(position));
            txnIds.set(position, txnId);
            return true;
        }

        private synchronized void untrace(TxnId txnId)
        {
            txnIdMap.compute(txnId, (ignore, cur) -> {
                if (cur == null)
                    return null;

                cur.values().removeIf(events -> events.owner == this && truncateAndShouldRemove(events));

                return cur.isEmpty() ? null : cur;
            });
        }

        private synchronized boolean trace(TxnId txnId)
        {
            EnumMap<CoordinationKind, TxnEvents> map = new EnumMap<>(CoordinationKind.class);
            for (CoordinationKind eventType : childTypes.iterable(CoordinationKind::forOrdinal))
            {
                TxnEvents events = new TxnEvents();
                events.mode = childBucketMode;
                events.permits = childTypePermits;
                events.owner = this;
                map.put(eventType, events);
            }
            return null == txnIdMap.putIfAbsent(txnId, map);
        }

        synchronized void set(Function<TracePattern, TracePattern> pattern, BucketMode newBucketMode, BucketMode newChildBucketMode, CoordinationKinds newChildTypes, int newChildTypePermits, int newPermits, int newTotal)
        {
            Invariants.require(newPermits != 0);
            Invariants.require(newChildTypePermits != 0);
            this.pattern = pattern.apply(this.pattern);
            if (newBucketMode != null)
                this.bucketMode = newBucketMode;
            if (newChildBucketMode != null)
                this.childBucketMode = newChildBucketMode;
            if (newChildTypePermits >= 0)
                this.childTypePermits = newChildTypePermits;
            if (newChildTypes != null)
                this.childTypes = newChildTypes;
            if (newPermits >= 0)
                this.permits = newPermits;
            if (newTotal >= 0)
                this.total = newTotal;
        }

        synchronized void clear()
        {
            for (TxnId txnId : txnIds)
                untrace(txnId);
            txnIds.clear();
        }
    }

    private static final AtomicLong lastNowMicros = new AtomicLong();
    private static long uniqueNowMicros()
    {
        long nowMicros = Clock.Global.currentTimeMillis() * 1000;
        while (true)
        {
            long last = lastNowMicros.get();
            if (last >= nowMicros)
                return lastNowMicros.incrementAndGet();
            if (lastNowMicros.compareAndSet(last, nowMicros))
                return nowMicros;
        }
    }

    final Map<TxnId, EnumMap<CoordinationKind, TxnEvents>> txnIdMap = new ConcurrentHashMap<>();
    final CopyOnWriteArrayList<TracePatternState> allPatterns = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<TracePatternState> traceNewPatterns = new CopyOnWriteArrayList<>();
    final AtomicInteger totalEventCount = new AtomicInteger();

    public Tracing trace(TxnId txnId, @Nullable Participants<?> participants, CoordinationKind eventType)
    {
        if (!txnIdMap.containsKey(txnId) && !maybeTrace(txnId, participants, eventType, NewOrFailure.NEW, traceNewPatterns))
            return null;

        class Register implements BiFunction<TxnId, EnumMap<CoordinationKind, TxnEvents>, EnumMap<CoordinationKind, TxnEvents>>
        {
            TxnEvent event;

            @Override
            public EnumMap<CoordinationKind, TxnEvents> apply(TxnId id, EnumMap<CoordinationKind, TxnEvents> cur)
            {
                if (cur == null)
                    return null;

                TxnEvents curState = cur.get(eventType);
                if (curState == null || curState.permits == 0)
                    return cur;

                if (curState.chance < 1.0f && ThreadLocalRandom.current().nextFloat() >= curState.chance)
                    return cur;

                if (curState.permits > curState.size)
                {
                    if (totalEventCount.incrementAndGet() >= MAX_EVENTS)
                    {
                        totalEventCount.decrementAndGet();
                        ClientWarn.instance.warn("Too many Accord trace events stored already; delete some to continue tracing");
                        noSpamLogger.warn("Too many Accord trace events stored already; delete some to continue tracing");
                        return cur;
                    }
                    curState.addInternal(event = new TxnEvent());
                    return cur;
                }

                if (++curState.total < 0)
                    curState.total = Integer.MAX_VALUE;
                int position = curState.mode.position(curState.permits, curState.total);
                if (position < curState.permits)
                    curState.set(position, event = new TxnEvent());
                return cur;
            }
        }
        Register register = new Register();
        txnIdMap.compute(txnId, register);
        return register.event;
    }

    // null values, or values < 0, are ignored
    public boolean set(TxnId txnId, CoordinationKind eventType, BucketMode newMode, int newPermits, int newTotal, float newChance, boolean unsetManagedByPattern)
    {
        AtomicBoolean failure = new AtomicBoolean();
        Invariants.requireArgument(newPermits != 0);
        Invariants.requireArgument(Float.isNaN(newChance) || (newChance <= 1.0f && newChance > 0f));
        txnIdMap.compute(txnId, (id, cur) -> {
            TxnEvents state;
            if (newPermits > 0)
            {
                if (cur == null)
                    cur = new EnumMap<>(CoordinationKind.class);

                state = cur.computeIfAbsent(eventType, ignore -> new TxnEvents());
                state.permits = newPermits;
            }
            else
            {
                state = cur == null ? null : cur.get(eventType);
                if (state == null || state.permits == 0)
                {
                    failure.set(true);
                    return cur;
                }
            }

            if (newMode != null)
                state.mode = newMode;
            if (newTotal >= 0)
                state.total = newTotal;
            if (!Float.isNaN(newChance))
                state.chance = newChance;
            if (unsetManagedByPattern)
                state.owner = null;
            return cur;
        });
        return !failure.get();
    }

    public void erasePermits(TxnId txnId)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur == null)
                return null;

            Iterator<TxnEvents> iter = cur.values().iterator();
            while (iter.hasNext())
            {
                TxnEvents state = iter.next();
                state.permits = 0;
                if (state.isEmpty()) iter.remove();
            }
            return cur.isEmpty() ? null : cur;
        });
    }

    public void erasePermits(TxnId txnId, CoordinationKind eventType)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur != null)
            {
                TxnEvents curState = cur.get(eventType);
                if (curState != null)
                {
                    if (!curState.isEmpty()) curState.permits = 0;
                    else
                    {
                        cur.remove(eventType);
                        if (cur.isEmpty())
                            return null;
                    }
                }
            }
            return cur;
        });
    }

    public void eraseEvents(TxnId txnId)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur == null)
                return null;

            cur.values().removeIf(this::truncateAndShouldRemove);

            return cur.isEmpty() ? null : cur;
        });
    }

    public void eraseEvents(TxnId txnId, CoordinationKind eventType)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur != null)
            {
                TxnEvents events = cur.get(eventType);
                if (events == null)
                    return cur;

                if (truncateAndShouldRemove(events))
                {
                    cur.remove(eventType);
                    if (cur.isEmpty())
                        return null;
                }
            }
            return cur;
        });
    }

    public void eraseEventsBefore(TxnId txnId, CoordinationKind eventType, long timestamp)
    {
        txnIdMap.compute(txnId, (id, cur) -> {
            if (cur != null)
            {
                TxnEvents events = cur.get(eventType);
                if (events == null)
                    return cur;

                int i = 0;
                while (i < events.size() && events.get(i).idMicros < timestamp)
                    ++i;

                if (truncateAndShouldRemove(i, events))
                {
                    cur.remove(eventType);
                    if (cur.isEmpty())
                        return null;
                }
            }
            return cur;
        });
    }

    private boolean truncateAndShouldRemove(TxnEvents events)
    {
        return truncateAndShouldRemove(events.size, events);
    }

    private boolean truncateAndShouldRemove(int remove, TxnEvents events)
    {
        Invariants.require(events.size >= remove);
        totalEventCount.addAndGet(-remove);
        events.truncate(remove);
        if (events.size > 0)
            return false;

        events.total = 0;
        return events.permits == 0;
    }

    public void eraseAllEvents()
    {
        txnIdMap.keySet().forEach(this::eraseEvents);
    }

    public void eraseAllPermits()
    {
        txnIdMap.keySet().forEach(this::erasePermits);
    }

    public void forEach(Predicate<TxnId> include, ConsumeState forEach)
    {
        txnIdMap.forEach((txnId, state) -> {
            if (include.test(txnId))
            {
                // ensure lock is held for duration of callback
                txnIdMap.compute(txnId, (id, cur) -> {
                    if (cur != null)
                        cur.forEach((event, events) -> forEach.accept(txnId, event, events));
                    return cur;
                });
            }
        });
    }

    public void setPattern(int id, Function<TracePattern, TracePattern> pattern, BucketMode newBucketMode, BucketMode newChildBucketMode, CoordinationKinds newChildTypes, int newChildTypePermits, int newPermits, int newTotal)
    {
        synchronized (allPatterns)
        {
            TracePatternState state = findPattern(id, false);
            TracePatternState update = state != null ? state : new TracePatternState(id);
            boolean prevTraceNew = state != null && state.pattern.traceNew != null;
            update.set(pattern, newBucketMode, newChildBucketMode, newChildTypes, newChildTypePermits, newPermits, newTotal);
            if (state == null)
                allPatterns.add(update);
            if (update.pattern.traceNew != null && !prevTraceNew)
                traceNewPatterns.add(update);
            else if (update.pattern.traceNew == null && prevTraceNew)
                traceNewPatterns.remove(update);
        }
    }

    public void erasePattern(int id)
    {
        TracePatternState removed = findPattern(id, true);
        if (removed != null)
            removed.clear();
    }


    public void erasePatternTraces(int id)
    {
        TracePatternState state = findPattern(id, false);
        if (state != null)
            state.clear();
    }

    private TracePatternState findPattern(int id, boolean remove)
    {
        synchronized (allPatterns)
        {
            for (int i = 0; i < allPatterns.size() ; ++i)
            {
                TracePatternState state = allPatterns.get(i);
                if (state.id == id)
                {
                    if (remove)
                    {
                        allPatterns.remove(i);
                        if (state.pattern.traceNew != null)
                            traceNewPatterns.remove(state);
                    }
                    return state;
                }
            }
        }
        return null;
    }

    public void eraseAllPatterns()
    {
        List<TracePatternState> removed = new ArrayList<>();
        allPatterns.removeIf(p -> { removed.add(p); return true; });
        removed.forEach(TracePatternState::clear);
    }

    public void eraseAllPatternTraces()
    {
        for (TracePatternState state : allPatterns)
            state.clear();
    }

    public void forEachPattern(Consumer<TracePatternState> consumer)
    {
        allPatterns.forEach(pattern -> {
            synchronized (pattern)
            {
                consumer.accept(pattern);
            }
        });
    }

    @Override
    public void onFailed(Throwable failure, TxnId txnId, Participants<?> participants, CoordinationKind kind)
    {
        if (failure != null)
            maybeTrace(txnId, participants, kind, NewOrFailure.FAILURE, allPatterns);
    }

    private boolean maybeTrace(TxnId txnId, @Nullable Participants<?> participants, CoordinationKind kind, NewOrFailure newOrFailure, List<TracePatternState> patterns)
    {
        if (patterns.isEmpty())
            return false;

        for (TracePatternState state : patterns)
        {
            if (state.maybeAdd(txnId, participants, kind, newOrFailure))
                return true;
        }
        return false;
    }
}
