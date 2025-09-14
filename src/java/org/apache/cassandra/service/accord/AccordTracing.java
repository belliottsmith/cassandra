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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Tracing;
import accord.api.TraceEventType;
import accord.local.CommandStore;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.service.accord.AccordTracing.BucketMode.LEAKY;

public class AccordTracing
{
    private static final int MAX_EVENTS = 10000;
    private static final Logger logger = LoggerFactory.getLogger(AccordTracing.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    public enum BucketMode
    {
        LEAKY, SAMPLE, RING
    }

    public interface ConsumeState
    {
        void accept(TxnId txnId, TraceEventType eventType, BucketMode mode, int permits, int total, List<Event> events);
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

    public static class Event implements Tracing, Comparable<Event>
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
        public int compareTo(Event that)
        {
            return Long.compareUnsigned(this.idMicros, that.idMicros);
        }

        public List<Message> messages()
        {
            return Collections.unmodifiableList(messages);
        }
    }

    static class TraceState extends AbstractList<Event>
    {
        public BucketMode mode = LEAKY;
        int permits, size, total;
        Event[] events;

        void addInternal(Event event)
        {
            if (events == null) events = new Event[10];
            else if (size == events.length) events = Arrays.copyOf(events, events.length * 2);
            events[size++] = event;
        }

        void truncate(int eraseBefore)
        {
            System.arraycopy(events, eraseBefore, events, 0, size - eraseBefore);
            Arrays.fill(events, size - eraseBefore, size, null);
            size -= eraseBefore;
        }

        @Override
        public Event get(int index)
        {
            return events[index];
        }

        @Override
        public int size()
        {
            return size;
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

    final Map<TxnId, EnumMap<TraceEventType, TraceState>> stateMap = new ConcurrentHashMap<>();
    final AtomicInteger count = new AtomicInteger();

    public Tracing trace(TxnId txnId, TraceEventType eventType)
    {
        if (!stateMap.containsKey(txnId))
            return null;

        class Register implements BiFunction<TxnId, EnumMap<TraceEventType, TraceState>, EnumMap<TraceEventType, TraceState>>
        {
            Event event;

            @Override
            public EnumMap<TraceEventType, TraceState> apply(TxnId id, EnumMap<TraceEventType, TraceState> cur)
            {
                if (cur == null)
                    return null;

                TraceState curState = cur.get(eventType);
                if (curState == null || curState.permits == 0)
                    return cur;

                if (curState.permits > curState.size)
                {
                    if (count.incrementAndGet() >= MAX_EVENTS)
                    {
                        count.decrementAndGet();
                        ClientWarn.instance.warn("Too many Accord trace events stored already; delete some to continue tracing");
                        noSpamLogger.warn("Too many Accord trace events stored already; delete some to continue tracing");
                        return cur;
                    }
                    curState.addInternal(event = new Event());
                    return cur;
                }

                if (++curState.total < 0)
                    curState.total = Integer.MAX_VALUE;
                int position;
                switch (curState.mode)
                {
                    default: throw UnhandledEnum.unknown(curState.mode);
                    case LEAKY:
                        position = Integer.MAX_VALUE;
                        break;
                    case RING:
                        position = curState.total % curState.permits;
                        break;
                    case SAMPLE:
                        position = ThreadLocalRandom.current().nextInt(curState.total);
                }

                if (position < curState.permits)
                    curState.set(position, event = new Event());
                return cur;
            }
        }
        Register register = new Register();
        stateMap.compute(txnId, register);
        return register.event;
    }

    // null values, or values < 0, are ignored
    public boolean set(TxnId txnId, TraceEventType eventType, BucketMode newMode, int newPermits, int newTotal)
    {
        AtomicBoolean failure = new AtomicBoolean();
        Invariants.requireArgument(newPermits != 0);
        stateMap.compute(txnId, (id, cur) -> {
            TraceState state;
            if (newPermits > 0)
            {
                if (cur == null)
                    cur = new EnumMap<>(TraceEventType.class);

                state = cur.computeIfAbsent(eventType, ignore -> new TraceState());
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
            return cur;
        });
        return !failure.get();
    }

    public void erasePermits(TxnId txnId)
    {
        stateMap.compute(txnId, (id, cur) -> {
            if (cur == null)
                return null;

            Iterator<TraceState> iter = cur.values().iterator();
            while (iter.hasNext())
            {
                TraceState state = iter.next();
                state.permits = 0;
                if (state.isEmpty()) iter.remove();
            }
            return cur.isEmpty() ? null : cur;
        });
    }

    public void erasePermits(TxnId txnId, TraceEventType eventType)
    {
        stateMap.compute(txnId, (id, cur) -> {
            if (cur != null)
            {
                TraceState curState = cur.get(eventType);
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
        stateMap.compute(txnId, (id, cur) -> {
            if (cur == null)
                return null;

            Iterator<TraceState> iter = cur.values().iterator();
            while (iter.hasNext())
            {
                TraceState state = iter.next();
                count.addAndGet(-state.size());
                state.truncate(state.size());
                state.total = 0;
                if (state.permits == 0) iter.remove();
            }
            return cur.isEmpty() ? null : cur;
        });
    }

    public void eraseEvents(TxnId txnId, TraceEventType eventType)
    {
        stateMap.compute(txnId, (id, cur) -> {
            if (cur != null)
            {
                TraceState state = cur.get(eventType);
                if (state == null)
                    return cur;

                count.addAndGet(-state.size());
                state.truncate(state.size());
                state.total = 0;
                if (state.permits == 0)
                    cur.remove(eventType);
                if (cur.isEmpty())
                    return null;
            }
            return cur;
        });
    }

    public void eraseEventsBefore(TxnId txnId, TraceEventType eventType, long timestamp)
    {
        stateMap.compute(txnId, (id, cur) -> {
            if (cur != null)
            {
                TraceState state = cur.get(eventType);
                if (state == null)
                    return cur;

                int i = 0;
                while (i < state.size() && state.get(i).idMicros < timestamp)
                    ++i;
                state.truncate(i);
                count.addAndGet(-i);
                if (state.isEmpty())
                {
                    state.total = 0;
                    if (state.permits == 0)
                        cur.remove(eventType);
                }
                if (cur.isEmpty())
                    return null;
            }
            return cur;
        });
    }

    public void eraseAllEvents()
    {
        stateMap.keySet().forEach(this::eraseEvents);
    }

    public void eraseAllPermits()
    {
        stateMap.keySet().forEach(this::erasePermits);
    }

    public void forEach(Predicate<TxnId> include, ConsumeState forEach)
    {
        stateMap.forEach((txnId, state) -> {
            if (include.test(txnId))
            {
                // ensure lock is held for duration of callback
                stateMap.compute(txnId, (id, cur) -> {
                    if (cur != null)
                        cur.forEach((event, events) -> forEach.accept(txnId, event, events.mode, events.permits, events.total, Collections.unmodifiableList(events)));
                    return cur;
                });
            }
        });
    }
}
