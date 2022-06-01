/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */

/* QuickTheories test for CappedCoalescingMap and APNs functionality.
 *
 * The test executes the steps needed to publish events, scanning
 * to find the oldest deliverable (and deliverable/koldest) and mark
 * events delivered and acknowledged, as well
 * as the cleanup query to cover the individual event tombstones
 * with a range tombstone covering them all.
 *
 * The delivered/acknowledged message used is the last one read
 * by oldestdeliverable.  The preconditions for the steps
 * deliberately permit re-submission of deliver/acknowledge
 * to make sure they are idempotent.  It also supports
 * restarting the scan to simulate reconnection by the end user device.
 *
 * To exercise the time-based functionality in Apns the QT test
 * uses a fake version of now() in the FakeTimeFcts custom functions.
 * All times in the APNs functions are passed in as arguments and
 * the time is provided for all queries that create tombstones.
 *
 * For performance, the table is only created once and each
 * iteration of the test uses a sequentially allocated token.
 */
package com.apple.cie.cql3.functions;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;

import static com.apple.cie.cql3.functions.Apns.tupleType;
import static com.apple.cie.cql3.functions.FakeTime.fakeNow;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.Generate.byteArrays;
import static org.quicktheories.generators.Generate.bytes;
import static org.quicktheories.generators.Generate.pick;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;
import static org.quicktheories.impl.stateful.StatefulTheory.StepBased;
import static org.quicktheories.impl.stateful.StatefulTheory.builder;

public class ApnsCoalescingMapQtTest extends CQLTester
{
    private int tok = 0;

    @Before
    public void before()
    {
        createTable("CREATE TABLE %s" +
                    "(tok int," +
                    " group smallint," +
                    " topic blob," +
                    " events 'com.apple.cie.db.marshal.CappedCoalescingMapType(BytesType)', " +
                    " deliveries map<timeuuid,frozen<tuple<tinyint,timestamp>>>, " +
                    "PRIMARY KEY ((tok), group, topic))" +
                    "WITH default_time_to_live = 3024000 AND" +
                    " gc_grace_seconds = 0 AND " +
                    " COMPACTION = {'class': 'LeveledCompactionStrategy', 'enabled': 'false' };");
    }

    protected UntypedResultSet execute(String query, Object... values)
    {
        try
        {
            return super.execute(query, values);
        }
        catch (Throwable tr)
        {
            logger.error("execute {} with {} failed: {}", query, values, tr.getMessage());
            throw new RuntimeException(tr.getMessage(), tr);
        }
    }

    private final static Comparator<TimeUUID> ascendingTimeUUID = Comparator.comparingLong(TimeUUID::uuidTimestamp);
    private final static Comparator<ApnsEvent> ascendingApnsEvent = Comparator.comparing(e -> e.eventId, ascendingTimeUUID);

    static class ApnsEvent
    {
        final TimeUUID eventId;
        ByteBuffer coalescingKey; // reset on ack
        ByteBuffer message;
        Optional<Date> deliverableAfter;
        Optional<Integer> deliveriesLeft;

        ApnsEvent(TimeUUID eventId, ByteBuffer coalescingKey, ByteBuffer message)
        {
            this.eventId = eventId;
            this.coalescingKey = coalescingKey;
            this.message = message;
            this.deliverableAfter = Optional.empty();
            this.deliveriesLeft = Optional.empty();
        }
        ApnsEvent(TimeUUID eventId, ByteBuffer coalescingKey, ByteBuffer message, ByteBuffer deliveriesLeft, ByteBuffer deliverableAfter)
        {
            this.eventId = eventId;
            this.coalescingKey = coalescingKey;
            this.message = message;
            this.deliveriesLeft = Optional.ofNullable(deliveriesLeft).map(
              ByteType.instance::compose).map(dl -> (int) dl);
            this.deliverableAfter = Optional.ofNullable(deliverableAfter).map(
              TimestampType.instance::compose);
        }

        void deliver(int deliveriesLeft, Date deliverableAfter)
        {
            this.deliveriesLeft = Optional.of(deliveriesLeft);
            this.deliverableAfter = Optional.of(deliverableAfter);
        }

        void setAcknowledgedAt()
        {
            message = null;
            coalescingKey = null;
        }

        boolean isDeliverable()
        {
            return message != null && isDeliverableAt(FakeTime.fakeDate());
        }

        boolean isDeliverableAt(Date when) {
            return deliverableAfter.map(da -> {
                int cmp = when.compareTo(da);
                return cmp >= 0;
            }).orElse(true);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ApnsEvent apnsEvent = (ApnsEvent) o;
            return Objects.equals(eventId, apnsEvent.eventId) &&
                   Objects.equals(coalescingKey, apnsEvent.coalescingKey) &&
                   Objects.equals(message, apnsEvent.message) &&
                   Objects.equals(deliverableAfter, apnsEvent.deliverableAfter) &&
                   Objects.equals(deliveriesLeft, apnsEvent.deliveriesLeft);
        }

        public int hashCode()
        {
            return Objects.hash(eventId, message, deliverableAfter, deliveriesLeft);
        }

        public String toString()
        {
            return "ApnsEvent{" +
                   "eventId=" + eventId +
                   ", message=" + (message == null ? "(null)" : ByteBufferUtil.bytesToHex(message)) +
                   ", coalescingKey=" + (coalescingKey == null ? "(null)" : ByteBufferUtil.bytesToHex(coalescingKey)) +
                   ", deliverableAfter=" + deliverableAfter +
                   ", deliveriesLeft=" + deliveriesLeft +
                   '}';
        }
    }

    static class ApnsTopic implements Comparable<ApnsTopic> {
        final int queueLen;
        final short group;
        final ByteBuffer topicId;
        final Map<TimeUUID, ApnsEvent> events = new HashMap<>();

        ApnsTopic(int queueLen, short group, ByteBuffer topicId)
        {
            this.queueLen = queueLen;
            this.group = group;
            this.topicId = topicId;
        }

        public boolean equals(Object o) // only compares topicId
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ApnsTopic apnsTopic = (ApnsTopic) o;
            return Objects.equals(topicId, apnsTopic.topicId);
        }

        public int hashCode()
        {
            return Objects.hash(topicId);
        }

        public int compareTo(ApnsTopic o) // NB Compared group & topic - different from equals
        {
            int cmp;
            cmp = (int) this.group - (int) o.group;
            if (cmp == 0)
                cmp = ByteBufferUtil.compareUnsigned(this.topicId, o.topicId);
            return cmp;
        }

        public String toString()
        {
            return "ApnsTopic{" +
                   "queueLen=" + queueLen +
                   ", group=" + group +
                   ", topicId=" + ByteBufferUtil.bytesToHex(topicId) +
                   ", events=" + events +
                   '}';
        }

        void publish(ApnsEvent event)
        {
            // If this matches an existing coalescing key that has note been acknowledged,
            // remove it before adding the new one and trimming.  When a key is acknowledged the message/coalescing
            // key tuple deleted which means the cell resolver cannot spot older dupes.
            Set<ApnsEvent> matchingEvents = events.values().stream()
                                                  .filter(e -> e.coalescingKey != null &&
                                                               event.coalescingKey.equals(e.coalescingKey) &&
                                                               e.message != null)
                                                  .collect(Collectors.toSet());
            assert(matchingEvents.size() <= 1);
            matchingEvents.forEach(e -> events.remove(e.eventId));

            events.put(event.eventId, event);

            trimEvents();
        }

        private void trimEvents()
        {
            while (events.size() > queueLen)
            {
                //noinspection OptionalGetWithoutIsPresent events size is guaranteed non-zero
                TimeUUID lowestKey = events.keySet().stream().min(ascendingTimeUUID).get();
                ApnsEvent droppedEvent = events.get(lowestKey);
                assertEquals(lowestKey, droppedEvent.eventId);
                events.remove(droppedEvent.eventId);
            }
        }

        void ack(ApnsEvent ackedEvent)
        {
            events.putIfAbsent(ackedEvent.eventId, ackedEvent);
            events.get(ackedEvent.eventId).setAcknowledgedAt();
            trimEvents();
        }

        boolean anyDeliverable()
        {
            return oldestDeliverable().isPresent();
        }

        Stream<ApnsEvent> deliverable() {
            return events.values().stream()
                  .filter(ApnsEvent::isDeliverable)
                  .sorted(ascendingApnsEvent);
        }
        Optional<ApnsEvent> oldestDeliverable() {
            return deliverable().findFirst();
        }
    }

    class Model extends StepBased
    {
        /* Generator parameters */
        final static int minCap = 1;  // Make smaller than any defaults
        final static int maxCap = 15; // Make larger than any defaults
        final static int maxGroupNum = 10;
        final static int maxTopics = 4;
        final static int minDeliveryWindow = 1;
        final static int maxDeliveryWindow = 60000;
        final static int deliveryAttempts = 3;
        final static int minCoalescingKeyLen = 1;
        final static int maxCoalescingKeyLen = 256;

        /* Begin Per-test state - init/reset in setup() */
        List<ByteBuffer> coalesingKeys;
        int publishedMessages;
        Map<ByteBuffer,ApnsTopic> topics;
        short lastGroup;
        ByteBuffer lastTopicId;
        ApnsEvent lastEvent;
        long lastLiveCount;
        long lastCleanupMicros;
        int deliveryWindowMillis;
        /* End Per-Test state - reset in setup() */

        Optional<ApnsTopic> modelNextTopics(short afterGroup, ByteBuffer afterTopic)
        {
            return topics.values().stream()
                         .filter(at -> at.group > afterGroup || at.group == afterGroup && BytesType.instance.compare(at.topicId, afterTopic) >= 0)
                         .filter(ApnsTopic::anyDeliverable)
                         .sorted()
                         .findFirst();
        }

        ByteBuffer makeEvent(int eventNum)
        {
            return ByteBufferUtil.bytes(String.format("e%d", eventNum));
        }

        /*
        ** Generators
        */

        Gen<Integer> tickMillis() {
            return integers().between(1, 10000);
        }

        Gen<Integer> nextMessageNum() {
            // Slightly shady use of constant, would like to have the message numbers in the history trace
            return arbitrary().constant(() -> {
                publishedMessages++;
                return publishedMessages;
            });
        }

        final Supplier<Gen<ApnsTopic>> pickApnsTopic = () -> integers().between(0, topics.size() - 1).map(idx -> topics.values().toArray(new ApnsTopic[0])[idx]);
        final Supplier<Gen<ApnsTopic>> supplyLastTopic = () -> arbitrary().constant(topics.get(lastTopicId));
        final Supplier<Gen<ApnsEvent>> supplyLastEvent = () -> arbitrary().constant(lastEvent);
        final Supplier<Gen<Integer>> k = () -> integers().between(Integer.max(1, minCap - 1), maxCap + 1);

        Gen<ByteBuffer> genTopicId()
        {
            return Generate.byteArrays(integers().between(1, 20),
                                       Generate.bytes(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0))
                           .map((Function<byte[], ByteBuffer>) ByteBuffer::wrap);
        }

        Gen<ApnsTopic> genApnsTopic()
        {
            return integers().between(minCap, maxCap)
                      .zip(integers().between(1, maxGroupNum).map(Integer::shortValue),
                           genTopicId(),
                           ApnsTopic::new);
        }
        Gen<List<ApnsTopic>> genApnsTopics()
        {
            return lists().of(genApnsTopic()).ofSizeBetween(1, maxTopics);
        }
        Gen<Integer> genDeliveryWindow()
        {
            return integers().between(minDeliveryWindow, maxDeliveryWindow);
        }

        Gen<ByteBuffer> genCoalesingKey() { return byteArrays(integers().between(minCoalescingKeyLen, maxCoalescingKeyLen),
                                                              bytes(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0))
                                                   .map((Function<byte[], ByteBuffer>) ByteBuffer::wrap)
                                                   .describedAs(ByteBuffer::toString); }

        Gen<List<ByteBuffer>> genCoalescingKeys() {
            // Limit the max number of coalescing keys to double capacity to make it more
            // likely to exercise conditions where the coalescing key is re-used.
            return lists().of(genCoalesingKey()).ofSizeBetween(1, 2 * maxCap).map(l -> l.stream().distinct().collect(Collectors.toList()));
        }

        Gen<ByteBuffer> pickCoalesingKey() { return pick(coalesingKeys); }

        /*
        ** Step Functions
        */
        void setup(List<ApnsTopic> protoTopics, int deliveryWindowMillis, List<ByteBuffer> coalesingKeys)
        {
            tok++;
            logger.trace("setup tok={}", tok);
            //FakeTime.set(Instant.now(), 0);
            FakeTime.set(Instant.ofEpochMilli(1554827978001L), 0);

            publishedMessages = 0;
            topics = protoTopics.stream().distinct().collect(Collectors.toMap(at -> at.topicId, at -> at));
            this.deliveryWindowMillis = deliveryWindowMillis;
            this.coalesingKeys = coalesingKeys;

            restart();
            lastLiveCount = -1;
            lastCleanupMicros = -1;
        }

        void restart()
        {
            logger.trace("restart tok={}", tok);
            lastGroup = 0;
            lastTopicId = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            lastEvent = null;
        }

        void tick(int tickMillis)
        {
            logger.trace("tick tok={} tickMillis={}", tok, tickMillis);
            FakeTime.advanceMillis(tickMillis);
        }

        void publish(ApnsTopic topic, ByteBuffer coalescingKey, int eventNum)
        {
            logger.trace("publish tok={} topic={} eventNum={}", tok, topic, eventNum);
            TimeUUID eventId = fakeNow();
            ByteBuffer event = makeEvent(eventNum);

            tick(1);
            execute("UPDATE %s USING TIMESTAMP ? " +
                    "SET" +
                    " events[system.cap(?)] = null," +
                    " events[?] = (?, ?) " +
                    "WHERE tok = ?" +
                    " AND group = ?" +
                    " AND topic = ?",
                    FakeTime.fakeMicros(),
                    topic.queueLen,
                    eventId, coalescingKey, event,
                    tok,
                    topic.group,
                    topic.topicId);

            // Update model
            ApnsEvent modelEvent = new ApnsEvent(eventId, coalescingKey, event);
            topic.publish(modelEvent);
        }

        Set<ApnsEvent> deserializeDeliverableMap(ByteBuffer bb)
        {
            if (bb == null)
            {
                return null;
            }
            else
            {
                Map<TimeUUID, ByteBuffer> m = Apns.coalescingDeliverableMapType.compose(bb);
                return m.entrySet().stream().map(e -> {
                    ByteBuffer[] deliverable = Apns.coalescingDeliverableType.split(e.getValue());
                    ByteBuffer[] cmParts = Apns.coalescingKeyAndMessageType.split(deliverable[0]);
                    return new ApnsEvent(e.getKey(), cmParts[0], cmParts[1],
                                         deliverable[1], deliverable[2]);
                }).collect(Collectors.toSet());
            }
        }

        void deliverable()
        {
            logger.trace("deliverable tok={} lastGroup={} supplyLastTopic={}", tok, lastGroup, ByteBufferUtil.bytesToHex(lastTopicId));

            Optional<ApnsTopic> expected = modelNextTopics(lastGroup, lastTopicId);

            UntypedResultSet rs = execute(
            "SELECT tok," +
            " group," +
            " topic," +
            " system.deliverable(events, deliveries, totimestamp(fakenow())) AS deliverable," +
            " events," +     // just for debugging, not used by query/logic
            " deliveries " + // ditto
            "FROM %s " +
            "WHERE tok = ?" +
            " AND (group, topic) >= (?, ?)",
            tok, lastGroup, lastTopicId);

            Set<ApnsEvent> result = null;
            Iterator<UntypedResultSet.Row> rowIter = rs.iterator();

            // start the search from the previous oldestdeliverable, but do not
            // update group/topic with the results so that deliver/ack match the lastEvent.
            short group = lastGroup;
            ByteBuffer topicId = lastTopicId.duplicate();

            while (rowIter.hasNext() && result == null)
            {
                UntypedResultSet.Row r = rowIter.next();

                group = r.getShort("group");
                topicId = r.getBytes("topic");
                result = deserializeDeliverableMap(r.getBytes("deliverable"));
            }

            if (expected.isPresent())
            {
                ApnsTopic expected0 = expected.get();
                assertEquals("group", expected0.group, group);
                assertEquals("topicId", expected0.topicId, topicId);
            }
            assertEquals(expected.map(t -> t.deliverable().collect(Collectors.toSet())),
                         Optional.ofNullable(result));
        }

        void oldestdeliverable()
        {
            logger.trace("oldestdeliverable tok={} lastGroup={} supplyLastTopic={}", tok, lastGroup, ByteBufferUtil.bytesToHex(lastTopicId));

            Optional<ApnsTopic> expected = modelNextTopics(lastGroup, lastTopicId);

            UntypedResultSet rs = execute(
            "SELECT tok," +
            " group," +
            " topic," +
            " system.oldestdeliverable(events, deliveries, totimestamp(fakenow())) AS oldestdeliverable," +
            " events," +     // just for debugging, not used by query/logic
            " deliveries " + // ditto
            "FROM %s " +
            "WHERE tok = ?" +
            " AND (group, topic) >= (?, ?)",
            tok, lastGroup, lastTopicId);

            lastEvent = null;
            Iterator<UntypedResultSet.Row> rowIter = rs.iterator();

            while (rowIter.hasNext() && lastEvent == null)
            {
                UntypedResultSet.Row r = rowIter.next();

                lastGroup = r.getShort("group");
                lastTopicId = r.getBytes("topic");
                ByteBuffer od = r.getBytes("oldestdeliverable");
                assert (od != null);
                ByteBuffer[] odParts = Apns.coalescingOldestDeliverableType.split(od);
                if (odParts[2] != null)
                {
                    ByteBuffer[] cmParts = Apns.coalescingKeyAndMessageType.split(odParts[3]);
                    lastEvent = new ApnsEvent(TimeUUIDType.instance.compose(odParts[2]),
                                              cmParts[0], cmParts[1], odParts[4], odParts[5]);
                }
            }
            assertEquals(expected.flatMap(ApnsTopic::oldestDeliverable), Optional.ofNullable(lastEvent));
        }

        void koldestdeliverable(int k)
        {
            logger.trace("koldestdeliverable tok={} k={}", tok, k);

            Optional<ApnsTopic> expected = modelNextTopics(lastGroup, lastTopicId);

            String kLiteral = Int32Literals.literalName(k) + "()";
            UntypedResultSet rs = execute(
            "SELECT tok," +
            " group," +
            " topic," +
            " system.koldestdeliverable(events, deliveries, totimestamp(fakenow()), "+ kLiteral +") AS koldestdeliverable," +
            " events," +     // just for debugging, not used by query/logic
            " deliveries " + // ditto
            "FROM %s " +
            "WHERE tok = ?" +
            " AND (group, topic) >= (?, ?)",
            tok, lastGroup, lastTopicId);

            Set<ApnsEvent> result = null;
            Iterator<UntypedResultSet.Row> rowIter = rs.iterator();

            // start the search from the previous oldestdeliverable, but do not
            // update group/topic with the results so that deliver/ack match the lastEvent.
            short group = lastGroup;
            ByteBuffer topicId = lastTopicId.duplicate();

            while (rowIter.hasNext() && result == null)
            {
                UntypedResultSet.Row r = rowIter.next();

                group = r.getShort("group");
                topicId = r.getBytes("topic");
                ByteBuffer kod = r.getBytes("koldestdeliverable");
                assert (kod != null);
                ByteBuffer[] kodParts = Apns.coalescingKOldestDeliverableType.split(kod);
                if (kodParts[2] != null)
                {
                    result = deserializeDeliverableMap(kodParts[2]);
                }
            }
            if (expected.isPresent())
            {
                ApnsTopic expected0 = expected.get();
                assertEquals("group", expected0.group, group);
                assertEquals("topicId", expected0.topicId, topicId);
            }
            assertEquals(expected.map(t -> t.deliverable().limit(k).collect(Collectors.toSet())),
                         Optional.ofNullable(result));
        }

        void deliver(ApnsTopic topic)
        {
            logger.trace("deliver tok={} lastEvent={} fakeMicros={}", tok, lastEvent, FakeTime.fakeMicros());
            assert(lastEvent != null);

            if (BytesType.instance.compare(topic.topicId, lastTopicId) != 0)
            {
                topic = topics.get(lastTopicId);
                logger.error("Wrong topic passed into deliver");
            }


            byte deliveriesLeft = (byte) (lastEvent.deliveriesLeft.orElse(deliveryAttempts) - 1);
            Date deliverableAfter = new Date(FakeTime.fakeMillis() + deliveryWindowMillis);
            tick(1);
            execute("UPDATE %s USING TIMESTAMP ? " +
                    "SET deliveries[?] = (?, ?) " +
                    "WHERE tok = ? AND group = ? AND topic = ?",
                    FakeTime.fakeMicros(),
                    lastEvent.eventId, deliveriesLeft, deliverableAfter,
                    tok, lastGroup, lastTopicId);


            // Update model event to mark delivery attempt
            ApnsEvent modelEvent = topic.events.get(lastEvent.eventId);
            if (modelEvent != null)
            {
                // modelEvent may have been pushed out of the queue, in which case the query
                // will be a no-op, so no need to track anything in the model.
                modelEvent.deliver(deliveriesLeft, deliverableAfter);
            }
        }

        void ack(ApnsEvent eventForLog, ApnsTopic topic)
        {
            logger.trace("ack tok={} lastEvent={} fakeMicros={}", tok, lastEvent, FakeTime.fakeMicros());
            assert(lastEvent != null);
            assert(lastEvent == eventForLog);
            assert(topic != null);

            if (BytesType.instance.compare(topic.topicId, lastTopicId) != 0)
            {
                topic = topics.get(lastTopicId);
                logger.error("Wrong topic passed into deliver");
            }

            tick(1);
            execute("UPDATE %s USING TIMESTAMP ? " +
                    "SET events[system.cap(?)] = null," +
                    " events[?] = null," +
                    " deliveries[?] = null " +
                    "WHERE tok = ?" +
                    " AND group = ?" +
                    " AND topic = ?",
                    FakeTime.fakeMicros(),
                    topic.queueLen,
                    lastEvent.eventId,
                    lastEvent.eventId,
                    tok,
                    lastGroup,
                    lastTopicId);

            // Update the model to remove the coalescing key for the timestamp so that the tombstone is counted against
            // the number of active cells separate from the coalesced key.
            topic.ack(lastEvent);
        }

        void cleanupCheck()
        {
            logger.trace("cleanupCheck tok={}", tok);
            assert(lastLiveCount == -1);

            UntypedResultSet rs = execute("SELECT count(events) AS livecount FROM %s WHERE tok = ?", tok);
            lastCleanupMicros = FakeTime.fakeMicros();
            lastLiveCount = rs.one().getLong("livecount");

            if (lastLiveCount > 0)
                lastLiveCount = -1; // do not move on to delete if events to deliver
        }

        void cleanupDelete()
        {
            logger.trace("cleanupDelete tok={}", tok);
            assert(lastLiveCount == 0);

            execute("DELETE FROM %s USING TIMESTAMP ? WHERE tok = ?", lastCleanupMicros, tok);
            lastLiveCount = -1;
        }

        protected void initSteps()
        {
            addSetupStep(builder("setup", this::setup, this::genApnsTopics, this::genDeliveryWindow, this::genCoalescingKeys).build());
            addStep(builder("restart", this::restart).build());
            addStep(builder("tick", this::tick, this::tickMillis).build());
            addStep(builder("publish", this::publish, pickApnsTopic, this::pickCoalesingKey, this::nextMessageNum).build());
            addStep(builder("deliverable", this::deliverable).build());
            addStep(builder("oldestdeliverable", this::oldestdeliverable).build());
            addStep(builder("koldestdeliverable", this::koldestdeliverable, k).build());
            addStep(builder("deliver", (eventForLog, topic) -> deliver(topic), supplyLastEvent, supplyLastTopic).precondition(() -> lastEvent != null).build());
            addStep(builder("ack", this::ack, supplyLastEvent, supplyLastTopic).precondition(() -> lastEvent != null && lastEvent.deliverableAfter.isPresent()).build());
            addStep(builder("cleanupCheck", this::cleanupCheck).precondition(() -> lastLiveCount == -1).build());
            addStep(builder("cleanupDelete", this::cleanupDelete).precondition(() -> lastLiveCount != -1).build());
        }
    }

    @Test
    public void qtTest()
    {
        qt().withStatefulModel(Model::new).withUnlimitedExamples().withTestingTime(1, TimeUnit.MINUTES).checkStateful();
    }

    @Ignore
    @Test
    public void qtCounterExample()
    {
        // for use in IDE, update the path to the counterexample of interest
        // Seed was 2628013606530882
//        qt().withFixedSeed(568812520048558L).withStatefulModel(Model::new).withUnlimitedExamples().withTestingTime(1, TimeUnit.MINUTES).checkStateful();
        qt().withCounterExample("/var/folders/jb/x8npdrt178z31ltm4fv6140c0000gn/T/qt.6665189370222537139.shrunk.counterex").stateful(Model::new); // 34 steps
    }
}
