/*
 * Copyright (c) 2018-2019 Apple, Inc. All rights reserved.
 */
package com.apple.cie.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.apple.cie.db.marshal.CappedCoalescingMapType;
import com.apple.cie.db.marshal.CappedSortedMapType;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.NativeScalarFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.TimeUUID;

/** APNs nDeepQ support functions.
 *
 * These functions make it possible to select the next deliverable message without having to retrieve
 * all events and past delivery history.
 *  - deliverable(events, deliveries, timestamp) - all deliverable events in the queue
 *  - oldestdeliverable(events, deliveries, timestamp) - single oldest deliverable event and counts of
 *        remaining deliverable/undeliverable events
 *  - koldestdeliverable(events, deliveries, timestamp, k) - k oldest deliverable events and counts of
  *       remaining deliverable/undeliverable events
 *
 * The schema needs to contain fields of the following types for storing the events and delivery history.
 * <pre>
       cevents 'com.apple.cie.db.marshal.CappedCoalescingMapType(BytesType)', -- OR
       qevents 'com.apple.cie.db.marshal.CappedSortedMapType(BytesType)', -- AND
       deliveries map<timeuuid,frozen<tuple<tinyint,timestamp>>>,
 * </pre>
 *
 * The delivery history contains the number of remaining delivery attempts, and the next timestamp the message
 * is deliverable after. When any of the functions run, they compute a list of undeliverable eventIds,
 * then filter the list of events and return the requested values.  Only the deliverable after timestamp
 * is used to filter - so that the application sees events that have exceeded their delivery limit and can
 * take action to remove them {@literal UPDATE event[cap(?)] = null, event[?] = null, deliveries[?] = null, ...}
 *
 * deliveries is a simple map to avoid edge cases where an event with delivery problems is pushed out of the
 * queue
 */
public abstract class Apns
{
    /* Tuple type for recording delivery of an event. CQL type tuple<tinyint,timestamp> */
    private static final TupleType deliveryType = tupleType
    (
        ByteType.instance,     // 0: number of deliveries left
        TimestampType.instance // 1: deliverable after timestamp
    );

    /* Tuple type for returning deliverable events. CQL type tuple<blob,tinyint,timestamp> */
    @VisibleForTesting
    static final TupleType sortedDeliverableType = tupleType
    (
        BytesType.instance,    // 0: event (value from the events CappedSortedMap)
        ByteType.instance,     // 1: number of deliveries left
        TimestampType.instance // 2: deliverable after timestamp
    );

    @VisibleForTesting
    static final TupleType coalescingKeyAndMessageType = tupleType
    (
        BytesType.instance,    // 0: coalescing key
        BytesType.instance      // 1: message
    );

    /* Tuple type for returning deliverable events. CQL type tuple<tuple<blob,blob>,tinyint,timestamp> */
    @VisibleForTesting
    static final TupleType coalescingDeliverableType = tupleType
    (
        coalescingKeyAndMessageType, // 0: event (value from the events CappedSortedMap)
        ByteType.instance,     // 1: number of deliveries left
        TimestampType.instance // 2: deliverable after timestamp
    );

    /* Types for events */
    private static final CappedSortedMapType<ByteBuffer> sortedEventsType = new CappedSortedMapType<>(BytesType.instance);
    private static final CappedCoalescingMapType<ByteBuffer> coalescingEventsType = new CappedCoalescingMapType<>(BytesType.instance);

    /* Map of deliveries for the events */
    private static final MapType<TimeUUID, ByteBuffer> deliveriesMapType = MapType.getInstance(TimeUUIDType.instance, deliveryType, true);

    /* Tuple type returned by oldestdeliverable for CappedSortedMaps */
    @VisibleForTesting
    static final TupleType sortedOldestDeliverableType = tupleType(
        Int32Type.instance,       // 0: undeliverable
        Int32Type.instance,       // 1: deliveries left
        sortedEventsType.getKeysType(),  // 2: oldest deliverable messageid
        sortedEventsType.getValuesType(),// 3: oldest deliverable message
        ByteType.instance,        // 4: oldest deliverable delivery attempts
        TimestampType.instance    // 5: oldest deliverable deliverable after
    );

    /* Tuple type returned by oldestdeliverable for CappedCoalescingMaps */
    @VisibleForTesting
    static final TupleType coalescingOldestDeliverableType = tupleType(
        Int32Type.instance,       // 0: undeliverable
        Int32Type.instance,       // 1: deliveries left
        coalescingEventsType.getKeysType(), // 2: oldest deliverable messageid
        coalescingEventsType.getValuesType(), // 3: oldest deliverable tuple
        ByteType.instance,        // 4: oldest deliverable delivery attempts
        TimestampType.instance    // 5: oldest deliverable deliverable after
    );

    /* Map returned by deliverable - map<uuid,tuple<blob,tinyint,timestamp>> */
    @VisibleForTesting
    static final MapType<TimeUUID, ByteBuffer> sortedDeliverableMapType = MapType.getInstance(sortedEventsType.getKeysType(), sortedDeliverableType, true);

    /* Map returned by deliverable - map<uuid,tuple<tuple<,blob>,tinyint,timestamp>> */
    @VisibleForTesting
    static final MapType<TimeUUID, ByteBuffer> coalescingDeliverableMapType = MapType.getInstance(coalescingEventsType.getKeysType(), coalescingDeliverableType, true);

    /* Tuple type returned by koldestdeliverable on sortedmap */
    @VisibleForTesting
    static final TupleType sortedKOldestDeliverableType = tupleType
    (
        Int32Type.instance, // 0: undeliverable
        Int32Type.instance, // 1: deliveries left
        sortedDeliverableMapType  // 2: k oldest deliverable messages
    );

    /* Tuple type returned by koldestdeliverable on coalescingmap */
    @VisibleForTesting
    static final TupleType coalescingKOldestDeliverableType = tupleType
    (
        Int32Type.instance, // 0: undeliverable
        Int32Type.instance, // 1: deliveries left
        coalescingDeliverableMapType  // 2: k oldest deliverable messages
    );

    private static final MapSerializer<ByteBuffer, ByteBuffer> bytesBytesMapSerializer =
        MapSerializer.newInstance(BytesSerializer.instance, BytesSerializer.instance, TimeUUIDType.instance.comparatorSet);

    private static boolean isDeliverable(ByteBuffer deliveryTuple, ByteBuffer afterTimestamp)
    {
        ByteBuffer[] deliveryInfo = deliveryType.split(deliveryTuple);
        assert deliveryInfo.length == 2 : "DeliveryInfo fixed to tuple<tinyint,timestamp>";
        int cmp = TimestampType.instance.compare(deliveryInfo[1], afterTimestamp);
        return cmp <= 0; // deliverable if deliverableAfter <= now  - deliverable after is later.
    }

    static class Deliverability
    {
        int undeliverableCount;
        int deliverableCount;
        final List<Map.Entry<ByteBuffer, ByteBuffer>> deliverable; // deliverable items in reverse timeuuid order

        Deliverability(int maxDeliverable)
        {
            undeliverableCount = 0;
            deliverableCount = 0;
            deliverable = new ArrayList<>(maxDeliverable);
        }
    }
    // Partition the events into a list of deliverables, and keep counts of the total deliverable
    // and undeliverable items.
    private static Deliverability partitionByDeliverability(Map<ByteBuffer, ByteBuffer> eventsMap,
                                                            Map<ByteBuffer, ByteBuffer> deliveries,
                                                            ByteBuffer after)
    {
        Deliverability result = new Deliverability(eventsMap.size());

        if (eventsMap.isEmpty()) // handle eventsMap being a Collections.emptyMap
            return result;

        // The events map is really a LinkedHashMap in key order - which is by reverse timeuuid
        assert eventsMap instanceof LinkedHashMap : "Implementation of MapSerializer.deserializeForNativeProtocol changed - expect a LinkedHashMap";
        LinkedHashMap<ByteBuffer,ByteBuffer> events = (LinkedHashMap<ByteBuffer,ByteBuffer>) eventsMap;

        for (Map.Entry<ByteBuffer, ByteBuffer> e : events.entrySet())
        {
            ByteBuffer delivery = deliveries.get(e.getKey());
            if (null != delivery && !isDeliverable(delivery, after))
            {
                result.undeliverableCount++;
            }
            else
            {
                result.deliverable.add(e);
                result.deliverableCount++;
            }
        }
        return result;
    }

    private static final ByteBuffer[] emptyDelivery = new ByteBuffer[] { null, null };

    private static ByteBuffer makeDeliverable(ByteBuffer message, ByteBuffer deliveryTuple)
    {
        ByteBuffer[] deliveryInfo = deliveryTuple != null
                                  ? deliveryType.split(deliveryTuple)
                                  : emptyDelivery;
        return makeTuple(message, deliveryInfo[0], deliveryInfo[1]);
    }

    private static Function makeDeliverable(MapType<?,?> eventsType, MapType<TimeUUID, ByteBuffer> deliverableMapType)
    {
        return new NativeScalarFunction("deliverable", deliverableMapType, eventsType, deliveriesMapType, TimestampType.instance)
        {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer eventsBB = parameters.get(0);
                ByteBuffer deliveriesBB = parameters.get(1);
                ByteBuffer afterBB = parameters.get(2);

                Map<ByteBuffer, ByteBuffer> events = deserializeMap(eventsBB);
                Map<ByteBuffer, ByteBuffer> deliveries = deserializeMap(deliveriesBB);

                Map<ByteBuffer, ByteBuffer> deliverables = Maps.newHashMapWithExpectedSize(events.size());
                for (Map.Entry<ByteBuffer, ByteBuffer> e : events.entrySet())
                {
                    ByteBuffer delivery = deliveries.get(e.getKey());
                    if (null == delivery || isDeliverable(delivery, afterBB))
                    {
                        deliverables.put(e.getKey(), makeDeliverable(e.getValue(), deliveries.get(e.getKey())));
                    }
                }

                return deliverables.isEmpty() ? null : bytesBytesMapSerializer.serialize(deliverables);
            }
        };
    }

    private static Function makeOldestDeliverable(MapType<?,?> eventsType, TupleType oldestDeliverableType)
    {
        return new NativeScalarFunction("oldestdeliverable", oldestDeliverableType, eventsType, deliveriesMapType, TimestampType.instance)
        {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                Map<ByteBuffer, ByteBuffer> events = deserializeMap(parameters.get(0));
                Map<ByteBuffer, ByteBuffer> deliveries = deserializeMap(parameters.get(1));
                ByteBuffer after = parameters.get(2);

                Deliverability d = partitionByDeliverability(events, deliveries, after);

                if (d.deliverableCount == 0)
                    return makeTuple(makeInt32(d.undeliverableCount), makeInt32(d.deliverableCount), null, null, null, null);

                Map.Entry<ByteBuffer, ByteBuffer> oldest = d.deliverable.get(d.deliverable.size() - 1);
                ByteBuffer delivery = deliveries.get(oldest.getKey());

                if (delivery != null)
                {
                    ByteBuffer[] splitDelivery = deliveryType.split(delivery);
                    return makeTuple(makeInt32(d.undeliverableCount),
                                     makeInt32(d.deliverableCount - 1),
                                     oldest.getKey(),
                                     oldest.getValue(),
                                     splitDelivery[0],
                                     splitDelivery[1]);
                }

                return makeTuple(makeInt32(d.undeliverableCount),
                                 makeInt32(d.deliverableCount - 1),
                                 oldest.getKey(),
                                 oldest.getValue(),
                                 null,
                                 null);
            }
        };
    }

    private static final Function makeKOldestDeliverable(MapType<?,?> eventsType, TupleType koldestDeliverableType)
    {
        return new NativeScalarFunction("koldestdeliverable", koldestDeliverableType, eventsType, deliveriesMapType, TimestampType.instance, Int32Type.instance)
        {
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                Map<ByteBuffer, ByteBuffer> events = deserializeMap(parameters.get(0));
                Map<ByteBuffer, ByteBuffer> deliveries = deserializeMap(parameters.get(1));
                ByteBuffer after = parameters.get(2);
                int k = Int32Type.instance.compose(parameters.get(3));

                Deliverability d = partitionByDeliverability(events, deliveries, after);

                if (d.deliverableCount == 0)
                    return makeTuple(makeInt32(d.undeliverableCount), makeInt32(d.deliverableCount), null);

                int delivering = Integer.min(k, d.deliverableCount);

                Map<ByteBuffer, ByteBuffer> deliverable = Maps.newHashMapWithExpectedSize(delivering);
                for (Map.Entry<ByteBuffer, ByteBuffer> e : d.deliverable.subList(d.deliverable.size() - delivering, d.deliverable.size()))
                {
                    deliverable.put(e.getKey(), makeDeliverable(e.getValue(), deliveries.get(e.getKey())));
                }

                return makeTuple(makeInt32(d.undeliverableCount),
                                 makeInt32(d.deliverableCount - deliverable.size()),
                                 deliverable.isEmpty() ? null : bytesBytesMapSerializer.serialize(deliverable));
            }
        };
    }

    @VisibleForTesting
    public static TupleType tupleType(AbstractType<?>... types)
    {
        return new TupleType(Arrays.asList(types));
    }

    private static ByteBuffer makeTuple(ByteBuffer... components)
    {
        return TupleType.buildValue(components);
    }

    private static ByteBuffer makeInt32(int value)
    {
        return Int32Serializer.instance.serialize(value);
    }

    private static Map<ByteBuffer, ByteBuffer> deserializeMap(ByteBuffer bb)
    {
        if (bb == null || bb.remaining() == 0)
            return Collections.emptyMap();
        else
            return bytesBytesMapSerializer.deserialize(bb);
    }

    private static final Function sortedDeliverableFct = makeDeliverable(sortedEventsType, sortedDeliverableMapType);
    private static final Function sortedOldestDeliverableFct = makeOldestDeliverable(sortedEventsType,
                                                                                     sortedOldestDeliverableType);
    private static final Function sortedKOldestDeliverableFct = makeKOldestDeliverable(sortedEventsType,
                                                                                       sortedKOldestDeliverableType);
    private static final Function coalescingDeliverableFct = makeDeliverable(coalescingEventsType, coalescingDeliverableMapType);
    private static final Function coalescingOldestDeliverableFct = makeOldestDeliverable(coalescingEventsType,
                                                                                         coalescingOldestDeliverableType);
    private static final Function coalescingKOldestDeliverableFct = makeKOldestDeliverable(coalescingEventsType,
                                                                                           coalescingKOldestDeliverableType);

    @SuppressWarnings("unused")
    public static Collection<Function> all()
    {
        return ImmutableList.of(sortedDeliverableFct,        coalescingDeliverableFct,
                                sortedOldestDeliverableFct,  coalescingOldestDeliverableFct,
                                sortedKOldestDeliverableFct, coalescingKOldestDeliverableFct);
    }
}
