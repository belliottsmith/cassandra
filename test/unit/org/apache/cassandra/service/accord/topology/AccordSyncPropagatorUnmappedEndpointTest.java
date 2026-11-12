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

package org.apache.cassandra.service.accord.topology;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Agent;
import accord.impl.TestAgent;
import accord.impl.basic.MonitoredPendingQueue;
import accord.impl.basic.Pending;
import accord.impl.basic.PendingQueue;
import accord.impl.basic.RandomDelayQueue;
import accord.impl.basic.SimulatedDelayedExecutorService;
import accord.local.Node;
import accord.messages.SimpleReply;
import accord.utils.RandomSource;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.concurrent.AdaptingScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.tcm.ValidatingClusterMetadataService;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * A node may learn of an epoch (e.g. from a peer) before its endpoint mapping knows some of the epoch's nodes;
 * notifications to such a node must be retried until the mapping catches up, rather than being orphaned.
 */
public class AccordSyncPropagatorUnmappedEndpointTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ValidatingClusterMetadataService.createAndRegister(Version.MIN_ACCORD_VERSION);
    }

    private static InetAddressAndPort address(int value)
    {
        try
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(ByteBufferUtil.bytes(value).array()), 1);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    /** Delivers every notification and acks it immediately; records who received what. */
    private static class RecordingDelivery implements MessageDelivery
    {
        final List<String> sent = new ArrayList<>();

        @Override
        public <REQ> void send(Message<REQ> message, InetAddressAndPort to) { throw new UnsupportedOperationException(); }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
        {
            sent.add(to + " <- " + message.payload);
            ((RequestCallback) cb).onResponse(message.responseWith(SimpleReply.Ok));
        }

        @Override
        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection) { throw new UnsupportedOperationException(); }

        @Override
        public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to) { throw new UnsupportedOperationException(); }

        @Override
        public <V> void respond(V response, Message<?> message) { throw new UnsupportedOperationException(); }
    }

    static class Mapper implements AccordEndpointMap
    {
        final Set<Integer> mapped = ConcurrentHashMap.newKeySet();

        @Override
        public Node.Id mappedIdOrNull(InetAddressAndPort endpoint, Object log) { throw new UnsupportedOperationException(); }

        @Override
        public InetAddressAndPort mappedEndpointOrNull(Node.Id id, Object log) { return mapped.contains(id.id) ? address(id.id) : null; }

        @Override
        public NodeStatus nodeStatus(Node.Id id) { return mapped.contains(id.id) ? NodeStatus.HEALTHY : NodeStatus.UNKNOWN; }
    }

    @Test
    public void notificationToNotYetMappedNodeIsDeliveredOnceMappingCatchesUp()
    {
        Gossiper.instance.clearUnsafe();
        Node.Id n1 = new Node.Id(1), n2 = new Node.Id(2), n3 = new Node.Id(3);
        for (Node.Id id : new Node.Id[]{ n1, n2, n3 })
            Gossiper.instance.endpointStateMap.put(address(id.id), new EndpointState(HeartBeatState.empty())); // alive

        // deterministic simulated scheduler (bounded drain below), as in AccordSyncPropagatorTest
        List<Throwable> failures = new ArrayList<>();
        RandomSource rs = RandomSource.wrap(new Random(42));
        PendingQueue queue = new MonitoredPendingQueue(failures, new RandomDelayQueue.Factory(rs).get());
        Agent agent = new TestAgent.RethrowAgent();
        ScheduledExecutorPlus scheduler = new AdaptingScheduledExecutorPlus(new SimulatedDelayedExecutorService(queue, agent, null));

        // local ClusterMetadata (epoch 10) only knows nodes 1 and 2
        Mapper mapper = new Mapper();
        mapper.mapped.add(1);
        mapper.mapped.add(2);

        RecordingDelivery delivery = new RecordingDelivery();
        AccordSyncPropagator propagator = new AccordSyncPropagator(n1, mapper, delivery, scheduler);

        // epoch 11 (fetched from a peer) adds node 3; node 1 becomes ready to coordinate it
        Pending.Global.setNoActiveOrigin();
        propagator.onReadyToCoordinate(11, SortedArrayList.ofSorted(n1, n2, n3));

        // local ClusterMetadata catches up to epoch 11: node 3 now has an endpoint
        mapper.mapped.add(3);

        // let any retry run (retries back off up to 15 minutes); bounded so the test stops by itself
        Pending.Global.clearActiveOrigin();
        int steps = 0;
        while (queue.size() > 0 && steps < 10_000)
        {
            Runnable next = (Runnable) queue.poll();
            if (next == null) break;
            Pending.Global.setActiveOrigin((Pending) next);
            next.run();
            steps++;
            Pending.Global.clearActiveOrigin();
        }
        assertEquals("simulated scheduler failures: " + failures, 0, failures.size());

        long toNode3 = delivery.sent.stream().filter(s -> s.startsWith(address(3).toString())).count();
        String state = "sent=" + delivery.sent + ", scheduledTasksRun=" + steps + ", propagator=" + propagator;
        assertFalse("pending sync notification for epoch 11 orphaned (never acked, never retried); " + state, propagator.hasPending(11));
        assertEquals("node 3 never received node 1's ready-to-coordinate notification for epoch 11; " + state, 1, toNode3);
    }
}
