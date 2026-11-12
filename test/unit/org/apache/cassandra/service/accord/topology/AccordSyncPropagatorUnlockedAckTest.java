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

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import accord.local.Node;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.tcm.ValidatingClusterMetadataService;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * AccordSyncPropagator guards {@code pending} (agrona maps, not thread safe) with its monitor, so the ack of a
 * removed peer's notification in notify() must also hold it, as notify() is invoked outside the monitor both from
 * report() and from the retry task.
 */
public class AccordSyncPropagatorUnlockedAckTest
{
    static final Node.Id SELF = new Node.Id(1);
    static final Node.Id PEER = new Node.Id(2);
    static final long E = 10;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ValidatingClusterMetadataService.createAndRegister(Version.MIN_ACCORD_VERSION);
    }

    static class Mapper implements AccordEndpointMap
    {
        volatile NodeStatus status;
        volatile boolean removed;

        Mapper(NodeStatus status) { this.status = status; }

        @Override
        public Node.Id mappedIdOrNull(InetAddressAndPort endpoint, Object log) { throw new UnsupportedOperationException(); }

        @Override
        public InetAddressAndPort mappedEndpointOrNull(Node.Id id, Object log)
        {
            try { return InetAddressAndPort.getByAddress(InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte) id.id })); }
            catch (Exception e) { throw new RuntimeException(e); }
        }

        @Override
        public NodeStatus nodeStatus(Node.Id id) { return !id.equals(PEER) ? NodeStatus.HEALTHY : removed ? NodeStatus.REMOVED : status; }
    }

    static AccordSyncPropagator.PendingNodes pendingOf(AccordSyncPropagator p) throws Exception
    {
        Field f = AccordSyncPropagator.class.getDeclaredField("pending");
        f.setAccessible(true);
        return (AccordSyncPropagator.PendingNodes) f.get(p);
    }

    static void setPending(AccordSyncPropagator p, AccordSyncPropagator.PendingNodes nodes) throws Exception
    {
        Field f = AccordSyncPropagator.class.getDeclaredField("pending");
        f.setAccessible(true);
        f.set(p, nodes);
    }

    /** Report (PEER, E) while PEER is UNAVAILABLE, so the notification is parked for retry; return the retry task. */
    static Runnable reportAndCaptureRetry(AccordSyncPropagator propagator, ScheduledExecutorPlus scheduler)
    {
        propagator.onReadyToCoordinate(E, SortedArrayList.copyUnsorted(java.util.Arrays.asList(SELF, PEER), Node.Id[]::new));
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(scheduler).schedule(retry.capture(), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        return retry.getValue();
    }

    /**
     * Mutual exclusion: while another thread holds the propagator monitor (as report()/hasPending()/
     * onNodesRemoved() do), the retry task for a now-removed peer must not mutate {@code pending}.
     */
    @Test
    public void retryAckMustNotMutatePendingWhileMonitorHeld() throws Exception
    {
        Mapper mapper = new Mapper(AccordEndpointMap.NodeStatus.UNAVAILABLE);
        ScheduledExecutorPlus scheduler = Mockito.mock(ScheduledExecutorPlus.class);
        MessageDelivery messaging = Mockito.mock(MessageDelivery.class);
        AccordSyncPropagator propagator = new AccordSyncPropagator(SELF, mapper, messaging, scheduler);

        Runnable retry = reportAndCaptureRetry(propagator, scheduler);
        Assert.assertTrue(propagator.hasPending(E));

        // PEER is removed from the cluster: the retry acks
        mapper.removed = true;

        Thread retryThread = new Thread(retry, "retry-scheduler");
        boolean mutatedWhileLocked;
        synchronized (propagator)
        {
            retryThread.start();
            retryThread.join(2000);
            mutatedWhileLocked = !pendingOf(propagator).containsKey(PEER.id);
        }
        retryThread.join(5000);
        Assert.assertFalse("retry thread acked (removed PEER from pending) while another thread held the propagator monitor: " +
                           "AccordSyncPropagator.notify UNKNOWN branch mutates pending without synchronization",
                           mutatedWhileLocked);
        Assert.assertFalse(propagator.hasPending());
    }

    /**
     * The retry thread S acks (PEER, E); were it not to hold the monitor, then between its
     * isEmpty() check and remove(PEER), thread R reports (PEER, E+1) under the monitor and puts it into the
     * PendingEpochs object S is about to drop. The pending E+1 state is lost although its message is in flight.
     */
    @Test
    public void reportDuringUnlockedAckIsNotLost() throws Exception
    {
        Mapper mapper = new Mapper(AccordEndpointMap.NodeStatus.UNAVAILABLE);
        ScheduledExecutorPlus scheduler = Mockito.mock(ScheduledExecutorPlus.class);
        MessageDelivery messaging = Mockito.mock(MessageDelivery.class); // records sends, never replies (in flight)
        AccordSyncPropagator propagator = new AccordSyncPropagator(SELF, mapper, messaging, scheduler);

        Runnable retry = reportAndCaptureRetry(propagator, scheduler);
        mapper.removed = true;

        AtomicReference<Throwable> rFailure = new AtomicReference<>();
        AtomicReference<Thread> rThread = new AtomicReference<>();
        Thread retryThread = Thread.currentThread();
        // Only to force the interleaving at the exact point S is preempted between PendingNodes.ack's
        // isEmpty() check and its remove(id): copy the real map into a subclass whose remove(int) runs R first.
        AccordSyncPropagator.PendingNodes hooked = new AccordSyncPropagator.PendingNodes()
        {
            boolean fired;
            @Override
            public AccordSyncPropagator.PendingEpochs remove(int key)
            {
                if (!fired && Thread.currentThread() == retryThread && key == PEER.id)
                {
                    fired = true;
                    // TopologyManager thread R: PEER is (again) a healthy member for E+1, report E+1.
                    Thread r = new Thread(() -> {
                        try
                        {
                            mapper.status = AccordEndpointMap.NodeStatus.HEALTHY;
                            mapper.removed = false;
                            propagator.onReadyToCoordinate(E + 1, SortedArrayList.copyUnsorted(java.util.Arrays.asList(SELF, PEER), Node.Id[]::new));
                        }
                        catch (Throwable t) { rFailure.set(t); }
                    }, "topology-report");
                    rThread.set(r);
                    r.start();
                    try { r.join(2000); } // R blocks here if S held the monitor (fixed code); bounded wait
                    catch (InterruptedException e) { throw new RuntimeException(e); }
                }
                return super.remove(key);
            }
        };
        AccordSyncPropagator.PendingNodes real = pendingOf(propagator);
        real.forEach(hooked::put);
        setPending(propagator, hooked);

        retry.run(); // S: the scheduler's retry task, run on this thread

        Assert.assertNotNull("interleaving hook did not fire", rThread.get());
        rThread.get().join(5000); // with a fixed (locked) ack R was blocked until S finished
        Assert.assertFalse(rThread.get().isAlive());
        if (rFailure.get() != null) throw new AssertionError("report thread failed", rFailure.get());
        Mockito.verify(messaging, Mockito.times(1)).sendWithCallback(Mockito.any(), Mockito.any(), Mockito.any());

        Assert.assertTrue("sync-complete for epoch " + (E + 1) + " was sent to PEER and is unacknowledged, but its pending state was lost: " +
                          "pending=" + pendingOf(propagator),
                          propagator.hasPending(E + 1));
    }
}
