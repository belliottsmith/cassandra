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

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.InboundMessageHandler.MessageProcessor;
import org.apache.cassandra.net.async.MessageGenerator.UniformPayloadGenerator;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.vint.VIntCoding;

import static java.lang.Math.min;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.utils.ApproximateTime.Measurement.ALMOST_SAME_TIME;

public class ConnectionBurnTest extends ConnectionTest
{
    static
    {
        // stop updating ALMOST_SAME_TIME so that we get consistent message expiration times
        ApproximateTime.stop(ALMOST_SAME_TIME);
    }

    static class Inbound
    {
        final Map<InetAddressAndPort, Map<InetAddressAndPort, InboundMessageHandlers>> handlersByRecipientThenSender;
        final InboundSockets sockets;

        Inbound(List<InetAddressAndPort> endpoints, GlobalInboundSettings settings, MessageProcessor process, Function<InboundMessageCallbacks, InboundMessageCallbacks> callbacks)
        {
            ResourceLimits.Limit globalLimit = new ResourceLimits.Concurrent(settings.globalReserveLimit);
            InboundMessageHandler.WaitQueue globalWaitQueue = InboundMessageHandler.WaitQueue.global(globalLimit);
            Map<InetAddressAndPort, Map<InetAddressAndPort, InboundMessageHandlers>> handlersByRecipientThenSender = new HashMap<>();
            List<InboundConnectionSettings> bind = new ArrayList<>();
            for (InetAddressAndPort recipient : endpoints)
            {
                Map<InetAddressAndPort, InboundMessageHandlers> handlersBySender = new HashMap<>();
                for (InetAddressAndPort sender : endpoints)
                    handlersBySender.put(sender, new InboundMessageHandlers(recipient, sender, settings.queueCapacity, settings.endpointReserveLimit, globalLimit, globalWaitQueue, process, callbacks));

                handlersByRecipientThenSender.put(recipient, handlersBySender);
                bind.add(settings.template.withHandlers(handlersBySender::get).withBindAddress(recipient));
            }
            this.sockets = new InboundSockets(bind);
            this.handlersByRecipientThenSender = handlersByRecipientThenSender;
        }
    }


    private static class Test implements MessageProcessor
    {
        private final IVersionedSerializer<byte[]> serializer = new IVersionedSerializer<byte[]>()
        {
            public void serialize(byte[] t, DataOutputPlus out, int version) throws IOException
            {
                long id = MessageGenerator.getId(t);
                forId(id).onSerialize(id, version);
                out.writeUnsignedVInt(t.length);
                out.write(t);
            }

            public byte[] deserialize(DataInputPlus in, int version) throws IOException
            {
                int length = Ints.checkedCast(in.readUnsignedVInt());
                byte[] result = new byte[length];
                in.readFully(result);
                long id = MessageGenerator.getId(result);
                forId(id).onDeserialize(id, version);
                return result;
            }

            public long serializedSize(byte[] t, int version)
            {
                return t.length + VIntCoding.computeUnsignedVIntSize(t.length);
            }
        };

        static class Builder
        {
            long time;
            TimeUnit timeUnit;
            int endpoints;
            MessageGenerators generators;
            OutboundConnectionSettings outbound;
            GlobalInboundSettings inbound;
            public Builder time(long time, TimeUnit timeUnit) { this.time = time; this.timeUnit = timeUnit; return this; }
            public Builder endpoints(int endpoints) { this.endpoints = endpoints; return this; }
            public Builder inbound(GlobalInboundSettings inbound) { this.inbound = inbound; return this; }
            public Builder outbound(OutboundConnectionSettings outbound) { this.outbound = outbound; return this; }
            public Builder generators(MessageGenerators generators) { this.generators = generators; return this; }
            Test build() { return new Test(endpoints, generators, inbound, outbound, timeUnit.toNanos(time)); }
        }

        static Builder builder() { return new Builder(); }

        private static final int messageIdsPerConnection = 1 << 20;

        final long runForNanos;
        final int version;
        final List<InetAddressAndPort> endpoints;
        final Inbound inbound;
        final Connection[] connections;
        final long[] connectionMessageIds;
        final ExecutorService executor = Executors.newCachedThreadPool();

        private Test(int simulateEndpoints, MessageGenerators messageGenerators, GlobalInboundSettings inboundSettings, OutboundConnectionSettings outboundTemplate, long runForNanos)
        {
            this.endpoints = endpoints(simulateEndpoints);
            this.inbound = new Inbound(endpoints, inboundSettings, this, this::wrap);
            this.connections = new Connection[endpoints.size() * endpoints.size() * 3];
            this.connectionMessageIds = new long[connections.length];
            this.version = outboundTemplate.acceptVersions == null ? current_version : outboundTemplate.acceptVersions.max;
            this.runForNanos = runForNanos;

            int i = 0;
            long minId = 0, maxId = messageIdsPerConnection - 1;
            for (InetAddressAndPort recipient : endpoints)
            {
                for (InetAddressAndPort sender : endpoints)
                {
                    InboundMessageHandlers inboundHandlers = inbound.handlersByRecipientThenSender.get(recipient).get(sender);
                    OutboundConnectionSettings template = outboundTemplate.withDefaultReserveLimits();
                    ResourceLimits.Limit reserveEndpointCapacityInBytes = new ResourceLimits.Concurrent(template.applicationReserveSendQueueEndpointCapacityInBytes);
                    ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(reserveEndpointCapacityInBytes, template.applicationReserveSendQueueGlobalCapacityInBytes);
                    for (ConnectionType type : ConnectionType.MESSAGING_TYPES)
                    {
                        this.connections[i] = new Connection(sender, recipient, type, inboundHandlers, template, reserveCapacityInBytes, messageGenerators.get(type), minId, maxId);
                        this.connectionMessageIds[i] = minId;
                        minId = maxId + 1;
                        maxId += messageIdsPerConnection;
                        ++i;
                    }
                }
            }
        }

        Connection forId(long messageId)
        {
            int i = Arrays.binarySearch(connectionMessageIds, messageId);
            if (i < 0) i = -2 -i;
            Connection connection = connections[i];
            assert connection.minId <= messageId && connection.maxId >= messageId;
            return connection;
        }

        public void run() throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, TimeoutException
        {
            Reporters reporters = new Reporters(endpoints, connections);
            try
            {
                long deadline = System.nanoTime() + runForNanos;
                Verb._TEST_2.unsafeSetSerializer(() -> serializer);
                inbound.sockets.open().get();

                CountDownLatch failed = new CountDownLatch(1);
                for (Connection connection : connections)
                    connection.start(failed::countDown, executor, deadline);

                executor.execute(() -> {
                    Thread.currentThread().setName("Test-SetInFlight");
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    List<Connection> connections = new ArrayList<>(Arrays.asList(this.connections));
                    while (!Thread.currentThread().isInterrupted())
                    {
                        Collections.shuffle(connections);
                        int total = random.nextInt(1 << 20, 128 << 20);
                        for (int i = connections.size() - 1; i >= 1 ; --i)
                        {
                            int average = total / (i + 1);
                            int max = random.nextInt(1, min(2 * average, total - 2));
                            int min = random.nextInt(0, max);
                            connections.get(i).controller.setInFlightByteBounds(min, max);
                            total -= max;
                        }
                        connections.get(0).controller.setInFlightByteBounds(random.nextInt(0, total), total);
                        Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
                    }
                });

                while (deadline > System.nanoTime() && failed.getCount() > 0)
                {
                    reporters.update();
                    reporters.print();
                    Uninterruptibles.awaitUninterruptibly(failed, 30L, TimeUnit.SECONDS);
                }

                executor.shutdownNow();
                ExecutorUtils.awaitTermination(1L, TimeUnit.MINUTES, executor);
            }
            finally
            {
                reporters.update();
                reporters.print();

                inbound.sockets.close().get();
                new FutureCombiner(Arrays.stream(connections)
                                         .map(c -> c.outbound.close(false))
                                         .collect(Collectors.toList()))
                .get();
            }
        }

        class WrappedInboundCallbacks implements InboundMessageCallbacks
        {
            final InboundMessageCallbacks wrapped;
            WrappedInboundCallbacks(InboundMessageCallbacks wrapped)
            {
                this.wrapped = wrapped;
            }

            public void onProcessed(int messageSize)
            {
                throw new IllegalStateException(); // only the wrapped Callbacks should be invoked
            }

            public void onArrived(Message.Header header, long timeElapsed, TimeUnit unit)
            {
                forId(header.id).onArrived(header, timeElapsed, unit);
                wrapped.onArrived(header, timeElapsed, unit);
            }

            public void onExpired(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
            {
                forId(header.id).onExpired(messageSize, header, timeElapsed, unit);
                wrapped.onExpired(messageSize, header, timeElapsed, unit);
            }

            public void onArrivedExpired(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
            {
                forId(header.id).onArrivedExpired(messageSize, header, timeElapsed, unit);
                wrapped.onArrivedExpired(messageSize, header, timeElapsed, unit);
            }

            public void onFailedDeserialize(int messageSize, Message.Header header, Throwable t)
            {
                forId(header.id).onFailedDeserialize(messageSize, header, t);
                wrapped.onFailedDeserialize(messageSize, header, t);
            }
        }

        public InboundMessageCallbacks wrap(InboundMessageCallbacks wrap)
        {
            return new WrappedInboundCallbacks(wrap);
        }

        public void process(Message<?> message, int messageSize, InboundMessageCallbacks callbacks)
        {
            forId(message.id()).process(message, messageSize, ((WrappedInboundCallbacks)callbacks).wrapped);
        }
    }

    public static String prettyPrintMemory(long size)
    {
        if (size >= 1000 * 1000 * 1000)
            return String.format("%.0fG", size / (double) (1 << 30));
        if (size >= 1000 * 1000)
            return String.format("%.0fM", size / (double) (1 << 20));
        return String.format("%.0fK", size / (double) (1 << 10));
    }

    static List<InetAddressAndPort> endpoints(int count)
    {
        return IntStream.rangeClosed(1, count)
                        .mapToObj(ConnectionBurnTest::endpoint)
                        .collect(Collectors.toList());
    }

    private static InetAddressAndPort endpoint(int i)
    {
        try
        {
            return InetAddressAndPort.getByName("127.0.0." + i);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void test(GlobalInboundSettings inbound, OutboundConnectionSettings outbound) throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, TimeoutException
    {
        MessageGenerator small = new UniformPayloadGenerator(0, 1, (1 << 15));
        MessageGenerator large = new UniformPayloadGenerator(0, 1, (1 << 16) + (1 << 15));
        MessageGenerators generators = new MessageGenerators(small, large);
        outbound = outbound.withApplicationSendQueueCapacityInBytes(1 << 18)
                           .withApplicationReserveSendQueueCapacityInBytes(1 << 30, new ResourceLimits.Concurrent(Integer.MAX_VALUE));

        Test.builder()
            .generators(generators)
            .endpoints(4)
            .inbound(inbound)
            .outbound(outbound)
            .time(2L, TimeUnit.DAYS)
            .build().run();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, TimeoutException
    {
        DatabaseDescriptor.daemonInitialization();
        GlobalInboundSettings inboundSettings = new GlobalInboundSettings()
                                                .withQueueCapacity(1 << 18)
                                                .withEndpointReserveLimit(1 << 20)
                                                .withGlobalReserveLimit(1 << 21)
                                                .withTemplate(new InboundConnectionSettings());
        test(inboundSettings, new OutboundConnectionSettings(null)
//                              .withAcceptVersions(new MessagingService.AcceptVersions(VERSION_30, VERSION_30))
                              .withTcpUserTimeoutInMS(0));
        MessagingService.instance().socketFactory.shutdownNow();
    }

}
