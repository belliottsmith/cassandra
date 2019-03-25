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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Message.InvalidLegacyProtocolMagic;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.Crc.InvalidCrc;
import org.apache.cassandra.net.async.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.async.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.async.ResourceLimits.Limit;
import org.apache.cassandra.net.async.ResourceLimits.Outcome;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.Math.min;

/**
 * Parses incoming messages as per the 3.0/3.11/4.0 internode messaging protocols.
 */
public final class InboundMessageHandler extends ChannelInboundHandlerAdapter implements MessageCallbacks
{
    public interface MessageProcessor
    {
        void process(Message<?> message, int messageSize, MessageCallbacks callbacks);
    }

    public interface OnHandlerClosed
    {
        void call(InboundMessageHandler handler);
    }

    private static final Logger logger = LoggerFactory.getLogger(InboundMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    private static final Message.Serializer serializer = Message.serializer;

    private boolean isClosed;
    private boolean isBlocked;

    private final ReadSwitch readSwitch;

    private final Channel channel;
    private final InetAddressAndPort peer;
    private final int version;

    private final int largeThreshold;
    private final ExecutorService largeExecutor;

    private final long queueCapacity;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long queueSize = 0L;
    private static final AtomicLongFieldUpdater<InboundMessageHandler> queueSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandler.class, "queueSize");

    private final Limit endpointReserveCapacity;
    private final WaitQueue endpointWaitQueue;

    private final Limit globalReserveCapacity;
    private final WaitQueue globalWaitQueue;

    private final OnHandlerClosed onClosed;
    private final MessageCallbacks callbacks;
    private final MessageProcessor processor;

    private int largeBytesRemaining; // remainig bytes we need to supply to the coprocessor to deserialize the in-flight large message
    private int skipBytesRemaining;  // remaining bytes we need to skip to get over the expired message

    // wait queue handle, non-null if we overrun endpoint or global capacity and request to be resumed once it's released
    private WaitQueue.Ticket ticket = null;

    long receivedCount, receivedBytes;
    int corruptFramesRecovered, corruptFramesUnrecovered;

    private LargeCoprocessor largeCoprocessor;

    private volatile int largeUnconsumedBytes; // unconsumed bytes in all ByteBufs queued up in all coprocessors
    private static final AtomicIntegerFieldUpdater<InboundMessageHandler> largeUnconsumedBytesUpdater =
        AtomicIntegerFieldUpdater.newUpdater(InboundMessageHandler.class, "largeUnconsumedBytes");

    /**
     * Signal to the underlying frame decoder to pause or resume read activity.
     */
    public interface ReadSwitch
    {
        void resume();
        void pause();
    }

    /*
     * Context for next invocation of channelRead(). On occasion, it's necessary to override endpoint and global reserves,
     * and bound processing to just one message. This is the case when we go beyond either per-endpoing reserve capacity
     * or global reserve capacity.
     */
    private static class NextReadContext
    {
        private boolean pauseAfterOneMessage;
        private Limit endpointReserve;
        private Limit globalReserve;
    }
    private final NextReadContext context = new NextReadContext();

    InboundMessageHandler(ReadSwitch readSwitch,

                          Channel channel,
                          InetAddressAndPort peer,
                          int version,

                          int largeThreshold,
                          ExecutorService largeExecutor,

                          long queueCapacity,
                          Limit endpointReserveCapacity,
                          Limit globalReserveCapacity,
                          WaitQueue endpointWaitQueue,
                          WaitQueue globalWaitQueue,

                          OnHandlerClosed onClosed,
                          MessageCallbacks callbacks,
                          MessageProcessor processor)
    {
        this.readSwitch = readSwitch;

        this.channel = channel;
        this.peer = peer;
        this.version = version;

        this.largeThreshold = largeThreshold;
        this.largeExecutor = largeExecutor;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = endpointReserveCapacity;
        this.endpointWaitQueue = endpointWaitQueue;
        this.globalReserveCapacity = globalReserveCapacity;
        this.globalWaitQueue = globalWaitQueue;

        this.onClosed = onClosed;
        this.callbacks = callbacks;
        this.processor = processor;

        // set up next read context
        context.pauseAfterOneMessage = false;
        context.endpointReserve = endpointReserveCapacity;
        context.globalReserve = globalReserveCapacity;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws InvalidLegacyProtocolMagic, InvalidCrc
    {
        if (msg instanceof IntactFrame)
            readIntactFrame((IntactFrame) msg);
        else
            readCorruptFrame((CorruptFrame) msg);
    }

    private void readIntactFrame(IntactFrame frame) throws InvalidLegacyProtocolMagic
    {
        SharedBytes bytes = frame.contents;
        ByteBuffer buf = bytes.get();
        int readableBytes = buf.remaining();

        if (frame.isSelfContained)
        {
            isBlocked = processMessages(true, frame.contents);
        }
        else if (largeBytesRemaining == 0 && skipBytesRemaining == 0)
        {
            isBlocked = processMessages(false, frame.contents);
        }
        else if (largeBytesRemaining > 0)
        {
            receivedBytes += readableBytes;
            largeBytesRemaining -= readableBytes;

            boolean isKeepingUp = largeCoprocessor.supply(bytes.sliceAndConsume(readableBytes).atomic());
            if (largeBytesRemaining == 0)
            {
                receivedCount++;
                stopCoprocessor();
            }

            if (!isKeepingUp)
            {
                isBlocked = true;
                readSwitch.pause();
            }
        }
        else if (skipBytesRemaining > 0)
        {
            receivedBytes += readableBytes;
            skipBytesRemaining -= readableBytes;

            bytes.skipBytes(readableBytes);
            if (skipBytesRemaining == 0)
                receivedCount++;
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    private void readCorruptFrame(CorruptFrame frame) throws InvalidCrc
    {
        if (!frame.isRecoverable())
        {
            corruptFramesUnrecovered++;
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
        }

        int frameSize = frame.frameSize;
        receivedBytes += frameSize;

        if (frame.isSelfContained)
        {
            noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading messages from {} (corrupted self-contained frame)", peer);
        }
        else if (largeBytesRemaining == 0 && skipBytesRemaining == 0)
        {
            corruptFramesUnrecovered++;
            noSpamLogger.error("Invalid, unrecoverable CRC mismatch detected while reading messages from {} (corrupted first frame of a message)", peer);
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else if (largeBytesRemaining > 0)
        {
            stopCoprocessor();
            skipBytesRemaining = largeBytesRemaining - frameSize;
            largeBytesRemaining = 0;
            noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading a large message from {}", peer);
        }
        else if (skipBytesRemaining > 0)
        {
            skipBytesRemaining -= frameSize;
            if (skipBytesRemaining == 0)
                receivedCount++;
            noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading a message from {}", peer);
        }
        else
        {
            throw new IllegalStateException();
        }

        corruptFramesRecovered++;
    }

    private boolean processMessages(boolean contained, SharedBytes bytes) throws InvalidLegacyProtocolMagic
    {
        boolean isBlocked = false;
        boolean shouldPause = false;

        while (bytes.isReadable())
        {
            isBlocked = !processMessage(bytes, contained, context.endpointReserve, context.globalReserve);
            if (shouldPause = (isBlocked || context.pauseAfterOneMessage))
                break;
        }

        if (shouldPause)
            readSwitch.pause();

        /*
         * Reset processing context to normal
         */
        context.pauseAfterOneMessage = false;
        context.endpointReserve = endpointReserveCapacity;
        context.globalReserve = globalReserveCapacity;

        return isBlocked;
    }

    private boolean processMessage(SharedBytes bytes, boolean contained, Limit endpointReserve, Limit globalReserve) throws InvalidLegacyProtocolMagic
    {
        ByteBuffer buf = bytes.get();
        int size = serializer.messageSize(buf, buf.position(), buf.limit(), version);

        long currentTimeNanos = ApproximateTime.nanoTime();
        long id = serializer.getId(buf, version);
        long createdAtNanos = serializer.getCreatedAtNanos(buf, peer, version);
        long expiresAtNanos = serializer.getExpiresAtNanos(buf, createdAtNanos, version);

        if (expiresAtNanos < currentTimeNanos)
        {
            onArrivedExpired(size, id, serializer.getVerb(buf, version), currentTimeNanos - createdAtNanos, TimeUnit.NANOSECONDS);

            int skipped = contained ? size : buf.remaining();
            receivedBytes += skipped;
            bytes.skipBytes(skipped);

            if (contained)
                receivedCount++;
            else
                skipBytesRemaining = size - skipped;

            return true;
        }

        switch (acquireCapacity(endpointReserve, globalReserve, size))
        {
            case INSUFFICIENT_ENDPOINT:
                enterCapacityWaitQueue(endpointWaitQueue, size, expiresAtNanos, this::onEndpointReserveCapacityRegained);
                return false;
            case INSUFFICIENT_GLOBAL:
                enterCapacityWaitQueue(globalWaitQueue, size, expiresAtNanos, this::onGlobalReserveCapacityRegained);
                return false;
        }

        boolean callBackOnFailure = serializer.getCallBackOnFailure(buf, version);

        if (contained && size <= largeThreshold)
            return processMessageOnEventLoop(bytes, size, id, expiresAtNanos, callBackOnFailure);
        else if (size <= buf.remaining())
            return processMessageOffEventLoopContained(bytes, size, id, expiresAtNanos, callBackOnFailure);
        else
            return processMessageOffEventLoopUncontained(bytes, size, id, expiresAtNanos, callBackOnFailure);
    }

    private void enterCapacityWaitQueue(WaitQueue queue, int bytesRequested, long expiresAtNanos, Consumer<Limit> onCapacityRegained)
    {
        ticket = queue.registerAndSignal(bytesRequested, expiresAtNanos, channel.eventLoop(), onCapacityRegained, this::exceptionCaught, this::resumeIfNotBlocked);
    }

    private boolean processMessageOnEventLoop(SharedBytes bytes, int size, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        receivedCount++;
        receivedBytes += size;

        ByteBuffer buf = bytes.get();
        final int begin = buf.position();
        final int end = buf.limit();
        buf.limit(begin + size); // cap to expected message size

        Message<?> message = null;
        try (DataInputBuffer in = new DataInputBuffer(buf, false))
        {
            message = serializer.deserialize(in, peer, version);
        }
        catch (UnknownTableException | UnknownColumnException e)
        {
            noSpamLogger.info("{} caught while reading a small message from {}", e.getClass().getSimpleName(), peer, e);
            onFailedDeserialize(size, id, expiresAtNanos, callBackOnFailure, e);
        }
        catch (IOException e)
        {
            logger.error("Unexpected IOException caught while reading a small message from {}", peer, e);
            onFailedDeserialize(size, id, expiresAtNanos, callBackOnFailure, e);
        }
        catch (Throwable t)
        {
            releaseCapacity(size);
            onFailedDeserialize(size, id, expiresAtNanos, callBackOnFailure, t);
            throw t;
        }
        finally
        {
            buf.position(begin + size);
            buf.limit(end);

            if (null == message)
                releaseCapacity(size);
        }

        if (null != message)
            processor.process(message, size, this);

        return true;
    }

    private boolean processMessageOffEventLoopContained(SharedBytes bytes, int size, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        receivedCount++;
        receivedBytes += size;

        LargeCoprocessor coprocessor = new LargeCoprocessor(size, id, expiresAtNanos, callBackOnFailure);
        boolean isKeepingUp = coprocessor.supplyAndCloseWithoutSignaling(bytes.sliceAndConsume(size).atomic());
        largeExecutor.submit(coprocessor);
        return isKeepingUp;
    }

    private boolean processMessageOffEventLoopUncontained(SharedBytes bytes, int size, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        int readableBytes = bytes.readableBytes();
        receivedBytes += readableBytes;

        startCoprocessor(size, id, expiresAtNanos, callBackOnFailure);
        boolean isKeepingUp = largeCoprocessor.supply(bytes.sliceAndConsume(readableBytes).atomic());
        largeBytesRemaining = size - readableBytes;
        return isKeepingUp;
    }

    /*
     * Wrapped message callbacks to ensure capacity is released onProcessed() and onExpired()
     */

    @Override
    public void onProcessed(int messageSize)
    {
        releaseCapacity(messageSize);
        callbacks.onProcessed(messageSize);
    }

    @Override
    public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
    {
        releaseCapacity(messageSize);
        callbacks.onExpired(messageSize, id, verb, timeElapsed, unit);
    }

    @Override
    public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
    {
        callbacks.onArrivedExpired(messageSize, id, verb, timeElapsed, unit);
    }

    @Override
    public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
    {
        callbacks.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
    }

    private void onCoprocessorCaughtUp()
    {
        assert channel.eventLoop().inEventLoop();

        if (!isClosed)
        {
            isBlocked = false;
            readSwitch.resume();
        }
    }

    private void onEndpointReserveCapacityRegained(Limit endpointReserve)
    {
        ticket = null;
        onReserveCapacityRegained(endpointReserve, globalReserveCapacity);
    }

    private void onGlobalReserveCapacityRegained(Limit globalReserve)
    {
        ticket = null;
        onReserveCapacityRegained(endpointReserveCapacity, globalReserve);
    }

    private void onReserveCapacityRegained(Limit endpointReserve, Limit globalReserve)
    {
        assert channel.eventLoop().inEventLoop();

        if (!isClosed)
        {
            isBlocked = false;

            context.pauseAfterOneMessage = true;
            context.endpointReserve = endpointReserve;
            context.globalReserve = globalReserve;

            readSwitch.resume();
        }
    }

    private void resumeIfNotBlocked()
    {
        assert channel.eventLoop().inEventLoop();

        if (!isClosed && !isBlocked)
            readSwitch.resume();
    }

    private Outcome acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes)
    {
        long currentQueueSize = queueSize;

        /*
         * acquireCapacity() is only ever called on the event loop, and as such queueSize is only ever increased
         * on the event loop. If there is enough capacity, we can safely addAndGet() and immediately return.
         */
        if (currentQueueSize + bytes <= queueCapacity)
        {
            queueSizeUpdater.addAndGet(this, bytes);
            return Outcome.SUCCESS;
        }

        long allocatedExcess = min(currentQueueSize + bytes - queueCapacity, bytes);
        Outcome outcome = ResourceLimits.tryAllocate(endpointReserve, globalReserve, allocatedExcess);
        if (outcome != Outcome.SUCCESS)
            return outcome;

        long newQueueSize = queueSizeUpdater.addAndGet(this, bytes);
        long actualExcess = min(newQueueSize - queueCapacity, bytes);

        if (actualExcess != allocatedExcess) // can be smaller if a release happened since
            ResourceLimits.release(endpointReserve, globalReserve, allocatedExcess - actualExcess);

        return Outcome.SUCCESS;
    }

    private void releaseCapacity(int bytes)
    {
        long oldQueueSize = queueSizeUpdater.getAndAdd(this, -bytes);
        if (oldQueueSize > queueCapacity)
        {
            long excess = min(oldQueueSize - queueCapacity, bytes);
            ResourceLimits.release(endpointReserveCapacity, globalReserveCapacity, excess);

            endpointWaitQueue.signal();
            globalWaitQueue.signal();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        exceptionCaught(cause);
    }

    private void exceptionCaught(Throwable cause)
    {
        readSwitch.pause();

        JVMStabilityInspector.inspectThrowable(cause);

        if (cause instanceof Message.InvalidLegacyProtocolMagic)
            logger.error("Invalid, unrecoverable CRC mismatch detected while reading messages from {} - closing the connection", peer);
        else
            logger.error("Unexpected exception caught while processing inbound messages from {}; terminating connection", peer, cause);

        channel.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
    }

    /*
     * Clean up after ourselves
     */
    private void close()
    {
        isClosed = true;

        if (null != largeCoprocessor)
            stopCoprocessor();

        if (null != ticket)
        {
            ticket.invalidate();
            ticket = null;
        }

        onClosed.call(this);
    }

    private void startCoprocessor(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        largeCoprocessor = new LargeCoprocessor(messageSize, id, expiresAtNanos, callBackOnFailure);
        largeExecutor.submit(largeCoprocessor);
    }

    private void stopCoprocessor()
    {
        largeCoprocessor.stop();
        largeCoprocessor = null;
    }

    /**
     * This will execute on a thread that isn't a netty event loop.
     */
    private final class LargeCoprocessor implements Runnable
    {
        private final int messageSize;
        private final long id;
        private final long expiresAtNanos;
        private final boolean callBackOnFailure;

        private final AsyncMessagingInputPlus input;

        private final int maxUnconsumedBytes;

        private LargeCoprocessor(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure)
        {
            this.messageSize = messageSize;
            this.id = id;
            this.expiresAtNanos = expiresAtNanos;
            this.callBackOnFailure = callBackOnFailure;

            this.input = new AsyncMessagingInputPlus(this::onBufConsumed);
            /*
             * Allow up to 2x large message threshold bytes of ByteBufs in coprocessors' queues before pausing reads
             * from the channel. Signal the handler to resume reading from the channel once we've consumed enough
             * bytes from the queues to drop below this threshold again.
             */
            maxUnconsumedBytes = largeThreshold * 2;
        }

        public void run()
        {
            String priorThreadName = null;
            try
            {
                priorThreadName = Thread.currentThread().getName();
                Thread.currentThread().setName("MessagingService-Inbound-" + peer + "-LargeMessage-" + messageSize);

                processLargeMessage();
            }
            finally
            {
                if (null != priorThreadName)
                    Thread.currentThread().setName(priorThreadName);
            }
        }

        private void processLargeMessage()
        {
            Message<?> message = null;
            try
            {
                message = serializer.deserialize(input, peer, version);
            }
            catch (UnknownTableException | UnknownColumnException e)
            {
                noSpamLogger.info("{} caught while reading a large message from {}", e.getClass().getSimpleName(), peer, e);
                onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, e);
            }
            catch (IOException e)
            {
                logger.error("Unexpected IOException caught while reading a large message from {}", peer, e);
                onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("Unexpected exception caught while reading a large message from {}", peer, t);
                onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
                channel.pipeline().context(InboundMessageHandler.this).fireExceptionCaught(t);
            }
            finally
            {
                if (null == message)
                    releaseCapacity(messageSize);

                input.close();
            }

            if (null != message)
                processor.process(message, messageSize, InboundMessageHandler.this);
        }

        void stop()
        {
            input.requestClosure();
        }

        /*
         * Returns true if coprocessor is keeping up and can accept more input, false if it's fallen behind.
         */
        boolean supply(SharedBytes bytes)
        {
            int unconsumed = largeUnconsumedBytesUpdater.addAndGet(InboundMessageHandler.this, bytes.readableBytes());
            input.supply(bytes);
            return unconsumed <= maxUnconsumedBytes;
        }

        boolean supplyAndCloseWithoutSignaling(SharedBytes bytes)
        {
            int unconsumed = largeUnconsumedBytesUpdater.addAndGet(InboundMessageHandler.this, bytes.readableBytes());
            input.supplyAndCloseWithoutSignaling(bytes);
            return unconsumed <= maxUnconsumedBytes;
        }

        private void onBufConsumed(int size)
        {
            int unconsumed = largeUnconsumedBytesUpdater.addAndGet(InboundMessageHandler.this, -size);
            int prevUnconsumed = unconsumed + size;

            if (unconsumed <= maxUnconsumedBytes && prevUnconsumed > maxUnconsumedBytes)
                channel.eventLoop().submit(InboundMessageHandler.this::onCoprocessorCaughtUp);
        }
    }

    public static final class WaitQueue
    {
        /*
         * Callback scheduler states
         */
        private static final int NOT_RUNNING = 0;
        @SuppressWarnings("unused")
        private static final int RUNNING     = 1;
        private static final int RUN_AGAIN   = 2;
        private volatile int scheduled;
        private static final AtomicIntegerFieldUpdater<WaitQueue> scheduledUpdater =
            AtomicIntegerFieldUpdater.newUpdater(WaitQueue.class, "scheduled");

        private final Limit reserveCapacity;

        private final ConcurrentLinkedQueue<Ticket> queue = new ConcurrentLinkedQueue<>();

        public WaitQueue(Limit reserveCapacity)
        {
            this.reserveCapacity = reserveCapacity;
        }

        private Ticket registerAndSignal(int bytesRequested,
                                         long expiresAtNanos,
                                         EventLoop eventLoop,
                                         Consumer<Limit> processOneCallback,
                                         Consumer<Throwable> errorCallback,
                                         Runnable resumeCallback)
        {
            Ticket ticket = new Ticket(this, bytesRequested, expiresAtNanos, eventLoop, processOneCallback, errorCallback, resumeCallback);
            queue.add(ticket);
            signal();
            return ticket;
        }

        void signal()
        {
            if (queue.isEmpty())
                return;

            if (NOT_RUNNING == scheduledUpdater.getAndUpdate(this, i -> Integer.min(RUN_AGAIN, i + 1)))
            {
                do
                {
                    schedule();
                }
                while (RUN_AGAIN == scheduledUpdater.getAndDecrement(this));
            }
        }

        private void schedule()
        {
            Map<EventLoop, ResumeProcessing> tasks = null;

            long nanoTime = ApproximateTime.nanoTime();

            Ticket t;
            while ((t = queue.peek()) != null)
            {
                if (!t.call()) // invalidated
                {
                    queue.poll();
                    continue;
                }

                if (t.isLive(nanoTime) && !reserveCapacity.tryAllocate(t.bytesRequested))
                {
                    t.reset();
                    break;
                }

                if (null == tasks)
                    tasks = new IdentityHashMap<>();

                tasks.computeIfAbsent(t.eventLoop, e -> new ResumeProcessing()).add(queue.poll());
            }

            if (null != tasks)
                tasks.forEach(EventExecutorGroup::submit);
        }

        class ResumeProcessing implements Runnable
        {
            List<Ticket> tickets = new ArrayList<>();

            private void add(Ticket ticket)
            {
                tickets.add(ticket);
            }

            public void run()
            {
                long capacity = 0L;

                for (Ticket t : tickets)
                    capacity += t.bytesRequested;

                Limit limit = new ResourceLimits.Basic(capacity);

                /*
                 * For each handler, process one message off the current frame.
                 */
                try
                {
                    for (Ticket ticket : tickets)
                    {
                        try
                        {
                            ticket.processOneCallback.accept(limit);
                        }
                        catch (Throwable t)
                        {
                            try
                            {
                                ticket.errorCallback.accept(t);
                            }
                            catch (Throwable e)
                            {
                                // no-op
                            }
                        }
                    }
                }
                finally
                {
                    /*
                     * Free up any unused global capacity, if any. Will be non-zero if one or more handlers were closed
                     * when we attempted to run their callback or used more of their personal allowance.
                     */
                    reserveCapacity.release(limit.remaining());
                }

                /*
                 * For each handler, resume normal processing.
                 */
                for (Ticket ticket : tickets)
                {
                    try
                    {
                        ticket.resumeCallback.run();
                    }
                    catch (Throwable t)
                    {
                        try
                        {
                            ticket.errorCallback.accept(t);
                        }
                        catch (Throwable e)
                        {
                            // no-op
                        }
                    }
                }
            }
        }

        static final class Ticket
        {
            private static final int WAITING     = 0;
            private static final int CALLED      = 1;
            private static final int INVALIDATED = 2;

            private volatile int state;
            private static final AtomicIntegerFieldUpdater<Ticket> stateUpdater =
                AtomicIntegerFieldUpdater.newUpdater(Ticket.class, "state");

            private final WaitQueue waitQueue;
            private final int bytesRequested;
            private final long expiresAtNanos;
            private final EventLoop eventLoop;

            private final Consumer<Limit> processOneCallback;
            private final Consumer<Throwable> errorCallback;
            private final Runnable resumeCallback;

            private Ticket(WaitQueue waitQueue,
                           int bytesRequested,
                           long expiresAtNanos,
                           EventLoop eventLoop,
                           Consumer<Limit> processOneCallback,
                           Consumer<Throwable> errorCallback,
                           Runnable resumeCallback)
            {
                this.waitQueue = waitQueue;
                this.bytesRequested = bytesRequested;
                this.expiresAtNanos = expiresAtNanos;
                this.eventLoop = eventLoop;

                this.processOneCallback = processOneCallback;
                this.errorCallback = errorCallback;
                this.resumeCallback = resumeCallback;
            }

            boolean isInvalidated()
            {
                return state == INVALIDATED;
            }

            boolean isLive(long currentTimeNanos)
            {
                return currentTimeNanos <= expiresAtNanos;
            }

            void invalidate()
            {
                if (stateUpdater.compareAndSet(this, WAITING, INVALIDATED))
                    waitQueue.signal();
            }

            private boolean call()
            {
                return stateUpdater.compareAndSet(this, WAITING, CALLED);
            }

            private void reset()
            {
                state = WAITING;
            }
        }
    }
}
