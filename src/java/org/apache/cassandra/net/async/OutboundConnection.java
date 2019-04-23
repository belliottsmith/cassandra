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
import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.channel.unix.Errors;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseNotifier;
import io.netty.util.concurrent.SucceededFuture;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundConnectionInitiator.Result.MessagingSuccess;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.OutboundConnectionInitiator.*;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;
import static org.apache.cassandra.net.async.ResourceLimits.*;
import static org.apache.cassandra.net.async.ResourceLimits.Outcome.*;
import static org.apache.cassandra.net.async.SocketFactory.*;
import static org.apache.cassandra.utils.Throwables.isCausedBy;

/**
 * Represents a connection type to a peer, and handles the state transistions on the connection and the netty {@link Channel}.
 * The underlying socket is not opened until explicitly requested (by sending a message).
 *
 * TODO: complete this description
 *
 * Aside from a few administrative methods, the main entry point to sending a message is {@link #enqueue(Message)}.
 * Any thread may send a message (enqueueing it to {@link #queue}), but only one thread may consume messages from this
 * queue.  There is a single delivery thread - either the event loop, or a companion thread - that has logical ownership
 * of the queue, but other threads may temporarily take ownership in order to perform book keeping, pruning, etc.,
 * to ensure system stability.
 *
 * {@link Delivery#run()} is the main entry point for consuming messages from the queue, and executes either on the event
 * loop or on a non-dedicated companion thread.  This processing is activated via {@link Delivery#execute()}.
 *
 * Almost all internal state maintenance on this class occurs on the eventLoop, a single threaded executor which is
 * assigned in the constructor.  Further details are outlined below in the class.  Some behaviours require coordination
 * between the eventLoop and the companion thread (if any).  Some minimal set of behaviours are permitted to occur on
 * producers to ensure the connection remains healthy and does not overcommit resources.
 *
 * All methods are safe to invoke from any thread unless otherwise stated.
 */
public class OutboundConnection
{
    static final Logger logger = LoggerFactory.getLogger(OutboundConnection.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS);

    private static final AtomicLongFieldUpdater<OutboundConnection> submittedUpdater = AtomicLongFieldUpdater.newUpdater(OutboundConnection.class, "submittedCount");
    private static final AtomicLongFieldUpdater<OutboundConnection> pendingCountAndBytesUpdater = AtomicLongFieldUpdater.newUpdater(OutboundConnection.class, "pendingCountAndBytes");
    private static final AtomicLongFieldUpdater<OutboundConnection> droppedDueToOverloadUpdater = AtomicLongFieldUpdater.newUpdater(OutboundConnection.class, "overloadCount");
    private static final AtomicLongFieldUpdater<OutboundConnection> droppedBytesDueToOverloadUpdater = AtomicLongFieldUpdater.newUpdater(OutboundConnection.class, "overloadBytes");
    private static final AtomicReferenceFieldUpdater<OutboundConnection, Future> closingUpdater = AtomicReferenceFieldUpdater.newUpdater(OutboundConnection.class, Future.class, "closing");
    private static final AtomicReferenceFieldUpdater<OutboundConnection, Future> scheduledCloseUpdater = AtomicReferenceFieldUpdater.newUpdater(OutboundConnection.class, Future.class, "scheduledClose");

    private final EventLoop eventLoop;
    private final Delivery delivery;

    private final OutboundMessageQueue queue;
    /** the number of bytes we permit to queue to the network without acquiring any shared resource permits */
    private final long pendingCapacityInBytes;
    /** the number of messages and bytes queued for flush to the network,
     * including those that are being flushed but have not been completed,
     * packed into a long (top 20 bits for count, bottom 42 for bytes)*/
    private volatile long pendingCountAndBytes = 0;
    /** global shared limits that we use only if our local limits are exhausted;
     *  we allocate from here whenever queueSize > queueCapacity */
    private final EndpointAndGlobal reserveCapacityInBytes;

    private volatile long submittedCount = 0;   // updated with cas
    private volatile long overloadCount = 0;    // updated with cas
    private volatile long overloadBytes = 0;    // updated with cas
    private long expiredCount = 0;              // updated with queue lock held
    private long expiredBytes = 0;              // updated with queue lock held
    private long errorCount = 0;                // updated only by delivery thread
    private long errorBytes = 0;                // updated by delivery thread only
    private long sent;                          // updated by delivery thread only
    private long sentBytes;                     // updated by delivery thread only
    private long successfulConnections;         // updated by event loop only
    private long connectionAttempts;            // updated by event loop only

    private static final int pendingByteBits = 42;
    private static boolean isMaxPendingCount(long pendingCountAndBytes)
    {
        return (pendingCountAndBytes & (-1L << pendingByteBits)) == (-1L << pendingByteBits);
    }

    private static int pendingCount(long pendingCountAndBytes)
    {
        return (int) (pendingCountAndBytes >>> pendingByteBits);
    }

    private static long pendingBytes(long pendingCountAndBytes)
    {
        return pendingCountAndBytes & (-1L >>> (64 - pendingByteBits));
    }

    private static long pendingCountAndBytes(long pendingCount, long pendingBytes)
    {
        return (pendingCount << pendingByteBits) | pendingBytes;
    }

    /*
     * The following four fields define our connection behaviour; the template contains the user-specified desired properties,
     * and the settings are produced by combining those specifications with any system defaults and knowledge about the endpoint.
     *
     * New templates may be provided by external calls, at which point we will disconnect reconnect to enact any modifications.
     * The messaging version is negotiated on each connection attempt, as it may change at any time.
     * The settings are generated from the template on every connection attempt, as any system-wide properties,
     * as well as implied defaults based on messagingVersion (e.g. port to connect on with encryption) may change.
     */

    private final ConnectionType type;
    // settings is updated whenever template is, which may occur in advance of a reconnection attempt;
    // in this scenario, they briefly do not represent those currently in use, but those we intend to use imminently
    private volatile OutboundConnectionSettings template;
    private volatile OutboundConnectionSettings settings;
    private volatile int messagingVersion;
    private volatile FrameEncoder.PayloadAllocator payloadAllocator;

    /*
     * All of the volatile fields may only be updated by the eventLoop.
     * They should be read extremely carefully by other threads, and updated carefully by the eventLoop if so read.
     *
     * For example, the deliveryThread will access the {@link channel} concurrently with the eventLoop under normal operation,
     * so any attempt to close the channel must ensure the deliveryThread has acquiesced first.
     *
     * The AtomicReference {@link closing} may be read and written by any thread, but once set should never be updated.
     *
     * Those that are not final, volatile or atomic may only be accessed by the eventLoop; for either read or write.
     */

    /** The underlying Netty channel we are connected to our endpoint via; this may be closed, or null */
    private volatile Channel channel;
    /** We cannot depend on channel.isOpen, as we may be closing it asynchronously */
    private volatile boolean isConnected;
    /** If we are not connected, are trying to connect, and have failed to connect at least once */
    private volatile boolean isFailingToConnect;

    /** True => connection is permanently closed. */
    private volatile boolean isClosed;
    /** The connection is being permanently closed */
    private volatile Future<Void> closing;
    /** The connection is being permanently closed in the near future */
    private volatile Future<Void> scheduledClose;

    /** Periodic message expiry scheduled while we are disconnected; this will be cancelled and cleared each time we connect */
    private Future<?> whileDisconnected;

    /**
     * Currently being (re)connected; this may be cancelled (if closing) or waited on (for delivery)
     * Note that the work managed by this future may be performed asynchronously, and not on the eventLoop.
     * It may also be completed outside of the eventLoop, though its listeners will all be invoked on the eventLoop.
     */
    private Future<?> connecting;

    OutboundConnection(ConnectionType type, OutboundConnectionSettings template, EndpointAndGlobal reserveCapacityInBytes)
    {
        // use the best guessed messaging version for a node
        // this could be wrong, e.g. because the node is upgraded between gossip arrival and our connection attempt
        this.messagingVersion = MessagingService.instance().versions.get(template.endpoint);
        this.template = template;
        this.settings = template.withDefaults(type, messagingVersion);
        this.type = type;
        this.eventLoop = settings.socketFactory.defaultGroup().next();
        this.pendingCapacityInBytes = settings.applicationSendQueueCapacityInBytes;
        this.reserveCapacityInBytes = reserveCapacityInBytes;
        this.queue = new OutboundMessageQueue(this::onTimeout);
        this.delivery = type == ConnectionType.LARGE_MESSAGES
                        ? new LargeMessageDelivery()
                        : new EventLoopDelivery();
        scheduleMaintenanceWhileDisconnected();
    }

    /**
     * This is the main entry point for enqueuing a message to be sent to the remote peer.
     */
    public void enqueue(Message message) throws ClosedChannelException
    {
        if (isClosed())
            throw new ClosedChannelException();

        submittedUpdater.incrementAndGet(this);
        switch (acquireCapacity(canonicalSize(message)))
        {
            case INSUFFICIENT_ENDPOINT:
                // if we're overloaded to one endpoint, we may be accumulating expirable messages, so
                // attempt an expiry to see if this makes room for our newer message.
                // this is an optimisation only; messages will be expired on ~100ms cycle, and by Delivery when it runs
                if (queue.maybePruneExpired() &&
                    SUCCESS == acquireCapacity(canonicalSize(message)))
                    break;
            case INSUFFICIENT_GLOBAL:
                onOverload(message);
                return;
        }

        queue.add(message);
        delivery.execute();

        // we might race with the channel closing; if this happens, to ensure this message eventually arrives
        // we need to remove ourselves from the queue and throw a ClosedChannelException, so that another channel
        // can be opened in our place to try and send on.
        if (isClosed() && queue.undoAdd(message))
        {
            releaseCapacity(1, canonicalSize(message));
            throw new ClosedChannelException();
        }
    }

    /**
     * Try to acquire the necessary resource permits for a number of pending bytes for this connection.
     *
     * Since the owner limit is shared amongst multiple connections, our semantics cannot be super trivial.
     * Were they per-connection, we could simply perform an atomic increment of the queue size, then
     * allocate any excess we need in the reserve, and on release free everything we see from both.
     * Since we are coordinating two independent atomic variables we have to track every byte we allocate in reserve
     * and ensure it is matched by a corresponding released byte. We also need to be sure we do not permit another
     * releasing thread to release reserve bytes we have not yet - and may never - actually reserve.
     *
     * As such, we have to first check if we would need reserve bytes, then allocate them *before* we increment our
     * queue size.  We only increment the queue size if the reserve bytes are definitely not needed, or we could first
     * obtain them.  If in the process of obtaining any reserve bytes the queue size changes, we have some bytes that are
     * reserved for us, but may be a different number to that we need.  So we must continue to track these.
     *
     * In the happy path, this is still efficient as we simply CAS
     */
    private Outcome acquireCapacity(long bytes)
    {
        return acquireCapacity(1, bytes);
    }

    private Outcome acquireCapacity(long count, long bytes)
    {
        long increment = pendingCountAndBytes(count, bytes);
        long unusedClaimedReserve = 0;
        Outcome outcome = null;
        loop: while (true)
        {
            long current = pendingCountAndBytes;
            if (isMaxPendingCount(current))
            {
                outcome = INSUFFICIENT_ENDPOINT;
                break;
            }

            long next = current + increment;
            if (pendingBytes(next) <= pendingCapacityInBytes)
            {
                if (pendingCountAndBytesUpdater.compareAndSet(this, current, next))
                {
                    outcome = SUCCESS;
                    break;
                }
                continue;
            }

            if (isFailingToConnect)
            {
                outcome = INSUFFICIENT_ENDPOINT;
                break;
            }

            long requiredReserve = min(bytes, pendingBytes(next) - pendingCapacityInBytes);
            if (unusedClaimedReserve < requiredReserve)
            {
                long extraGlobalReserve = requiredReserve - unusedClaimedReserve;
                switch (outcome = reserveCapacityInBytes.tryAllocate(extraGlobalReserve))
                {
                    case INSUFFICIENT_ENDPOINT:
                    case INSUFFICIENT_GLOBAL:
                        break loop;
                    case SUCCESS:
                        unusedClaimedReserve += extraGlobalReserve;
                }
            }

            if (pendingCountAndBytesUpdater.compareAndSet(this, current, next))
            {
                unusedClaimedReserve -= requiredReserve;
                break;
            }
        }

        if (unusedClaimedReserve > 0)
            reserveCapacityInBytes.release(unusedClaimedReserve);

        return outcome;
    }

    /**
     * Mark a number of pending bytes as flushed to the network, releasing their capacity for new outbound messages.
     */
    @VisibleForTesting
    void releaseCapacity(long count, long bytes)
    {
        long decrement = pendingCountAndBytes(count, bytes);
        long prev = pendingCountAndBytesUpdater.getAndAdd(this, -decrement);
        if (pendingBytes(prev) > pendingCapacityInBytes)
        {
            long excess = min(pendingBytes(prev) - pendingCapacityInBytes, bytes);
            reserveCapacityInBytes.release(excess);
        }
    }

    /**
     * Take any necessary cleanup action after a message has been rejected before reaching the queue
     */
    private void onOverload(Message<?> msg)
    {
        droppedDueToOverloadUpdater.incrementAndGet(this);
        droppedBytesDueToOverloadUpdater.addAndGet(this, canonicalSize(msg));
        noSpamLogger.warn("{} overloaded; dropping {} message (queue: {} local, {} endpoint, {} global)",
                          id(),
                          FBUtilities.prettyPrintMemory(canonicalSize(msg)),
                          FBUtilities.prettyPrintMemory(pendingBytes()),
                          FBUtilities.prettyPrintMemory(reserveCapacityInBytes.endpoint.using()),
                          FBUtilities.prettyPrintMemory(reserveCapacityInBytes.global.using()));
        MessagingService.instance().callbacks.removeAndExpire(msg.id);
    }

    /**
     * Take any necessary cleanup action after a message has been selected to be discarded from the queue.
     *
     * Only to be invoked while holding OutboundMessageQueue.WithLock
     */
    private boolean onTimeout(Message<?> msg)
    {
        releaseCapacity(1, canonicalSize(msg));
        expiredCount += 1;
        expiredBytes += canonicalSize(msg);
        noSpamLogger.warn("{} dropping message of type {} whose timeout expired before reaching the network", id(), msg.verb);
        MessagingService.instance().callbacks.removeAndExpire(msg.id);
        return true;
    }

    /**
     * Take any necessary cleanup action after a message has been selected to be discarded from the queue.
     *
     * Only to be invoked by the delivery thread
     */
    private void onError(Message<?> msg, Throwable t)
    {
        JVMStabilityInspector.inspectThrowable(t);
        releaseCapacity(1, canonicalSize(msg));
        errorCount += 1;
        errorBytes += canonicalSize(msg);
        logger.warn("{} dropping message of type {} due to error", id(), msg.verb, t);
        MessagingService.instance().callbacks.removeAndExpire(msg.id);
    }

    /**
     * Take any necessary cleanup action after a message has been selected to be discarded from the queue on close.
     * Note that this is only for messages that were queued prior to closing without graceful flush, OR
     * for those that are unceremoniously dropped when we decide close has been trying to complete for too long.
     */
    private void onDiscardOnClosing(Message<?> msg)
    {
        releaseCapacity(1, canonicalSize(msg));
        MessagingService.instance().callbacks.removeAndExpire(msg.id);
    }

    /**
     * Delivery bundles the following:
     *
     *  - the work that is necessary to actually deliver messages safely, and handle any exceptional states
     *  - the ability to schedule delivery for some time in the future
     *  - the ability to schedule some non-delivery work to happen some time in the future, that is guaranteed
     *    NOT to coincide with delivery for its duration, including any data that is being flushed (e.g. for closing channels)
     *      - this feature is *not* efficient, and should only be used for infrequent operations
     */
    private abstract class Delivery extends AtomicInteger implements Runnable
    {
        final ExecutorService executor;

        // the AtomicInteger we extend always contains some combination of these bit flags, representing our current run state

        /** Not running, and will not be scheduled again until transitioned to a new state */
        private static final int STOPPED               = 0;
        /** Currently executing (may only be scheduled to execute, or may be about to terminate);
         *  will stop at end of this run, without rescheduling */
        private static final int EXECUTING             = 1;
        /** Another execution has been requested; a new execution will begin some time after this state is taken */
        private static final int EXECUTE_AGAIN         = 2;
        /** We are currently executing and will submit another execution before we terminate */
        private static final int EXECUTING_AGAIN       = EXECUTING | EXECUTE_AGAIN;
        /** Will begin a new execution some time after this state is taken, but only once some condition is met.
         *  This state will initially be taken in tandem with EXECUTING, but if delivery completes without clearing
         *  the state, the condition will be held on its own until {@link #executeAgain} is invoked */
        private static final int WAITING_TO_EXECUTE    = 4;

        /**
         * Force all task execution to stop, once any currently in progress work is completed
         */
        private volatile boolean terminated;

        /**
         * Is there asynchronous delivery work in progress.
         *
         * This temporarily prevents any {@link #stopAndRun} work from being performed.
         * Once both inProgress and stopAndRun are set we perform no more delivery work until one is unset,
         * to ensure we eventually run stopAndRun.
         *
         * This should be updated and read only on the Delivery thread.
         */
        private boolean inProgress = false;

        /**
         * Request a task's execution while there is no delivery work in progress.
         *
         * This is to permit cleanly tearing down a connection without interrupting any messages that might be in flight.
         * If stopAndRun is set, we should not enter doRun() until a corresponding setInProgress(false) occurs.
         */
        final AtomicReference<Runnable> stopAndRun = new AtomicReference<>();

        Delivery(ExecutorService executor)
        {
            this.executor = executor;
        }

        /**
         * Ensure that any messages or stopAndRun that were queued prior to this invocation will be seen by at least
         * one future invocation of the delivery task, unless delivery has already been terminated.
         */
        public void execute()
        {
            if (get() < EXECUTE_AGAIN && STOPPED == getAndUpdate(i -> i == STOPPED ? EXECUTING: i | EXECUTE_AGAIN))
                executor.execute(this);
        }

        private boolean isExecuting(int state)
        {
            return 0 != (state & EXECUTING);
        }

        /**
         * This method is typically invoked after WAITING_TO_EXECUTE is set.
         *
         * However WAITING_TO_EXECUTE does not need to be set; all this method needs to ensure is that
         * delivery unconditionally performs one new execution promptly.
         */
        void executeAgain()
        {
            /**
             * if we are already executing, set EXECUTING_AGAIN and leaves scheduling to the currently running one
             * otherwise, set ourselves unconditionally to EXECUTING and schedule ourselves immediately
             */
            if (!isExecuting(getAndUpdate(i -> isExecuting(i) ? EXECUTING : EXECUTING_AGAIN)))
                executor.execute(this);
        }

        /**
         * Invoke this when we cannot make further progress now, but we guarantee that we will execute later when we can.
         * This simply communicates to {@link #run} that we should not schedule ourselves again, just unset the EXECUTING bit.
         */
        void promiseToExecuteLater()
        {
            set(EXECUTING | WAITING_TO_EXECUTE);
        }

        /**
         * Called when exiting {@link #run} to schedule another run if necessary.
         *
         * If we are currently executing, we only reschedule if the present state is EXECUTING_AGAIN.
         * If this is the case, we clear the EXECUTE_AGAIN bit (setting ourselves to EXECUTING), and reschedule.
         * Otherwise, we clear the EXECUTING bit and terminate, which will set us to either STOPPED or WAITING_TO_EXECUTE
         * (or possibly WAITING_TO_EXECUTE | EXECUTE_AGAIN, which is logically the same as WAITING_TO_EXECUTE)
         */
        private void maybeExecuteAgain()
        {
            /**
             * If we are
             */
            if (EXECUTING_AGAIN == getAndUpdate(i -> i == EXECUTING_AGAIN ? EXECUTING : (i & ~EXECUTING)))
                executor.execute(this);
        }

        /**
         * No more tasks or delivery will be executed, once any in progress complete.
         */
        public void terminate()
        {
            terminated = true;
        }

        /**
         * Only to be invoked by the Delivery task.
         *
         * If true, indicates that we have begun asynchronous delivery work, so that
         * we cannot safely stopAndRun until it completes.
         *
         * Once it completes, we ensure any stopAndRun task has a chance to execute
         * by ensuring delivery is scheduled.
         *
         * If stopAndRun is also set, we should not enter doRun() until a corresponding
         * setInProgress(false) occurs.
         */
        void setInProgress(boolean inProgress)
        {
            boolean wasInProgress = this.inProgress;
            this.inProgress = inProgress;
            if (!inProgress && wasInProgress)
                executeAgain();
        }

        /**
         * Invalidate the channel if the exception indicates the channel is in a bad state
         */
        boolean invalidateChannelIfConnectionReset(Channel channel, Throwable cause)
        {
            // TODO: should we just invalidate the channel whatever happens?
            if (isCausedBy(cause, t ->    isConnectionReset(t)
                                       || t instanceof Errors.NativeIoException
                                       || t instanceof AsyncChannelOutputPlus.FlushException))
            {
                invalidateChannel(channel, cause);
                return false;
            }
            return true;
        }

        /**
         * Handle a failure during delivery that invalidates the channel
         */
        void invalidateChannel(Channel channel, Throwable cause)
        {
            JVMStabilityInspector.inspectThrowable(cause);
            if (isCausedByConnectionReset(cause))
                logger.debug("{} channel closed by provider", id(), cause);
            else
                logger.error("{} channel in potentially inconsistent state after error; closing", id(), cause);

            closeChannelNow(channel);
        }

        /**
         * Perform some delivery work.
         *
         * Must never be invoked directly, only via {@link #execute()}
         */
        public void run()
        {
            /** do/while handling setup for {@link #doRun()}, and repeat invocations thereof */
            do
            {
                if (terminated)
                    return;

                if (null != stopAndRun.get())
                {
                    // if we have an external request to perform, attempt it - if no async delivery is in progress

                    if (inProgress)
                    {
                        // if we are in progress, we cannot do anything;
                        // so, exit and rely on setInProgress(false) executing us
                        // (which must happen later, since it must happen on this thread)
                        promiseToExecuteLater();
                        break;
                    }

                    stopAndRun.getAndSet(null).run();
                }

                if (!isConnected())
                {
                    // if we have messages yet to deliver, or a task to run, we need to reconnect and try again
                    // we try to reconnect before running another stopAndRun so that we do not infinite loop in close
                    if (hasPending() || null != stopAndRun.get())
                    {
                        promiseToExecuteLater();
                        requestConnect().addListener(f -> executeAgain());
                    }
                    break;
                }
            }
            while (doRun());

            maybeExecuteAgain();
        }

        /**
         * @return true if we should run again immediately;
         *         always false for eventLoop executor, as want to service other channels
         */
        abstract boolean doRun();

        /**
         * Schedule a task to run later on the delivery thread while delivery is not in progress,
         * i.e. there are no bytes in flight to the network buffer.
         *
         * Does not guarantee to run promptly if there is no current connection to the remote host.
         * May wait until a new connection is established, or a connection timeout elapses, before executing.
         *
         * Update the shared atomic property containing work we want to interrupt message processing to perform,
         * the invoke schedule() to be certain it gets run.
         */
        void stopAndRun(Runnable run)
        {
            stopAndRun.accumulateAndGet(run, OutboundConnection::andThen);
            execute();
        }

        /**
         * Schedule a task to run on the eventLoop, guaranteeing that delivery will not occur while the task is performed.
         */
        abstract void stopAndRunOnEventLoop(Runnable run);

    }

    /**
     * Delivery that runs entirely on the eventLoop
     *
     * Since this has single threaded access to most of its environment, it can be simple and efficient, however
     * it must also have bounded run time, and limit its resource consumption to ensure other channels serviced by the
     * eventLoop can also make progress.
     *
     * This operates on modest buffers, no larger than the {@link OutboundConnections#LARGE_MESSAGE_THRESHOLD} and
     * filling at most one at a time before writing (potentially asynchronously) to the socket.
     *
     * We track the number of bytes we have in flight, ensuring no more than a user-defined maximum at any one time.
     */
    class EventLoopDelivery extends Delivery
    {
        private int flushingBytes;
        private boolean isWritable = true;

        EventLoopDelivery()
        {
            super(eventLoop);
        }

        /**
         * {@link Delivery#doRun}
         *
         * Since we are on the eventLoop, in order to ensure other channels are serviced
         * we never return true to request another run immediately.
         *
         * If there is more work to be done, we submit ourselves for execution once the eventLoop has time.
         */
        @SuppressWarnings("resource")
        boolean doRun()
        {
            if (!isWritable)
                return false;

            // pendingBytes is updated before queue.size() (which triggers notEmpty, and begins delivery),
            // so it is safe to use it here to exit delivery
            // this number is inaccurate for old versions, but we don't mind terribly - we'll send at least one message,
            // and get round to it eventually (though we could add a fudge factor for some room for older versions)
            int maxSendBytes = (int) min(pendingBytes() - flushingBytes, LARGE_MESSAGE_THRESHOLD);
            if (maxSendBytes == 0)
                return false;

            FrameEncoder.Payload sending = null;
            int canonicalSize = 0; // number of bytes we must use for our resource accounting
            int sendingBytes = 0;
            int sendingCount = 0;
            try (OutboundMessageQueue.WithLock withLock = queue.lockOrCallback(System.nanoTime(), this::execute))
            {
                if (withLock == null)
                    return false; // we failed to acquire the queue lock, so return; we will be scheduled again when the lock is available

                sending = payloadAllocator.allocate(true, maxSendBytes);
                DataOutputBufferFixed out = new DataOutputBufferFixed(sending.buffer);

                Message<?> next;
                while ( null != (next = withLock.peek()) )
                {
                    try
                    {
                        int messageSize = next.serializedSize(messagingVersion);
                        if (messageSize > sending.remaining())
                        {
                            // if we don't have enough room to serialize the next message, we have either
                            //  1) run out of room after writing some messages successfully; this might mean that we are
                            //     overflowing our highWaterMark, or that we have just filled our buffer
                            //  2) we have a message that is too large for this connection; this can happen if a message's
                            //     size was calculated for the wrong messaging version when enqueued.
                            //     In this case we want to write it anyway, so simply allocate a large enough buffer.

                            if (sendingBytes > 0)
                                break;

                            sending.release();
                            sending = null; // set to null to prevent double-release if we fail to allocate our new buffer
                            sending = payloadAllocator.allocate(true, messageSize);
                            out = new DataOutputBufferFixed(sending.buffer);
                        }

                        Tracing.instance.traceOutgoingMessage(next, settings.connectTo);
                        Message.serializer.serialize(next, out, messagingVersion);

                        if (sending.length() != sendingBytes + messageSize)
                            throw new IOException("Calculated serializedSize " + messageSize + " did not match actual " + (sending.length() - sendingBytes));

                        canonicalSize += canonicalSize(next);
                        sendingCount += 1;
                        sendingBytes += messageSize;
                    }
                    catch (Throwable t)
                    {
                        onError(next, t);
                        assert sending != null;
                        // reset the buffer to ignore the message we failed to serialize
                        sending.trim(sendingBytes);
                    }
                    withLock.removeHead(next);
                }
                if (0 == sendingBytes)
                    return false;

                sending.finish();
                ChannelFuture flushResult = AsyncChannelPromise.writeAndFlush(channel, sending);
                sending = null;

                if (flushResult.isSuccess())
                {
                    sent += sendingCount;
                    sentBytes += sendingBytes;
                }
                else
                {
                    flushingBytes += canonicalSize;
                    setInProgress(true);

                    boolean hasOverflowed = flushingBytes >= settings.flushHighWaterMark;
                    if (hasOverflowed)
                    {
                        isWritable = false;
                        promiseToExecuteLater();
                    }

                    int releaseBytesFinal = canonicalSize;
                    int sendingBytesFinal = sendingBytes;
                    int sendingCountFinal = sendingCount;
                    Channel closeChannelOnFailure = channel;
                    flushResult.addListener(future -> {

                        releaseCapacity(sendingCountFinal, releaseBytesFinal);
                        flushingBytes -= releaseBytesFinal;
                        if (flushingBytes == 0)
                            setInProgress(false);

                        if (!isWritable && flushingBytes <= settings.flushLowWaterMark)
                        {
                            isWritable = true;
                            executeAgain();
                        }

                        if (future.isSuccess())
                        {
                            sent += sendingCountFinal;
                            sentBytes += sendingBytesFinal;
                        }
                        else
                        {
                            errorCount += sendingCountFinal;
                            errorBytes += sendingBytesFinal;
                            invalidateChannel(closeChannelOnFailure, future.cause());
                        }
                    });
                    canonicalSize = 0;
                }
            }
            catch (Throwable t)
            {
                errorCount += sendingCount;
                errorBytes += sendingBytes;
                invalidateChannel(channel, t);
            }
            finally
            {
                if (canonicalSize > 0)
                    releaseCapacity(sendingCount, canonicalSize);

                if (sending != null)
                    sending.release();

                if (pendingBytes() > flushingBytes && isWritable)
                    execute();
            }

            return false;
        }

        void stopAndRunOnEventLoop(Runnable run)
        {
            stopAndRun(run);
        }
    }

    /**
     * Delivery that coordinates between the eventLoop and another (non-dedicated) thread
     *
     * This is to service messages that are too large to fully serialize on the eventLoop, as they could block
     * prompt service of other requests.  Since our serializers assume blocking IO, the easiest approach is to
     * ensure a companion thread performs blocking IO that, under the hood, is serviced by async IO on the eventLoop.
     *
     * Most of the work here is handed off to {@link AsyncChannelOutputPlus}, with our main job being coordinating
     * when and what we should run.
     *
     * To avoid allocating a huge number of threads across a cluster, we utilise the shared methods of {@link Delivery}
     * to ensure that only one run() is actually scheduled to run at a time - this permits us to use any {@link ExecutorService}
     * as a backing, with the number of threads defined only by the maximum concurrency needed to deliver all large messages.
     * We use a shared caching {@link java.util.concurrent.ThreadPoolExecutor}, and rename the Threads that service
     * our connection on entry and exit.
     */
    class LargeMessageDelivery extends Delivery
    {
        static final int DEFAULT_BUFFER_SIZE = 32 * 1024;

        LargeMessageDelivery()
        {
            super(settings.socketFactory.synchronousWorkExecutor);
        }

        /**
         * A simple wrapper of {@link Delivery#run} to set the current Thread name for the duration of its execution.
         */
        public void run()
        {
            String threadName, priorThreadName = null;
            try
            {
                priorThreadName = Thread.currentThread().getName();
                threadName = "Messaging-OUT-" + settings.from + "->" + settings.endpoint + '-' + type;
                Thread.currentThread().setName(threadName);

                super.run();
            }
            finally
            {
                if (priorThreadName != null)
                    Thread.currentThread().setName(priorThreadName);
            }
        }

        @SuppressWarnings("resource") // this suppression is for the ecj compiler
        boolean doRun()
        {
            Message<?> send = queue.tryPoll(System.nanoTime(), this::execute);
            if (send == null)
                return false;

            AsyncMessagingOutputPlus out = new AsyncMessagingOutputPlus(channel, DEFAULT_BUFFER_SIZE, payloadAllocator);
            try
            {
                Tracing.instance.traceOutgoingMessage(send, settings.connectTo);
                Message.serializer.serialize(send, out, messagingVersion);

                int messageSize = send.serializedSize(messagingVersion);
                if (out.position() != messageSize)
                    throw new IOException("Calculated serializedSize " + messageSize + " did not match actual " + out.position());

                out.close();
                sent += 1;
                sentBytes += messageSize;
                releaseCapacity(1, canonicalSize(send));
                return hasPending();
            }
            catch (Throwable t)
            {
                onError(send, t);
                out.discard();
                if (out.flushed() > 0 ||
                    isCausedBy(t, cause ->    isConnectionReset(cause)
                                           || cause instanceof Errors.NativeIoException
                                           || cause instanceof AsyncChannelOutputPlus.FlushException))
                {
                    invalidateChannel(channel, t);
                    return false; // wait until we have reconnected
                }
                return true;
            }
        }

        void stopAndRunOnEventLoop(Runnable run)
        {
            stopAndRun(() -> {
                try
                {
                    runOnEventLoop(run).await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /*
     * Size used for capacity enforcement purposes. Using current messaging version no matter what the peer's version is.
     */
    private int canonicalSize(Message<?> message)
    {
        return message.serializedSize(current_version);
    }

    /**
     *  Attempt to open a new channel to the remote endpoint.
     *
     *  Most of the actual work is performed by OutboundConnectionInitiator, this class just manages
     *  our book keeping on either success or failure.
     *
     *  All methods and state in this class are intended only to be accessed by the eventLoop
     */
    class Connect
    {
        /**
         * If we fail to connect, we want to try and connect again before any messages timeout.
         * However, we update this each time to ensure we do not retry unreasonably often, and settle on a periodicity
         * that might lead to timeouts in some aggressive systems.
         */
        private long retryRateMillis = DatabaseDescriptor.getMinRpcTimeout(MILLISECONDS) / 2;

        /**
         * If we failed for any reason, try again
         */
        void onFailure(Throwable cause)
        {
            if (cause instanceof ConnectException)
                noSpamLogger.info("{} failed to connect", id(), cause);
            else
                noSpamLogger.error("{} failed to connect", id(), cause);

            JVMStabilityInspector.inspectThrowable(cause);
            isFailingToConnect = true;
            if (hasPending())
                connecting = eventLoop.schedule(this::attempt, max(100, retryRateMillis), MILLISECONDS);
            else
                assert connecting == null;

            retryRateMillis = min(1000, retryRateMillis * 2);
        }

        void onCompletedHandshake(Result<MessagingSuccess> result)
        {
            switch (result.outcome)
            {
                case SUCCESS:
                    // it is expected that close, if successful, has already cancelled us; so we do not need to worry about leaking connections
                    assert !isClosed;
                    whileDisconnected.cancel(false);
                    whileDisconnected = null;
                    MessagingSuccess success = result.success();
                    if (messagingVersion != success.messagingVersion)
                    {
                        messagingVersion = success.messagingVersion;
                        settings = template.withDefaults(type, messagingVersion);
                    }
                    payloadAllocator = success.allocator;
                    channel = success.channel;

                    channel.pipeline().addLast("handleExceptionalStates", new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                            super.channelInactive(ctx);
                            logger.info("{} channel closed by provider", id());
                            closeChannelNow(ctx.channel());
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                        {
                            if (isCausedByConnectionReset(cause))
                                logger.info("{} channel closed by provider", id(), cause);
                            else
                                logger.error("{} unexpected error; closing", id(), cause);

                            closeChannelGracefully(channel);
                        }
                    });

                    isFailingToConnect = false;
                    isConnected = true;
                    ++successfulConnections;

                    logger.info("{} successfully connected, version = {}, compress = {}, coalescing = {}, encryption = {}", id(), messagingVersion, settings.withCompression,
                                     settings.coalescingStrategy != null ? settings.coalescingStrategy : CoalescingStrategies.Strategy.DISABLED,
                                encryptionLogStatement(settings.encryption));
                    break;

                case RETRY:
                    if (logger.isTraceEnabled())
                        logger.trace("{} incorrect legacy peer version predicted; reconnecting", id());
                    // the messaging version we connected with was incorrect; try again with the one supplied by the remote host
                    messagingVersion = result.retry().withMessagingVersion;
                    settings = template.withDefaults(type, messagingVersion);
                    MessagingService.instance().versions.set(settings.endpoint, messagingVersion);
                    attempt();
                    break;

                case INCOMPATIBLE:
                    // we cannot communicate with this peer given its messaging version; mark this as any other failure, and continue trying
                    Throwable t = new IOException(String.format("Incompatible peer: %s, messaging version: %s",
                                                                settings.endpoint, result.incompatible().maxMessagingVersion));
                    t.fillInStackTrace();
                    onFailure(t);
                    break;

                default:
                    throw new AssertionError();
            }
        }

        /**
         * Initiate all the actions required to establish a working, valid connection. This includes
         * opening the socket, negotiating the internode messaging handshake, and setting up the working
         * Netty {@link Channel}. However, this method will not block for all those actions: it will only
         * kick off the connection attempt, setting the @{link #connecting} future to track its completion.
         *
         * Note: this should only be invoked on the event loop.
         */
        void attempt()
        {
            ++connectionAttempts;
            // system defaults etc might have changed, so refresh before connect
            settings = template.withDefaults(type, messagingVersion);
            if (messagingVersion > settings.acceptVersions.max)
            {
                messagingVersion = settings.acceptVersions.max;
                settings = template.withDefaults(type, messagingVersion);
                assert messagingVersion <= settings.acceptVersions.max;
            }
            connecting = initiateMessaging(eventLoop, type, settings, messagingVersion)
                         .addListener(future -> {
                             connecting = null;
                             if (future.isCancelled())
                                 return;
                             if (future.isSuccess())
                                 onCompletedHandshake((Result<MessagingSuccess>) future.getNow());
                             else
                                 onFailure(future.cause());
                         });
        }
    }

    /**
     * Returns a future that completes when we are _maybe_ reconnected.
     *
     * The connection attempt is guaranteed to have completed (successfully or not) by the time any listeners are invoked,
     * so if a reconnection attempt is needed, it is already scheduled.
     */
    private Future<?> requestConnect()
    {
        Future<?> inProgress = connecting;
        if (inProgress != null)
            return inProgress;

        Promise<Object> promise = AsyncPromise.uncancellable(eventLoop);
        runOnEventLoop(() -> {
            if (isClosed()) // never going to connect
            {
                promise.tryFailure(new ClosedChannelException());
            }
            else if (isConnected())  // already connected
            {
                promise.trySuccess(null);
            }
            else
            {
                if (connecting == null)
                {
                    assert eventLoop.inEventLoop();
                    assert connecting == null;
                    assert !isConnected();
                    new Connect().attempt();
                }
                connecting.addListener(new PromiseNotifier<>(promise));
            }
        });
        return promise;
    }

    /**
     * Change the IP address on which we connect to the peer. We will attempt to connect to the new address if there
     * was a previous connection, and new incoming messages as well as existing {@link #queue} messages will be sent there.
     * Any outstanding messages in the existing channel will still be sent to the previous address (we won't/can't move them from
     * one channel to another).
     *
     * Returns null if the connection is closed.
     */
    Future<Void> reconnectWithNewTemplate(OutboundConnectionSettings newTemplate)
    {
        Promise<Void> done;
        done = AsyncPromise.uncancellable(eventLoop);
        runOnEventLoop(() -> {
            template = newTemplate;
            settings = template.withDefaults(type, messagingVersion);
            closeChannelGracefully(channel, done);
        });
        return done;
    }

    /**
     * Schedules regular cleaning of the connection's state while it is disconnected from its remote endpoint.
     *
     * To be run only by the eventLoop or in the constructor
     */
    private void scheduleMaintenanceWhileDisconnected()
    {
        assert whileDisconnected == null;
        whileDisconnected = eventLoop.scheduleAtFixedRate(queue::maybePruneExpired, 100L, 100L, TimeUnit.MILLISECONDS);
    }

    /**
     * See {@link #closeChannelGracefully(Channel, Promise)}
     */
    private void closeChannelGracefully(Channel closeIfIs)
    {
        delivery.stopAndRunOnEventLoop(closeChannelTask(closeIfIs));
    }

    /**
     * Schedule a safe close of the provided channel, if it has not already been closed.
     *
     * This means ensuring that delivery has stopped so that we do not corrupt or interrupt any
     * in progress transmissions.
     *
     * The actual closing of the channel is performed asynchronously, to simplify our internal state management
     * and promptly get the connection going again; the close is considered to have succeeded as soon as we
     * have set our internal state.
     */
    private Promise<Void> closeChannelGracefully(Channel closeIfIs, Promise<Void> done)
    {
        if (!done.setUncancellable())
            throw new IllegalStateException();

        // delivery will immediately continue after this, triggering a reconnect if necessary;
        // this might mean a slight delay for large message delivery, as the connect will be scheduled
        // asynchronously, so we must wait for a second turn on the eventLoop
        delivery.stopAndRunOnEventLoop(() -> {
            closeChannelTask(closeIfIs).run();
            done.setSuccess(null);
        });
        return done;
    }

    /**
     * The channel is already known to be invalid, so there's no point waiting for a clean break in delivery.
     *
     * Delivery will be executed again if necessary after this completes.
     */
    private void closeChannelNow(Channel closeIfIs)
    {
        runOnEventLoop(closeChannelTask(closeIfIs)).addListener(future -> {
            // ensure we will run delivery again at some point, if we have work
            // (could have multiple rounds of exceptions, with many waiting messages and no new ones)
            if (hasPending())
                delivery.execute();
        });
    }

    private Runnable closeChannelTask(Channel closeIfIs)
    {
        return () -> {
            if (closeIfIs != null && channel == closeIfIs)
            {
                // no need to wait until the channel is closed to set ourselves as disconnected (and potentially open a new channel)
                Channel close = channel;
                isConnected = false;
                payloadAllocator = null;
                channel = null;
                scheduleMaintenanceWhileDisconnected();
                close.close().addListener(future -> {
                    if (!future.isSuccess())
                        logger.info("Problem closing channel {}", channel, future.cause());
                });
            }
        };
    }

    /**
     * Interrupt the connection, i.e. close any currently open connection and force a reconnect, without losing any messages.
     */
    public Future<Void> interrupt()
    {
        return closeChannelGracefully(channel, new AsyncPromise<>(eventLoop));
    }

    /**
     * Schedule this connection to be permanently closed; only one close may be scheduled,
     * any future scheduled closes are referred to the original triggering one (which may have a different schedule)
     */
    public Future<Void> scheduleClose(long time, TimeUnit unit, boolean flushQueue)
    {
        Promise<Void> scheduledClose = AsyncPromise.uncancellable(eventLoop);
        if (!scheduledCloseUpdater.compareAndSet(this, null, scheduledClose))
            return this.scheduledClose;

        eventLoop.schedule(() -> close(flushQueue).addListener(new PromiseNotifier<>(scheduledClose)), time, unit);
        return scheduledClose;
    }

    /**
     * Permanently close this connection.
     *
     * Immediately prevent any new messages from being enqueued - these will throw ClosedChannelException.
     * The close itself happens asynchronously on the eventLoop, so a Future is returned to help callers
     * wait for its completion.
     *
     * The flushQueue parameter indicates if any outstanding messages should be delivered before closing the connection.
     *
     *  - If false, any already flushed or in-progress messages are completed, and the remaining messages are cleared
     *    before the connection is promptly torn down.
     *
     * - If true, we attempt delivery of all queued messages.  If necessary, we will continue to open new connections
     *    to the remote host until they have been delivered.  Only if we continue to fail to open a connection for
     *    an extended period of time will we drop any outstanding messages and close the connection.
     */
    public Future<Void> close(boolean flushQueue)
    {
        // ensure only one close attempt can be in flight
        Promise<Void> closing = AsyncPromise.uncancellable(eventLoop);
        if (!closingUpdater.compareAndSet(this, null, closing))
            return this.closing;

        /**
         * Now define a cleanup closure, that will be deferred until it is safe to do so.
         * Once run it:
         *   - immediately _logically_ closes the channel by updating this object's fields, but defers actually closing
         *   - cancels any in-flight connection attempts
         *   - cancels any maintenance work that might be scheduled
         *   - clears any waiting messages on the queue
         *   - terminates the delivery thread
         *   - finally, schedules any open channel's closure, and propagates its completion to the close promise
         */
        Runnable eventLoopCleanup = () -> {
            Runnable onceNotConnecting = () -> {
                // start by setting ourselves to definitionally closed
                Channel closeChannel = channel;
                isConnected = false;
                isClosed = true;
                channel = null;

                try
                {
                    // note that we never clear the queue, to ensure that an enqueue has the opportunity to remove itself
                    // if it raced with close, to potentially requeue the message on a replacement connection

                    // we terminate delivery here, to ensure that any listener to {@link connecting} do not schedule more work
                    delivery.terminate();

                    // stop periodic cleanup
                    if (whileDisconnected != null)
                        whileDisconnected.cancel(true);
                    whileDisconnected = null;

                    // close the channel
                    if (closeChannel == null) closing.setSuccess(null);
                    else closeChannel.close().addListener(new PromiseNotifier<>(closing));
                    closeChannel = null;
                }
                catch (Throwable t)
                {
                    // in case of unexpected exception, signal completion and try to close the channel
                    closing.trySuccess(null);
                    try
                    {
                        // could be null when we start cleanup
                        if (closeChannel != null)
                            closeChannel.close();
                    }
                    catch (Throwable t2)
                    {
                        t.addSuppressed(t2);
                        logger.error("Failed to close connection cleanly:", t);
                    }
                    throw t;
                }
            };

            // stop any in-flight connection attempts, which may be asynchronous
            if (connecting != null && !connecting.cancel(true) && !connecting.isCancelled())
                connecting.addListener(future -> onceNotConnecting.run());
            else
                onceNotConnecting.run();
        };

        /**
         * If we want to shutdown gracefully, flushing any outstanding messages, we have to do it very carefully.
         * Things to note:
         *
         *  - It is possible flushing messages will require establishing a new connection
         *    (However, if a connection cannot be established, we do not want to keep trying)
         *  - We have to negotiate with a separate thread, so we must be sure it is not in-progress before we stop (like channel close)
         *  - Cleanup must still happen on the eventLoop
         *
         *  To achieve all of this, we schedule a recurring operation on the delivery thread, executing while delivery
         *  is between messages, that checks if the queue is empty; if it is, it schedules cleanup on the eventLoop.
         */

        Runnable clearQueue = () -> queue.runEventually(System.nanoTime(), withLock -> withLock.consume(this::onDiscardOnClosing));

        if (flushQueue)
        {
            // just keep scheduling on delivery executor a check to see if we're done; there should always be one
            // delivery attempt between each invocation, unless there is a wider problem with delivery scheduling
            class FinishDelivery implements Runnable
            {
                public void run()
                {
                    if (!hasPending())
                        delivery.stopAndRunOnEventLoop(eventLoopCleanup);
                    else
                        delivery.stopAndRun(() -> {
                            if (isFailingToConnect)
                                clearQueue.run();
                            run();
                        });
                }
            }

            delivery.stopAndRun(new FinishDelivery());
        }
        else
        {
            delivery.stopAndRunOnEventLoop(() -> {
                clearQueue.run();
                eventLoopCleanup.run();
            });
        }

        return closing;
    }

    /**
     * Run the task immediately if we are the eventLoop, otherwise queue it for execution on the eventLoop.
     */
    private Future<?> runOnEventLoop(Runnable runnable)
    {
        if (!eventLoop.inEventLoop())
            return eventLoop.submit(runnable);

        runnable.run();
        return new SucceededFuture<>(eventLoop, null);
    }

    public boolean isConnected()
    {
        return isConnected;
    }

    private static boolean isConnected(Channel channel)
    {
        return channel != null && channel.isOpen();
    }

    boolean isClosed()
    {
        return isClosed;
    }

    private String id()
    {
        Channel channel = this.channel;
        String channelId = isConnected() && isConnected(channel)
                           ? channel.id().asShortText()
                           : "[no-channel]";
        return settings.from + "->" + settings.endpoint + '-' + type + '-' + channelId;
    }

    @Override
    public String toString()
    {
        return id();
    }

    public boolean hasPending()
    {
        return 0 != pendingCountAndBytes;
    }

    public int pendingCount()
    {
        return pendingCount(pendingCountAndBytes);
    }

    public long pendingBytes()
    {
        return pendingBytes(pendingCountAndBytes);
    }

    public long sentCount()
    {
        // not volatile, but shouldn't matter
        return sent;
    }

    public long sentBytes()
    {
        // not volatile, but shouldn't matter
        return sentBytes;
    }

    public long submittedCount()
    {
        // not volatile, but shouldn't matter
        return submittedCount;
    }

    public long dropped()
    {
        return overloadCount + expiredCount;
    }

    public long overloadBytes()
    {
        return overloadBytes;
    }

    public long overloadCount()
    {
        return overloadCount;
    }

    public long expiredCount()
    {
        return expiredCount;
    }

    public long expiredBytes()
    {
        return expiredBytes;
    }

    public long errorCount()
    {
        return errorCount;
    }

    public long errorBytes()
    {
        return errorBytes;
    }

    public long successfulConnections()
    {
        return successfulConnections;
    }

    public long connectionAttempts()
    {
        return connectionAttempts;
    }

    private static Runnable andThen(Runnable a, Runnable b)
    {
        if (a == null || b == null)
            return a == null ? b : a;
        return () -> { a.run(); b.run(); };
    }

    @VisibleForTesting
    public ConnectionType type()
    {
        return type;
    }

    @VisibleForTesting
    OutboundConnectionSettings settings()
    {
        return settings;
    }

    @VisibleForTesting
    EventLoop unsafeGetEventLoop()
    {
        return eventLoop;
    }

    @VisibleForTesting
    void unsafeAddToQueue(Message<?> msg)
    {
        pendingCountAndBytesUpdater.addAndGet(this, pendingCountAndBytes(1, canonicalSize(msg)));
        queue.unsafeAdd(msg);
    }

    @VisibleForTesting
    int messagingVersion()
    {
        return messagingVersion;
    }

    @VisibleForTesting
    void unsafeSetMessagingVersion(int messagingVersion)
    {
        this.messagingVersion = messagingVersion;
        this.settings = template.withDefaults(type, messagingVersion);
    }

    @VisibleForTesting
    void unsafeStartDelivery()
    {
        delivery.execute();
    }

    @VisibleForTesting
    Future<?> unsafeGetWhileDisconnected()
    {
        return whileDisconnected;
    }

    @VisibleForTesting
    void unsafeRunOnDelivery(Runnable run)
    {
        delivery.stopAndRun(run);
    }

    @VisibleForTesting
    void unsafeSetChannel(Channel channel)
    {
        this.channel = channel;
    }

    @VisibleForTesting
    Channel unsafeGetChannel()
    {
        return channel;
    }

    @VisibleForTesting
    void unsafeSetClosed(boolean closed)
    {
        closingUpdater.set(this, closed ? eventLoop.newSucceededFuture(null) : null);
        this.isConnected = false;
        this.isClosed = true;
    }

    @VisibleForTesting
    boolean unsafeAcquireCapacity(long amount)
    {
        return SUCCESS == acquireCapacity(amount);
    }

    @VisibleForTesting
    boolean unsafeAcquireCapacity(long count, long amount)
    {
        return SUCCESS == acquireCapacity(count, amount);
    }

    @VisibleForTesting
    void unsafeReleaseCapacity(long amount)
    {
        releaseCapacity(1, amount);
    }

}