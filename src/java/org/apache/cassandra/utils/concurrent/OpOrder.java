/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * <p>A class for providing synchronization between producers and consumers that do not
 * communicate directly with each other, but where the consumers need to process their
 * work in contiguous batches. In particular this is useful for both CommitLog and Memtable
 * where the producers (writing threads) are modifying a structure that the consumer
 * (flush executor) only batch syncs, but needs to know what 'position' the work is at
 * for co-ordination with other processes,
 *
 * <p>The typical usage is something like:
 * <pre>
     public final class ExampleShared
     {
        final OpOrder order = new OpOrder();
        volatile SharedState state;

        static class SharedState
        {
            volatile Barrier barrier;

            // ...
        }

        public void consume()
        {
            SharedState state = this.state;
            state.setReplacement(new State())
            state.doSomethingToPrepareForBarrier();

            state.barrier = order.newBarrier();
            // issue() MUST be called after newBarrier() else barrier.isAfter()
            // will always return true, and barrier.await() will fail
            state.barrier.issue();

            // wait for all producer work started prior to the barrier to complete
            state.barrier.await();

            // change the shared state to its replacement, as the current state will no longer be used by producers
            this.state = state.getReplacement();

            state.doSomethingWithExclusiveAccess();
        }

        public void produce()
        {
            try (Group opGroup = order.start())
            {
                SharedState s = state;
                while (s.barrier != null && !s.barrier.isAfter(opGroup))
                    s = s.getReplacement();
                s.doProduceWork();
            }
        }
    }
 * </pre>
 */
public class OpOrder
{
    /**
     * Constant that when an Ordered.running is equal to, indicates the Ordered is complete
     */
    private static final int FINISHED = -1;

    /**
     * A linked list starting with the most recent Ordered object, i.e. the one we should start new operations from,
     * with (prev) links to any incomplete Ordered instances, and (next) links to any potential future Ordered instances.
     * Once all operations started against an Ordered instance and its ancestors have been finished the next instance
     * will unlink this one
     */
    private volatile Group current = new Group();

    /**
     * Start an operation against this OpOrder.
     * Once the operation is completed Ordered.close() MUST be called EXACTLY once for this operation.
     *
     * @return the Ordered instance that manages this OpOrder
     */
    public Group start()
    {
        while (true)
        {
            Group current = this.current;
            if (current.register())
                return current;
        }
    }

    /**
     * Start an operation that can be used for a longer running transaction, that periodically reaches points that
     * can be considered to restart the transaction. This is stronger than 'safe', as it declares that all guarded
     * entry points have been exited and will be re-entered, or an equivalent guarantee can be made that no reclaimed
     * resources are being referenced.
     *
     * <pre>
     * SyncingOrdered ord = startSync();
     * while (...)
     * {
     *     ...
     *     ord.sync();
     * }
     * ord.finish();
     *</pre>
     * is semantically equivalent to (but more efficient than):
     *<pre>
     * Ordered ord = start();
     * while (...)
     * {
     *     ...
     *     ord.close();
     *     ord = start();
     * }
     * ord.close();
     * </pre>
     */
    public SyncingGroup startSync()
    {
        return new SyncingGroup();
    }

    /**
     * Creates a new barrier. The barrier is only a placeholder until barrier.issue() is called on it,
     * after which all new operations will start against a new Group that will not be accepted
     * by barrier.isAfter(), and barrier.await() will return only once all operations started prior to the issue
     * have completed.
     *
     * @return
     */
    public Barrier newBarrier()
    {
        return new Barrier();
    }

    public Group getCurrent()
    {
        return current;
    }

    /**
     * Represents a group of identically ordered operations, i.e. all operations started in the interval between
     * two barrier issuances. For each register() call this is returned, close() must be called exactly once.
     * It should be treated like taking a lock().
     */
    public static final class Group implements Comparable<Group>, AutoCloseable
    {
        /**
         * In general this class goes through the following stages:
         * 1) LIVE:      many calls to register() and close()
         * 2) FINISHING: a call to expire() (after a barrier issue), means calls to register() will now fail,
         *               and we are now 'in the past' (new operations will be started against a new Ordered)
         * 3) FINISHED:  once the last close() is called, this Ordered is done. We call unlink().
         * 4) ZOMBIE:    all our operations are finished, but some operations against an earlier Ordered are still
         *               running, or tidying up, so unlink() fails to remove us
         * 5) COMPLETE:  all operations started on or before us are FINISHED (and COMPLETE), so we are unlinked
         *
         * Two other parallel states are SAFE and ISBLOCKING:
         *
         * safe => all running operations were paused, so safe wrt memory access. Like a java safe point, except
         * that it only provides ordering guarantees, as the safe point can be exited at any point. The only reason
         * we require all to be stopped is to avoid tracking which threads are safe at any point, though we may want
         * to do so in future.
         *
         * isBlocking => a barrier that is waiting on us (either directly, or via a future Ordered) is blocking general
         * progress. This state is entered by calling Barrier.markBlocking(). If the running operations are blocked
         * on a Signal that is also registered with the isBlockingSignal (probably through isSafeBlockingSignal)
         * then they will be notified that they are blocking forward progress, and may take action to avoid that.
         */

        private volatile Group prev, next;
        private final long id; // monotonically increasing id for compareTo()
        private volatile int running = 0; // number of operations currently running.  < 0 means we're expired, and the count of tasks still running is -(running + 1)
        private volatile int safe; // number of operations currently running but 'safe' (see below)
        private volatile boolean isBlocking; // indicates running operations are blocking future barriers
        private final WaitQueue isBlockingSignal = new WaitQueue(); // signal to wait on to indicate isBlocking is true
        private final WaitQueue waiting = new WaitQueue(); // signal to wait on for completion

        static final AtomicIntegerFieldUpdater<Group> runningUpdater = AtomicIntegerFieldUpdater.newUpdater(Group.class, "running");
        static final AtomicIntegerFieldUpdater<Group> safeUpdater = AtomicIntegerFieldUpdater.newUpdater(Group.class, "safe");

        // constructs first instance only
        private Group()
        {
            this.id = 0;
        }

        private Group(Group prev)
        {
            this.id = prev.id + 1;
            this.prev = prev;
        }

        // prevents any further operations starting against this Ordered instance
        // if there are no running operations, calls unlink; otherwise, we let the last op to close call it.
        // this means issue() won't have to block for ops to finish.
        private void expire()
        {
            while (true)
            {
                int current = running;
                if (current < 0)
                    throw new IllegalStateException();
                if (runningUpdater.compareAndSet(this, current, -1 - current))
                {
                    // if we're already finished (no running ops), unlink ourselves
                    if (current == 0)
                    {
                        maybeSignalSafe();
                        unlink();
                    }
                    return;
                }
            }
        }

        // attempts to start an operation against this Ordered instance, and returns true if successful.
        private boolean register()
        {
            while (true)
            {
                int current = running;
                if (current < 0)
                    return false;
                if (runningUpdater.compareAndSet(this, current, current + 1))
                    return true;
            }
        }

        /**
         * To be called exactly once for each register() call this object is returned for, indicating the operation
         * is complete
         */
        public void close()
        {
            assert next == null || next.prev == this;
            assert running != -1;
            while (true)
            {
                int current = running;
                if (current < 0)
                {
                    // after expire() we are in the negatives so we have active
                    // ops until current + 1 == -1
                    if (runningUpdater.compareAndSet(this, current, current + 1))
                    {
                        maybeSignalSafe();
                        if (current + 1 == FINISHED)
                        {
                            // if we're now finished, unlink ourselves
                            unlink();
                        }
                        return;
                    }
                }
                else if (runningUpdater.compareAndSet(this, current, current - 1))
                {
                    maybeSignalSafe();
                    return;
                }
            }
        }

        /**
         * called once we know all operations started against this Ordered have completed,
         * however we do not know if operations against its ancestors have completed, or
         * if its descendants have completed ahead of it, so we attempt to create the longest
         * chain from the oldest still linked Ordered. If we can't reach the oldest through
         * an unbroken chain of completed Ordered, we abort, and leave the still completing
         * ancestor to tidy up.
         */
        private void unlink()
        {
            // unlink any FINISHED (not nec. COMPLETE) nodes directly behind us
            // we only modify ourselves in this step, not anybody behind us, so we don't have to worry about races
            // this is equivalent to walking the list, but potentially makes life easier for any unlink operations
            // that proceed us if we aren't yet COMPLETE
            // note we leave the forward (next) chain fully intact at this stage
            Group start = this;
            Group prev = this.prev;
            while (prev != null)
            {
                // if we hit an unfinished ancestor, we're not COMPLETE, so leave it to the ancestor to clean up when done
                if (prev.running != FINISHED)
                    return;
                start = prev;
                this.prev = prev = prev.prev;
            }

            // our waiters are good to go, so signal them
            this.waiting.signalAll();

            // now walk from earliest to latest, unlinking pointers as we go to tidy up for GC, and waking up any blocking threads
            // we don't stop with the list before us though, as we may have finished late, so once we've destroyed the list
            // behind us we carry on until we hit a node that isn't FINISHED
            // we can safely abort if we ever hit null, as any unlink that finished before us would have been completely
            // unlinked by its prev pointer before we started, so we'd never visit it, so it must have been running
            // after we called unlink, by which point we were already marked FINISHED, so it would tidy up just as well as we intend to
            while (true)
            {
                Group next = start.next;
                start.next = null;
                start.waiting.signalAll();
                if (next == null)
                    return;
                next.prev = null;
                if (next.running != FINISHED)
                    return;
                start = next;
            }
        }

        /**
         * @return true if a barrier we are behind is, or may be, blocking general progress,
         * so we should try more aggressively to progress
         */
        public boolean isBlocking()
        {
            return isBlocking;
        }

        /**
         * register to be signalled when a barrier waiting on us is, or maybe, blocking general progress,
         * so we should try more aggressively to progress
         */
        public WaitQueue.Signal isBlockingSignal()
        {
            return isBlockingSignal.register();
        }

        /**
         * internal convenience method indicating if all running operations ON THIS ORDERED ONLY (not preceding ones)
         * are currently 'safe' (generally means waiting on a SafeSignal), i.e. that they are currently guaranteed
         * not to be in the middle of reading memory guarded by this OpOrder. This is used to prevent blocked
         * operations from preventing off-heap allocator GC progress.
         */
        private boolean isSafe()
        {
            int safe = this.safe;
            int running = this.running;
            return (safe == -1 - running) | (safe == running);
        }

        /**
         * indicate a running operation is 'safe' wrt memory accesses, i.e. is waiting or at some other safe point.
         * must be proceeded by a single call to markOneUnsafe()
         */
        public void markOneSafe()
        {
            safeUpdater.incrementAndGet(this);
            maybeSignalSafe();
        }

        private void maybeSignalSafe()
        {
            if (isSafe())
                waiting.signalAll();
        }

        /**
         * indicate a running operation that was 'safe' wrt memory accesses, is no longer.
         * if possible, should be used through safe(Signal)
         */
        public void markOneUnsafe()
        {
            safeUpdater.decrementAndGet(this);
        }

        /**
         * wrap the provided signal to mark the thread as safe during any waiting
         */
        public WaitQueue.Signal safe(WaitQueue.Signal signal)
        {
            return new SafeSignal(signal);
        }

        /**
         * wrap the provided signal to mark the thread as safe during any waiting, and to be signalled if the
         * operation gets marked blocking
         */
        public WaitQueue.Signal safeIsBlockingSignal(WaitQueue.Signal signal)
        {
            return new SafeSignal(WaitQueue.any(signal, isBlockingSignal()));
        }

        /**
         * A wrapper class that simply marks safe/unsafe on entry/exit, and delegates to the wrapped signal
         */
        private class SafeSignal implements WaitQueue.Signal
        {

            final WaitQueue.Signal delegate;
            private SafeSignal(WaitQueue.Signal delegate)
            {
                this.delegate = delegate;
            }

            @Override
            public boolean isSignalled()
            {
                return delegate.isSignalled();
            }

            @Override
            public boolean isCancelled()
            {
                return delegate.isCancelled();
            }

            @Override
            public boolean isSet()
            {
                return delegate.isSet();
            }

            @Override
            public boolean checkAndClear()
            {
                return delegate.checkAndClear();
            }

            @Override
            public void cancel()
            {
                delegate.cancel();
            }

            @Override
            public void awaitUninterruptibly()
            {
                markOneSafe();
                try
                {
                    delegate.awaitUninterruptibly();
                }
                finally
                {
                    markOneUnsafe();
                }
            }

            @Override
            public void await() throws InterruptedException
            {
                markOneSafe();
                try
                {
                    delegate.await();
                }
                finally
                {
                    markOneUnsafe();
                }
            }

            @Override
            public boolean awaitUntil(long until) throws InterruptedException
            {
                markOneSafe();
                try
                {
                    return delegate.awaitUntil(until);
                }
                finally
                {
                    markOneUnsafe();
                }
            }
        }

        public int compareTo(Group that)
        {
            // we deliberately use subtraction, as opposed to Long.compareTo() as we care about ordering
            // not which is the smaller value, so this permits wrapping in the unlikely event we exhaust the long space
            long c = this.id - that.id;
            if (c > 0)
                return 1;
            else if (c < 0)
                return -1;
            else
                return 0;
        }
    }

    /**
     * see {@link #startSync}
     */
    public final class SyncingGroup implements AutoCloseable
    {

        private Group current = OpOrder.this.start();

        /**
         * Called periodically to indicate we have reached a safe point wrt data guarded by this OpOrdering
         */
        public void sync()
        {
            if (current != OpOrder.this.current)
            {
                // only swap the operation if we're behind the present
                current.close();
                current = OpOrder.this.start();
            }
        }

        public Group current()
        {
            return current;
        }

        /**
         * Called once our transactions have completed. May safely be called multiple times, with each extra call
         * a no-op.
         */
        public void close()
        {
            if (current != null)
                current.close();
            current = null;
        }

        /**
         * Called once we have finished if we want to use this SyncingOrdered again. Equivalent to starting a new
         * SyncingOrdered.
         */
        public void restart()
        {
            if (current != null)
                current.close();
            current = OpOrder.this.start();
        }

    }

    /**
     * This class represents a synchronisation point providing ordering guarantees on operations started
     * against the enclosing OpOrder.  When issue() is called upon it (may only happen once per Barrier), the
     * Barrier atomically partitions new operations from those already running (by expiring the current Group),
     * and activates its isAfter() method which indicates if an operation was started before or after this partition.
     * It offers methods to determine, or block until, all prior operations have finished, and a means to indicate
     * to those operations that they are blocking forward progress. See {@link OpOrder} for idiomatic usage.
     */
    public final class Barrier
    {
        // this Barrier was issued after all Group operations started against orderOnOrBefore
        private volatile Group orderOnOrBefore;

        /**
         * @return true if @param group was started prior to the issuing of the barrier.
         *
         * (Until issue is called, always returns true, but if you rely on this behavior more than transiently,
         * between exposing the Barrier and calling issue() soon after, you are Doing It Wrong.)
         */
        public boolean isAfter(Group group)
        {
            if (orderOnOrBefore == null)
                return true;
            // we subtract to permit wrapping round the full range of Long - so we only need to ensure
            // there are never Long.MAX_VALUE * 2 total Group objects in existence at any one time, which will
            // take care of itself
            return orderOnOrBefore.id - group.id >= 0;
        }

        /**
         * Issues (seals) the barrier, meaning no new operations may be issued against it, and expires the current
         * Group.  Must be called after exposing the barrier for isAfter() to be properly synchronised, and before
         * any call to any other method on the Barrier.
         */
        public void issue()
        {
            if (orderOnOrBefore != null)
                throw new IllegalStateException("Can only call issue() once on each Barrier");

            final Group current;
            synchronized (OpOrder.this)
            {
                current = OpOrder.this.current;
                orderOnOrBefore = current;
                OpOrder.this.current = current.next = new Group(current);
            }
            current.expire();
        }

        /**
         * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
         */
        public void markBlocking()
        {
            Group current = orderOnOrBefore;
            while (current != null)
            {
                current.isBlocking = true;
                current.isBlockingSignal.signalAll();
                current = current.prev;
            }
        }

        /**
         * Register to be signalled once allPriorOpsAreFinished() or allPriorOpsAreFinishedOrSafe() may return true
         */
        public WaitQueue.Signal register()
        {
            return orderOnOrBefore.waiting.register();
        }

        /**
         * @return true if all operations started prior to barrier.issue() have completed
         */
        public boolean allPriorOpsAreFinished()
        {
            Group current = orderOnOrBefore;
            if (current == null)
                throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
            return current.prev == null && current.running == -1;
        }

        /**
         * wait for all operations started prior to issuing the barrier to complete
         */
        public void await()
        {
            while (!allPriorOpsAreFinished())
            {
                WaitQueue.Signal signal = register();
                if (allPriorOpsAreFinished())
                {
                    signal.cancel();
                    break;
                }
                else
                    signal.awaitUninterruptibly();
            }
        }

        /**
         * @return true if all operations started prior to barrier.issue() have either completed or have each been
         * marked safe at least once since the barrier was issued (current implementation requires the safe point
         * be reached after calling awaitSafe())
         */
        public void awaitSafe()
        {
            // if the prior ops are all finished, we're done
            if (allPriorOpsAreFinished())
                return;
            // otherwise, we walk the chain backwards from ourselves, stopping whenever we encounter
            // a NOT safe group, and waiting on its queue until we're signalled in time to witness the safe condition
            Group current = orderOnOrBefore;
            while (current != null)
            {
                if (current.isSafe())
                {
                    // it's safe, so carry on backwards
                    current = current.prev;
                }
                else
                {
                    // otherwise register to be alerted when it is safe/finished, then reconfirm we haven't raced
                    // with a change in this property
                    WaitQueue.Signal signal = current.waiting.register();
                    if (current.isSafe())
                    {
                        // if it's safe, we can cancel the signal and move onto the next oldest in the chain
                        signal.cancel();
                        current = current.prev;
                    }
                    else
                        signal.awaitUninterruptibly();
                }
            }
        }

        /**
         * returns the Group we are waiting on - any Group with .compareTo(getSyncPoint()) <= 0
         * must complete before await() returns
         */
        public Group getSyncPoint()
        {
            return orderOnOrBefore;
        }
    }
}
