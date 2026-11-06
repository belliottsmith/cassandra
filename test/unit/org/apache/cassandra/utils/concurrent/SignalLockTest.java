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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test has been authored entirely by LLM.
 *
 * Unit tests for the await/signal contract of {@link SignalLock}, i.e. the states in which
 * {@link SignalLock#awaitAsyncOrLock} must return rather than park:
 * <ul>
 *   <li>an async permit is already available as we begin to wait - the waiter must serve itself, since a permit made
 *       available with {@code incrementAsyncWork(false)} carries no signal, so nothing will wake it;</li>
 *   <li>a permit is signalled to us while we wait - the permit is transferred to us, and must not be consumed twice;</li>
 *   <li>work requiring the lock is signalled - we must acquire the lock;</li>
 * </ul>
 * plus {@link SignalLock#ensureNotOwner}, which must release a reentrant hold in full.
 */
public class SignalLockTest
{
    private static final long TIMEOUT_SECONDS = 10;

    /**
     * A permit made available before we begin waiting comes with no signal, so nothing will ever wake us: the waiter
     * must notice the available permit itself instead of parking.
     */
    @Test
    public void awaitReturnsWhenPermitAlreadyAvailable() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        assertThat(lock.incrementAsyncWork(false)).isTrue();
        assertThat(lock.asyncSignalCount()).isEqualTo(1);

        Waiter waiter = new Waiter(lock, 0);
        waiter.start();

        assertThat(waiter.awaitResult()).describedAs("acquired the lock instead of the available permit").isFalse();
        assertThat(lock.asyncSignalCount()).describedAs("permit was not consumed").isZero();
        assertThat(lock.waitingThreadCount()).describedAs("waiter left itself registered as waiting").isZero();
    }

    /** A permit signalled to a parked waiter is transferred to it, and so must not be consumed a second time. */
    @Test
    public void awaitReturnsWhenPermitSignalled() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        Waiter waiter = new Waiter(lock, 0);
        waiter.start();
        waiter.awaitWaiting();

        assertThat(lock.incrementAsyncWork(true)).isTrue();

        assertThat(waiter.awaitResult()).describedAs("acquired the lock instead of the signalled permit").isFalse();
        assertThat(lock.asyncSignalCount()).describedAs("the signalled permit was consumed twice").isZero();
        assertThat(lock.waitingThreadCount()).describedAs("waiter left itself registered as waiting").isZero();
    }

    /** Every waiter must be woken when enough permits are burst signalled for all of them. */
    @Test
    public void burstSignalWakesEveryWaiter() throws Throwable
    {
        SignalLock lock = new SignalLock(3);
        Waiter[] waiters = { new Waiter(lock, 0), new Waiter(lock, 1), new Waiter(lock, 2) };
        for (Waiter waiter : waiters)
            waiter.start();
        for (Waiter waiter : waiters)
            waiter.awaitWaiting();

        for (int i = 0 ; i < waiters.length ; ++i)
            assertThat(lock.incrementAsyncWork(false)).isTrue();
        lock.propagateAsyncWorkSignals(waiters.length);

        for (Waiter waiter : waiters)
            assertThat(waiter.awaitResult()).describedAs("acquired the lock instead of a permit").isFalse();
        assertThat(lock.asyncSignalCount()).describedAs("permits were not all consumed").isZero();
        assertThat(lock.waitingThreadCount()).describedAs("a waiter left itself registered as waiting").isZero();
    }

    /** A waiter signalled that there is work for the lock must acquire the lock, and be able to release it. */
    @Test
    public void awaitTakesLockWhenLockWorkSignalled() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        Waiter waiter = new Waiter(lock, 0);
        waiter.start();
        waiter.awaitWaiting();

        lock.signalLockWork();

        assertThat(waiter.awaitResult()).describedAs("did not acquire the lock").isTrue();
        assertThat(lock.hasOwner()).describedAs("waiter did not release the lock").isFalse();
        assertThat(lock.waitingThreadCount()).describedAs("waiter left itself registered as waiting").isZero();
    }

    /** unlockAll must release however many times we have acquired the lock, so that another thread may take it. */
    @Test
    public void ensureNotOwnerReleasesReentrantHold() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        lock.lock();
        lock.lock();
        assertThat(lock.ensureNotOwner()).describedAs("wrong number of acquisitions released").isEqualTo(2);
        assertThat(lock.hasOwner()).describedAs("lock was not released").isFalse();

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try { assertThat(lock.tryLock()).describedAs("another thread could not take the lock").isTrue(); }
            catch (Throwable t) { failure.set(t); }
        }, "other");
        other.start();
        other.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertThat(other.isAlive()).describedAs("another thread blocked on the released lock").isFalse();
        if (failure.get() != null)
            throw failure.get();
    }

    /**
     * Timed lock work: on release, the owner nominates exactly one waiting thread to acquire the lock at the declared
     * time; the other waiting thread is not disturbed.
     */
    @Test
    public void releaseNominatesOneTimedWaiter() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        Waiter w0 = new Waiter(lock, 0), w1 = new Waiter(lock, 1);
        w0.clearLockWorkAt = w1.clearLockWorkAt = true;
        w0.start(); w1.start();
        w0.awaitWaiting(); w1.awaitWaiting();

        long at = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
        lock.lock();
        lock.signalLockWorkAtExclusive(at);
        lock.unlock();
        assertThat(lock.timedThread()).describedAs("no timed thread was nominated").isEqualTo(1);

        assertThat(w1.awaitResult()).describedAs("the timed thread did not acquire the lock").isTrue();
        assertThat(w1.returnedAt).describedAs("the timed thread acquired the lock early").isGreaterThanOrEqualTo(at);
        assertThat(lock.timedThread()).describedAs("the timed thread did not relinquish its nomination").isEqualTo(-1);
        assertThat(w0.isWaiting()).describedAs("the other waiter was disturbed").isTrue();
        lock.signalAllRegistered();
        assertThat(w0.awaitResult()).isFalse();
    }

    /** If nobody is waiting when the owner releases, the first thread to begin waiting nominates itself. */
    @Test
    public void waiterNominatesItselfIfNoTimedThread() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        long at = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
        lock.lock();
        lock.signalLockWorkAtExclusive(at);
        lock.unlock();
        assertThat(lock.timedThread()).isEqualTo(-1);

        Waiter w0 = new Waiter(lock, 0);
        w0.clearLockWorkAt = true;
        w0.start();
        assertThat(w0.awaitResult()).describedAs("the self-nominated thread did not acquire the lock").isTrue();
        assertThat(w0.returnedAt).isGreaterThanOrEqualTo(at);
        assertThat(lock.lockWorkAt()).isEqualTo(SignalLock.NO_LOCK_WORK_AT);
    }

    /**
     * Other threads may acquire and release the lock (re-declaring the same or a later time) without disturbing the
     * timed thread, which retains its nomination; but declaring an earlier time wakes it to re-arm.
     */
    @Test
    public void timedThreadSurvivesOtherOwnersAndIsKickedByEarlierDeadline() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        Waiter w0 = new Waiter(lock, 0);
        w0.clearLockWorkAt = true;
        w0.start();
        w0.awaitWaiting();

        long start = System.nanoTime();
        lock.lock();
        lock.signalLockWorkAtExclusive(start + TimeUnit.SECONDS.toNanos(5));
        lock.unlock();
        assertThat(lock.timedThread()).isEqualTo(0);

        for (int i = 0 ; i < 100 ; ++i)
        {
            assertThat(lock.tryLock()).describedAs("could not steal the lock from a timed waiter").isTrue();
            lock.signalLockWorkAtExclusive(start + TimeUnit.SECONDS.toNanos(5 + i));
            lock.unlock();
        }
        assertThat(lock.timedThread()).describedAs("the timed thread lost its nomination").isEqualTo(0);
        assertThat(w0.isWaiting()).isTrue();

        long at = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
        lock.lock();
        lock.signalLockWorkAtExclusive(at);
        lock.unlock();
        assertThat(w0.awaitResult()).describedAs("the timed thread did not acquire the lock").isTrue();
        assertThat(w0.returnedAt).isGreaterThanOrEqualTo(at);
        assertThat(w0.returnedAt - start).describedAs("the timed thread was not woken to re-arm").isLessThan(TimeUnit.SECONDS.toNanos(4));
    }

    /** In spin mode, a spinning (so unregistered) waiter must still acquire the lock once timed lock work falls due */
    @Test
    public void spinningWaiterAcquiresTimedLockWork() throws Throwable
    {
        SignalLock lock = new SignalLock(2, 100, 0, TimeUnit.MICROSECONDS);
        Waiter w0 = new Waiter(lock, 0), w1 = new Waiter(lock, 1);
        w0.clearLockWorkAt = w1.clearLockWorkAt = true;
        long at = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
        lock.lock();
        lock.signalLockWorkAtExclusive(at);
        lock.unlock();
        w0.start(); w1.start();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (w0.thread.isAlive() && w1.thread.isAlive() && System.nanoTime() < deadline)
            Thread.sleep(1);
        Waiter acquired = w0.thread.isAlive() ? w1 : w0, other = acquired == w0 ? w1 : w0;
        assertThat(acquired.awaitResult()).describedAs("no spinning waiter acquired the lock").isTrue();
        assertThat(acquired.returnedAt).isGreaterThanOrEqualTo(at);
        // the other waiter is (probably) still spinning, so is released by an async permit; but spinning waiters each
        // check for timed lock work, so it may have raced to perform a (benign) spurious acquisition of its own
        lock.incrementAsyncWork(true);
        other.awaitResult();
    }

    /** Async work goes to a waiter that is not timed, if there is one; otherwise the timed thread relinquishes its role. */
    @Test
    public void asyncWorkPrefersUntimedWaiter() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        Waiter w0 = new Waiter(lock, 0), w1 = new Waiter(lock, 1);
        w0.start(); w1.start();
        w0.awaitWaiting(); w1.awaitWaiting();

        lock.lock();
        lock.signalLockWorkAtExclusive(System.nanoTime() + TimeUnit.SECONDS.toNanos(10));
        lock.unlock();
        int timed = lock.timedThread();
        assertThat(timed).isGreaterThanOrEqualTo(0);
        Waiter timedWaiter = timed == 0 ? w0 : w1, untimedWaiter = timed == 0 ? w1 : w0;

        assertThat(lock.incrementAsyncWork(true)).isTrue();
        assertThat(untimedWaiter.awaitResult()).describedAs("the untimed waiter took the lock instead of the permit").isFalse();
        assertThat(lock.timedThread()).describedAs("the timed thread was disturbed").isEqualTo(timed);

        assertThat(lock.incrementAsyncWork(true)).isTrue();
        assertThat(timedWaiter.awaitResult()).describedAs("the timed waiter took the lock instead of the permit").isFalse();
        assertThat(lock.timedThread()).describedAs("the timed thread did not relinquish its nomination").isEqualTo(-1);
        assertThat(lock.asyncSignalCount()).isZero();
    }

    /** Awaits on one registered thread, recording whether it acquired the lock (and releasing it if so). */
    private static class Waiter
    {
        final SignalLock lock;
        final int index;
        final AtomicInteger result = new AtomicInteger(Integer.MIN_VALUE);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread thread;
        /** if we acquire the lock, clear any (timed) lock work before releasing it */
        volatile boolean clearLockWorkAt;
        volatile long returnedAt;

        Waiter(SignalLock lock, int index)
        {
            this.lock = lock;
            this.index = index;
            this.thread = new Thread(this::run, "waiter" + index);
        }

        void start()
        {
            lock.register(index, thread);
            thread.start();
        }

        private void run()
        {
            try
            {
                boolean tookLock = lock.awaitAsyncOrLock(index);
                returnedAt = System.nanoTime();
                if (tookLock)
                {
                    if (clearLockWorkAt)
                    {
                        // as an owner would, having performed the work
                        lock.clearLockWork();
                        lock.signalLockWorkAtExclusive(SignalLock.NO_LOCK_WORK_AT);
                    }
                    lock.unlock();
                }
                result.set(tookLock ? 1 : 0);
            }
            catch (Throwable t)
            {
                failure.set(t);
                result.set(-1);
            }
        }

        /** wait until we have registered ourselves as waiting, so that a signal has someone to wake */
        void awaitWaiting()
        {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (0 == (lock.state() & (1L << index)) && System.nanoTime() < deadline)
                Thread.yield();
            assertThat(lock.state() & (1L << index)).describedAs("waiter never registered itself as waiting").isNotZero();
        }

        boolean isWaiting()
        {
            return thread.isAlive() && 0 != (lock.state() & (1L << index));
        }

        /** @return true if the waiter acquired the lock; fails if it is still parked */
        boolean awaitResult() throws Throwable
        {
            thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            if (thread.isAlive())
            {
                StringBuilder out = new StringBuilder("waiter did not return; state=" + Long.toHexString(lock.state()));
                for (StackTraceElement element : thread.getStackTrace())
                    out.append("\n\tat ").append(element);
                assertThat(false).describedAs(out.toString()).isTrue();
            }
            if (failure.get() != null)
                throw failure.get();
            return result.get() == 1;
        }
    }
}
