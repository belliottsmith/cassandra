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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

import accord.utils.Invariants;

import org.apache.cassandra.utils.Nemesis;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A lock that supports semi-asynchronous operation; a thread may take the lock synchronously,
 * or it may await a signal that it should acquire the lock (to perform synchronous work),
 * or a signal that work has been made available for it outside the lock.
 *
 * In some systems it may be that LockSupport.unpark() often results in the calling thread becoming the callee,
 * which can be harmful if the caller owns the lock. To avoid this problem, we support making available permits
 * that can be acquired asynchronously, and only on releasing the lock do we attempt to actually directly wake
 * any threads. In this mode of operation it is up to the user to unlock when there is sufficient work to signal.
 *
 * TODO (expected): once we support a high enough JDK, port to compareAndExchange/weakCompareAndSet as appropriate
 */
public final class SignalLock implements Lock
{
    private static final AtomicLongFieldUpdater<SignalLock> stateUpdater = AtomicLongFieldUpdater.newUpdater(SignalLock.class, "state");

    private static final long HAS_LOCK_WORK = 0x8000000000000000L;
    /*
     * LOCK_THREAD holds the nominee (if any): the thread (+2, so that ANONYMOUS_OWNER is 1 and none is 0) that is
     * nominated to take the lock. LOCK_TIMED indicates the kind of nomination:
     *  - active (LOCK_TIMED unset): the nominee should take the lock now, to perform lock work. To avoid unnecessary
     *    wakeups we pick one waiting thread to unpark. Exists only while the lock is not owned, and is consumed by
     *    any acquisition of the lock (including a steal).
     *  - timed (LOCK_TIMED set): a registered nominee that should take the lock at lockWorkAt (see
     *    signalLockWorkAtExclusive). May persist while the lock is owned, so that it survives other threads
     *    acquiring and releasing the lock; the nominee relinquishes its nomination (in the same step) before doing
     *    anything else. If lock work arises while there is a timed nominee, it is upgraded to an active nominee.
     * The owner of the lock is not recorded in the state; only that it is owned.
     */
    private static final int LOCK_THREAD_SHIFT = 64 - 7;
    private static final int LOCK_OWNED_SHIFT = LOCK_THREAD_SHIFT - 1;
    private static final int ASYNC_SIGNAL_COUNT_SHIFT = LOCK_OWNED_SHIFT - 6;
    private static final int ENABLED_THREAD_COUNT_SHIFT = ASYNC_SIGNAL_COUNT_SHIFT - 6;
    private static final int LOCK_TIMED_SHIFT = ENABLED_THREAD_COUNT_SHIFT - 1;
    public static final int MAX_THREADS = LOCK_TIMED_SHIFT;
    public static final int THREAD_ID_MASK = 0x3f;
    public static final int MAX_SIGNAL_COUNT = THREAD_ID_MASK;
    private static final long LOCK_THREAD_MASK = (long) THREAD_ID_MASK << LOCK_THREAD_SHIFT;
    private static final long LOCK_OWNED = 1L << LOCK_OWNED_SHIFT;
    private static final long LOCK_TIMED = 1L << LOCK_TIMED_SHIFT;
    private static final long ASYNC_SIGNAL_INCREMENT = 1L << ASYNC_SIGNAL_COUNT_SHIFT;
    private static final long ENABLED_THREAD_COUNT_INCREMENT = 1L << ENABLED_THREAD_COUNT_SHIFT;
    private static final long ENABLED_THREAD_COUNT_MASK = (long)THREAD_ID_MASK << ENABLED_THREAD_COUNT_SHIFT;
    public static final long NO_LOCK_WORK_AT = Long.MIN_VALUE;
    private static final long WAITING_THREADS_MASK = -1L >>> (64 - MAX_THREADS);

    private final boolean allowStealing;
    private final Thread[] registered;
    @Nemesis private volatile long state;
    private Thread owner;
    /** the registered index of the owner, or ANONYMOUS_OWNER; written only by the owner */
    private int ownerIndex = ANONYMOUS_OWNER;
    private final ConcurrentLinkedQueue<Thread> locking = new ConcurrentLinkedQueue<>();
    private final AtomicLongArray stopLog;
    private final AtomicLong stopCheck;
    private final long stopCheckIntervalNanos;
    private final long spinIntervalNanos;
    /**
     * The nanoTime at which there will be work for the lock, or NO_LOCK_WORK_AT. Written only by the lock owner.
     * See {@link #signalLockWorkAtExclusive}.
     */
    private volatile long lockWorkAt = NO_LOCK_WORK_AT;
    /** written only by the lock owner: lockWorkAt has moved earlier, so we must wake any timed nominee to re-arm */
    private boolean kickTimed;
    private boolean preferRegistered;
    private boolean autoPreferRegistered = true;

    private int depth;

    public SignalLock(int threadCount)
    {
        this(true, threadCount, 0, 0, null);
    }

    public SignalLock(int threadCount, long spinInterval, long stopCheckInterval, TimeUnit units)
    {
        this(true, threadCount, spinInterval, stopCheckInterval, units);
    }

    public SignalLock(boolean allowStealing, int threadCount, long spinInterval, long stopCheckInterval, TimeUnit units)
    {
        Invariants.requireArgument(threadCount <= MAX_THREADS, "Supports at most " + MAX_THREADS + " registered threads");
        Invariants.require(MAX_SIGNAL_COUNT >= MAX_THREADS, "%d permits cannot serve %d threads", MAX_SIGNAL_COUNT, MAX_THREADS);
        this.allowStealing = allowStealing;
        this.registered = new Thread[threadCount];
        this.state = (long)threadCount << ENABLED_THREAD_COUNT_SHIFT;
        this.spinIntervalNanos = spinInterval <= 0 ? 0 : units.toNanos(spinInterval);
        if (stopCheckInterval > 0)
        {
            this.stopLog = new AtomicLongArray(threadCount);
            this.stopCheck = new AtomicLong();
            this.stopCheckIntervalNanos = units.toNanos(stopCheckInterval);
        }
        else
        {
            this.stopLog = null;
            this.stopCheck = null;
            this.stopCheckIntervalNanos = -1;
        }
    }

    public void register(int index, Thread thread)
    {
        registered[index] = thread;
    }

    public Thread registeredThread(int index)
    {
        return registered[index];
    }

    public void lock()
    {
        Thread self = Thread.currentThread();
        if (tryLock(self))
            return;

        locking.add(self);
        while (true)
        {
            long cur = state;
            if (isLockAvailable(cur, ANONYMOUS_OWNER, self))
            {
                long upd = acquire(cur, ANONYMOUS_OWNER);
                if (!stateUpdater.compareAndSet(this, cur, upd))
                    continue;

                Invariants.require(depth == 0);
                Invariants.require(owner == null);
                depth = 1;
                owner = self;
                ownerIndex = ANONYMOUS_OWNER;

                Invariants.require(locking.peek() == self);
                locking.poll();
                return;
            }

            LockSupport.park();
        }
    }

    @Override
    public boolean tryLock()
    {
        return tryLock(Thread.currentThread());
    }

    private boolean tryLock(Thread self)
    {
        return tryLock(ANONYMOUS_OWNER, self);
    }

    public boolean tryLock(int thread)
    {
        Invariants.require(thread >= 0 && thread < registered.length);
        Thread self = registeredThread(thread);
        Invariants.require(self == Thread.currentThread(), "caller is not the thread registered at index %d", thread);
        return tryLock(thread, self);
    }

    private boolean tryLock(int threadOrAnonymous, Thread self)
    {
        if (owner != self)
        {
            while (true)
            {
                long cur = state;
                if (!isLockAvailable(cur, threadOrAnonymous, null))
                    return false;

                long upd = acquire(cur, threadOrAnonymous);
                if (!stateUpdater.compareAndSet(this, cur, upd))
                    continue;

                Invariants.require(depth == 0);
                Invariants.require(owner == null);
                owner = self;
                ownerIndex = threadOrAnonymous;
                break;
            }
        }

        ++depth;
        return true;
    }

    /**
     * Wait for the lock to be signalled and acquire it, or wait for additional work to be signalled.
     * Return true if we acquired the lock.
     */
    public boolean awaitAsyncOrLock(int thread)
    {
        return awaitAsyncOrLock(thread, 1);
    }

    public boolean awaitAsyncOrLock(int thread, int burstSignalOnAcquire)
    {
        long threadBit = threadBit(thread);
        Thread self = registered[thread];
        Invariants.require(self == Thread.currentThread(), "%s is not the thread registered at index %d (%s)", Thread.currentThread(), thread, self);
        // if we still own the lock we have leaked it, most likely by unwinding a failure without releasing every
        // (re)acquisition
        Invariants.require(owner != self, "%s already owns this lock with depth %d (state %s)", self, depth, Long.toHexString(state));
        Invariants.require((state & threadBit) == 0, "thread %d is already registered as waiting (state %s)", thread, Long.toHexString(state));

        boolean hasPaused = false;
        if (spinIntervalNanos > 0)
        {
            while (true)
            {
                long cur = state;
                // spinning threads are not registered as waiting, so cannot be nominated as the timed thread; so we
                // must each check for any timed lock work that has fallen due
                long at = lockWorkAt;
                if (at != NO_LOCK_WORK_AT && !hasLockWork(cur) && at - nanoTime() <= 0)
                {
                    setHasLockWork();
                    continue;
                }

                if (hasLockWork(cur) && isLockAvailable(cur, thread, null))
                {
                    if (tryTakeLockInAwaitLoop(cur, thread, 0L, self, hasPaused)) return true;
                    else continue;
                }
                else if (tryAcquireAsyncInAwaitLoop(cur, thread, 0L, hasPaused, burstSignalOnAcquire))
                {
                    return false;
                }

                int enabledCount = enabledThreadCount(cur);
                if (thread >= enabledCount || enabledCount <= 1)
                    break; // if we're a disabled thread, or the last thread, move to blocking loop

                hasPaused = pause(thread, hasPaused);
                int waitingEnabledThreadCount = waitingEnabledThreadCount(cur);
                int multiplier = 1 + (waitingEnabledThreadCount == 0 ? 0 : ThreadLocalRandom.current().nextInt(2 * waitingEnabledThreadCount(cur)));
                LockSupport.parkNanos(spinIntervalNanos * multiplier);
            }
        }

        long cur = registerWaiting(thread, threadBit);
        while (true)
        {
            do   // dummy do-while-false loop, to ensure we refresh cur
            {
                boolean isSignalled = (cur & threadBit) == 0;
                if (isSignalled)
                {
                    // only signalAllRegistered can signal us directly without also clearing any nomination of ours
                    cur = relinquishNomination(thread);
                    unpause(thread, hasPaused);
                    propagateAsyncWorkSignals(cur, burstSignalOnAcquire);
                    return false;
                }

                boolean isNominee = lockThread(cur) == thread;
                boolean isTimedNominee = isNominee && isTimed(cur);
                if ((hasLockWork(cur) || (isNominee && !isTimedNominee)) && isLockAvailable(cur, thread, null))
                {
                    if (tryTakeLockInAwaitLoop(cur, thread, threadBit, self, hasPaused)) return true;
                    else continue;
                }

                long remaining = 0;
                if (isTimedNominee)
                {
                    long at = lockWorkAt;
                    if (at == NO_LOCK_WORK_AT)
                    {
                        // nothing to wait for any more; if this fails, we will retry with the latest state
                        stateUpdater.compareAndSet(this, cur, clearNominee(cur));
                        continue;
                    }

                    remaining = at - nanoTime();
                    if (remaining <= 0)
                    {
                        // the deadline has passed: take the lock if we can (consuming our nomination); otherwise
                        // relinquish our nomination as lock work, which the owner will see
                        if (isLockAvailable(cur, thread, null))
                        {
                            if (tryTakeLockInAwaitLoop(cur, thread, threadBit, self, hasPaused)) return true;
                        }
                        else stateUpdater.compareAndSet(this, cur, clearNominee(cur) | HAS_LOCK_WORK);
                        continue;
                    }
                }

                // while we are the timed nominee, we leave async work to any other waiting thread
                if ((!isTimedNominee || !hasOtherWaiter(cur, thread)) && tryAcquireAsyncInAwaitLoop(cur, thread, threadBit, hasPaused, burstSignalOnAcquire))
                    return false;

                hasPaused = pause(thread, hasPaused);
                if (isTimedNominee) LockSupport.parkNanos(remaining);
                else LockSupport.park();
            }
            while (false);

            cur = state;
        }
    }

    /**
     * Register ourselves as waiting. If there will be lock work at some future time and no nominee, we nominate
     * ourselves (timed) in the same step, so that there is a timed nominee whenever any registered thread is waiting.
     */
    private long registerWaiting(int thread, long threadBit)
    {
        while (true)
        {
            long cur = state;
            long upd = cur | threadBit;
            if (lockThread(cur) == NO_NOMINEE && lockWorkAt != NO_LOCK_WORK_AT && thread < enabledThreadCount(cur))
                upd = setNominee(upd, thread, true);
            if (stateUpdater.compareAndSet(this, cur, upd))
                return upd;
        }
    }

    private long relinquishNomination(int thread)
    {
        while (true)
        {
            long cur = state;
            if (lockThread(cur) != thread || stateUpdater.compareAndSet(this, cur, clearNominee(cur)))
                return state;
        }
    }

    private boolean tryTakeLockInAwaitLoop(long cur, int thread, long threadBitIfWaiting, Thread self, boolean hasPaused)
    {
        // expects caller to have already checked for direct signal, so that the xor below can only clear our bit
        Invariants.require((cur & threadBitIfWaiting) == threadBitIfWaiting);
        long upd = acquire(cur, thread) ^ threadBitIfWaiting;
        if (!stateUpdater.compareAndSet(this, cur, upd))
            return false;

        Invariants.require(depth == 0);
        Invariants.require(owner == null);
        owner = self;
        ownerIndex = thread;
        depth = 1;
        unpause(thread, hasPaused);
        return true;
    }

    private boolean pause(int thread, boolean hasPaused)
    {
        if (!hasPaused && stopLog != null)
            stopLog.set(thread, nanoTime());
        return true;
    }

    private void unpause(int thread, boolean hasPaused)
    {
        if (hasPaused && stopLog != null)
        {
            long start = stopLog.getAndSet(thread, 0);
            long stop = nanoTime();
            updateStopCheck(stop - start, stop);
        }
    }

    private void updateStopCheck(long stoppedFor, long now)
    {
        long current = stopCheck.addAndGet(stoppedFor);
        if (current > now && stopCheck.compareAndSet(current, now - stopCheckIntervalNanos))
            decrementEnabledThreadCount();
        else if (current < now - 2 * stopCheckIntervalNanos)
            stopCheck.compareAndSet(current, now - stopCheckIntervalNanos);
    }

    @Override
    public void unlock()
    {
        unlock(1, false);
    }

    public void unlock(int burstSignal)
    {
        unlock(burstSignal, false);
    }

    public boolean unlockAndAcquireAsyncWork()
    {
        return unlockAndAcquireAsyncWork(1);
    }

    public boolean unlockAndAcquireAsyncWork(int burstSignal)
    {
        return unlock(burstSignal, true);
    }

    // if we're the owner, release the lock and return the depth to which we had acquired the lock
    public int ensureNotOwner()
    {
        if (owner != Thread.currentThread())
            return 0;

        int released = depth;
        depth = 1;
        unlock(1, false);
        return released;
    }

    private boolean unlock(int burstSignal, boolean acquire)
    {
        Thread self = owner;
        Invariants.require(self == Thread.currentThread());
        Invariants.require(!acquire || depth == 1, "Cannot acquire async work with reentrancy (depth " + depth + ')');
        Invariants.require(isLockOwned(state), "releasing a lock that state %d does not report as owned", state);
        return --depth == 0 && releaseAndSignalExclusive(burstSignal, acquire);
    }

    private boolean releaseAndSignalExclusive(int burstSignal, boolean acquire)
    {
        boolean acquired = acquire && tryAcquireAsyncWork();
        // a registered thread releasing with unlockAndAcquireAsyncWork that has not acquired any async work will next
        // await work (see unlockAndAcquireAsyncWork), so may nominate itself for any timed lock work
        int nominateSelf = acquire && !acquired ? ownerIndex : ANONYMOUS_OWNER;
        owner = null;
        ownerIndex = ANONYMOUS_OWNER;
        Thread next = locking.peek();
        boolean preferRegistered = this.preferRegistered;
        boolean hasLockWorkAt = lockWorkAt != NO_LOCK_WORK_AT;
        while (true)
        {
            long cur = state;
            int nominee = lockThread(cur);
            // while owned, there may only be a timed nominee
            Invariants.require(nominee == NO_NOMINEE || isTimed(cur));
            long upd = cur & ~LOCK_OWNED;
            int unpark = NO_THREAD;
            boolean unparkNext = false;

            boolean wakeupLockWork = hasLockWork(cur) && hasMoreWaitersThanSignals(cur);
            if (wakeupLockWork && (preferRegistered || next == null))
            {
                // signal a registered thread to take the lock: if there is a timed nominee, we upgrade it
                if (nominee >= 0) upd &= ~LOCK_TIMED;
                else upd = setNominee(upd, nominee = pickThreadToSignalForLock(cur), false);
                unpark = nominee;
                if (next != null && autoPreferRegistered)
                    this.preferRegistered = !preferRegistered;
            }
            else if (next != null)
            {
                // a timed nominee does not prevent anonymous waiters from taking the lock, so it may remain
                if (nominee == NO_NOMINEE) upd = setNominee(upd, ANONYMOUS_OWNER, false);
                unparkNext = true;
                if (wakeupLockWork && autoPreferRegistered)
                    this.preferRegistered = !preferRegistered;
            }
            else if (!hasLockWorkAt)
            {
                if (nominee != NO_NOMINEE)
                    upd = clearNominee(upd);
            }
            else if (nominee == NO_NOMINEE)
            {
                // there will be lock work later: nominate (preferably) ourselves, or a waiting thread that won't be
                // consumed by async work
                if (nominateSelf >= 0 && nominateSelf < enabledThreadCount(cur))
                {
                    upd = setNominee(upd, nominateSelf, true);
                }
                else if (hasMoreWaitersThanSignals(cur))
                {
                    unpark = pickThreadToSignalForLock(cur);
                    upd = setNominee(upd, unpark, true);
                }
            }
            else if (kickTimed)
            {
                unpark = nominee;
            }

            if (stateUpdater.compareAndSet(this, cur, upd))
            {
                kickTimed = false;
                if (unpark >= 0 && unpark != NO_THREAD) unpark(unpark);
                if (unparkNext)
                {
                    LockSupport.unpark(next);
                }
                else if (!wakeupLockWork)
                {
                    next = locking.peek();
                    if (next != null)
                        LockSupport.unpark(next);
                }
                break;
            }

            // cancel the flip of prefer registered (if any)
            this.preferRegistered = preferRegistered;
        }

        propagateAsyncWorkSignals(burstSignal);
        return acquired;
    }

    /**
     * Must be invoked by the lock owner. Declares that there will be work for the lock at {@code atNanos} (by nanoTime),
     * or no such work if {@code NO_LOCK_WORK_AT}, replacing any earlier declaration.
     *
     * At most one registered thread is nominated (timed) to acquire the lock at (or soon after) this time: on release of
     * the lock (preferring the releasing thread, if it will next await work), or as it begins waiting. It remains
     * nominated across other threads acquiring and releasing the lock, so that it is not disturbed by them, and it
     * relinquishes its nomination (in the same step) before doing anything else. If the time moves later the nominee
     * will simply wake early and wait again, but if it moves earlier we wake it on release to re-arm.
     *
     * If no registered thread is waiting, then all are busy and will next acquire the lock (or nominate themselves) when
     * they have no other work.
     */
    public void signalLockWorkAtExclusive(long atNanos)
    {
        Invariants.require(owner == Thread.currentThread());
        long prev = lockWorkAt;
        if (prev == atNanos)
            return;

        lockWorkAt = atNanos;
        if (atNanos != NO_LOCK_WORK_AT && (prev == NO_LOCK_WORK_AT || atNanos - prev < 0))
            kickTimed = true;
    }

    public long lockWorkAt()
    {
        return lockWorkAt;
    }

    /** the timed nominee, or -1 if none */
    public int timedThread()
    {
        return timedThread(state);
    }

    private boolean isLockAvailable(long cur, int thread, Thread waiting)
    {
        if (isLockOwned(cur))
            return false;

        // only an active nominee may prevent another thread taking the lock
        boolean hasActiveNominee = hasActiveNominee(cur);
        if (thread >= 0 || waiting == null)
            return allowStealing || !hasActiveNominee || lockThread(cur) == thread;

        return locking.peek() == waiting && (!hasActiveNominee || lockThread(cur) == ANONYMOUS_OWNER);
    }

    private boolean tryAcquireAsyncInAwaitLoop(long cur, int thread, long threadBitIfWaiting, boolean hasPaused, int burstSignalOnAcquire)
    {
        // expects caller to have already checked for direct signal
        Invariants.require((cur & threadBitIfWaiting) == threadBitIfWaiting);
        while (true)
        {
            int signals = asyncSignalCount(cur);
            if (signals == 0)
                return false;

            // iff we have a waiting bit, it is known to be set, so the xor clears it: we are no longer waiting, we own a permit
            long upd = clearTimedNomineeIf((cur - ASYNC_SIGNAL_INCREMENT) ^ threadBitIfWaiting, thread);
            if (!stateUpdater.compareAndSet(this, cur, upd))
            {
                cur = upd = state;

                // confirm our signalled state hasn't changed (can only change if threadBitIfWaiting != 0)
                if ((cur & threadBitIfWaiting) == threadBitIfWaiting)
                    continue;

                // if it's changed, it must now be zero, which is the same as us taking it directly
            }

            propagateAsyncWorkSignals(upd, burstSignalOnAcquire);
            unpause(thread, hasPaused);
            return true;
        }
    }

    public boolean incrementAsyncWork(boolean signalIfWaiting)
    {
        while (true)
        {
            long cur = state;
            int count = asyncSignalCount(cur);
            if (count == MAX_SIGNAL_COUNT)
                return false;

            if (signalIfWaiting)
            {
                int thread = pickThreadToSignalAsync(cur);
                if (thread != NO_THREAD)
                {
                    long upd = clearTimedNomineeIf(cur ^ threadBit(thread), thread);
                    if (stateUpdater.compareAndSet(this, cur, upd))
                        return unpark(thread);
                    continue;
                }
            }

            long upd = cur + ASYNC_SIGNAL_INCREMENT;
            if (stateUpdater.compareAndSet(this, cur, upd))
                return true;
        }
    }

    public boolean tryAcquireAsyncWork()
    {
        while (true)
        {
            long cur = state;
            int count = asyncSignalCount(cur);
            if (count == 0)
                return false;

            long upd = cur - ASYNC_SIGNAL_INCREMENT;
            if (stateUpdater.compareAndSet(this, cur, upd))
                return true;
        }
    }

    public void propagateAsyncWorkSignals()
    {
        propagateAsyncWorkSignals(Integer.MAX_VALUE);
    }

    public void propagateAsyncWorkSignals(int burstSignal)
    {
        propagateAsyncWorkSignals(state, burstSignal);
    }

    private void propagateAsyncWorkSignals(long cur, int burstSignal)
    {
        while (burstSignal > 0)
        {
            int signals = asyncSignalCount(cur);
            int thread = pickThreadToSignalAsync(cur);
            // if the lock is owned, we leave one permit for the owner: it is likely to have made the work available, and
            // to acquire it on releasing the lock (see unlockAndAcquireAsyncWork); if it does not, it will propagate the
            // permit on release. Otherwise, a thread we wake may cascade the owner's permit to another thread, leaving the
            // owner with no work, so that its work must instead await another thread waking.
            if (signals <= (isLockOwned(cur) ? 1 : 0) || thread == NO_THREAD)
                return;

            long threadBit = threadBit(thread);
            Invariants.require((threadBit & cur) != 0);
            long upd = clearTimedNomineeIf((cur - ASYNC_SIGNAL_INCREMENT) ^ threadBit, thread);
            if (!stateUpdater.compareAndSet(this, cur, upd)) cur = state;
            else
            {
                unpark(thread);
                --burstSignal;
                cur = upd;
            }
        }
    }

    /**
     * Signal a waiting registered thread to try and acquire the lock.
     * Return true if the lock is held, or the signal has been propagated
     */
    private void wakeupForLockWork()
    {
        while (true)
        {
            long cur = state;

            if (!hasLockWork(cur) || isLockOwned(cur) || hasActiveNominee(cur))
                return;

            // signal lock owners from end backwards, and async work from front forwards; but upgrade any timed nominee
            int thread = lockThread(cur);
            long upd;
            if (thread >= 0)
            {
                upd = cur & ~LOCK_TIMED;
            }
            else
            {
                thread = pickThreadToSignalForLock(cur);
                if (thread < 0)
                    return;
                upd = setNominee(cur, thread, false);
            }

            if (stateUpdater.compareAndSet(this, cur, upd))
            {
                unpark(thread);
                return;
            }
        }
    }

    private boolean unpark(int thread)
    {
        LockSupport.unpark(registered[thread]);
        return true;
    }

    /**
     * Signal that there is work to do on the lock
     * If the lock is free and there are any waiting threads, wake one
     */
    public void signalLockWork()
    {
        if (setHasLockWork())
            wakeupForLockWork();
    }

    /**
     * Signal that there is work to do on the lock
     * If the lock is free and there are any waiting threads, wake one
     */
    public void signalLockWorkExclusive()
    {
        setHasLockWork();
    }

    private boolean setHasLockWork()
    {
        return 0 == (stateUpdater.getAndUpdate(this, v -> v | HAS_LOCK_WORK) & HAS_LOCK_WORK);
    }

    public void signalAllRegistered()
    {
        stateUpdater.updateAndGet(this, v -> v & ~WAITING_THREADS_MASK);
        for (Thread thread : registered)
        {
            if (thread != null)
                LockSupport.unpark(thread);
        }
    }

    /**
     * Work that requires the lock has been finished.
     * This must be called prior to draining all work that has been asynchronously submitted for processing with the lock.
     * Otherwise, the caller must check whether new work has been submitted prior to calling unlock,
     * and if so first call signalLockExclusive().
     */
    public void clearLockWork()
    {
        stateUpdater.updateAndGet(this, v -> v & ~HAS_LOCK_WORK);
    }

    public long state()
    {
        return state;
    }

    public int asyncSignalCount()
    {
        return asyncSignalCount(state);
    }

    public int waitingThreadCount()
    {
        return waitingThreadCount(state);
    }

    public int enabledThreadCount()
    {
        return enabledThreadCount(state);
    }

    // if enabledThreadCount() == 1 return false; otherwise reduce by one
    private boolean decrementEnabledThreadCount()
    {
        while (true)
        {
            long cur = state;
            if (enabledThreadCount(cur) == 1)
                return false;

            if (stateUpdater.compareAndSet(this, cur, cur - ENABLED_THREAD_COUNT_INCREMENT))
                return true;
        }
    }

    public int addAndGetEnabledThreadCount(int delta)
    {
        // note: delta may meet or exceed threadCount(), as the new count is clamped to it below
        Invariants.require(delta > 0, "%d is not a positive delta", delta);
        while (true)
        {
            long cur = state;
            int count = enabledThreadCount(cur);
            int newCount = Math.min(threadCount(), count + delta);
            if (count == newCount)
                return 0;

            if (stateUpdater.compareAndSet(this, cur, setEnabledThreadCount(cur, newCount)))
            {
                propagateAsyncWorkSignals(1);
                return newCount - count;
            }
        }
    }

    public void setEnabledThreadCount(int count)
    {
        Invariants.require(count > 0 && count <= threadCount());
        stateUpdater.accumulateAndGet(this, count, SignalLock::setEnabledThreadCount);
        propagateAsyncWorkSignals(1);
    }

    public int threadCount()
    {
        return registered.length;
    }

    public int waitingEnabledThreadCount()
    {
        return waitingEnabledThreadCount(state);
    }

    public int activeThreadCount()
    {
        long cur = state;
        int active = registered.length - waitingThreadCount(cur);
        int lockThread = lockThread(cur);
        if (lockThread >= 0 && !isTimed(cur) && (cur & threadBit(lockThread)) != 0)
            ++active;
        return active;
    }

    public boolean hasLockWork()
    {
        return hasLockWork(state);
    }

    public boolean hasOwner()
    {
        return isLockOwned(state);
    }

    public boolean isOwner()
    {
        return Thread.currentThread() == owner;
    }

    // no between-thread visibility guarantees; use isLockedOwned for visibility
    public Thread unsafeOwner()
    {
        return owner;
    }

    public void setAutoPreferRegistered(boolean autoPreferRegistered)
    {
        this.autoPreferRegistered = autoPreferRegistered;
    }

    public void setPreferRegistered(boolean preferRegistered)
    {
        this.preferRegistered = preferRegistered;
    }

    private static final int NO_THREAD = 64;
    private static final int ANONYMOUS_OWNER = -1;
    private static final int NO_NOMINEE = -2;
    private static int pickThreadToSignalAsync(long state)
    {
        long candidates = state & threadMask(enabledThreadCount(state));
        int nominee = lockThread(state);
        if (nominee >= 0)
        {
            // prefer not to disturb a timed nominee, but if it is the only candidate it must relinquish its nomination
            long without = candidates & ~threadBit(nominee);
            if (!isTimed(state) || without != 0)
                candidates = without;
        }
        return Long.numberOfTrailingZeros(candidates);
    }

    // invoked only when there is no nominee; returns -1 (i.e. ANONYMOUS_OWNER) if there are no candidates
    private static int pickThreadToSignalForLock(long state)
    {
        return 63 - Long.numberOfLeadingZeros(state & threadMask(enabledThreadCount(state)));
    }

    private static boolean hasOtherWaiter(long state, int thread)
    {
        return 0 != (state & threadMask(enabledThreadCount(state)) & ~threadBit(thread));
    }

    /**
     * The state after {@code thread} (or ANONYMOUS_OWNER) acquires the lock: any active nomination is consumed (or
     * stolen), as is our own timed nomination; another thread's timed nomination survives.
     */
    private static long acquire(long state, int thread)
    {
        Invariants.require(!isLockOwned(state));
        long upd = state | LOCK_OWNED;
        int nominee = lockThread(state);
        if (nominee != NO_NOMINEE && (!isTimed(state) || nominee == thread))
            upd = clearNominee(upd);
        return upd;
    }

    public static boolean isTimed(long state)
    {
        return 0 != (state & LOCK_TIMED);
    }

    private static boolean hasActiveNominee(long state)
    {
        return lockThread(state) != NO_NOMINEE && !isTimed(state);
    }

    /** the timed nominee, or -1 if none */
    public static int timedThread(long state)
    {
        return isTimed(state) ? lockThread(state) : -1;
    }

    private static long setNominee(long state, int thread, boolean timed)
    {
        Invariants.require(lockThread(state) == NO_NOMINEE);
        Invariants.require(!timed || thread >= 0);
        Invariants.require(timed || !isLockOwned(state));
        state |= (long) (thread + 2) << LOCK_THREAD_SHIFT;
        return timed ? state | LOCK_TIMED : state;
    }

    private static long clearNominee(long state)
    {
        return state & ~(LOCK_THREAD_MASK | LOCK_TIMED);
    }

    private static long clearTimedNomineeIf(long state, int thread)
    {
        return timedThread(state) == thread ? clearNominee(state) : state;
    }

    public static boolean isLockOwned(long state)
    {
        return 0 != (state & LOCK_OWNED);
    }

    /** the nominee (registered thread or ANONYMOUS_OWNER), or NO_NOMINEE; NOT the owner, which is not recorded in the state */
    public static int lockThread(long state)
    {
        return (int) ((state & LOCK_THREAD_MASK) >>> LOCK_THREAD_SHIFT) - 2;
    }

    public static String toString(long state)
    {
        return String.format("%016x[owned=%s,nominee=%d,timed=%s,hasLockWork=%s,asyncSignals=%d,enabled=%d,waiting=%d]",
                             state, isLockOwned(state), lockThread(state), isTimed(state), hasLockWork(state),
                             asyncSignalCount(state), enabledThreadCount(state), waitingThreadCount(state));
    }

    @Override
    public String toString()
    {
        return "SignalLock" + toString(state);
    }

    public static int asyncSignalCount(long state)
    {
        return (int) ((state >>> ASYNC_SIGNAL_COUNT_SHIFT) & THREAD_ID_MASK);
    }

    public static int waitingThreadCount(long state)
    {
        return Long.bitCount(state & WAITING_THREADS_MASK);
    }

    public static int enabledThreadCount(long state)
    {
        return (int) ((state >>> ENABLED_THREAD_COUNT_SHIFT) & THREAD_ID_MASK);
    }

    private static long setEnabledThreadCount(long state, long count)
    {
        Invariants.require(count <= MAX_THREADS);
        return (state & ~ENABLED_THREAD_COUNT_MASK) | ((long)count << ENABLED_THREAD_COUNT_SHIFT);
    }

    public static int activeEnabledThreadCount(long state)
    {
        return activeThreadCount(enabledThreadCount(state), state);
    }

    public static int activeThreadCount(int threadCount, long state)
    {
        return threadCount - Long.bitCount(state & threadMask(threadCount));
    }

    private static long threadMask(int threadCount)
    {
        return (1L << threadCount) - 1;
    }

    public static int waitingEnabledThreadCount(long state)
    {
        return Long.bitCount(state & threadMask(enabledThreadCount(state)));
    }

    public static boolean hasMoreWaitersThanSignals(long state)
    {
        return waitingEnabledThreadCount(state) > asyncSignalCount(state);
    }

    private static long threadBit(int threadIndex)
    {
        return 1L << threadIndex;
    }

    private static boolean hasLockWork(long state)
    {
        return (state & HAS_LOCK_WORK) != 0;
    }

    public void lockInterruptibly()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition()
    {
        throw new UnsupportedOperationException();
    }
}
