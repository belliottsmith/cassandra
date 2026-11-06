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

package org.apache.cassandra.service.accord.execution;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.SafeState;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.execution.AccordCacheEntryFifoPromotionTest.World;
import org.apache.cassandra.service.accord.execution.Task.State;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * An abandoned ref is not ours to place or move. A continuation (or a task that has started) that finds one of its
 * entries unsafe to read, or failed to load, does not fail, but abandons the ref and carries on with its other keys.
 * Two routes lead there:
 *
 * <ol>
 *   <li>at setup, completePresetupExclusive abandons a presetup ref to an unsafe entry without ever placing a claim on
 *       it (and queueOnKeysExclusive then skips it, as an abandoned ref is not uninitialised);</li>
 *   <li>once queued, onFailingKeyExclusive abandons the ref, but deliberately retains the claim it holds until the task
 *       completes, to block others from witnessing a partial update (AccordFailedKeyTest R6).</li>
 * </ol>
 *
 * The fifo upgrade of an INCR task holding its txnId on its first run (prepareExclusiveMayThrow) must leave both alone.
 * It used to move every ref to fifo, so for (1) moveToFifo either refused outright when the entry had no queue or only a
 * bare task, failing the prepare and so the whole continuation, or, given a full queue, planted a fifo claim that nothing
 * accounted for ahead of whoever was queued there (which, being the new head, was then also reported ready to the batch,
 * failing the prepare when it came to lock the abandoned ref).
 */
public class AccordExecutorFifoUpgradeAbandonedKeyTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void clearInlineMocks()
    {
        org.mockito.Mockito.framework().clearInlineMocks();
    }

    /** route (1), with nothing queued on the abandoned entry: the upgrade must not fail the prepare */
    @Test
    public void upgradeIgnoresAbandonedPresetupKeyWithNoQueue()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> tH = world.commandEntry(), k = world.keyEntry(), k2 = world.keyEntry();
        SafeTask<?> h = continuation(world, tH, k, k2);
        k.setUnsafeToRead();
        abandonAtPresetup(h, k);
        placeAllExcept(world, h, k);

        world.firstRun(h); // fails here before the fix: "cannot move to fifo on k: queue is null"

        assertTrue(h.isCacheQueuedFifo());
        assertFalse(k.contains(h));
        assertTrue(k.unsafeQueuedTasks().isEmpty());
    }

    /**
     * As above, but with other tasks queued on the abandoned entry (continuations, which may wait on an unsafe entry),
     * enough that it has a full queue: the upgrade must not plant a fifo claim ahead of them
     */
    @Test
    public void upgradeDoesNotPlantClaimOnAbandonedPresetupKey()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> tH = world.commandEntry(), tX = world.commandEntry(), tY = world.commandEntry();
        AccordCacheEntry<?, ?, ?> k = world.keyEntry(), k2 = world.keyEntry();

        SafeTask<?> x = world.syncTask("X", 30, tX, k);
        x.setContinuation();
        world.placeAll(x);
        SafeTask<?> y = world.syncTask("Y", 31, tY, k);
        y.setContinuation();
        world.placeAll(y);
        assertTrue(x.is(State.WAITING_TO_RUN));

        SafeTask<?> h = continuation(world, tH, k, k2);
        k.setUnsafeToRead();
        abandonAtPresetup(h, k);
        placeAllExcept(world, h, k);

        world.firstRun(h);

        assertTrue(h.isCacheQueuedFifo());
        assertFalse("the upgrade planted a claim on an entry we abandoned", k.contains(h));
        assertEquals(Arrays.asList(x, y), k.unsafeQueuedTasks());
        assertTrue("X was revoked by a claim nothing accounts for", x.is(State.WAITING_TO_RUN));
    }

    /**
     * route (2): an entry becomes unsafe to read while a continuation is queued on it (as when a failed ATOMIC task
     * reclaims its fifo head): the continuation abandons the ref but retains its claim, which its upgrade leaves where it is
     */
    @Test
    public void upgradeLeavesClaimRetainedByAbandonedKey()
    {
        World world = new World();
        AccordCacheEntry<?, ?, ?> tH = world.commandEntry(), t0 = world.commandEntry(), k = world.keyEntry(), k2 = world.keyEntry();

        // T0 is an ATOMIC task leading k, which locks it to run, fails, and reclaims its fifo head
        SafeTask<?> h = continuation(world, tH, k, k2);
        SafeTask<?> t = world.atomicSetup("T0", 10, t0, k);
        world.waitOnTxns(t);
        world.placeAll(h);
        assertEquals(Arrays.asList(t, h), k.unsafeQueuedTasks());
        k.lockExclusive(t, AccordCacheEntry.LockMode.RELEASE_QUEUE);
        k.setUnsafeToRead();
        k.reclaimFifoHead(t);

        assertTrue(h.refs.get(k.key()).isAbandoned());
        assertTrue("the claim is retained", k.contains(h));
        assertEquals(Arrays.asList(t, h), k.unsafeQueuedTasks());
        assertEquals(1, k.unsafeFifoSize());

        world.firstRun(h);

        // fifo on the entries it will process, and its retained claim on k untouched (still behind T0, by priority)
        assertTrue(h.isCacheQueuedFifo());
        assertEquals(Arrays.asList(t, h), k.unsafeQueuedTasks());
        assertEquals(1, k.unsafeFifoSize());
    }

    private static SafeTask<?> continuation(World world, AccordCacheEntry<?, ?, ?>... refs)
    {
        SafeTask<?> h = world.incrTask("H", 20, refs);
        h.setContinuation();
        return h;
    }

    /** the real completePresetupExclusive, for a presetup ref to an entry that is unsafe to read */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void abandonAtPresetup(SafeTask<?> task, AccordCacheEntry<?, ?, ?> entry)
    {
        SafeState<?> ref = new SaferCommandsForKey((AccordCacheEntry) entry);
        ((Map) task.refs).put(entry.key(), ref);
        try
        {
            Method m = SafeTask.class.getDeclaredMethod("completePresetupExclusive", SafeState.class, int.class);
            m.setAccessible(true);
            m.invoke(task, ref, 0);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e.getCause());
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
        assertTrue(ref.isAbandoned());
        assertFalse(entry.contains(task));
    }

    /** place every declared entry except {@code skip}, which the task has abandoned */
    private static void placeAllExcept(World world, SafeTask<?> task, AccordCacheEntry<?, ?, ?> skip)
    {
        java.util.List<AccordCacheEntry<?, ?, ?>> declared = new java.util.ArrayList<>(world.declared.get(task));
        declared.remove(skip);
        world.declared.put(task, declared);
        world.placeAll(task);
    }
}
