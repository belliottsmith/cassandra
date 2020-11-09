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

package org.apache.cassandra.service.paxos.cleanup;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.uncommitted.UncommittedPaxosKey;
import org.apache.cassandra.utils.CloseableIterator;

public class PaxosCleanupLocalCoordinator extends AbstractFuture<PaxosCleanupResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosCleanupLocalCoordinator.class);
    private static final UUID INTERNAL_SESSION = new UUID(0, 0);

    private static final ConcurrentMap<UUID, PaxosTableRepairs> tableRepairsMap = new ConcurrentHashMap<>();

    private final UUID session;
    private final UUID cfId;
    private final CFMetaData cfm;
    private final Collection<Range<Token>> ranges;
    private final CloseableIterator<UncommittedPaxosKey> uncommittedIter;
    private int count = 0;

    private final Map<DecoratedKey, PaxosTableRepairs.QueueableRepair> inflight = new HashMap<>();
    private final PaxosTableRepairs tableRepairs;

    private PaxosCleanupLocalCoordinator(UUID session, UUID cfId, Collection<Range<Token>> ranges, CloseableIterator<UncommittedPaxosKey> uncommittedIter)
    {
        this.session = session;
        this.cfId = cfId;
        this.cfm = Schema.instance.getCFMetaData(cfId);
        this.ranges = ranges;
        this.uncommittedIter = uncommittedIter;
        this.tableRepairs = tableRepairsMap.computeIfAbsent(cfId, k -> new PaxosTableRepairs());
    }

    public synchronized void start()
    {
        if (cfm == null)
        {
            fail("Unknown cfID: " + cfId);
            return;
        }

        logger.info("Completing uncommitted paxos instances for {} on ranges {}", cfm.ksAndCFName, ranges);

        scheduleKeyRepairsOrFinish();
    }

    public static PaxosCleanupLocalCoordinator create(PaxosCleanupRequest request)
    {
        CloseableIterator<UncommittedPaxosKey> iterator = PaxosState.uncommittedTracker().uncommittedKeyIterator(request.cfId, request.ranges);
        return new PaxosCleanupLocalCoordinator(request.session, request.cfId, request.ranges, iterator);
    }

    public static PaxosCleanupLocalCoordinator createForAutoRepair(UUID cfId, Collection<Range<Token>> ranges)
    {
        CloseableIterator<UncommittedPaxosKey> iterator = PaxosState.uncommittedTracker().uncommittedKeyIterator(cfId, ranges);
        return new PaxosCleanupLocalCoordinator(INTERNAL_SESSION, cfId, ranges, iterator);
    }

    /**
     * Schedule as many key repairs as we can, up to the paralellism limit. If no repairs are scheduled and
     * none are in flight when the iterator is exhausted, the session will be finished
     */
    private void scheduleKeyRepairsOrFinish()
    {
        int parallelism = DatabaseDescriptor.getPaxosRepairParallelism();
        Preconditions.checkArgument(parallelism > 0);
        while (inflight.size() < parallelism && uncommittedIter.hasNext())
            repairKey(uncommittedIter.next());

        if (inflight.isEmpty())
            finish();
    }

    private boolean repairKey(UncommittedPaxosKey uncommitted)
    {
        Preconditions.checkState(!inflight.containsKey(uncommitted.getKey()));
        ConsistencyLevel consistency = uncommitted.getConsistencyLevel();

        // we don't know the consistency of this operation, presumably because it originated
        // before we started tracking paxos cl, so we don't attempt to repair it
        if (consistency == null)
            return false;

        inflight.put(uncommitted.getKey(), tableRepairs.startOrQueue(uncommitted.getKey(), uncommitted.getConsistencyLevel(), cfm, result -> {
            if (result.wasSuccessful())
                onKeyFinish(uncommitted.getKey());
            else
                onKeyFailure(result.toString());
        }));
        return true;
    }

    private synchronized void onKeyFinish(DecoratedKey key)
    {
        inflight.remove(key);
        count++;

        scheduleKeyRepairsOrFinish();
    }

    private void complete(PaxosCleanupResponse response)
    {
        uncommittedIter.close();
        set(response);
    }

    private synchronized void onKeyFailure(String reason)
    {
        for (PaxosTableRepairs.QueueableRepair repair: inflight.values())
            repair.cancel();
        fail(reason);
    }

    private void fail(String reason)
    {
        logger.info("Failing paxos cleanup session for {} on ranges {}. Reason: {}", cfm.ksAndCFName, ranges, reason);
        complete(PaxosCleanupResponse.failed(session, reason));
    }

    private void finish()
    {
        logger.info("Completed {} uncommitted paxos instances for {} on ranges {}", count, cfm.ksAndCFName, ranges);
        complete(PaxosCleanupResponse.success(session));
    }
}
