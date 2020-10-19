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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.service.paxos.PaxosRepair;

/**
 * Coordinates repairs on a given key to prevent multiple repairs being scheduled for a single key
 */
public class PaxosTableRepairs
{
    public interface QueueableRepair
    {
        DecoratedKey key();
        QueueableRepair start();
        void cancel();
    }

    private static class KeyRepair
    {
        private final DecoratedKey key;

        private QueueableRepair active = null;
        private Queue<QueueableRepair> queued = null;

        private KeyRepair(DecoratedKey key)
        {
            this.key = key;
        }

        private void schedule(QueueableRepair repair)
        {
            Preconditions.checkState(active == null);

            active = repair;
            repair.start();
        }

        QueueableRepair startOrQueue(PaxosTableRepairs tableRepairs, DecoratedKey key, ConsistencyLevel consistency, CFMetaData cfm, Consumer<PaxosRepair.Result> onComplete)
        {
            Preconditions.checkArgument(this.key.equals(key));
            QueueableRepair repair = tableRepairs.createRepair(key, consistency, cfm, r -> {
                tableRepairs.onComplete(this);
                onComplete.accept(r);
            });

            if (active == null)
            {
                schedule(repair);
                return repair;
            }
            else
            {
                if (queued == null)
                {
                    queued = new ArrayDeque<>();
                }

                queued.add(repair);
            }

            return repair;
        }

        boolean completeAndScheduleNext()
        {
            Preconditions.checkState(active != null);
            active = null;
            if (queued != null && !queued.isEmpty())
            {
                schedule(queued.remove());
            }
            return active != null;
        }
    }

    private final Map<DecoratedKey, KeyRepair> keyRepairs = new HashMap<>();

    synchronized QueueableRepair startOrQueue(DecoratedKey key, ConsistencyLevel consistency, CFMetaData cfm, Consumer<PaxosRepair.Result> onComplete)
    {
        KeyRepair keyRepair = keyRepairs.computeIfAbsent(key, KeyRepair::new);
        return keyRepair.startOrQueue(this, key, consistency, cfm, onComplete);
    }

    synchronized void onComplete(KeyRepair keyRepair)
    {
        Preconditions.checkArgument(keyRepair == keyRepairs.get(keyRepair.key));
        if (!keyRepair.completeAndScheduleNext())
            keyRepairs.remove(keyRepair.key);
    }

    @VisibleForTesting
    synchronized boolean hasActiveRepairs(DecoratedKey key)
    {
        return keyRepairs.containsKey(key);
    }

    QueueableRepair createRepair(DecoratedKey key, ConsistencyLevel consistency, CFMetaData cfm, Consumer<PaxosRepair.Result> onComplete)
    {
        return PaxosRepair.create(consistency, key, cfm, onComplete);

    }

}
