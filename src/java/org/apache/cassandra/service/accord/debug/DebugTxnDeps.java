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

package org.apache.cassandra.service.accord.debug;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.primitives.Participants;
import accord.primitives.Routables;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.service.accord.IAccordService;

import static accord.primitives.Routables.Slice.Minimal;

public abstract class DebugTxnDeps<T extends DebugTxnDeps.TxnInfo, P> extends DebugTxnGraph<T, P>
{
    public static class TxnInfo implements Comparable<TxnInfo>
    {
        public final TxnId txnId;
        public final SaveStatus saveStatus;
        public final @Nullable Timestamp executeAt;
        public final Routables<?> via;

        public TxnInfo(TxnId txnId, SaveStatus saveStatus, @Nullable Timestamp executeAt, Routables<?> via)
        {
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.executeAt = executeAt;
            this.via = via;
        }

        @Override
        public int compareTo(@Nonnull TxnInfo that)
        {
            int c = compareExecuteAt(this.executeAt, that.executeAt);
            if (c == 0) c = this.txnId.compareTo(that.txnId);
            return c;
        }
    }

    public DebugTxnDeps(IAccordService service, TxnId root, @Nullable Participants<?> intersecting, TxnKindsAndDomains kinds, Timestamp min, int maxDepth, Consumer<TxnInfos<T>> visit)
    {
        super(service, root, intersecting, kinds, min, maxDepth, visit);
    }

    static int compareExecuteAt(Timestamp a, Timestamp b)
    {
        if (a == null || b == null)
            return a == b ? 0 : a == null ? -1 : 1;
        return a.compareTo(b);
    }
}
